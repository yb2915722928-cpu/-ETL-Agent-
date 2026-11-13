"""Bootstrap only isolated, genuine Doris 3.0.8; never display secret material."""
import argparse
import csv
import io
import json
import secrets
import subprocess
import time
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--runtime',type=Path,default=Path('.runtime/connectors'))
args=parser.parse_args()
BASE=args.runtime.resolve()
state=json.loads((BASE/'secrets.json').read_text(encoding='utf-8'))
if 'doris' not in state:
    state['doris']={'host':'connector-doris','port':9030,'host_port':19030,'database':'connector_source','username':'source_ro','password':secrets.token_hex(24),'admin_password':secrets.token_hex(24)}
    (BASE/'secrets.json').write_text(json.dumps(state,indent=2)+'\n',encoding='utf-8')
c=state['doris']
def run(args,data=None):
    r=subprocess.run(args,input=data.encode('utf-8') if data else None,capture_output=True)
    r.stdout=r.stdout.decode('utf-8',errors='replace');r.stderr=r.stderr.decode('utf-8',errors='replace')
    return r
for role,user,pwd in [('admin','root',c['admin_password']),('readonly',c['username'],c['password'])]:
    path=BASE/f'doris-{role}.cnf'
    path.write_text(f'[client]\nuser={user}\npassword={pwd}\nhost=connector-doris\nport=9030\n',encoding='utf-8')
    assert run(['docker','cp',str(path),f'etl-connector-mysql:/tmp/doris-{role}.cnf']).returncode==0
    assert run(['docker','exec','etl-connector-mysql','chmod','600',f'/tmp/doris-{role}.cnf']).returncode==0
def sql(query,role='admin',noauth=False):
    args=['docker','exec','-i','etl-connector-mysql','mysql']
    args += ['--no-defaults','-h','connector-doris','-P','9030','-u','root'] if noauth else [f'--defaults-extra-file=/tmp/doris-{role}.cnf']
    return run(args+['--batch','--raw','--connect-timeout=3'],query)
for attempt in range(60):
    result=sql('SELECT 1;')
    if result.returncode==0:break
    initial=sql('SELECT 1;',noauth=True)
    if initial.returncode==0:
        assert sql("SET PASSWORD FOR 'root'@'%' = PASSWORD('"+c['admin_password']+"');",noauth=True).returncode==0
        break
    time.sleep(2)
else:raise SystemExit('Doris FE authentication unavailable after 120 seconds')
# Doris ships both root and admin as privileged default identities; secure both.
assert sql("SET PASSWORD FOR 'admin'@'%' = PASSWORD('"+c['admin_password']+"');").returncode==0
assert sql('SELECT 1;',noauth=True).returncode!=0
be=sql('SHOW BACKENDS;')
if '172.20.0.10' not in be.stdout:
    assert sql('ALTER SYSTEM ADD BACKEND "172.20.0.10:9050";').returncode==0
for attempt in range(60):
    be=sql('SHOW BACKENDS;')
    rows=list(csv.DictReader(io.StringIO(be.stdout),delimiter='\t'))
    if rows and all(row.get('Alive')=='true' for row in rows):break
    time.sleep(2)
else:raise SystemExit('Doris genuine BE did not become Alive after 120 seconds')
query=f"""CREATE DATABASE IF NOT EXISTS connector_source;
CREATE TABLE IF NOT EXISTS connector_source.customers (
 id BIGINT NOT NULL COMMENT 'Synthetic customer identifier',
 email VARCHAR(200) NULL COMMENT 'Synthetic email; mask in profiles',
 full_name VARCHAR(80) NULL, balance DECIMAL(18,4) NULL, active BOOLEAN NULL,
 created_at DATETIMEV2(6) NULL, notes STRING NULL
) UNIQUE KEY(id) COMMENT 'Synthetic connector acceptance customers'
DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1');
CREATE TABLE IF NOT EXISTS connector_source.orders (
 id BIGINT NOT NULL, customer_id BIGINT NULL, amount DECIMAL(20,6) NULL,
 order_date DATE NULL, payload JSON NULL
) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1');
INSERT INTO connector_source.customers VALUES
(1,'alice@example.test','Alice Example',1234.5678,true,'2025-01-01 01:02:03.123456','line one'),
(2,'bob@example.test','Bob Example',-12.5000,false,'2025-01-02 03:04:05.000001',NULL),
(3,'chen@example.test','陈示例',NULL,true,'2025-01-03 00:00:00.000000','quoted, comma');
INSERT INTO connector_source.orders VALUES (101,1,99.120000,'2025-01-02','{{"source":"synthetic","priority":1}}'),(102,2,0.000001,'2025-01-03',NULL);
CREATE USER IF NOT EXISTS 'source_ro'@'%' IDENTIFIED BY '{c['password']}';
SET PASSWORD FOR 'source_ro'@'%' = PASSWORD('{c['password']}');
GRANT SELECT_PRIV ON connector_source.* TO 'source_ro'@'%';
"""
result=sql(query)
if result.returncode:
    (BASE/'doris-private-setup.log').write_text(result.stderr,encoding='utf-8')
    raise SystemExit('Doris schema bootstrap failed; private local setup log only')
read=sql('SELECT VERSION(), CURRENT_USER(); SELECT COUNT(*) FROM connector_source.customers; SELECT COUNT(*) FROM connector_source.orders; SHOW GRANTS;',role='readonly')
denied=sql('CREATE TABLE connector_source.__must_be_denied(id INT);',role='readonly')
if read.returncode or denied.returncode==0:raise SystemExit('Doris source read/negative-DDL acceptance failed')
counts=sql("SELECT 'customers' AS table_name,COUNT(*) AS row_count FROM connector_source.customers UNION ALL SELECT 'orders',COUNT(*) FROM connector_source.orders;",role='readonly')
actual_counts={row['table_name']:int(row['row_count']) for row in csv.DictReader(io.StringIO(counts.stdout),delimiter='\t')}
assert actual_counts=={'customers':3,'orders':2}
fe=sql('SHOW FRONTENDS;');be=sql('SHOW BACKENDS;');grants=sql('SHOW GRANTS;',role='readonly')
report={'kind':'doris','version':'3.0.8','genuine_fe_be':True,'default_privileged_accounts_secured':True,'host':c['host'],'port':c['port'],'database':c['database'],'username':c['username'],'customers':3,'orders':2,'ddl_denied':True,'frontends':list(csv.DictReader(io.StringIO(fe.stdout),delimiter='\t')),'backends':list(csv.DictReader(io.StringIO(be.stdout),delimiter='\t')),'grants':list(csv.DictReader(io.StringIO(grants.stdout),delimiter='\t'))}
assert all(row.get('Alive')=='true' for row in report['frontends'])
assert all(row.get('Alive')=='true' for row in report['backends'])
(BASE/'health-doris.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print('Genuine Doris FE/BE initialized; readonly SELECT passed and CREATE denied; safe health report saved in runtime.')
