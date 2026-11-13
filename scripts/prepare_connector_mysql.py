"""Create/reuse isolated, pinned MySQL acceptance infrastructure; never prints credentials.

Requires the platform's etl-agent_default network. Existing secrets and volumes
are reused. Only first-time init seeds data; this command never resets a volume.
"""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--runtime', type=Path, default=Path('.runtime/connectors'))
args = parser.parse_args()
base = args.runtime.resolve()
base.mkdir(parents=True, exist_ok=True, mode=0o700)
private = base / 'secrets.json'
state = json.loads(private.read_text(encoding='utf-8')) if private.exists() else {}
if 'mysql' not in state:
    state['mysql'] = {'host': 'connector-mysql', 'port': 3306, 'host_port': 13306,
                      'database': 'connector_source', 'username': 'source_ro',
                      'password': secrets.token_urlsafe(24), 'admin_password': secrets.token_urlsafe(24)}
    private.write_text(json.dumps(state, indent=2) + '\n', encoding='utf-8')
    os.chmod(private, 0o600)
config = state['mysql']
(base / 'mysql.env').write_text(f"MYSQL_ROOT_PASSWORD={config['admin_password']}\nMYSQL_DATABASE=connector_source\n", encoding='utf-8')
for role, user, pwd in [('admin', 'root', config['admin_password']), ('readonly', config['username'], config['password'])]:
    path = base / f'mysql-{role}.cnf'
    path.write_text(f'[client]\nuser={user}\npassword={pwd}\nhost=127.0.0.1\n', encoding='utf-8')
    os.chmod(path, 0o600)
init = base / 'mysql-init'
init.mkdir(exist_ok=True)
(init / '001-source.sql').write_text(f"""USE connector_source;
CREATE TABLE customers (
 id BIGINT PRIMARY KEY, email VARCHAR(200) NOT NULL UNIQUE, full_name VARCHAR(80),
 balance DECIMAL(18,4), active BOOLEAN NOT NULL, created_at DATETIME(6), notes TEXT
);
INSERT INTO customers VALUES
 (1,'alice@example.test','Alice Example',1234.5678,1,'2025-01-01 01:02:03.123456','line one'),
 (2,'bob@example.test','Bob Example',-12.5000,0,'2025-01-02 03:04:05.000001',NULL),
 (3,'chen@example.test','陈示例',NULL,1,'2025-01-03 00:00:00.000000','quoted, comma');
CREATE TABLE orders (id BIGINT PRIMARY KEY,customer_id BIGINT NOT NULL,amount DECIMAL(20,6),order_date DATE,payload JSON,CONSTRAINT fk_customer FOREIGN KEY(customer_id) REFERENCES customers(id));
INSERT INTO orders VALUES (101,1,99.120000,'2025-01-02','{{"source":"synthetic","priority":1}}'),(102,2,0.000001,'2025-01-03',NULL);
CREATE VIEW customer_public AS SELECT id,full_name,active FROM customers;
CREATE USER 'source_ro'@'%' IDENTIFIED BY '{config['password']}';
GRANT SELECT, SHOW VIEW ON connector_source.* TO 'source_ro'@'%';
""", encoding='utf-8')
(base / 'compose.mysql.yaml').write_text('''name: etl-connectors
services:
  mysql:
    image: mysql@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b
    container_name: etl-connector-mysql
    env_file: mysql.env
    command: [--innodb-buffer-pool-size=134217728, --performance-schema=OFF]
    ports: ['127.0.0.1:13306:3306']
    volumes:
      - mysql_data:/var/lib/mysql
      - ./mysql-init:/docker-entrypoint-initdb.d:ro
      - ./mysql-admin.cnf:/run/secrets/admin.cnf:ro
      - ./mysql-readonly.cnf:/run/secrets/readonly.cnf:ro
    healthcheck:
      test: [CMD-SHELL, 'cp /run/secrets/readonly.cnf /tmp/readonly.cnf && chmod 600 /tmp/readonly.cnf && mysql --defaults-extra-file=/tmp/readonly.cnf -N -e "SELECT 1" connector_source']
      interval: 3s
      timeout: 3s
      retries: 40
    networks:
      isolated:
        aliases: [connector-mysql]
      control:
        aliases: [connector-mysql]
volumes:
  mysql_data:
networks:
  isolated:
    name: etl-connectors
    ipam:
      config:
        - subnet: 172.20.0.0/16
  control:
    external: true
    name: etl-agent_default
''', encoding='utf-8')
result = subprocess.run(['docker', 'compose', '-f', str(base / 'compose.mysql.yaml'), 'up', '-d', '--wait', '--wait-timeout', '150'], capture_output=True)
if result.returncode:
    (base / 'mysql-private-setup.log').write_bytes(result.stdout + result.stderr)
    raise SystemExit('MySQL startup failed; inspect private runtime setup log')
print('Pinned real MySQL started; existing credentials/volume retained. Private credentials stay in runtime directory.')
