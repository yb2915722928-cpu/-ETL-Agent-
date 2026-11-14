"""V12 真实 Zeta 受管搬运 L2 验证：固定版本、提交/作业关联、查询与停止语义、
真实影子表比对、真实必要字节来源及其新鲜度条件。

所有结论都来自真实引擎、真实 MySQL 与真实 Doris 的实测；任何一项缺少实测证据
都记为条件，不用模拟或文档推断填充 L2 成绩。
"""
import hashlib
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'backend'))
from app import real_engine as engine  # noqa: E402

ENGINE_URL = os.environ.get('REAL_ENGINE_URL', 'http://127.0.0.1:18080')
RUNTIME_DIR = os.environ.get('REAL_RUNTIME_DIR', r'E:\课件\23-实战\生产级 ETL-Agent 数据集成平台\.runtime\seatunnel')
DORIS = {'host': '127.0.0.1', 'port': 19030, 'database': 'etl_agent_l2'}
MYSQL = {'host': '127.0.0.1', 'port': 13306, 'database': 'connector_source'}
SOURCE_QUERY = 'SELECT id, full_name FROM connector_source.customers ORDER BY id'
SHADOW_DDL = ('CREATE TABLE IF NOT EXISTS etl_agent_l2.{table} (id BIGINT NOT NULL, full_name VARCHAR(255) NOT NULL) '
              'DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")')
STREAM_LOAD_COLUMNS = ('Label', 'Db', 'Table', 'ClientIp', 'Status', 'Message', 'Url', 'TotalRows', 'LoadedRows',
                       'FilteredRows', 'UnselectedRows', 'LoadBytes', 'StartTime', 'FinishTime', 'User', 'Comment')


def request(method, path, body=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(ENGINE_URL + path, data=data, method=method,
                                 headers={'Content-Type': 'application/json'} if data else {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as error:
        raise AssertionError('真实引擎 %s %s -> %s %s' % (method, path, error.code,
                                                        error.read().decode()[:600])) from None


def tolerant_get(path, attempts=15):
    """真实引擎在历史与作业信息尚未落定时会返回 500；这不是失败，必须重试。"""
    last = None
    for attempt in range(attempts):
        try:
            return request('GET', path)
        except AssertionError as error:
            if 'is null' not in str(error):
                raise
            last = error
            time.sleep(0.5 * (attempt + 1))
    raise AssertionError('真实引擎 %s 始终不可读：%s' % (path, last))


def job_info(job_id):
    """读作业信息；内部对落定竞态重试。"""
    return tolerant_get('/job-info/' + str(job_id))


def credentials():
    """凭据只从部署私密配置物化一次，不打印、不写进证据。"""
    import pymysql
    with open(os.path.join(RUNTIME_DIR, 'private-probe', 'doris-measured-config.json'), encoding='utf-8') as handle:
        probe = json.load(handle)
    sink, source = probe['sink'][0], probe['source'][0]
    writer = {'host': DORIS['host'], 'port': DORIS['port'], 'username': sink['username'],
              'password': sink['password'], 'database': sink['database']}
    reader = {'host': MYSQL['host'], 'port': MYSQL['port'], 'username': source['user'],
              'password': source['password'], 'database': MYSQL['database'],
              'jdbc_url': source['url'], 'driver': source['driver'], 'query': SOURCE_QUERY}
    return writer, reader, pymysql


def query(credential, statement, pymysql):
    connection = pymysql.connect(host=credential['host'], port=credential['port'], user=credential['username'],
                                 password=credential['password'], database=credential['database'],
                                 connect_timeout=10)
    try:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            rows = cursor.fetchall()
            return rows, [column[0] for column in cursor.description or []]
    finally:
        connection.close()


def digest(path):
    sha1 = hashlib.sha1()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            sha1.update(block)
    return sha1.hexdigest()


def wait_status(job_id, wanted, seconds=180):
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        info = job_info(job_id)
        last = info.get('jobStatus')
        if last in wanted:
            return info
        time.sleep(1)
    raise AssertionError('真实作业 %s 未在 %ds 内进入 %s，最后状态 %s' % (job_id, seconds, wanted, last))


def build_job(writer, reader, run_id, name, base_table):
    """按冻结事实构建真实作业；影子表名由 base_table 与 run_id 决定性地推出。"""
    managed = {'command_id': 'c-' + run_id + '-' + name, 'run_id': 'r-' + run_id + '-' + name, 'engine_url': ENGINE_URL}
    facts = {'plan': {'source': {'table': 'customers'}, 'target': {'table': base_table}},
             'source': {'connection_id': 'real-source', 'resource_id': 'customers', 'resource_fingerprint': 'fp',
                        'schema_hash': 'sh', 'secret_reference_id': 'sr', 'secret_version': 1},
             'target': {'connection_id': 'real-target', 'resource_id': base_table, 'resource_fingerprint': 'fp',
                        'schema_hash': 'sh', 'secret_reference_id': 'tr', 'secret_version': 1}}
    job = engine.real_job_request(managed, facts, {**writer, 'fenodes': 'connector-doris:8030',
                                  'query_port': 9030, 'label_prefix': 'etl_agent_l2_' + run_id}, reader)
    job['env']['job.name'] = name
    return job, managed, 'etl_agent_l2_' + run_id, engine.shadow_table(base_table, managed['run_id'])


def main(output):
    writer, reader, pymysql = credentials()
    passed, conditions = [], []
    run_id = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

    def prepare(name):
        """构建真实作业并物化本次运行的影子表；影子表名由运行身份决定性地推出。"""
        job, _, label_prefix, shadow = build_job(writer, reader, run_id, name, 'customers')
        query(writer, SHADOW_DDL.format(table=shadow), pymysql)
        return job, label_prefix, shadow

    overview = request('GET', '/overview')
    digests = {item['artifact']: digest(os.path.join(RUNTIME_DIR, 'downloads', item['artifact']))
               for item in (*engine.REAL_CONNECTORS, engine.REAL_DRIVER)}
    ok, pin_conditions = engine.verify_real_runtime(overview, digests, digests[engine.REAL_DRIVER['artifact']])
    assert ok, pin_conditions
    passed.append('real runtime pin: engine %s/%s, connector and driver digests verified'
                  % (overview['projectVersion'], overview['gitCommitAbbrev']))

    expected = [(int(row[0]), row[1]) for row in query(reader, SOURCE_QUERY, pymysql)[0]]
    assert expected, '真实源读取失败：合成输入为空'

    job, label_prefix, shadow = prepare('etl_agent_l2_transfer')
    submitted = request('POST', '/submit-job', job)
    job_id = str(submitted['jobId'])
    info = job_info(job_id)
    assert info.get('jobId') == job_id and info.get('jobName') == 'etl_agent_l2_transfer', info
    passed.append('real submission and job association: job %s correlated by id and env job name' % job_id)
    conditions.append({'reason': 'job_info_race_retryable',
                       'note': '真实引擎对 startTimestamp / finishedJobMetrics 尚未落定的作业与历史返回 500；客户端必须重试，不能据此判定未知作业或盲目再提交'})

    unknown = request('GET', '/job-info/999999999999')
    assert unknown.get('jobId') == '999999999999' and 'jobStatus' not in unknown, '未知作业未回显请求 ID'
    finished = wait_status(job_id, ('FINISHED', 'FAILED'))
    assert finished['jobStatus'] == 'FINISHED', finished.get('errorMsg')
    metrics = finished['metrics']
    assert int(metrics['SourceReceivedCount']) == len(expected), '真实源读取计数与合成输入不一致'
    passed.append('real query semantics: job-info %s source rows, unknown job echoes the requested id'
                  % metrics['SourceReceivedCount'])

    rows = [(int(row[0]), row[1]) for row in query(writer, 'SELECT id, full_name FROM %s ORDER BY id' % shadow, pymysql)[0]]
    assert rows == expected, '真实影子表结果与合成输入不一致'
    assert query(writer, "SHOW TABLES FROM etl_agent_l2 LIKE 'customers'", pymysql)[0] == (), '正式表被写入'
    passed.append('real shadow comparison: %d shadow rows equal the synthetic input, target table untouched' % len(rows))

    label = engine.stream_load_label(label_prefix, shadow, job_id)
    receipt, columns = query(writer, "SHOW STREAM LOAD WHERE Label LIKE '%s'" % label, pymysql)
    receipt = dict(zip(columns, receipt[0])) if receipt else None
    load_bytes = int(receipt['LoadBytes']) if receipt else None
    byte_kind, sink_bytes, condition = engine.measured_bytes(receipt, DORIS['database'], shadow, label)
    if receipt and byte_kind == 'measured':
        assert int(receipt['LoadedRows']) == len(expected), '回执行数与合成输入不一致'
        age = (engine.utcnow() - datetime.fromisoformat(receipt['FinishTime'].replace(' ', 'T') + '+00:00')).total_seconds()
        passed.append('real measured byte source: Doris receipt LoadBytes=%d for %d rows (engine estimate %s)'
                      % (sink_bytes, int(receipt['LoadedRows']), metrics['SinkWriteBytes']))
        conditions.append({'reason': 'measured_byte_source_freshness', 'receipt_age_seconds': age,
                           'engine_estimate_bytes': int(metrics['SinkWriteBytes']),
                           'note': 'Doris 回执是唯一实测字节来源；FE fetch_stream_load_record_interval_second 决定其新鲜度'})
    else:
        conditions.append(condition or {'reason': 'measured_byte_source_unavailable',
                                        'source': engine.MEASURED_BYTE_SOURCE['name']})

    cancel_job, _, _ = prepare('etl_agent_l2_cancel')
    cancel_id = str(request('POST', '/submit-job', cancel_job)['jobId'])
    ack = request('POST', '/stop-job/' + cancel_id, {'jobId': cancel_id, 'isStopWithSavePoint': False})
    canceled = wait_status(cancel_id, ('CANCELED',), seconds=90)
    assert int(canceled['metrics']['SinkWriteBytes']) <= len(expected), '取消后写入数超过合成输入'
    passed.append('real minimal cancel: job %s acknowledged %s and reached %s with sink count frozen at %s'
                  % (cancel_id, ack, canceled['jobStatus'], canceled['metrics']['SinkWriteBytes']))

    lost_job, _, lost_shadow = prepare('etl_agent_l2_lost')
    lost_id = str(request('POST', '/submit-job', lost_job)['jobId'])
    request('POST', '/stop-job/' + lost_id, {'jobId': lost_id, 'isStopWithSavePoint': False})
    reconciled = wait_status(lost_id, ('CANCELED',), seconds=90)
    assert reconciled['jobId'] == lost_id and reconciled['jobName'] == 'etl_agent_l2_lost', reconciled
    conditions.append({'reason': 'finished_jobs_history_poisoned',
                       'detail': 'GET /finished-jobs 对历史中 startTime 为空的作业返回 500，当前无法按名列举已结束作业',
                       'consequence': '响应丢失只能靠提交响应里持久化的作业 ID 经 /job-info/{id} 核对；不能靠列举发现，也不能盲目再提交'})

    resubmitted = str(request('POST', '/submit-job', {**lost_job, 'env': {**lost_job['env'], 'job.name':
                    'etl_agent_l2_lost_again'}})['jobId'])
    request('POST', '/stop-job/' + resubmitted, {'jobId': resubmitted, 'isStopWithSavePoint': False})
    wait_status(resubmitted, ('CANCELED',), seconds=90)
    assert resubmitted != lost_id, '重复投递复用了同一作业 ID，无法识别重复'
    assert engine.stream_load_label('etl_agent_l2_' + run_id, lost_shadow, resubmitted) != \
        engine.stream_load_label('etl_agent_l2_' + run_id, lost_shadow, lost_id), '重复投递的写入标签相同'
    passed.append('real duplicate-submission diagnostic (not a managed unknown-lock test): job id %s; resubmit '
                  'would be a distinct job id %s with a distinct write label, so it stays detectable'
                  % (lost_id, resubmitted))

    # This direct protocol probe has no Broker writer binding, live byte channel,
    # external-write quiescence check or scoped cleanup. A final receipt cannot
    # turn those missing production prerequisites into a passing managed run.
    allowed, gate = engine.transfer_gate(False, False, False, True)
    report = {'passed': passed, 'level': 'real_protocol_probe', 'managed_l2_accepted': False,
              'conditions': conditions,
              'real_transfer_gate': {'allowed': allowed, 'conditions': gate},
              'engine': {'version': overview['projectVersion'], 'commit': overview['gitCommitAbbrev'],
                         'jobs': [job_id, cancel_id, lost_id, resubmitted]}, 'shadow_table': shadow,
              'measured_bytes': sink_bytes, 'engine_estimate_bytes': int(metrics['SinkWriteBytes']),
              'deployment_changes': [{'target': 'etl-connector-doris',
                                      'change': "GRANT Create_priv ON etl_agent_l2.* TO etl_l2_probe_writer@'%'",
                                      'evidence': 'artifacts/issue-13/writer-grant.json',
                                      'reason': '执行写入账号必须能在已批准库内物化本次运行的影子表；不放大到别的库或全局'}],
              'root_deployment_untouched': False,
              'root_deployment_change_note': '只按上面一条记录扩大执行写入账号在已批准库内的建表权限；未改动任何容器、镜像、端口或引擎配置',
              'simulation_or_docs_filled': False}
    with open(output, 'w', encoding='utf-8') as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write('\n')
    print(json.dumps({'passed': passed, 'level': 'real_protocol_probe', 'managed_l2_accepted': False,
                      'conditions': [c['reason'] for c in conditions]},
                     ensure_ascii=False))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'real-engine-l2.json')
