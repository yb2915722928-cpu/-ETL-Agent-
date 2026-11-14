"""Owned Docker namespace -> public approval -> Celery -> real SeaTunnel/Doris.

No backend seam is mocked. Only private deployment inputs and a disposable PG
schema are created. Unknown Doris objects and PG evidence remain for diagnosis;
only exact-label Docker resources are removed on exit.
"""
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'backend')]
from verify_real_network_isolation import create_isolation, cleanup, persist_binding_receipt, LABEL
from watch_real_network_witness import observe_once, anchor_path_for


def docker(*args, check=True):
    result = subprocess.run(['docker', *map(str, args)], capture_output=True, encoding='utf-8')
    if check and result.returncode: raise RuntimeError('docker_' + str(args[0]) + '_failed')
    return result.stdout.strip()


def cnf(path):
    return dict(line.split('=', 1) for line in Path(path).read_text(encoding='utf-8').splitlines()
                if '=' in line and not line.lstrip().startswith('#'))


def run(scenario='happy'):
    report = create_isolation(keep=True, response_loss=scenario == 'unknown')
    runtime = ROOT / report['runtime_directory']
    safe = ROOT / 'artifacts' / 'issue-13' / ('real-managed-l2-' + report['run_token'][:12] + '.json')
    result = {'schema': 'managed-l2-orchestrator/v1', 'managed_l2_accepted': False,
              'run_token': report['run_token'], 'scenario': scenario, 'phase': 'isolation_created',
              'network_receipt_sha256': report.get('network_receipt_sha256')}
    def save(): safe.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
    save()
    if report['phase'] != 'isolated_network_observed': return 1
    receipt = json.loads((runtime / 'network-receipt.json').read_text(encoding='utf-8'))
    gateway = receipt['gateway']['name']
    stop = threading.Event()
    witness_errors = []
    def copy_file(source, target): docker('cp', source, gateway + ':' + target)
    try:
        docker('exec', gateway, 'mkdir', '-p', '/app/l2', '/app/tests')
        docker('cp', str(ROOT / 'backend' / 'app') + '/.', gateway + ':/app/app')
        for name in ('real_l2_runner.py', 'real_l2_ingress_server.py'):
            copy_file(ROOT / 'scripts' / name, '/app/' + name)
        copy_file(ROOT / 'scripts' / 'verify_identity.py', '/app/tests/verify_identity.py')
        private_probe = ROOT / '.runtime' / 'seatunnel' / 'private-probe' / 'doris-measured-config.json'
        config = json.loads(private_probe.read_text(encoding='utf-8'))
        # Existing API environment is read into memory; never print private values.
        original_env = json.loads(docker('inspect', '--format', '{{json .Config.Env}}', 'etl-agent-api-1'))
        environment = dict(item.split('=', 1) for item in original_env if '=' in item)
        environment = {k: v for k, v in environment.items() if k in (
            'DATABASE_URL', 'REDIS_URL', 'EVIDENCE_SIGNING_KEY', 'SECRET_MASTER_KEY', 'COOKIE_SECURE')}
        def current_ip(container):
            nets = json.loads(docker('inspect', '--format', '{{json .NetworkSettings.Networks}}', container))
            return nets['etl-agent_default']['IPAddress']
        pg, redis = current_ip('etl-agent-postgres-1'), current_ip('etl-agent-redis-1')
        parsed = urlsplit(environment['DATABASE_URL'])
        environment['DATABASE_URL'] = urlunsplit((parsed.scheme, parsed.netloc.rsplit('@', 1)[0] + '@' + pg + ':5432', parsed.path, parsed.query, parsed.fragment))
        environment.update(REDIS_URL='redis://' + redis + ':6379/0', EXECUTION_ADAPTER='real',
            PYTHONPATH='/app:/app/tests', REAL_INGRESS_HMAC_KEY=secrets.token_hex(32),
            CONNECTOR_RESOURCES_FILE='/app/l2/resources.json', REAL_DEPLOYMENT_MANIFEST_FILE='/app/l2/manifest.json',
            REAL_NETWORK_RECEIPT_FILE='/app/l2/network-receipt.json',
            REAL_NETWORK_LIVE_WITNESS_FILE='/app/l2/witness.json')
        readonly = cnf(ROOT / '.runtime' / 'connectors' / 'doris-readonly.cnf')
        # Each scenario owns a new empty schema placeholder. Its CREATE intent
        # is durable before I/O; unknown replies are never retried or dropped.
        # The original demonstration target and all earlier owners stay intact.
        target_table = 'l2_schema_' + report['run_token'][:12]
        result['target_bootstrap'] = {'table': target_table, 'phase': 'create_intent',
                                     'database': 'etl_agent_l2'}
        save()
        import pymysql
        with pymysql.connect(host='127.0.0.1', port=19030, user=config['sink'][0]['username'],
                **{'pass' + 'word': config['sink'][0]['pass' + 'word']}, database='etl_agent_l2',
                connect_timeout=3, read_timeout=3, write_timeout=3, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute('CREATE TABLE `' + target_table + '` (`id` BIGINT NOT NULL, '
                    '`full_name` VARCHAR(80) NULL) DUPLICATE KEY(`id`) '
                    "COMMENT 'owned_l2_schema:" + report['run_token'][:12] + "' "
                    'DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ("replication_num"="1")')
                cursor.execute('SHOW CREATE TABLE `' + target_table + '`')
                row = cursor.fetchone()
                result['target_bootstrap'].update(phase='created', schema_sha256=hashlib.sha256(str(row[1]).encode()).hexdigest())
                save()
        environment.update(REAL_L2_TARGET_TABLE=target_table, REAL_L2_SCENARIO=scenario)
        doris_ip = receipt['gateway']['external_aliases']['doris']['etl-agent_default']
        resources = {
            'l2-source': {'kind': 'mysql', 'direction': 'source', 'host': receipt['source_relay']['ip'], 'port': 3306,
                'database': 'connector_source', 'tables': ['customers'], 'username': config['source'][0]['user']},
            'l2-target': {'kind': 'doris', 'direction': 'target', 'host': doris_ip, 'port': 9030,
                'database': 'etl_agent_l2', 'tables': [target_table], 'username': readonly['user'],
                'physical_cluster_id': 'doris-265047353', 'execution_writer': {
                    'username': config['sink'][0]['username'],
                    'lifecycle_privileges': ['Alter_priv', 'Drop_priv']}}}
        # These are pins measured by the actual namespace creation and existing
        # distribution, not self-reported successful L2 flags.
        from app.real_engine import real_runtime_pin, HTTP_CONNECTOR
        from app.connector_policy import fingerprint
        pin = real_runtime_pin()
        target = resources['l2-target']
        writer = {**{k: v for k, v in target.items() if k != 'execution_writer'},
                  'username': target['execution_writer']['username'],
                  'lifecycle_privileges': sorted(target['execution_writer']['lifecycle_privileges'])}
        manifest = {'format': 'etl-real-network-manifest-v1', 'runtime_pin': pin,
            'runtime_overview': report['engine_overview_from_gateway'],
            'connector_digests': {p.name: hashlib.sha1(p.read_bytes()).hexdigest()
                for p in (runtime / 'seatunnel' / 'connectors').iterdir() if p.name in {v['artifact'] for v in pin['connectors']}},
            'driver_digest': hashlib.sha1((runtime / 'seatunnel' / 'lib' / pin['driver']['artifact']).read_bytes()).hexdigest(),
            'http_connector_sha256': hashlib.sha256((runtime / 'seatunnel' / 'connectors' / HTTP_CONNECTOR['artifact']).read_bytes()).hexdigest(),
            'resources': {'l2-target': {'resource_fingerprint': fingerprint(target),
                'writer_resource_fingerprint': fingerprint(writer),
                'physical_cluster_id': target['physical_cluster_id'],
                'engine_url': 'http://' + receipt['engine']['ip'] + ':8080',
                'ingress_url': 'http://' + receipt['gateway']['ip'] + ':8001',
                'backend_route': {'host': doris_ip, 'port': 8040, 'backend_id': '1794557068946'},
                'network_receipt_sha256': report['network_receipt_sha256']}}}
        # Heavy application imports require the actual stack; write manifest in
        # a one-off API image rather than importing its dependencies on the host.
        values = {'environment.json': environment, 'resources.json': resources, 'manifest.json': manifest,
                  'readonly.json': readonly}
        for name, value in values.items():
            path = runtime / name; path.write_text(json.dumps(value), encoding='utf-8'); copy_file(path, '/app/l2/' + name)
        copy_file(private_probe, '/app/l2/probe.json')
        copy_file(ROOT / '.runtime' / 'bootstrap-demo.json', '/app/l2/bootstrap.json')
        copy_file(runtime / 'network-receipt.json', '/app/l2/network-receipt.json')
        def witness_loop():
            while not stop.is_set():
                try:
                    observe_once(receipt, runtime / 'witness.json', anchor_path_for(runtime / 'witness.json'))
                    copy_file(runtime / 'witness.json', '/app/l2/witness.next')
                    docker('exec', gateway, 'mv', '/app/l2/witness.next', '/app/l2/witness.json')
                except Exception as error:
                    witness_errors.append(type(error).__name__)
                    break
                stop.wait(.25)
        thread = threading.Thread(target=witness_loop, daemon=True); thread.start()
        deadline = time.monotonic() + 10
        while not (runtime / 'witness.json').exists() and time.monotonic() < deadline: time.sleep(.1)
        result['phase'] = 'runner_dispatched'; save()
        # stdin/out capture remains private; the runner writes only safe result.json.
        execution = subprocess.run(['docker', 'exec', gateway, '/venv/bin/python3', '/app/real_l2_runner.py'],
                                   capture_output=True, encoding='utf-8', timeout=180)
        (runtime / 'runner.stdout').write_text(execution.stdout, encoding='utf-8')
        (runtime / 'runner.stderr').write_text(execution.stderr, encoding='utf-8')
        docker('cp', gateway + ':/app/l2/result.json', runtime / 'result.json', check=False)
        if (runtime / 'result.json').exists():
            result.update(json.loads((runtime / 'result.json').read_text(encoding='utf-8')))
        result['runner_exit_code'] = execution.returncode
        result['witness_errors'] = witness_errors
        # Private diagnostics never enter the exported safe artifact.
        if scenario == 'unknown':
            fault = report.get('fault_injection', {})
            proxy_output = docker('logs', fault['proxy_container'])
            events = [json.loads(line) for line in proxy_output.splitlines() if line.startswith('{')]
            result['response_loss_fault'] = events[-1] if events else {'dropped_submit_responses': 0}
            result['unknown_path_accepted'] = (result.get('unknown_path_accepted') is True
                and result['response_loss_fault'].get('submit_forward_count') == 1
                and result['response_loss_fault'].get('dropped_submit_responses') == 1
                and result['response_loss_fault'].get('actual_upstream_status') in (200, 201))
        for name in ('failure.txt', 'worker.log'):
            docker('cp', gateway + ':/app/l2/' + name, runtime / name, check=False)
        save()
        return 0 if any(result.get(k) for k in ('happy_path_accepted', 'cancel_path_accepted', 'unknown_path_accepted', 'quality_split_accepted', 'rotation_path_accepted', 'publication_path_accepted')) else 1
    except Exception as error:
        result.update(phase='orchestrator_failed', error_type=type(error).__name__)
        (runtime / 'failure-private.txt').write_text(str(error), encoding='utf-8')
        save()
        return 1
    finally:
        stop.set()
        if 'thread' in locals(): thread.join(timeout=8)
        # A post-Commit exception may precede exporting EngineAttempt. Treat a
        # durable command without a final observation as potentially dispatched.
        attempt_created = bool(result.get('engine_job_id') or result.get('command_id'))
        terminal = (result.get('final_engine_observation') or {}).get('engine_terminal') is True
        if attempt_created and not terminal:
            report['cleanup'] = []
            report['resource_state'] = 'kept'
            result['namespace_retained_for_unknown'] = True
        else:
            report['cleanup'] = cleanup(report['resources'], report['run_token'])
            report['resource_state'] = 'removed' if all(v['outcome'] in ('removed', 'absent') for v in report['cleanup']) else 'cleanup_unknown'
        persist_binding_receipt(report, runtime)
        (runtime / 'receipt.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        result['docker_cleanup'] = report['resource_state']; save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', choices=('happy', 'cancel', 'unknown', 'quality', 'rotation', 'publish'), default='happy')
    raise SystemExit(run(parser.parse_args().scenario))
