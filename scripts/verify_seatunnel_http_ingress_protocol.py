"""Isolated 2.3.12 FakeSource -> Http protocol PoC; never writes Doris.

Creates only its own labeled container/network/runtime directory. Does not modify
an existing container or copy private job directories. Two distinct preselected
job IDs exercise HTTP 200 and 503; no submit retries, no real-L2 qualification.
"""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[1]
MAVEN = 'https://repo.maven.apache.org/maven2/org/apache/seatunnel/connector-http-base/2.3.12/connector-http-base-2.3.12.jar'
METRICS = ('SourceReceivedCount', 'SinkWriteCount', 'IntermediateQueueSize')


def docker(*args, capture=False):
    result = subprocess.run(['docker', *args], check=True, stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                            stderr=subprocess.PIPE, text=True)
    return result.stdout.strip() if capture else None


def request(base, method, path, payload=None):
    body = json.dumps(payload).encode() if payload is not None else None
    query = urllib.request.Request(base + path, data=body, method=method,
                                   headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(query, timeout=5) as response:
        return json.loads(response.read(1024 * 1024))


def main(rest_port=18082, receiver_port=18083):
    for port in (rest_port, receiver_port):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', port))
    token = uuid.uuid4().hex
    name = 'etl-http-ingress-' + token[:12]
    network = name + '-network'
    runtime = ROOT / '.runtime' / name
    runtime.mkdir(parents=True, exist_ok=False)
    destination = ROOT / 'artifacts' / 'issue-13' / 'http-ingress-protocol.json'
    if destination.exists():
        destination = destination.with_name('http-ingress-protocol-' + token[:12] + '.json')
    destination.parent.mkdir(parents=True, exist_ok=True)
    report = {'level': 'real_http_ingress_protocol_probe', 'managed_l2_accepted': False,
              'exclusive_egress_accepted': False, 'container': name, 'network': network,
              'image': 'aefac91816d2', 'rest_port': rest_port, 'receiver_port': receiver_port,
              'runtime_directory': str(runtime.relative_to(ROOT)), 'phase': 'setup_intent', 'jobs': []}
    def persist():
        destination.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    persist()
    observations = {'accept': [], 'reject': []}
    class Receiver(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            mode = self.path.removeprefix('/' + token + '/')
            length = int(self.headers.get('Content-Length', '0'))
            if mode not in observations or not 0 < length <= 65536:
                self.send_error(400)
                return
            raw = self.rfile.read(length)
            safe = {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
            try:
                value = json.loads(raw)
                safe['single_json_object'] = isinstance(value, dict)
                safe['field_names'] = sorted(value) if isinstance(value, dict) else []
            except ValueError:
                safe['single_json_object'] = False
            observations[mode].append(safe)
            self.send_response(200 if mode == 'accept' else 503)
            self.send_header('Content-Length', '2')
            self.end_headers()
            self.wfile.write(b'{}')
    server = ThreadingHTTPServer(('0.0.0.0', receiver_port), Receiver)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    created_container = created_network = False
    try:
        # Copy ONLY executable/distribution files; never config private subdirectories.
        distribution = runtime / 'seatunnel'
        distribution.mkdir()
        for directory in ('bin', 'lib', 'starter', 'plugins', 'connectors'):
            docker('cp', 'etl-seatunnel-probe:/opt/seatunnel/' + directory, str(distribution / directory))
        config = distribution / 'config'
        config.mkdir()
        for filename in ('seatunnel.yaml', 'hazelcast.yaml', 'hazelcast-master.yaml', 'hazelcast-worker.yaml',
                         'hazelcast-client.yaml', 'jvm_options', 'jvm_master_options', 'jvm_worker_options',
                         'jvm_client_options', 'seatunnel-env.sh', 'log4j2.properties', 'log4j2_client.properties', 'plugin_config'):
            docker('cp', 'etl-seatunnel-probe:/opt/seatunnel/config/' + filename, str(config / filename))
        # New network has no existing engine peers; also give the cluster a unique name.
        for filename in ('hazelcast.yaml', 'hazelcast-master.yaml', 'hazelcast-worker.yaml', 'hazelcast-client.yaml'):
            path = config / filename
            text = path.read_text(encoding='utf-8')
            import re
            text = re.sub(r'(?m)^(\s*cluster-name:)\s*.*$', r'\1 ' + name, text)
            path.write_text(text, encoding='utf-8')
        report['phase'] = 'maven_download_intent'
        persist()
        with urllib.request.urlopen(MAVEN, timeout=30) as response:
            jar = response.read(64 * 1024 * 1024)
        with urllib.request.urlopen(MAVEN + '.sha1', timeout=15) as response:
            expected = response.read(256).decode().strip().split()[0]
        actual = hashlib.sha1(jar).hexdigest()
        if actual != expected or len(expected) != 40:
            raise ValueError('maven_checksum_mismatch')
        (distribution / 'connectors' / 'connector-http-base-2.3.12.jar').write_bytes(jar)
        report['connector'] = {'url': MAVEN, 'sha1': actual, 'sha256': hashlib.sha256(jar).hexdigest(),
                               'maven_sha1_verified': True}
        report['phase'] = 'container_start_intent'
        persist()
        docker('network', 'create', '--label', 'etl.protocol=' + token, network)
        created_network = True
        docker('run', '-d', '--name', name, '--label', 'etl.protocol=' + token, '--network', network,
               '-p', '127.0.0.1:' + str(rest_port) + ':8080', '--mount',
               'type=bind,source=' + str(distribution) + ',target=/opt/seatunnel', '--entrypoint', 'bash',
               'aefac91816d2', '-lc', 'export PATH=/opt/java/bin:$PATH; cd /opt/seatunnel; exec bash bin/seatunnel-cluster.sh')
        created_container = True
        base = 'http://127.0.0.1:' + str(rest_port)
        deadline = time.monotonic() + 100
        while True:
            try:
                overview = request(base, 'GET', '/overview')
                if overview.get('projectVersion') != '2.3.12':
                    raise ValueError('engine_version_mismatch')
                report['engine'] = {key: overview.get(key) for key in ('projectVersion', 'gitCommitAbbrev')}
                break
            except (OSError, ValueError):
                if time.monotonic() >= deadline:
                    raise OSError('isolated_engine_start_unknown') from None
                time.sleep(2)
        for mode in ('accept', 'reject'):
            identifier = str(uuid.uuid4().int % (2 ** 62) + 1)
            job_name = 'http_protocol_' + mode + '_' + token[:12]
            job = {'env': {'job.mode': 'BATCH', 'parallelism': 1, 'job.name': job_name},
                   'source': [{'plugin_name': 'FakeSource', 'row.num': 2, 'split.num': 1,
                               'schema': {'fields': {'id': 'int', 'name': 'string'}}}],
                   'sink': [{'plugin_name': 'Http', 'url': 'http://host.docker.internal:' + str(receiver_port) + '/' + token + '/' + mode,
                             'array_mode': False, 'batch_size': 1, 'retry': 0, 'connect_timeout_ms': 1500,
                             'socket_timeout_ms': 1500}]}
            entry = {'mode': mode, 'job_id': identifier, 'job_name': job_name, 'phase': 'submit_intent',
                     'submit_attempts': 1, 'samples': [], 'receiver': []}
            report['jobs'].append(entry)
            persist()  # Durable intent BEFORE the single POST. Never submit this ID again.
            try:
                response = request(base, 'POST', '/submit-job?jobId=' + identifier, job)
                entry['submit_id_matches'] = str(response.get('jobId')) == identifier
            except Exception as error:
                entry['submit_unknown_type'] = type(error).__name__
            start = time.monotonic()
            deadline = start + 90
            while time.monotonic() < deadline:
                sample = {'elapsed_seconds': round(time.monotonic() - start, 3)}
                try:
                    info = request(base, 'GET', '/job-info/' + identifier)
                    sample['associated'] = str(info.get('jobId')) == identifier and info.get('jobName') == job_name
                    sample['state'] = info.get('jobStatus') if sample['associated'] else None
                    metrics = info.get('metrics')
                    sample['metrics_present'] = isinstance(metrics, dict)
                    sample['metrics'] = {key: metrics[key] for key in METRICS if key in metrics} if isinstance(metrics, dict) else None
                    sample['missing_metrics'] = [key for key in METRICS if not isinstance(metrics, dict) or key not in metrics]
                except urllib.error.HTTPError as error:
                    sample.update(http_status=error.code, state=None, metrics=None, outcome='unknown')
                except Exception as error:
                    sample.update(error_type=type(error).__name__, state=None, metrics=None, outcome='unknown')
                entry['samples'].append(sample)
                entry['receiver'] = list(observations[mode])
                persist()
                if sample.get('associated') and sample.get('state') in ('FINISHED', 'FAILED', 'CANCELED'):
                    entry['terminal_state'] = sample['state']
                    # Preserve post-terminal async metrics errors as unknown, never zero.
                    for delay in (0.0, 2.0):
                        if delay:
                            time.sleep(delay)
                        try:
                            final = request(base, 'GET', '/job-info/' + identifier)
                            metrics = final.get('metrics')
                            entry.setdefault('post_terminal', []).append({'associated': str(final.get('jobId')) == identifier and final.get('jobName') == job_name,
                                'state': final.get('jobStatus'), 'metrics': {k: metrics[k] for k in METRICS if k in metrics} if isinstance(metrics, dict) else None})
                        except urllib.error.HTTPError as error:
                            entry.setdefault('post_terminal', []).append({'outcome': 'unknown', 'http_status': error.code})
                        except Exception as error:
                            entry.setdefault('post_terminal', []).append({'outcome': 'unknown', 'error_type': type(error).__name__})
                    break
                time.sleep(2)
            entry['receiver'] = list(observations[mode])
            entry['request_count'] = len(observations[mode])
            entry['http_delivery_accepted'] = mode == 'accept' and entry['request_count'] == 2 and all(x['single_json_object'] for x in observations[mode])
            entry['connector_fail_closed'] = mode == 'reject' and entry.get('terminal_state') == 'FAILED'
            entry['phase'] = 'observed'
            persist()
        report['phase'] = 'completed'
    except Exception as error:
        report['failed_phase'] = report['phase']
        report['phase'] = 'unknown'
        report['error_type'] = type(error).__name__
        if isinstance(error, urllib.error.HTTPError):
            report['http_status'] = error.code
        # Exception text may include environment values or request internals.
        raise RuntimeError('isolated_http_protocol_probe_unknown') from None
    finally:
        if created_container:
            try:
                docker('rm', '-f', name)
                report['own_container_removed'] = True
            except Exception:
                report['own_container_removed'] = False
        if created_network:
            try:
                docker('network', 'rm', network)
                report['own_network_removed'] = True
            except Exception:
                report['own_network_removed'] = False
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)
        persist()
        print(json.dumps({'artifact': str(destination.relative_to(ROOT)), 'phase': report['phase'],
                          'managed_l2_accepted': False, 'jobs': [{'mode': j['mode'], 'terminal_state': j.get('terminal_state'),
                          'request_count': j.get('request_count'), 'connector_fail_closed': j.get('connector_fail_closed')} for j in report['jobs']]}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rest-port', type=int, default=18082)
    parser.add_argument('--receiver-port', type=int, default=18083)
    args = parser.parse_args()
    main(args.rest_port, args.receiver_port)
