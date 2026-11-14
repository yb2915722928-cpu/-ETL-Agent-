"""Single, credential-free network isolation proof; not a managed L2 acceptance.

Default removes only resources carrying this run's exact label. --keep leaves a
fixed-IP namespace for an L2 runner. --cleanup RECEIPT removes that receipt's own
resources after validating their labels. Never changes a host/global firewall.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shutil
import socket
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
LABEL = 'etl.real-network'
ENGINE_IMAGE = 'aefac91816d2'
TOOLS_BASE = 'etl-agent-l1-api:issue10'
TOOLS_BASE_ID = 'sha256:a1a302d6fb1ad46d8618b8febd7b645038080893aed6d1bd3768a415ff0cadc3'
HTTP_JAR_SHA256 = 'a2a8ab002697dea5f070a8c77edd44f5c53da8ac8cb3ff26b71ccefcefac3ca9'
CONFIG_FILES = ('seatunnel.yaml', 'hazelcast.yaml', 'hazelcast-master.yaml',
                'hazelcast-worker.yaml', 'hazelcast-client.yaml', 'jvm_options',
                'jvm_master_options', 'jvm_worker_options', 'jvm_client_options',
                'seatunnel-env.sh', 'log4j2.properties', 'log4j2_client.properties', 'plugin_config')

# This helper executes only in newly created containers. No credentials or SQL.
HELPER = r'''
import argparse, hashlib, http.server, json, os, re, socket, subprocess, threading
import urllib.request

def emit(value): print(json.dumps(value), flush=True)
def tcp(host, port):
    try:
        with socket.create_connection((host, port), timeout=0.6): pass
        return {"connected": True}
    except OSError as e:
        return {"connected": False, "errno": e.errno, "error_type": type(e).__name__}

def probe(targets):
    emit({"netns": os.readlink('/proc/self/ns/net'), "checks": [
        dict(t, **tcp(t['host'], t['port'])) for t in json.loads(targets)]})

def relay(target):
    upstream, port = target.rsplit(':', 1)
    def copy(src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data: break
                dst.sendall(data)
        except OSError: pass
        finally:
            try: dst.shutdown(socket.SHUT_WR)
            except OSError: pass
    def handle(client):
        try:
            with client, socket.create_connection((upstream, int(port)), timeout=3) as peer:
                peer.settimeout(None)
                thread = threading.Thread(target=copy, args=(client, peer), daemon=True)
                thread.start(); copy(peer, client); thread.join(timeout=2)
        except OSError: client.close()
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', 3306)); server.listen()
        while True:
            client, _ = server.accept()
            threading.Thread(target=handle, args=(client,), daemon=True).start()

def gateway():
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_GET(self):
            body = b'{"network_probe_only":true}'
            self.send_response(200); self.send_header('Content-Length', str(len(body)))
            self.end_headers(); self.wfile.write(body)
    server = http.server.ThreadingHTTPServer(('0.0.0.0', 8001), Handler)
    # The host may copy a managed ingress into this run's disposable filesystem.
    # Take over the listener without restarting the container or changing its
    # network namespace, capability pins, mounts or restart anchor.
    def takeover():
        import time
        while not os.path.exists('/app/l2/gateway-takeover'):
            time.sleep(0.1)
        server.shutdown()
    threading.Thread(target=takeover, daemon=True).start()
    server.serve_forever()
    server.server_close()
    os.execv('/venv/bin/python3', ['/venv/bin/python3', '/app/real_l2_ingress_server.py'])

def response_loss_proxy():
    # Fault injection forwards bytes once to the actual REST server. It does not
    # parse, log or persist the job request or any credentials in its headers.
    import http.client
    lock = threading.Lock()
    counters = {'submit_forward_count': 0, 'dropped_submit_responses': 0, 'get_forward_count': 0}
    class Proxy(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def forward(self):
            length = int(self.headers.get('Content-Length', '0'))
            if length < 0 or length > 1048576:
                self.send_error(413); return
            body = self.rfile.read(length) if length else None
            submit = self.command == 'POST' and self.path.split('?', 1)[0] == '/submit-job'
            with lock:
                if submit: counters['submit_forward_count'] += 1
                if self.command == 'GET': counters['get_forward_count'] += 1
                drop = submit and counters['submit_forward_count'] == 1
            upstream = http.client.HTTPConnection('127.0.0.1', 8081, timeout=10)
            try:
                headers = {key: value for key, value in self.headers.items()
                           if key.lower() not in ('host', 'connection', 'transfer-encoding')}
                upstream.request(self.command, self.path, body=body, headers=headers)
                reply = upstream.getresponse()
                raw = reply.read(1048576)
                with lock:
                    if drop:
                        counters['dropped_submit_responses'] += 1
                        counters['actual_upstream_status'] = reply.status
                    emit(dict(counters))
                if drop:
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                self.send_response(reply.status)
                for key, value in reply.getheaders():
                    if key.lower() not in ('content-length', 'connection', 'transfer-encoding'):
                        self.send_header(key, value)
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers(); self.wfile.write(raw)
            except (OSError, http.client.HTTPException):
                self.close_connection = True
            finally:
                upstream.close()
        do_GET = forward
        do_POST = forward
    http.server.ThreadingHTTPServer(('0.0.0.0', 8080), Proxy).serve_forever()


def firewall(source, ingress, fault='none'):
    rules = ['*filter', ':INPUT ACCEPT [0:0]', ':FORWARD DROP [0:0]', ':OUTPUT DROP [0:0]',
      '-A OUTPUT -p udp --dport 53 -j DROP', '-A OUTPUT -p tcp --dport 53 -j DROP',
      '-A OUTPUT -o lo -j ACCEPT',
      '-A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT',
      '-A OUTPUT -d '+source+'/32 -p tcp --dport 3306 -m conntrack --ctstate NEW -j ACCEPT',
      '-A OUTPUT -d '+ingress+'/32 -p tcp --dport 8001 -m conntrack --ctstate NEW -j ACCEPT', 'COMMIT', '']
    if fault == 'response-loss':
        rules.insert(4, '-A INPUT ! -i lo -p tcp --dport 8081 -j DROP')
    raw = '\n'.join(rules)
    subprocess.run(['iptables-restore'], input=raw, text=True, check=True)
    v6 = '*filter\n:INPUT ACCEPT [0:0]\n:FORWARD DROP [0:0]\n:OUTPUT DROP [0:0]\n-A OUTPUT -o lo -j ACCEPT\n-A OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT\nCOMMIT\n'
    subprocess.run(['ip6tables-restore'], input=v6, text=True, check=True)
    # Drop Docker's embedded-DNS DNAT before its rewritten port reaches filter.
    # The filter rules above also block normal DNS on every non-loopback route.
    for proto in ('udp', 'tcp'):
        subprocess.run(['iptables','-t','raw','-A','OUTPUT','-d','127.0.0.11/32',
                        '-p',proto,'--dport','53','-j','DROP'], check=True)
    snapshot()

def snapshot():
    saved = subprocess.check_output(['iptables-save'], text=True)
    saved6 = subprocess.check_output(['ip6tables-save'], text=True)
    def normalize(raw):
        return '\n'.join(re.sub(r'\[\d+:\d+\]', '[0:0]', line)
                         for line in raw.splitlines() if not line.startswith('#'))+'\n'
    normalized, normalized6 = normalize(saved), normalize(saved6)
    emit({'netns': os.readlink('/proc/self/ns/net'), 'ipv4_rules': normalized,
          'ipv6_rules': normalized6, 'rules_sha256': hashlib.sha256((normalized+normalized6).encode()).hexdigest()})

p=argparse.ArgumentParser(); p.add_argument('mode'); p.add_argument('args', nargs='*'); a=p.parse_args()
if a.mode == 'relay': relay(a.args[0])
elif a.mode == 'gateway': gateway()
elif a.mode == 'response-loss-proxy': response_loss_proxy()
elif a.mode == 'probe': probe(a.args[0])
elif a.mode == 'firewall': firewall(*a.args)
elif a.mode == 'snapshot': snapshot()
elif a.mode == 'hostip': emit({'host': socket.gethostbyname('host.docker.internal')})
elif a.mode == 'http':
    with urllib.request.urlopen(a.args[0], timeout=3) as r:
        data=json.loads(r.read(1048576))
        emit({k: data[k] for k in ('projectVersion','gitCommitAbbrev','network_probe_only') if k in data})
'''


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def docker(*args):
    result = subprocess.run(['docker', *map(str, args)], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, encoding='utf-8')
    if result.returncode:
        # Docker diagnostics can contain runtime values; surface only a safe class.
        raise RuntimeError('docker_' + str(args[0]) + '_failed')
    return result.stdout.strip()


def inspect(name, template):
    return json.loads(docker('inspect', '--format', template, name))


def network_info(name):
    return json.loads(docker('network', 'inspect', '--format',
                            '{"id":{{json .Id}},"internal":{{json .Internal}},"ipam":{{json .IPAM.Config}}}', name))


def own_label(kind, name):
    if kind == 'network':
        return docker('network', 'inspect', '--format', '{{index .Labels "' + LABEL + '"}}', name)
    if kind == 'image':
        return docker('image', 'inspect', '--format', '{{index .Config.Labels "' + LABEL + '"}}', name)
    return docker('inspect', '--format', '{{index .Config.Labels "' + LABEL + '"}}', name)


def cleanup(resources, token):
    removed = []
    for kind, name in resources:
        try:
            if kind == 'image':
                present = docker('image', 'ls', '--format', '{{.Repository}}:{{.Tag}}').splitlines()
            elif kind == 'network':
                present = docker('network', 'ls', '--format', '{{.Name}}').splitlines()
            else:
                present = docker('ps', '-a', '--format', '{{.Names}}').splitlines()
            if name not in present:
                removed.append({'kind': kind, 'name': name, 'outcome': 'absent'})
                continue
            if own_label(kind, name) != token:
                raise RuntimeError('resource_label_mismatch')
            if kind == 'container': docker('rm', '-f', name)
            elif kind == 'network': docker('network', 'rm', name)
            elif kind == 'image': docker('image', 'rm', name)
            removed.append({'kind': kind, 'name': name, 'outcome': 'removed'})
        except Exception as error:
            removed.append({'kind': kind, 'name': name, 'outcome': 'unknown', 'error_type': type(error).__name__})
    return removed


def probe_network_namespace(engine_name, tools_image_id, helper_path, token, targets):
    """Observe TCP paths in an existing owned engine netns; return safe facts.

    The caller supplies targets from its current frozen deployment, then binds
    the resulting evidence. This helper grants no authority and changes no rules.
    """
    if own_label('container', engine_name) != token:
        raise RuntimeError('engine_label_mismatch')
    helper_path = Path(helper_path).resolve()
    expected_runtime = (ROOT / '.runtime' / ('real-network-' + token[:12])).resolve()
    if helper_path.parent != expected_runtime or helper_path.name != 'helper.py':
        raise RuntimeError('helper_runtime_scope_mismatch')
    name = 'etl-real-network-' + token[:12] + '-check-' + uuid.uuid4().hex[:6]
    try:
        output = json.loads(docker('run', '--name', name, '--label', LABEL + '=' + token,
            '--network', 'container:' + engine_name, '--cap-drop', 'ALL',
            '--security-opt', 'no-new-privileges:true', '--mount',
            'type=bind,source=' + str(helper_path) + ',target=/proof.py,readonly',
            '--entrypoint', '/venv/bin/python3', tools_image_id, '/proof.py', 'probe', json.dumps(targets)))
        if output['netns'] != docker('exec', engine_name, 'readlink', '/proc/self/ns/net'):
            raise RuntimeError('probe_namespace_mismatch')
        return output
    finally:
        outcomes = cleanup([('container', name)], token)
        if any(item['outcome'] not in ('removed', 'absent') for item in outcomes):
            raise RuntimeError('probe_cleanup_unknown')


def persist_binding_receipt(report, runtime):
    """Make the raw canonical file hash identical to the manifest binding digest."""
    if 'network_receipt' not in report:
        return
    receipt = report['network_receipt']
    receipt['resource_state'] = report['resource_state']
    receipt['verification_phase'] = report['phase']
    raw = canonical(receipt)
    path = runtime / 'network-receipt.json'
    path.write_bytes(raw)  # No newline/BOM: manifest binds these exact UTF-8 bytes.
    report['network_receipt_sha256'] = hashlib.sha256(raw).hexdigest()
    report['network_receipt_file'] = str(path.relative_to(ROOT))


def create_isolation(keep=False, response_loss=False):
    """Create/observe one run and return its safe report for a managed runner."""
    token = uuid.uuid4().hex
    prefix = 'etl-real-network-' + token[:12]
    runtime = ROOT / '.runtime' / ('real-network-' + token[:12])
    runtime.mkdir(parents=True, exist_ok=False)
    destination = ROOT / 'artifacts' / 'issue-13' / 'real-network-isolation.json'
    if destination.exists():
        # Preserve every earlier public observation, including unknown outcomes.
        destination = runtime / 'network-proof.json'
    destination.parent.mkdir(parents=True, exist_ok=True)
    label = LABEL + '=' + token
    network = prefix + '-internal'
    names = {role: prefix + '-' + role for role in ('engine', 'gateway', 'source', 'guardian', 'probe', 'faultproxy')}
    tools = prefix + '-tools:local'
    resources = []
    report = {'schema': 'real-network-isolation/v1', 'level': 'network_isolation_probe',
              'managed_l2_accepted': False, 'created_at': datetime.now(timezone.utc).isoformat(),
              'run_token': token, 'label': label, 'runtime_directory': str(runtime.relative_to(ROOT)),
              'phase': 'setup_intent', 'keep_requested': keep, 'resources': resources}
    def persist():
        destination.write_text(json.dumps(report, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
        (runtime / 'receipt.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    def run_helper(role, mode, *args, netns=None, privileged=False):
        name = names[role]
        command = ['run', '--name', name, '--label', label, '--network', netns or 'container:' + names['engine'],
                   '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true']
        if privileged: command += ['--cap-add', 'NET_ADMIN']
        command += ['--mount', 'type=bind,source=' + str(runtime / 'helper.py') + ',target=/proof.py,readonly',
                    '--entrypoint', '/venv/bin/python3', tools, '/proof.py', mode, *args]
        # Register before create: cleanup must also cover an interrupted docker run.
        resources.insert(0, ('container', name))
        output = json.loads(docker(*command))
        docker('rm', name)
        resources.remove(('container', name))
        return output
    def service(role, mode, args=()):
        name = names[role]
        resources.insert(0, ('container', name))
        docker('run', '-d', '--name', name, '--label', label, '--network', network, '--ip', ips[role],
               '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '--mount',
               'type=bind,source=' + str(runtime / 'helper.py') + ',target=/proof.py,readonly',
               '--entrypoint', '/venv/bin/python3', tools, '/proof.py', mode, *args)
        docker('network', 'connect', 'etl-agent_default', name)
    persist()
    try:
        # Verify ports requested by the task are free; this proof publishes none.
        for port in (18082, 18083):
            with socket.socket() as sock: sock.bind(('127.0.0.1', port))
        existing = [network_info(n) for n in docker('network', 'ls', '--format', '{{.Name}}').splitlines()]
        occupied = [ipaddress.ip_network(c['Subnet']) for n in existing for c in (n['ipam'] or []) if c.get('Subnet')]
        subnet = next(ipaddress.ip_network('10.203.' + str(i) + '.0/24') for i in range(1, 255)
                      if all(not ipaddress.ip_network('10.203.' + str(i) + '.0/24').overlaps(c) for c in occupied))
        ips = {role: str(subnet.network_address + suffix) for role, suffix in (('engine', 2), ('gateway', 3), ('source', 4))}
        external = {}
        for role, container in (('mysql', 'etl-connector-mysql'), ('doris', 'etl-connector-doris'),
                                ('postgres', 'etl-connector-postgres'), ('redis', 'etl-agent-redis-1')):
            nets = inspect(container, '{{json .NetworkSettings.Networks}}')
            external[role] = {name: value['IPAddress'] for name, value in nets.items()}
        report['external_endpoint_addresses'] = external
        (runtime / 'helper.py').write_text(HELPER, encoding='utf-8')
        if docker('image', 'inspect', '--format', '{{.Id}}', TOOLS_BASE) != TOOLS_BASE_ID:
            raise RuntimeError('tools_base_image_mismatch')
        dockerfile = runtime / 'Dockerfile'
        dockerfile.write_text('FROM ' + TOOLS_BASE + '\nLABEL ' + label + '\nRUN apk add --no-cache iptables iproute2\n', encoding='utf-8')
        report['phase'] = 'tools_build_intent'; persist()
        resources.append(('image', tools))
        docker('build', '--label', label, '-t', tools, str(runtime))
        image_id = docker('image', 'inspect', '--format', '{{.Id}}', tools)
        distribution = runtime / 'seatunnel'; distribution.mkdir()
        for directory in ('bin', 'lib', 'starter', 'plugins', 'connectors'):
            docker('cp', 'etl-seatunnel-probe:/opt/seatunnel/' + directory, str(distribution / directory))
        config = distribution / 'config'; config.mkdir()
        for filename in CONFIG_FILES:
            docker('cp', 'etl-seatunnel-probe:/opt/seatunnel/config/' + filename, str(config / filename))
        if response_loss:
            path = config / 'seatunnel.yaml'
            original = path.read_text(encoding='utf-8')
            if original.count('port: 8080') != 1:
                raise RuntimeError('fault_rest_port_configuration_unknown')
            path.write_text(original.replace('port: 8080', 'port: 8081'), encoding='utf-8')
        for filename in ('hazelcast.yaml', 'hazelcast-master.yaml', 'hazelcast-worker.yaml', 'hazelcast-client.yaml'):
            path = config / filename
            path.write_text(re.sub(r'(?m)^(\s*cluster-name:)\s*.*$', r'\1 ' + prefix,
                                   path.read_text(encoding='utf-8')), encoding='utf-8')
        jars = list((ROOT / '.runtime').glob('etl-http-ingress-*/seatunnel/connectors/connector-http-base-2.3.12.jar'))
        if not jars: raise RuntimeError('pinned_http_connector_missing')
        jar = next((p for p in jars if hashlib.sha256(p.read_bytes()).hexdigest() == HTTP_JAR_SHA256), None)
        if jar is None: raise RuntimeError('pinned_http_connector_mismatch')
        shutil.copyfile(jar, distribution / 'connectors' / jar.name)
        report['phase'] = 'network_create_intent'; persist()
        docker('network', 'create', '--internal', '--subnet', str(subnet), '--label', label, network)
        resources.insert(len(resources)-1, ('network', network))
        service('source', 'relay', (external['mysql']['etl-agent_default'] + ':3306',))
        service('gateway', 'gateway')
        gate = runtime / 'control'; gate.mkdir()
        resources.insert(0, ('container', names['engine']))
        docker('run', '-d', '--name', names['engine'], '--label', label, '--network', network,
               '--ip', ips['engine'], '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
               '-e', 'JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8',
               '--mount', 'type=bind,source=' + str(distribution) + ',target=/opt/seatunnel',
               '--mount', 'type=bind,source=' + str(gate) + ',target=/proof-control,readonly',
               '--entrypoint', 'bash', ENGINE_IMAGE, '-lc',
               'while [ ! -f /proof-control/start ]; do sleep 0.2; done; export PATH=/opt/java/bin:$PATH; '
               'cd /opt/seatunnel; exec bash bin/seatunnel-cluster.sh')
        report['phase'] = 'namespace_firewall_intent'; persist()
        firewall = run_helper('guardian', 'firewall', ips['source'], ips['gateway'],
                              'response-loss' if response_loss else 'none', privileged=True)
        report['firewall'] = firewall
        # No NET_ADMIN process remains when Java starts.
        (gate / 'start').write_text('start after firewall guardian removed\n', encoding='utf-8')
        engine_netns = docker('exec', names['engine'], 'readlink', '/proc/self/ns/net')
        if engine_netns != firewall['netns']: raise RuntimeError('firewall_namespace_mismatch')
        if response_loss:
            # A credential-free sidecar shares only the original engine netns.
            # It cannot create routes or bypass the pinned OUTPUT firewall.
            resources.insert(0, ('container', names['faultproxy']))
            docker('run', '-d', '--name', names['faultproxy'], '--label', label,
                '--network', 'container:' + names['engine'], '--cap-drop', 'ALL',
                '--security-opt', 'no-new-privileges:true', '--mount',
                'type=bind,source=' + str(runtime / 'helper.py') + ',target=/proof.py,readonly',
                '--entrypoint', '/venv/bin/python3', tools, '/proof.py', 'response-loss-proxy')
            report['fault_injection'] = {'kind': 'real_submit_response_loss',
                'proxy_container': names['faultproxy'], 'public_port': 8080,
                'actual_rest_port': 8081, 'request_body_recorded': False}
        settings = subprocess.run(['docker', 'exec', names['engine'], '/opt/java/bin/java',
            '-XshowSettings:properties', '-version'], capture_output=True, text=True, encoding='utf-8')
        encoding_match = re.search(r'(?m)^\s*file.encoding\s*=\s*(\S+)\s*$', settings.stderr)
        if settings.returncode or not encoding_match or encoding_match[1] != 'UTF-8':
            raise RuntimeError('engine_java_utf8_unverified')
        report['engine_jvm_charset'] = {'file_encoding': encoding_match[1],
            'java_tool_options': '-Dfile.encoding=UTF-8', 'actual_java_settings_verified': True}
        base = 'http://' + ips['engine'] + ':8080/overview'
        report['phase'] = 'engine_start_observation'; persist()
        deadline = time.monotonic() + 120
        while True:
            try:
                version = json.loads(docker('exec', names['gateway'], '/venv/bin/python3', '/proof.py', 'http', base))
                if version.get('projectVersion') != '2.3.12': raise RuntimeError('engine_version_mismatch')
                report['engine_overview_from_gateway'] = version; break
            except RuntimeError:
                if time.monotonic() > deadline: raise RuntimeError('engine_rest_unavailable') from None
                time.sleep(2)
        report['gateway_ingress_http'] = run_helper('probe', 'http', 'http://' + ips['gateway'] + ':8001/')
        host = json.loads(docker('exec', names['gateway'], '/venv/bin/python3', '/proof.py', 'hostip'))['host']
        targets = [{'id': 'source_mysql', 'host': ips['source'], 'port': 3306, 'expected': 'allow'},
                   {'id': 'ingress_http', 'host': ips['gateway'], 'port': 8001, 'expected': 'allow'},
                   {'id': 'source_other_port', 'host': ips['source'], 'port': 3307, 'expected': 'deny'},
                   {'id': 'gateway_other_port', 'host': ips['gateway'], 'port': 8002, 'expected': 'deny'},
                   {'id': 'external_https', 'host': '1.1.1.1', 'port': 443, 'expected': 'deny'},
                   {'id': 'embedded_dns_tcp', 'host': '127.0.0.11', 'port': 53, 'expected': 'deny'}]
        for net, ip in external['doris'].items():
            for port in (8030, 8040, 9030): targets.append({'id': 'doris_' + net + '_' + str(port), 'host': ip, 'port': port, 'expected': 'deny'})
        app_gateway = next(c['Gateway'] for c in network_info('etl-agent_default')['ipam'] if c.get('Gateway'))
        for label_name, address in (('host_docker_internal', host), ('internal_bridge_gateway', str(subnet.network_address + 1)),
                                    ('connector_bridge_gateway', app_gateway)):
            for port in (19030, 18040, 18030):
                targets.append({'id': label_name + '_' + str(port), 'host': address, 'port': port, 'expected': 'deny'})
        for role, port in (('postgres', 5432), ('redis', 6379)):
            targets.append({'id': 'direct_' + role, 'host': external[role]['etl-agent_default'], 'port': port, 'expected': 'deny'})
        if response_loss:
            blocked = json.loads(docker('exec', names['gateway'], '/venv/bin/python3', '/proof.py', 'probe',
                json.dumps([{'id': 'unproxied_actual_rest', 'host': ips['engine'], 'port': 8081}])))
            if blocked['checks'][0]['connected']:
                raise RuntimeError('fault_actual_rest_exposed')
            report['fault_injection']['direct_rest_from_gateway_denied'] = True
        report['engine_probe'] = run_helper('probe', 'probe', json.dumps(targets))
        controls = [{'id': role, 'host': external[role]['etl-agent_default'], 'port': port}
                    for role, port in (('mysql', 3306), ('doris', 9030), ('doris', 8040), ('postgres', 5432), ('redis', 6379))]
        controls += [{'id': 'host_published_' + str(port), 'host': host, 'port': port} for port in (19030, 18040)]
        report['gateway_control_probe'] = json.loads(docker('exec', names['gateway'], '/venv/bin/python3', '/proof.py', 'probe', json.dumps(controls)))
        security = {}
        for role in ('engine', 'gateway', 'source'):
            security[role] = inspect(names[role], '{"id":{{json .Id}},"image":{{json .Image}},"started_at":{{json .State.StartedAt}},"restart_count":{{json .RestartCount}},"cap_add":{{json .HostConfig.CapAdd}},"cap_drop":{{json .HostConfig.CapDrop}},"privileged":{{json .HostConfig.Privileged}},"security_opt":{{json .HostConfig.SecurityOpt}},"mounts":[{{range $i,$v := .Mounts}}{{if $i}},{{end}}{"Destination":{{json $v.Destination}},"RW":{{$v.RW}},"Source":{{json $v.Source}},"Type":{{json $v.Type}}}{{end}}],"networks":{{json .NetworkSettings.Networks}}}')
            expected_mounts = {'/opt/seatunnel': 'seatunnel', '/proof-control': 'control'} if role == 'engine' else {'/proof.py': 'helper.py'}
            observed_mounts = security[role]['mounts']
            if {m['Destination'] for m in observed_mounts} != set(expected_mounts):
                raise RuntimeError('unexpected_container_mount')
            for mount in observed_mounts:
                suffix = '/' + runtime.name + '/' + expected_mounts[mount['Destination']]
                if mount['Type'] != 'bind' or not mount['Source'].replace('\\', '/').endswith(suffix):
                    raise RuntimeError('mount_outside_own_runtime')
            # Preserve validated source scope, destinations and modes; omit host paths.
            security[role]['mounts'] = [{'destination': m['Destination'], 'rw': m['RW'],
                                        'source_scope': 'own_run_runtime'} for m in observed_mounts]
        checks = report['engine_probe']['checks']
        if not all(c['connected'] == (c['expected'] == 'allow') for c in checks): raise RuntimeError('network_policy_observation_mismatch')
        if not all(c['connected'] for c in report['gateway_control_probe']['checks'] if not c['id'].startswith('host_published_')):
            raise RuntimeError('gateway_connector_control_unavailable')
        if report['engine_probe']['netns'] != engine_netns: raise RuntimeError('probe_namespace_mismatch')
        for proof in security.values():
            if proof['privileged'] or proof['cap_add'] or 'ALL' not in proof['cap_drop'] or 'no-new-privileges:true' not in proof['security_opt']:
                raise RuntimeError('container_security_mismatch')
        receipt = {'schema': 'managed-network-receipt/v1', 'run_token': token, 'label': label,
                   'network': {'name': network, **network_info(network)}, 'subnet': str(subnet),
                   'engine': {'name': names['engine'], 'ip': ips['engine'], 'rest_port': 8080, 'netns': engine_netns,
                               'file_encoding': report['engine_jvm_charset']['file_encoding']},
                   'gateway': {'name': names['gateway'], 'ip': ips['gateway'], 'ingress_port': 8001,
                               'external_network': 'etl-agent_default', 'external_aliases': external,
                               'application_mount_for_l2_runner': '/app/app'},
                   'source_relay': {'name': names['source'], 'ip': ips['source'], 'port': 3306,
                                    'upstream': external['mysql']['etl-agent_default'] + ':3306', 'credentials': 'none'},
                   'tools_image_id': image_id, 'engine_image_id': security['engine']['image'],
                   'http_connector_sha256': HTTP_JAR_SHA256, 'helper_sha256': hashlib.sha256(HELPER.encode()).hexdigest(),
                   'firewall_rules_sha256': firewall['rules_sha256'], 'container_security': security,
                   'observations_sha256': digest({'engine': report['engine_probe'], 'gateway': report['gateway_control_probe'],
                                                 'rest': report['engine_overview_from_gateway'], 'ingress': report['gateway_ingress_http']}),
                   'l2_runner_contract': 'Revalidate current Docker IDs, IPs, rules and capabilities before use; bind receipt digest to a newly registered resource manifest. Gateway probe HTTP is not real ingress. No Doris backend identity or grants verified.'}
        report['network_receipt'] = receipt
        report['network_receipt_sha256'] = digest(receipt)
        report['phase'] = 'isolated_network_observed'
    except Exception as error:
        report['failed_phase'] = report['phase']; report['phase'] = 'unknown'
        report['error_type'] = type(error).__name__
        report['error_code'] = str(error) if re.fullmatch(r'[a-z0-9_]+', str(error)) else 'redacted'
    finally:
        if keep and report['phase'] == 'isolated_network_observed':
            report['resource_state'] = 'kept'
        else:
            report['cleanup'] = cleanup(resources, token)
            report['resource_state'] = 'removed' if all(x['outcome'] in ('removed', 'absent') for x in report['cleanup']) else 'cleanup_unknown'
        persist_binding_receipt(report, runtime)
        persist()
    report['artifact'] = str(destination.relative_to(ROOT))
    persist()
    return report


def main(keep=False):
    report = create_isolation(keep=keep)
    print(json.dumps({key: report.get(key) for key in ('artifact', 'phase', 'resource_state',
                      'network_receipt_file', 'network_receipt_sha256', 'managed_l2_accepted')}))
    return 0 if report['phase'] == 'isolated_network_observed' and report['resource_state'] != 'cleanup_unknown' else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keep', action='store_true')
    parser.add_argument('--cleanup', type=Path, help='Use the run wrapper receipt.json (not network-receipt.json); remove exact-label resources and invalidate the canonical binding')
    args = parser.parse_args()
    if args.cleanup:
        value = json.loads(args.cleanup.read_text(encoding='utf-8'))
        runtime = ROOT / value['runtime_directory']
        if runtime.resolve().parent != (ROOT / '.runtime').resolve() or runtime.name != 'real-network-' + value['run_token'][:12]:
            raise RuntimeError('receipt_runtime_scope_mismatch')
        outcomes = cleanup(value['resources'], value['run_token'])
        value['cleanup'] = outcomes
        value['resource_state'] = 'removed' if all(x['outcome'] in ('removed', 'absent') for x in outcomes) else 'cleanup_unknown'
        persist_binding_receipt(value, runtime)
        text = json.dumps(value, indent=2) + '\n'
        args.cleanup.write_text(text, encoding='utf-8')
        (runtime / 'receipt.json').write_text(text, encoding='utf-8')
        print(json.dumps(outcomes))
        raise SystemExit(0 if value['resource_state'] == 'removed' else 1)
    else:
        raise SystemExit(main(args.keep))
