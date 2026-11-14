"""Live network isolation evidence for the real adapter.

The measured isolation proof (managed-network-receipt/v1) is produced by the
trusted host probe. A kept deployment can drift afterwards: a restart, a
capability change or a recreated container would silently move the data plane
back onto an unverified network. This module therefore validates the frozen
receipt bytes and then requires a *live* observation recorded no more than
LIVE_MAX_AGE_SECONDS ago.

Two trust domains share this validation:
  * the trusted host observer reads Docker and writes the witness file;
  * the managed worker only reads that file and never talks to Docker.
Neither domain may mark a route qualified on its own.
"""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess

WITNESS_SCHEMA = 'managed-network-witness/v1'
LIVE_MAX_AGE_SECONDS = 2
_MAX_RECEIPT_BYTES = 1048576
_LIVE_KEYS = ('container_id', 'image', 'started_at', 'restart_count', 'cap_add', 'cap_drop', 'privileged',
              'security_opt', 'addresses', 'network_endpoints')
_ENDPOINT_FIELDS = ('NetworkID', 'EndpointID', 'IPAddress', 'Gateway', 'GlobalIPv6Address', 'IPv6Gateway',
                    'IPPrefixLen', 'GlobalIPv6PrefixLen')
# The receipt names the relay container 'source_relay'; the witness keeps the
# short role name shared with container_security.
_ROLE_KEY = {'engine': 'engine', 'gateway': 'gateway', 'source': 'source_relay'}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def _sha256(value):
    import hashlib
    return hashlib.sha256(value).hexdigest()


def _require(condition, code):
    if not condition:
        raise ValueError(code)


def _digest_field(value, code):
    _require(isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value), code)
    return value


def _network_endpoints(networks):
    """Preserve network names and endpoint identity, not just reusable IPs."""
    _require(isinstance(networks, dict) and bool(networks), 'network_endpoints_invalid')
    endpoints = {}
    for name, endpoint in networks.items():
        _require(isinstance(name, str) and bool(name) and isinstance(endpoint, dict)
                 and all(field in endpoint for field in _ENDPOINT_FIELDS), 'network_endpoint_incomplete')
        _require(all(isinstance(endpoint[field], str) for field in _ENDPOINT_FIELDS[:6])
                 and endpoint['NetworkID'] and endpoint['EndpointID']
                 and type(endpoint['IPPrefixLen']) is int and 0 <= endpoint['IPPrefixLen'] <= 32
                 and type(endpoint['GlobalIPv6PrefixLen']) is int
                 and 0 <= endpoint['GlobalIPv6PrefixLen'] <= 128, 'network_endpoint_invalid')
        endpoints[name] = {field: endpoint[field] for field in _ENDPOINT_FIELDS}
    return endpoints


def _network_topology(network):
    _require(isinstance(network, dict) and set(network) == {'name', 'id', 'internal', 'ipam'}
             and isinstance(network['name'], str) and bool(network['name'])
             and isinstance(network['id'], str) and bool(network['id'])
             and network['internal'] is True and isinstance(network['ipam'], list)
             and bool(network['ipam']) and all(isinstance(config, dict) for config in network['ipam']),
             'network_topology_invalid')
    return network


def _read_receipt(raw):
    """Parse and structurally validate one frozen isolation receipt."""
    _require(isinstance(raw, (bytes, bytearray)) and 0 < len(raw) <= _MAX_RECEIPT_BYTES, 'network_receipt_invalid')
    try:
        receipt = json.loads(raw)
    except (UnicodeDecodeError, ValueError):
        raise ValueError('network_receipt_unparsable') from None
    _require(isinstance(receipt, dict) and receipt.get('schema') == 'managed-network-receipt/v1'
             and receipt.get('resource_state') == 'kept'
             and receipt.get('verification_phase') == 'isolated_network_observed',
             'network_receipt_not_live')
    for role, port_key in (('engine', 'rest_port'), ('gateway', 'ingress_port')):
        facts = receipt.get(role)
        _require(isinstance(facts, dict) and isinstance(facts.get('name'), str) and facts['name']
                 and isinstance(facts.get('ip'), str) and facts['ip']
                 and type(facts.get(port_key)) is int and 1 <= facts[port_key] <= 65535,
                 'network_receipt_' + role + '_invalid')
    _require(receipt['engine'].get('file_encoding') == 'UTF-8', 'network_receipt_engine_charset_invalid')
    relay = receipt.get('source_relay')
    _require(isinstance(relay, dict) and relay.get('credentials') == 'none'
             and type(relay.get('port')) is int and relay['port'] == 3306, 'network_receipt_relay_invalid')
    _digest_field(receipt.get('http_connector_sha256'), 'network_receipt_connector_invalid')
    _digest_field(receipt.get('firewall_rules_sha256'), 'network_receipt_firewall_invalid')
    _digest_field(receipt.get('observations_sha256'), 'network_receipt_observations_invalid')
    for key in ('tools_image_id', 'engine_image_id', 'helper_sha256', 'network'):
        _require(bool(receipt.get(key)), 'network_receipt_' + key + '_invalid')
    security = receipt.get('container_security')
    _require(isinstance(security, dict) and set(security) == {'engine', 'gateway', 'source'},
             'network_receipt_security_invalid')
    topology = _network_topology(receipt['network'])
    for role, proof in security.items():
        _require(isinstance(proof, dict) and proof.get('privileged') is False and not proof.get('cap_add')
                 and isinstance(proof.get('cap_drop'), list) and 'ALL' in proof['cap_drop']
                 and isinstance(proof.get('security_opt'), list)
                 and 'no-new-privileges:true' in proof['security_opt'], 'network_receipt_capability_invalid')
        _require(all(isinstance(proof.get(key), str) and proof[key] for key in ('id', 'image', 'started_at'))
                 and type(proof.get('restart_count')) is int and proof['restart_count'] >= 0
                 and isinstance(proof.get('networks'), dict) and proof['networks'],
                 'network_receipt_original_container_missing')
        endpoints = _network_endpoints(proof['networks'])
        _require(topology['name'] in endpoints
                 and endpoints[topology['name']]['NetworkID'] == topology['id']
                 and endpoints[topology['name']]['IPAddress'] == receipt[_ROLE_KEY[role]]['ip'],
                 'network_receipt_endpoint_binding_invalid')
        if role == 'engine':
            _require(set(endpoints) == {topology['name']}, 'network_receipt_engine_network_invalid')
    return receipt


def static_receipt(entry, raw):
    """Bind one manifest entry to its frozen receipt bytes."""
    _require(isinstance(entry, dict), 'deployment_resource_mismatch')
    _require(_sha256(raw) == entry.get('network_receipt_sha256'), 'network_receipt_digest_mismatch')
    receipt = _read_receipt(raw)
    from .real_engine import HTTP_CONNECTOR
    _require(receipt.get('http_connector_sha256') == HTTP_CONNECTOR['sha256'], 'network_receipt_connector_mismatch')
    return receipt


def _docker_json(arguments, code):
    result = subprocess.run(['docker', *arguments], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, encoding='utf-8', timeout=10)
    _require(result.returncode == 0, code)
    return result.stdout


def _inspect(receipt):
    # Emit only the result of an exact safe option match, never Config.Env:
    # other environment entries may contain credentials.
    template = ('{{$utf8 := false}}{{range .Config.Env}}'
                '{{if eq . "JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8"}}{{$utf8 = true}}{{end}}{{end}}'
                '{"java_utf8":{{$utf8}},"id":{{json .Id}},"image":{{json .Image}},'
                '"started_at":{{json .State.StartedAt}},'
                '"restart_count":{{json .RestartCount}},"cap_add":{{json .HostConfig.CapAdd}},'
                '"cap_drop":{{json .HostConfig.CapDrop}},"privileged":{{json .HostConfig.Privileged}},'
                '"security_opt":{{json .HostConfig.SecurityOpt}},'
                '"networks":{{json .NetworkSettings.Networks}}}')
    raw = _docker_json(['inspect', '--format', template,
        *[receipt[_ROLE_KEY[role]]['name'] for role in _ROLE_KEY]], 'network_live_inspect_failed')
    rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    _require(len(rows) == len(_ROLE_KEY), 'network_live_scope_invalid')
    return {role: {'container_id': row['id'], 'image': row['image'], 'started_at': row['started_at'],
        'restart_count': row['restart_count'], 'cap_add': row['cap_add'], 'cap_drop': row['cap_drop'],
        'privileged': row['privileged'], 'security_opt': row['security_opt'],
        'addresses': sorted({v['IPAddress'] for v in row['networks'].values() if v.get('IPAddress')}),
        'network_endpoints': _network_endpoints(row['networks']),
        **({'java_utf8': row.get('java_utf8')} if role == 'engine' else {})}
        for role, row in zip(_ROLE_KEY, rows)}


def _inspect_topology(receipt):
    template = ('{"name":{{json .Name}},"id":{{json .Id}},"internal":{{json .Internal}},'
                '"ipam":{{json .IPAM.Config}}}')
    raw = _docker_json(['network', 'inspect', '--format', template, receipt['network']['name']],
                       'network_live_topology_unavailable')
    return _network_topology(json.loads(raw))


def _firewall_snapshot(receipt):
    # The trusted host uses an ephemeral reader in the ORIGINAL namespace. It
    # receives no mounts, credentials or Docker socket and changes no rules.
    script = ('import hashlib,json,os,re,subprocess; '
        'parts=[subprocess.check_output([p],text=True) for p in ("iptables-save","ip6tables-save")]; '
        'normalize=lambda raw: "\\n".join(re.sub(r"\\[\\d+:\\d+\\]","[0:0]",line) '
        'for line in raw.splitlines() if not line.startswith("#"))+"\\n"; '
        'print(json.dumps({"netns":os.readlink("/proc/self/ns/net"),'
        '"rules_sha256":hashlib.sha256("".join(map(normalize,parts)).encode()).hexdigest()}))')
    from uuid import uuid4
    name = 'etl-real-witness-' + uuid4().hex
    try:
        raw = _docker_json(['run', '--rm', '--name', name, '--label', receipt['label'], '--network',
            'container:' + receipt['container_security']['engine']['id'], '--cap-drop', 'ALL',
            '--cap-add', 'NET_ADMIN', '--security-opt', 'no-new-privileges:true', '--entrypoint',
            '/venv/bin/python3', receipt['tools_image_id'], '-c', script], 'network_live_firewall_unavailable')
        return json.loads(raw)
    finally:
        # A Docker client timeout must not leave a namespace reader with NET_ADMIN.
        subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=10)


def observe_containers(receipt):
    """Observe original containers and their current namespace firewall in parallel."""
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as pool:
        inspected = pool.submit(_inspect, receipt)
        firewall = pool.submit(_firewall_snapshot, receipt)
        topology = pool.submit(_inspect_topology, receipt)
        live = inspected.result()
        live['engine']['firewall'] = firewall.result()
        live['engine']['topology'] = topology.result()
    return live


def _validate_live(receipt, live):
    _require(isinstance(live, dict) and set(live) == set(_ROLE_KEY), 'network_live_scope_invalid')
    for role, proof in live.items():
        expected_keys = set(_LIVE_KEYS) | ({'firewall', 'topology', 'java_utf8'} if role == 'engine' else set())
        _require(isinstance(proof, dict) and set(proof) == expected_keys, 'network_live_facts_invalid')
        pinned = receipt['container_security'][role]
        _require(proof['privileged'] is False and not proof['cap_add'] and 'ALL' in proof['cap_drop']
                 and 'no-new-privileges:true' in proof['security_opt'], 'network_live_capability_drift')
        _require(proof['container_id'] == pinned.get('id') and proof['image'] == pinned.get('image')
                 and proof['started_at'] == pinned.get('started_at')
                 and proof['restart_count'] == pinned.get('restart_count'), 'network_live_restart_drift')
        addresses = sorted({v['IPAddress'] for v in pinned['networks'].values() if v.get('IPAddress')})
        _require(proof['addresses'] == addresses and receipt[_ROLE_KEY[role]]['ip'] in addresses,
                 'network_live_ip_drift')
        _require(_network_endpoints(proof['network_endpoints']) == _network_endpoints(pinned['networks']),
                 'network_live_endpoint_drift')
    _require(receipt['engine'].get('file_encoding') == 'UTF-8' and live['engine']['java_utf8'] is True,
             'network_live_engine_charset_unverified')
    _require(_network_topology(live['engine']['topology']) == receipt['network'],
             'network_live_topology_drift')
    firewall = live['engine']['firewall']
    _require(isinstance(firewall, dict) and firewall.get('netns') == receipt['engine']['netns']
             and firewall.get('rules_sha256') == receipt['firewall_rules_sha256'], 'network_live_firewall_drift')


def witness_record(receipt, live, anchor, now):
    """Both the measured receipt and restart anchor must match this observation."""
    _validate_live(receipt, live)
    for role, proof in live.items():
        pinned = anchor[role]
        _require(pinned['container_id'] == proof['container_id'] and pinned['started_at'] == proof['started_at']
                 and pinned['restart_count'] == proof['restart_count'], 'network_live_restart_drift')
    return {'schema': WITNESS_SCHEMA, 'receipt_sha256': _sha256(canonical(receipt)),
            'observed_at': now.astimezone(timezone.utc).isoformat(), 'live': live}


def read_witness(path, receipt, now=None):
    """Validate a witness file written by the trusted host observer."""
    _require(bool(path), 'network_witness_missing')
    now = now or datetime.now(timezone.utc)
    try:
        raw = Path(path).read_bytes()
    except OSError:
        raise ValueError('network_witness_unreadable') from None
    _require(0 < len(raw) <= 65536, 'network_witness_invalid')
    try:
        record = json.loads(raw)
    except (UnicodeDecodeError, ValueError):
        raise ValueError('network_witness_unparsable') from None
    _require(isinstance(record, dict) and record.get('schema') == WITNESS_SCHEMA
             and record.get('receipt_sha256') == _sha256(canonical(receipt)), 'network_witness_binding_mismatch')
    try:
        observed = datetime.fromisoformat(record['observed_at'])
    except (KeyError, TypeError, ValueError):
        raise ValueError('network_witness_timestamp_invalid') from None
    if observed.tzinfo is None:
        raise ValueError('network_witness_timestamp_naive')
    observed, now = observed.astimezone(timezone.utc), now.astimezone(timezone.utc)
    _require(observed <= now, 'network_witness_from_future')
    _require(now - observed <= timedelta(seconds=LIVE_MAX_AGE_SECONDS), 'network_witness_stale')
    _validate_live(receipt, record.get('live'))
    return record


def verify_live_network(entry, raw, *, now=None, witness_file=None):
    """Frozen receipt plus one fresh live observation; anything unknown denies."""
    receipt = static_receipt(entry, raw)
    read_witness(witness_file if witness_file is not None else os.environ.get('REAL_NETWORK_LIVE_WITNESS_FILE'),
                 receipt, now or datetime.now(timezone.utc))
    return True
