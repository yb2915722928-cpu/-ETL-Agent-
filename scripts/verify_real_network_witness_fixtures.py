"""Offline checks for the real adapter's live network witness gate.

Not an L2 result: no Docker, no engine, no data plane. These checks prove the
decision logic of backend/app/real_network.py — a stale, drifted or rebound
witness can never qualify a route, and only a fresh matching one can.
"""
import copy
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))

from unittest.mock import patch
from app.real_network import (LIVE_MAX_AGE_SECONDS, canonical, read_witness, static_receipt,  # noqa: E402
                              observe_containers, verify_live_network, witness_record)

RECEIPT = {
    'schema': 'managed-network-receipt/v1', 'run_token': 'a' * 32, 'label': 'etl-real-network-probe',
    'resource_state': 'kept', 'verification_phase': 'isolated_network_observed',
    'network': {'name': 'etl-real-network-internal', 'id': 'internal-network-id', 'internal': True,
                'ipam': [{'Subnet': '10.203.1.0/24', 'Gateway': '10.203.1.1'}]},
    'engine': {'name': 'etl-real-network-engine', 'ip': '10.203.1.2', 'rest_port': 8080,
               'netns': 'netns:[4026531999]', 'file_encoding': 'UTF-8'},
    'gateway': {'name': 'etl-real-network-gateway', 'ip': '10.203.1.3', 'ingress_port': 8001,
                'external_network': 'etl-agent_default'},
    'source_relay': {'name': 'etl-real-network-source', 'ip': '10.203.1.4', 'port': 3306,
                     'upstream': 'connector-mysql:3306', 'credentials': 'none'},
    'tools_image_id': 'sha256:' + 'b' * 64, 'engine_image_id': 'sha256:' + 'c' * 64,
    'http_connector_sha256': 'a2a8ab002697dea5f070a8c77edd44f5c53da8ac8cb3ff26b71ccefcefac3ca9',
    'helper_sha256': 'e' * 64,
    'firewall_rules_sha256': 'f' * 64, 'observations_sha256': '0' * 64,
    'container_security': {role: {'privileged': False, 'cap_add': None, 'cap_drop': ['ALL'],
                                  'security_opt': ['no-new-privileges:true']}
                           for role in ('engine', 'gateway', 'source')},
}
LIVE = {
    'engine': {'container_id': 'engine-container-id', 'started_at': '2026-11-14T08:00:00.000000000Z',
               'restart_count': 0, 'cap_add': None, 'cap_drop': ['ALL'], 'privileged': False,
               'security_opt': ['no-new-privileges:true'], 'addresses': ['10.203.1.2']},
    'gateway': {'container_id': 'gateway-container-id', 'started_at': '2026-11-14T08:00:01.000000000Z',
                'restart_count': 0, 'cap_add': None, 'cap_drop': ['ALL'], 'privileged': False,
                'security_opt': ['no-new-privileges:true'], 'addresses': ['10.203.1.3', '172.19.0.10']},
    'source': {'container_id': 'source-container-id', 'started_at': '2026-11-14T08:00:02.000000000Z',
               'restart_count': 0, 'cap_add': None, 'cap_drop': ['ALL'], 'privileged': False,
               'security_opt': ['no-new-privileges:true'], 'addresses': ['10.203.1.4', '172.19.0.9']},
}
def endpoint(network_id, endpoint_id, address, gateway, prefix=24):
    return {'NetworkID': network_id, 'EndpointID': endpoint_id, 'IPAddress': address, 'Gateway': gateway,
            'GlobalIPv6Address': '', 'IPv6Gateway': '', 'IPPrefixLen': prefix, 'GlobalIPv6PrefixLen': 0}


for role, proof in LIVE.items():
    proof['image'] = RECEIPT['engine_image_id'] if role == 'engine' else RECEIPT['tools_image_id']
    networks = {'etl-real-network-internal': endpoint('internal-network-id', role + '-internal-endpoint',
                                                    proof['addresses'][0], '10.203.1.1')}
    if role != 'engine':
        networks['etl-agent_default'] = endpoint('external-network-id', role + '-external-endpoint',
                                                 proof['addresses'][1], '172.19.0.1', 16)
    RECEIPT['container_security'][role].update({
        'id': proof['container_id'], 'image': proof['image'], 'started_at': proof['started_at'],
        'restart_count': proof['restart_count'], 'networks': copy.deepcopy(networks)})
    proof['network_endpoints'] = networks
LIVE['engine']['firewall'] = {'netns': RECEIPT['engine']['netns'],
                              'rules_sha256': RECEIPT['firewall_rules_sha256']}
LIVE['engine']['topology'] = copy.deepcopy(RECEIPT['network'])
LIVE['engine']['java_utf8'] = True
ANCHOR = {role: {key: proof[key] for key in ('container_id', 'started_at', 'restart_count')}
          for role, proof in LIVE.items()}
NOW = datetime(2026, 11, 14, 9, 0, 0, tzinfo=timezone.utc)


def record_witness(witness, raw, live, anchor, recorded_at):
    try:
        record = witness_record(json.loads(raw), live, anchor, recorded_at)
    except ValueError:
        return False
    Path(witness).write_bytes(canonical(record))
    return True


def qualified_at(witness, raw, entry, now):
    try:
        return verify_live_network(entry, raw, now=now, witness_file=witness)
    except (ValueError, OSError):
        return False


def qualified(raw, live, anchor, now, witness_path, entry=None):
    if entry is None:
        import hashlib
        entry = {'network_receipt_sha256': hashlib.sha256(raw).hexdigest()}
    if not record_witness(witness_path, raw, live, anchor, now):
        return False  # The observer itself refuses to attest drifting containers.
    return qualified_at(witness_path, raw, entry, now)


def main(tmp=None):
    from tempfile import TemporaryDirectory
    raw = canonical(RECEIPT)
    entry = {'network_receipt_sha256': __import__('hashlib').sha256(raw).hexdigest()}
    with TemporaryDirectory() as folder:
        witness = str(Path(folder) / 'witness.json')
        assert qualified(raw, LIVE, ANCHOR, NOW, witness) is True, 'fresh witness must qualify'
        assert qualified(raw, LIVE, ANCHOR, NOW + timedelta(seconds=LIVE_MAX_AGE_SECONDS), witness) is True
        record_witness(witness, raw, LIVE, ANCHOR, NOW)
        assert qualified_at(witness, raw, entry, NOW + timedelta(seconds=LIVE_MAX_AGE_SECONDS)) is True
        assert qualified_at(witness, raw, entry, NOW + timedelta(seconds=LIVE_MAX_AGE_SECONDS + 1)) is False, \
            'a witness older than the staleness bound must deny'
        assert qualified_at(witness, raw, entry, NOW - timedelta(seconds=5)) is False, \
            'a future-dated witness must deny'
        drifted = copy.deepcopy(LIVE)
        drifted['engine']['restart_count'] = 1
        assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'a restarted container must deny'
        drifted = copy.deepcopy(LIVE)
        drifted['gateway']['container_id'] = 'another-container'
        assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'a replaced container must deny'
        drifted = copy.deepcopy(LIVE)
        drifted['source']['addresses'] = ['10.203.1.9']
        assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'an address change must deny'
        drifted = copy.deepcopy(LIVE)
        drifted['engine']['cap_add'] = ['NET_ADMIN']
        assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'a capability grant must deny'
        drifted = copy.deepcopy(LIVE)
        drifted['engine']['addresses'].append('172.19.0.99')
        assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'an extra egress network must deny'
        internal = RECEIPT['network']['name']
        endpoint_mutations = {
            'NetworkID': 'replacement-network-id', 'EndpointID': 'replacement-endpoint-id',
            'Gateway': '10.203.1.254', 'GlobalIPv6Address': 'fd00::2', 'IPv6Gateway': 'fd00::1',
            'IPPrefixLen': 16, 'GlobalIPv6PrefixLen': 64,
        }
        for field, value in endpoint_mutations.items():
            drifted = copy.deepcopy(LIVE)
            drifted['engine']['network_endpoints'][internal][field] = value
            assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, field + ' drift must deny'
        drifted = copy.deepcopy(LIVE)
        drifted['engine']['network_endpoints']['noninternal-same-ip'] = \
            drifted['engine']['network_endpoints'].pop(internal)
        assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'same IP on another network must deny'
        for field, value in (('id', 'recreated-topology-id'), ('internal', False),
                             ('ipam', [{'Subnet': '10.203.0.0/16', 'Gateway': '10.203.1.1'}])):
            drifted = copy.deepcopy(LIVE)
            drifted['engine']['topology'][field] = value
            assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'topology ' + field + ' drift must deny'
        for mutation in ('missing_id', 'noninternal', 'engine_external'):
            invalid = copy.deepcopy(RECEIPT)
            if mutation == 'missing_id':
                del invalid['network']['id']
            elif mutation == 'noninternal':
                invalid['network']['internal'] = False
            else:
                invalid['container_security']['engine']['networks']['unexpected-external'] = \
                    endpoint('external-id', 'external-endpoint', '10.203.1.2', '10.203.1.1')
            invalid_raw = canonical(invalid)
            try:
                static_receipt({'network_receipt_sha256': __import__('hashlib').sha256(invalid_raw).hexdigest()},
                               invalid_raw)
                raise AssertionError('invalid topology receipt accepted: ' + mutation)
            except ValueError:
                pass
        for charset in (None, 'US-ASCII'):
            invalid = copy.deepcopy(RECEIPT)
            if charset is None:
                del invalid['engine']['file_encoding']
            else:
                invalid['engine']['file_encoding'] = charset
            invalid_raw = canonical(invalid)
            try:
                static_receipt({'network_receipt_sha256': __import__('hashlib').sha256(invalid_raw).hexdigest()},
                               invalid_raw)
                raise AssertionError('missing or unverified engine charset accepted')
            except ValueError as error:
                assert str(error) == 'network_receipt_engine_charset_invalid'
        for option in (None, False):
            drifted = copy.deepcopy(LIVE)
            if option is None:
                del drifted['engine']['java_utf8']
            else:
                drifted['engine']['java_utf8'] = option
            assert not qualified(raw, drifted, ANCHOR, NOW, witness), 'missing JVM UTF8 option must deny'
        legacy = copy.deepcopy(LIVE)
        del legacy['engine']['network_endpoints']
        assert not qualified(raw, legacy, ANCHOR, NOW, witness), 'legacy witness lacks endpoint proof'
        # Drive the trusted-host Docker read path, not just witness mutation.
        def docker_output(arguments, code):
            if arguments[0] == 'network':
                return json.dumps(RECEIPT['network'])
            return '\n'.join(json.dumps({
                'id': proof['container_id'], 'image': proof['image'], 'started_at': proof['started_at'],
                'restart_count': proof['restart_count'], 'cap_add': proof['cap_add'],
                'cap_drop': proof['cap_drop'], 'privileged': proof['privileged'],
                'security_opt': proof['security_opt'], 'networks': proof['network_endpoints'],
                **({'java_utf8': proof['java_utf8']} if 'java_utf8' in proof else {})})
                for proof in LIVE.values())
        with patch('app.real_network._docker_json', side_effect=docker_output) as docker, \
             patch('app.real_network._firewall_snapshot', return_value=LIVE['engine']['firewall']):
            observed = observe_containers(RECEIPT)
            assert qualified(raw, observed, ANCHOR, NOW, witness)
            assert any(call.args[0][:2] == ['network', 'inspect'] for call in docker.call_args_list)
            inspect_template = next(call.args[0][2] for call in docker.call_args_list
                                    if call.args[0][0] == 'inspect')
            assert 'eq . "JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF-8"' in inspect_template
            assert 'json .Config.Env' not in inspect_template, 'do not expose environment values'
        for option in (None, False):
            def charset_docker_output(arguments, code):
                rows = docker_output(arguments, code)
                if arguments[0] == 'network':
                    return rows
                decoded = [json.loads(line) for line in rows.splitlines()]
                if option is None:
                    del decoded[0]['java_utf8']
                else:
                    decoded[0]['java_utf8'] = option
                return '\n'.join(json.dumps(row) for row in decoded)
            with patch('app.real_network._docker_json', side_effect=charset_docker_output), \
                 patch('app.real_network._firewall_snapshot', return_value=LIVE['engine']['firewall']):
                assert not qualified(raw, observe_containers(RECEIPT), ANCHOR, NOW, witness), \
                    'Docker reader must deny an absent exact UTF8 JVM option'
        def changed_docker_output(arguments, code):
            rows = docker_output(arguments, code)
            if arguments[0] == 'network':
                return rows
            decoded = [json.loads(line) for line in rows.splitlines()]
            decoded[0]['networks'][internal]['NetworkID'] = 'same-ip-new-network'
            return '\n'.join(json.dumps(row) for row in decoded)
        with patch('app.real_network._docker_json', side_effect=changed_docker_output), \
             patch('app.real_network._firewall_snapshot', return_value=LIVE['engine']['firewall']):
            assert not qualified(raw, observe_containers(RECEIPT), ANCHOR, NOW, witness), \
                'Docker inspector must preserve changed network identity'
        def noninternal_docker_output(arguments, code):
            rows = docker_output(arguments, code)
            if arguments[0] == 'network':
                network = json.loads(rows)
                network['internal'] = False
                return json.dumps(network)
            return rows
        with patch('app.real_network._docker_json', side_effect=noninternal_docker_output), \
             patch('app.real_network._firewall_snapshot', return_value=LIVE['engine']['firewall']):
            try:
                observe_containers(RECEIPT)
                raise AssertionError('Docker network inspector accepted a noninternal topology')
            except ValueError as error:
                assert str(error) == 'network_topology_invalid'
        drifted = copy.deepcopy(LIVE)
        drifted['engine']['firewall']['rules_sha256'] = '1' * 64
        assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'changed live firewall must deny'
        drifted = copy.deepcopy(LIVE)
        drifted['engine']['firewall']['netns'] = 'net:[replacement]'
        assert qualified(raw, drifted, ANCHOR, NOW, witness) is False, 'a different namespace must deny'
        drifted = copy.deepcopy(LIVE)
        drifted['engine']['restart_count'] = 1
        reanchored = {role: {key: proof[key] for key in ('container_id', 'started_at', 'restart_count')}
                      for role, proof in drifted.items()}
        assert qualified(raw, drifted, reanchored, NOW, witness) is False, 'a new anchor cannot erase receipt drift'
        rebound = copy.deepcopy(RECEIPT)
        rebound['engine']['ip'] = '10.203.1.9'
        rebound_entry = {'network_receipt_sha256': __import__('hashlib').sha256(canonical(rebound)).hexdigest()}
        assert qualified(canonical(rebound), LIVE, ANCHOR, NOW, witness, rebound_entry) is False, \
            'a rebound receipt must not inherit another proof'
        removed = copy.deepcopy(RECEIPT)
        removed['resource_state'] = 'removed'
        try:
            static_receipt({'network_receipt_sha256': __import__('hashlib').sha256(canonical(removed)).hexdigest()},
                           canonical(removed))
            raise AssertionError('a removed receipt must never be live')
        except ValueError:
            pass
        Path(witness).write_bytes(canonical({'schema': 'managed-network-witness/v1',
                                             'receipt_sha256': 'x' * 64, 'observed_at': NOW.isoformat(),
                                             'live': {role: {key: LIVE[role][key] for key in
                                                             ('container_id', 'started_at', 'restart_count',
                                                              'cap_add', 'cap_drop', 'privileged', 'addresses')}
                                                      for role in LIVE}}))
        assert qualified_at(witness, raw, entry, NOW) is False, 'an unbound witness must deny'
        Path(witness).write_text('not json', encoding='utf-8')
        assert qualified_at(witness, raw, entry, NOW) is False, 'an unparsable witness must deny'
        Path(witness).unlink()
        assert qualified_at(witness, raw, entry, NOW) is False, 'a missing witness must deny'
    print(json.dumps({'level': 'network_witness_boundary_fixture', 'cases': 43,
                      'docker_read_paths_mocked': True, 'real_l2_qualified': False}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
