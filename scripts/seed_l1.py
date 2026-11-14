"""Create public MySQL/Doris profiles and frozen versions for isolated L1 tests."""
import argparse
import json
from pathlib import Path
from verify_identity import Client, private_write


def seed(url, credentials, config, alias=False):
    maker = Client(url, [])
    maker.login(credentials, 'maker')
    base = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
    entries = {}
    for kind in ('mysql', 'doris'):
        resource = 'doris-alias' if kind == 'doris' and alias else 'acceptance-' + kind
        connection = maker.call('POST', base + '/connections', {'name': 'L1 readonly ' + kind,
            'resource_id': resource, 'password': config[kind]['password']}, 201)
        profile = maker.call('POST', base + '/connections/' + connection['id'] + '/probe', {}, 201)
        entries[kind] = (connection, profile)
    versions = {}
    for table in ('customers', 'orders'):
        source, target = entries['mysql'], entries['doris']
        source_table = next(t for t in source[1]['content']['tables'] if t['name'] == table)
        target_table = next(t for t in target[1]['content']['tables'] if t['name'] == table)
        names = {c['name'] for c in target_table['columns']}
        mappings = [{'source': c['name'], 'target': c['name'], 'operation': 'direct'} for c in source_table['columns'] if c['name'] in names]
        plan = {'version': 'etl-plan-v1', 'mode': 'bounded_full',
            'source': {'connection_id': source[0]['id'], 'profile_id': source[1]['id'], 'table': table},
            'target': {'connection_id': target[0]['id'], 'profile_id': target[1]['id'], 'table': table},
            'mappings': mappings, 'quality': [{'kind': 'not_null', 'field': mappings[0]['target']}],
            'budget': {'max_rows': 100000, 'max_bytes': 268435456, 'max_seconds': 600, 'max_reject_ratio': .2, 'max_amplification': 2},
            'tools': ['mysql.read', 'doris.write']}
        candidate = maker.call('POST', base + '/studio/candidates', {'name': 'L1 ' + table, 'plan': plan}, 201)
        assert candidate['assessment']['passed'], candidate['assessment']['gates']
        versions[table] = maker.call('POST', base + '/versions', {'candidate_id': candidate['id'], 'expected_revision': candidate['revision']}, 201)
    return versions


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8010')
    parser.add_argument('--runtime', type=Path, default=Path('.runtime/l1'))
    parser.add_argument('--connector-secrets', type=Path, required=True)
    args = parser.parse_args()
    credentials = json.loads((args.runtime / 'bootstrap-demo.json').read_text(encoding='utf-8'))
    config = json.loads(args.connector_secrets.read_text(encoding='utf-8'))
    versions = seed(args.url, credentials, config)
    admin = Client(args.url, [])
    admin.login(credentials, 'admin')
    tenant = '/api/tenants/' + credentials['tenant_id']
    project = admin.call('POST', tenant + '/projects', {'name': 'L1 physical alias project', 'slug': 'physical-alias'}, 201)
    for role in ('maker', 'data_checker', 'security_checker', 'operator', 'auditor'):
        admin.call('PUT', tenant + '/projects/' + project['id'] + '/members/' + credentials['accounts'][role]['id'], {'roles': [role]})
    alias_credentials = {**credentials, 'project_id': project['id']}
    aliases = seed(args.url, alias_credentials, config, alias=True)
    private_write(args.runtime / 'alias-credentials.json', alias_credentials)
    private_write(args.runtime / 'versions.json', {k: v['id'] for k, v in versions.items()})
    private_write(args.runtime / 'alias-versions.json', {k: v['id'] for k, v in aliases.items()})
    print('L1 public profiles and frozen customer/order versions ready in two projects.')
