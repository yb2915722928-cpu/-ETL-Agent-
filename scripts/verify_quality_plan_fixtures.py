"""Finite Maker quality-plan compiler checks; no engine or real L2 acceptance."""
from copy import deepcopy
import json
import os
from types import SimpleNamespace
from unittest.mock import patch

from app.connector_policy import fingerprint
from app.models import SecretReference
from app.studio_plan import assess_plan, freeze_facts


def main():
    source_table = {'name': 'input_rows', 'columns': [
        {'name': 'id', 'type': 'int', 'nullable': False, 'key': 'PRI'},
        {'name': 'name', 'type': 'varchar(64)', 'nullable': True, 'key': ''},
        {'name': 'status', 'type': 'int', 'nullable': True, 'key': ''}]}
    target_table = {'name': 'output_rows', 'columns': [
        {'name': 'label', 'type': 'varchar(64)', 'nullable': True, 'key': ''}]}
    resources, connections, profiles, secrets = {}, {}, {}, {}
    for side, kind, table in (('source', 'mysql', source_table), ('target', 'doris', target_table)):
        resource = {'kind': kind, 'direction': side, 'host': 'fixture.invalid', 'port': 3306 if side == 'source' else 9030,
            'database': 'fixture', 'tables': [table['name']], 'username': side + '_probe'}
        if side == 'target':
            resource.update(physical_cluster_id='fixture-cluster', execution_writer={'username': 'writer'})
        resources[side] = resource
        connection = SimpleNamespace(id=side, kind=kind, direction=side, status='ready', resource_id=side,
            resource_fingerprint=fingerprint(resource), tenant_id='tenant', project_id='project',
            secret_reference_id=side + '-secret', secret_version=1,
            writer_secret_reference_id='writer-secret' if side == 'target' else None,
            writer_secret_version=1 if side == 'target' else None)
        connections[side] = connection
        profiles[side] = SimpleNamespace(id=side, connection_id=side, secret_reference_id=connection.secret_reference_id,
            secret_version=1, resource_id=side, resource_fingerprint=connection.resource_fingerprint,
            schema_hash=fingerprint([table]), content={'tables': [table]})
        secrets[side + '-secret'] = SecretReference(id=side + '-secret', tenant_id='tenant', project_id='project', version=1)
    secrets['writer-secret'] = SecretReference(id='writer-secret', tenant_id='tenant', project_id='project', version=1)
    db = SimpleNamespace(get=lambda model, key: secrets.get(key))
    policy = SimpleNamespace(get=lambda key: resources[key])
    plan = {'source': {'connection_id': 'source', 'profile_id': 'source', 'table': 'input_rows'},
        'target': {'connection_id': 'target', 'profile_id': 'target', 'table': 'output_rows'},
        'mappings': [{'source': 'name', 'target': 'label'}], 'quality': [{'kind': 'not_null', 'field': 'label'}]}
    with patch('app.studio_plan.get_connection', side_effect=lambda db, t, p, key: connections[key]), \
         patch('app.studio_plan.get_profile', side_effect=lambda db, t, p, key: profiles[key]):
        for adapter in ('l1', 'real'):
            with patch.dict(os.environ, {'EXECUTION_ADAPTER': adapter}):
                original = assess_plan(db, policy, 'tenant', 'project', plan)
                assert original['passed'], original['gates']
                normalized = original['normalized_plan']
                if adapter == 'l1':
                    # Fixed golden from 295d0ad2's compiler, not current-code roundtripping.
                    legacy_hashes = {
                        'dag': '619b8bf7041cfe89e545461493b483974552ad45fbc09288318189fe655e11bb',
                        'execution_facts': 'b87272c306e07a871f5b04423067a01fbeb5c12262c30d4b95b40d507f980578',
                        'hocon': 'cf7ba2358fee0cc69418104d951cf8f41b77d4cc040b085dc2e6c755a292ef0d',
                        'normalized_plan': '900a34c1c2b43a46769c03bd445808aabdbc1b48d38733d074d70bf3e7279344',
                        'profile_refs': '45271e38b0e794ea1c3790aab225d16951327ed4c036ed691864e444efe2b659',
                        'versions': 'dc3fa64ba714f5067da66af642208e66f5cab5f19c18a29afc543403f1e1cb83'}
                    actual_hashes = {key: fingerprint(value) for key, value in freeze_facts(original).items()}
                    assert actual_hashes == legacy_hashes, (actual_hashes, legacy_hashes)
                assert 'cleaning' not in normalized and 'filters' not in normalized
                explicit_empty = assess_plan(db, policy, 'tenant', 'project', {**plan, 'cleaning': [], 'filters': []})
                assert explicit_empty['passed']
                assert explicit_empty['normalized_plan']['cleaning'] == explicit_empty['normalized_plan']['filters'] == []
                assert freeze_facts(explicit_empty) == freeze_facts(assess_plan(
                    db, policy, 'tenant', 'project', explicit_empty['normalized_plan']))
                assert 'allow_empty' not in normalized, 'legacy absent default must preserve frozen facts'
                for permission in (False, True):
                    explicit = assess_plan(db, policy, 'tenant', 'project', {**plan, 'allow_empty': permission})
                    assert explicit['passed']
                    assert explicit['execution_facts']['plan']['allow_empty'] is permission
                    expected = 'allow_complete_empty' if permission else 'veto_publication'
                    assert explicit['execution_facts']['runtime_supervision_contract']['empty_policy'] == expected
                    assert freeze_facts(explicit) == freeze_facts(assess_plan(
                        db, policy, 'tenant', 'project', explicit['normalized_plan']))
                source_table['columns'][2]['type'] = 'bigint'
                profiles['source'].schema_hash = fingerprint([source_table])
                for value, accepted in ((9007199254740991, True), (-9007199254740991, True),
                                        (9007199254740992, False), (-9007199254740992, False)):
                    filtered = assess_plan(db, policy, 'tenant', 'project',
                        {**plan, 'filters': [{'field': 'status', 'equals': value}]})
                    assert filtered['passed'] is accepted, (value, filtered['gates'])
                    if not accepted:
                        assert 'filter_integer_not_json_safe' in json.dumps(filtered['gates'])
                source_table['columns'][2]['type'] = 'int'
                profiles['source'].schema_hash = fingerprint([source_table])
                assert freeze_facts(assess_plan(db, policy, 'tenant', 'project', normalized)) == freeze_facts(original)
                configured_plan = {**plan, 'cleaning': [{'field': 'label', 'kind': 'trim'}],
                    'filters': [{'field': 'status', 'equals': 0}]}
                configured = assess_plan(db, policy, 'tenant', 'project', configured_plan)
                assert configured['passed'], configured['gates']
                frozen = freeze_facts(configured)
                assert frozen == freeze_facts(assess_plan(db, policy, 'tenant', 'project', frozen['normalized_plan']))
                assert fingerprint(frozen) != fingerprint(freeze_facts(original))
                assert json.loads(configured['hocon'])['filters'] == configured_plan['filters']
                assert configured['dag']['nodes'][2]['cleaning'] == configured_plan['cleaning']
                if adapter == 'real':
                    assert configured['execution_facts']['real_execution_contract']['source_columns'] == ['id', 'name', 'status']
                for mutation in ('unknown_kind', 'primary_trim', 'unknown_filter', 'wrong_value_type'):
                    changed = deepcopy(configured_plan)
                    if mutation == 'unknown_kind':
                        changed['cleaning'][0]['kind'] = 'sql'
                    elif mutation == 'primary_trim':
                        changed['mappings'][0]['source'] = 'id'
                    elif mutation == 'unknown_filter':
                        changed['filters'][0]['field'] = 'missing'
                    else:
                        changed['filters'][0]['equals'] = '0'
                    assert not assess_plan(db, policy, 'tenant', 'project', changed)['passed'], mutation
    print(json.dumps({'level': 'quality_plan_compiler_fixture', 'real_l2_qualified': False,
        'passed': ['L1 and real empty defaults freeze/reassess identically',
            'Maker cleaning and source filters are frozen into facts, HOCON and DAG',
            'unmapped filter column joins approved source scan',
            'unsupported operation, primary-key trim, unknown field and typed mismatch rejected']}))


if __name__ == '__main__':
    main()
