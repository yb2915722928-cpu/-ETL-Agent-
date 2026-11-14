"""Quality-domain PostgreSQL fixtures, NOT real engine/Doris L2 acceptance."""
from dataclasses import replace
import json
from uuid import uuid4

from sqlalchemy import select

from verify_managed_load_fixtures import FixtureAdapter, binding, journal_database
from app.managed_load import BatchKey, ManagedLoad
from app.quality_runtime import QualityRecord, QualityRun, QualityRuntime, route_body


class RowAdapter(FixtureAdapter):
    def send(self, frozen, label, body):
        result = super().send(frozen, label, body)
        fields = json.loads(result.raw)
        fields.update(NumberTotalRows=1, NumberLoadedRows=1, NumberFilteredRows=0, NumberUnselectedRows=0)
        return replace(result, raw=json.dumps(fields).encode())


def setup(sessions, *, status='Success'):
    run_id = str(uuid4())
    commands = {'valid': str(uuid4()), 'rejected': str(uuid4())}
    plan = {'mappings': [{'source': 'id', 'target': 'id', 'operation': 'direct'},
                         {'source': 'name', 'target': 'name', 'operation': 'direct'},
                         {'source': 'score', 'target': 'score', 'operation': 'direct'}],
            'quality': [{'kind': 'not_null', 'field': 'score'},
                        {'kind': 'numeric_range', 'field': 'score', 'minimum': 0, 'maximum': 10}],
            'cleaning': [{'field': 'name', 'kind': 'trim'}],
            'filters': [{'field': 'source_status', 'equals': 'skip'}]}
    quality = QualityRuntime(sessions)
    digest = quality.bind(run_id, plan, ['id'], commands)
    loads = ManagedLoad(RowAdapter(status=status), sessions)
    for route, command in commands.items():
        loads.bind(replace(binding(), command_id=command, run_id=run_id,
                           quality_contract_sha256=digest,
                           table='shadow_valid' if route == 'valid' else 'shadow_errors'))
    return run_id, commands, quality, loads


def close_routes(loads, commands):
    for command in commands.values():
        loads.close(command, 'quality-fixture-done')


def verify(sessions):
    passed = []
    run, commands, quality, loads = setup(sessions)
    records = [{'id': 1, 'name': '  private_value  ', 'score': 5, 'source_status': 'keep'},
               {'id': 2, 'name': 'private_value_2', 'score': None, 'source_status': None},
               {'id': 3, 'name': 'skip', 'score': 5, 'source_status': 'skip'}]
    decisions = [quality.judge(run, record) for record in records]
    assert decisions[0]['payload']['name'] == 'private_value'
    assert len(decisions[1]['errors']) == 2
    assert {x['code'] for x in decisions[1]['errors']} == {'NOT_NULL', 'RANGE_NULL'}
    assert decisions[2]['errors'] == [{'field': 'source_status', 'code': 'FILTERED'}]
    assert 'source_status' not in decisions[0]['payload']
    assert quality.judge(run, records[1]) == decisions[1]
    links = {}
    for index, decision in enumerate(decisions):
        body = route_body(decision, decision['route'])
        result = loads.send(commands[decision['route']], BatchKey(0, index), body)
        links[decision['stable_key']] = result['label']
    quality.source_progress(run, source_read=3, parsed=3, complete=True)
    close_routes(loads, commands)
    report = quality.report(run, links)
    assert not report['publication_veto'], report
    assert (report['source_read'], report['parsed'], report['evaluated'], report['valid'], report['rejected']) == (3, 3, 3, 1, 2)
    assert report['final_written'] == {'valid': 1, 'rejected': 2}
    assert quality.report(run, {}) == report  # Frozen report cannot be overwritten.
    with sessions() as db:
        rows = list(db.scalars(select(QualityRecord).where(QualityRecord.run_id == run)))
        facts = [{'digest': row.body_sha256, 'stage': row.stage_sha256, 'errors': row.errors} for row in rows]
        assert 'private_value' not in json.dumps(facts)
    passed.append('cleaning_null_multicode_filter_dedup_and_journal_reconciliation')

    configured = QualityRuntime(sessions)
    configured_run = str(uuid4())
    approved = {'mappings': [{'source': 'name', 'target': 'display_name', 'operation': 'direct'}],
        'quality': [{'kind': 'not_null', 'field': 'display_name'}],
        'cleaning': [{'field': 'display_name', 'kind': 'trim'}],
        'filters': [{'field': 'source_status', 'equals': None}]}
    routes = {'valid': str(uuid4()), 'rejected': str(uuid4())}
    configured.bind(configured_run, approved, ['id'], routes)
    decision = configured.judge(configured_run, {'id': '  stable-id  ', 'name': None, 'source_status': None})
    assert decision['route'] == 'rejected' and len(decision['errors']) == 2
    assert {error['code'] for error in decision['errors']} == {'NOT_NULL', 'FILTERED'}
    assert configured.judge(configured_run, {'id': '  stable-id  ', 'name': None, 'source_status': None}) == decision
    cleaned = configured.judge(configured_run, {'id': 2, 'name': ' trimmed ', 'source_status': 'keep'})
    assert cleaned['payload'] == {'display_name': 'trimmed'}
    for mutation in ('override', 'primary_key_cleaning'):
        try:
            if mutation == 'override':
                configured.bind(str(uuid4()), approved, ['id'], routes, cleaning=[])
            else:
                configured.bind(str(uuid4()), {**approved, 'cleaning': [{'field': 'display_name', 'kind': 'trim'}]},
                                ['name'], routes)
        except ValueError:
            pass
        else:
            raise AssertionError(mutation)
    with sessions() as db:
        persisted = db.get(QualityRun, configured_run)
        assert persisted.contract['plan'] == approved
        assert persisted.contract['cleaning'] == approved['cleaning'] and persisted.contract['filters'] == approved['filters']
    passed.append('approved_plan_configuration_renaming_null_source_filter_and_identity_protection')

    changed_run, _, changed, _ = setup(sessions)
    changed.judge(changed_run, records[0])
    conflict = changed.judge(changed_run, {**records[0], 'score': 9})
    assert conflict['publication_veto'] and conflict['route'] is None
    changed.source_progress(changed_run, source_read=1, parsed=1, complete=True)
    assert 'record_payload_conflict' in changed.report(changed_run, {})['reasons']
    passed.append('same_primary_key_changed_payload_latches_veto')

    failed_run, failed_commands, failed, failed_loads = setup(sessions, status='Fail')
    decision = failed.judge(failed_run, records[0])
    failure = failed_loads.send(failed_commands['valid'], BatchKey(0, 0), route_body(decision, 'valid'))
    failed.source_progress(failed_run, source_read=1, parsed=1, complete=True)
    close_routes(failed_loads, failed_commands)
    report = failed.report(failed_run, {decision['stable_key']: failure['label']})
    assert report['publication_veto'] and report['final_written']['valid'] == 0
    passed.append('either_sink_failure_is_not_engine_success')

    parse_run, _, parse, _ = setup(sessions)
    assert parse.judge(parse_run, {'name': 'missing original primary key'})['publication_veto']
    parse.source_progress(parse_run, source_read=2, parsed=1, complete=True, parse_failed=True)
    report = parse.report(parse_run, {})
    assert report['publication_veto'] and report['source_read'] == 2 and report['parsed'] == 1
    assert 'source_parse_failure' in report['reasons'] and report['evaluated'] == 0
    passed.append('unparseable_source_fails_batch_and_preserves_distinct_counts')

    wrong_run, wrong_commands, wrong, wrong_loads = setup(sessions)
    decision = wrong.judge(wrong_run, records[0])
    # A real successful receipt for different bytes must not prove this record.
    receipt = wrong_loads.send(wrong_commands['valid'], BatchKey(0, 0), b'{"other":1}\n')
    wrong.source_progress(wrong_run, source_read=1, parsed=1, complete=True)
    close_routes(wrong_loads, wrong_commands)
    report = wrong.report(wrong_run, {decision['stable_key']: receipt['label']})
    assert report['publication_veto'] and 'route_receipt_unverified' in report['reasons']
    passed.append('successful_receipt_for_wrong_payload_does_not_reconcile')
    return passed


if __name__ == '__main__':
    with journal_database() as sessions:
        for table in (QualityRun.__table__, QualityRecord.__table__):
            table.create(sessions.kw['bind'])
        results = verify(sessions)
    print(json.dumps({'level': 'postgresql_quality_fixture', 'real_l2_qualified': False,
                      'passed': results}, ensure_ascii=False))
