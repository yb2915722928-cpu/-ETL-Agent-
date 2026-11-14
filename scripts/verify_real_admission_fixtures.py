"""Real PG clocks + real resolver authorization, fake schema/network/load I/O; NOT L2."""
from copy import deepcopy
from datetime import datetime
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from unittest.mock import patch

from sqlalchemy import func, select
from verify_real_runtime_fixtures import database, seed, Objects, Engine
from verify_quality_fixtures import RowAdapter
from app.managed_load import LoadBatch, LoadRun, ManagedLoad
from app.models import Membership
from app.real_control import advance_real_control
from app.real_ingress import IngressBinding, IngressDenied, IngressService
from app.real_job import HTTP_CONNECTOR
from app.real_resources import RealResourceResolver
from app.real_runtime import submit_real, _token
from app.supervision_runtime import RuntimeSupervision


def verify(sessions, signer):
    passed, timings = [], []
    receipt = json.dumps({'schema': 'managed-network-receipt/v1', 'resource_state': 'kept',
        'verification_phase': 'isolated_network_observed', 'http_connector_sha256': HTTP_CONNECTOR['sha256'],
        'engine': {'ip': '192.0.2.4', 'rest_port': 8080, 'file_encoding': 'UTF-8'},
        'gateway': {'ip': '192.0.2.5', 'ingress_port': 8080}}).encode()
    # External source/target schema probe only. All signed command, membership,
    # bound secret references, preparation, writer and route checks remain real.
    probe_delay = [0]
    def schema_probe(db, policy, version):
        if probe_delay[0]:
            sleep(probe_delay[0])
        return deepcopy(version.frozen['execution_facts'])
    with TemporaryDirectory(prefix='admission-fixture-') as folder:
        path = Path(folder) / 'receipt.json'
        path.write_bytes(receipt)
        with patch.dict(os.environ, {'REAL_NETWORK_RECEIPT_FILE': str(path), 'REAL_DEPLOYMENT_MANIFEST_FILE': ''}), \
             patch('app.preparations.current_execution_facts', side_effect=schema_probe), \
             patch('app.real_runtime._external_authorization'):
            def fixture():
                probe_delay[0] = 0
                cid, policy, provider, manifest = seed(sessions, signer, receipt)
                objects, engine = Objects(sessions), Engine(sessions, cid)
                assert submit_real(cid, sessions=sessions, policy=policy, secrets=provider, manifest=manifest,
                    client_factory=lambda _: engine, network_verifier=lambda entry, raw: raw == receipt,
                    objects_factory=objects.factory) == 'submitted'
                resolver = RealResourceResolver(sessions=sessions, policy=policy, secrets=provider, manifest=manifest)
                adapter = RowAdapter()
                loads = ManagedLoad(adapter, sessions)
                authorization_ms = []
                def authorize(facts):
                    start = monotonic()
                    try:
                        return resolver.authorize(facts)
                    finally:
                        authorization_ms.append(round((monotonic() - start) * 1000, 2))
                ingress = IngressService(lambda _: loads, authorize, sessions)
                engine.observe = lambda identifier, name: {'job_id': identifier, 'job_name': name,
                    'engine_state': 'RUNNING', 'metrics': {'SourceReceivedCount': '1'}}
                original_window, original_judge = ingress._admission_window, ingress.quality.judge
                state = {'polls': 0, 'judges': 0, 'age_ms': None, 'stops': 0}
                def stop(identifier):
                    assert identifier == engine.original['engine_job_id']
                    state['stops'] += 1
                    return {'accepted': True}
                engine.stop = stop
                def publish():
                    state['control_status'] = advance_real_control(cid, sessions=sessions, ingress_factory=lambda _: ingress,
                        client_factory=lambda _: engine)
                def window(binding):
                    state['polls'] += 1
                    if state['polls'] == 1:
                        publish()
                    return original_window(binding)
                def judge(run_id, record):
                    with sessions() as db:
                        sample = db.get(RuntimeSupervision, cid).last_trusted
                        now = db.scalar(select(func.clock_timestamp()))
                    state['age_ms'] = (now - datetime.fromisoformat(sample['observed_at'])).total_seconds() * 1000
                    assert 0 <= state['age_ms'] <= 200, 'judge crossed original trusted window'
                    state['judges'] += 1
                    return original_judge(run_id, record)
                ingress._admission_window, ingress.quality.judge = window, judge
                return cid, ingress, adapter, state, authorization_ms, publish

            cid, ingress, adapter, state, auth_ms, publish = fixture()
            probe_delay[0] = .23
            start = monotonic()
            try:
                result = ingress.post(cid, _token(cid), b'{"id":1}')
            except IngressDenied as error:
                raise AssertionError('full resolver precheck consumed trusted window: ' + error.code) from None
            assert result['accepted'] and state['judges'] == adapter.calls == 1
            assert len(auth_ms) == 2 and min(auth_ms) >= 230
            timings.append({'case': 'slow_schema_preflight', 'authorization_ms': auth_ms,
                'judge_age_ms': round(state['age_ms'], 2), 'total_ms': round((monotonic() - start) * 1000, 2)})
            passed.append('full_resolver_preflight_before_window_and_current_send_recheck')

            def denied(cid, ingress, code):
                try:
                    ingress.post(cid, _token(cid), b'{"id":1}')
                    raise AssertionError('unsafe POST accepted')
                except IngressDenied as error:
                    assert error.code == code, error.code

            # Delay exactly the final barrier acquisition/check, not any clock.
            cid, ingress, adapter, state, auth_ms, publish = fixture()
            window = ingress._admission_window
            calls = [0]
            def slow_final_window(binding):
                calls[0] += 1
                if calls[0] == 2:
                    sleep(.23)
                return window(binding)
            ingress._admission_window = slow_final_window
            start = monotonic()
            denied(cid, ingress, 'ingress_admission_timeout')
            assert state['judges'] == adapter.calls == 0
            timings.append({'case': 'barrier_crossed_window', 'total_ms': round((monotonic() - start) * 1000, 2)})
            passed.append('strict_real_clock_rejects_expired_window_before_judge')

            def role(value):
                with sessions.begin() as db:
                    membership = db.get(Membership, ('project', 'operator'))
                    membership.roles = [value]
            for moment in ('after_preflight', 'while_waiting', 'after_judge'):
                cid, ingress, adapter, state, auth_ms, publish = fixture()
                if moment == 'after_preflight':
                    authorize = ingress.authorize
                    def revoke_after_preflight(facts):
                        authorize(facts)
                        role('auditor')
                    ingress.authorize = revoke_after_preflight
                elif moment == 'while_waiting':
                    window = ingress._admission_window
                    pending = [True]
                    def revoke_while_waiting(binding):
                        if pending[0]:
                            pending[0] = False
                            role('auditor')
                            return False
                        return window(binding)
                    ingress._admission_window = revoke_while_waiting
                else:
                    judge = ingress.quality.judge
                    def revoke_after_judge(run_id, record):
                        decision = judge(run_id, record)
                        role('auditor')
                        return decision
                    ingress.quality.judge = revoke_after_judge
                try:
                    denied(cid, ingress, 'ingress_record_or_load_failed' if moment == 'after_judge' else 'ingress_closed')
                    assert adapter.calls == 0 and ingress._binding(cid).closed
                    assert state['judges'] == (1 if moment == 'after_judge' else 0)
                    if moment != 'after_judge':
                        assert state['stops'] == 1
                finally:
                    role('operator')
                passed.append('current_role_revocation_' + moment + '_blocks_load')

            # Unknown/failed/missing receipt on any extra batch blocks the aggregate.
            for phase, receipt_value in (('unknown', None), ('receipt', None),
                                         ('receipt', {}), ('receipt', {'Status': 'Fail'})):
                cid, ingress, adapter, state, auth_ms, publish = fixture()
                publish()
                binding = ingress._binding(cid)
                assert IngressService._admission_window(ingress, binding)
                with sessions.begin() as db:
                    now = db.scalar(select(func.clock_timestamp()))
                    db.add(LoadBatch(label='fixture-extra-' + cid,
                        command_id=binding.facts['route_commands']['valid'], partition=0, sequence=99,
                        body_sha256='0' * 64, phase=phase, intent_at=now, receipt=receipt_value))
                assert not IngressService._admission_window(ingress, binding)
            passed.append('all_extra_unknown_or_missing_receipt_batches_block_window')

            for change in ('missing_route', 'closed_route', 'facts', 'token', 'stop'):
                cid, ingress, adapter, state, auth_ms, publish = fixture()
                publish()
                binding = ingress._binding(cid)
                assert IngressService._admission_window(ingress, binding)
                with sessions.begin() as db:
                    route = db.get(LoadRun, binding.facts['route_commands']['rejected'])
                    row = db.get(IngressBinding, cid)
                    if change == 'missing_route':
                        db.delete(route)
                    elif change == 'closed_route':
                        route.closed = True
                    elif change == 'facts':
                        row.facts = {**row.facts, 'source_primary_key': ['other']}
                    elif change == 'token':
                        row.token_sha256 = '0' * 64
                    else:
                        db.get(RuntimeSupervision, cid).stop_request = {'reason': 'operator'}
                assert not IngressService._admission_window(ingress, binding)
            passed.append('aggregate_retains_exact_routes_binding_token_and_stop_checks')
    return passed, timings


if __name__ == '__main__':
    with database() as (sessions, signer):
        passed, timings = verify(sessions, signer)
    print(json.dumps({'level': 'postgresql_real_admission_fixture', 'real_l2_qualified': False,
                      'passed': passed, 'timings': timings}))
