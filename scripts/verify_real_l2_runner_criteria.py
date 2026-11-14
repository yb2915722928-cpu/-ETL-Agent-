"""Pure report criteria checks; never import the runner's live infrastructure."""
import ast
from copy import deepcopy
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'scripts/real_l2_runner.py'
OLD = ROOT / 'artifacts/issue-13/real-managed-l2-c54dc8879e06.json'


def criteria(report):
    tree = ast.parse(SOURCE.read_text(encoding='utf-8'))
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                 and node.name == 'rotation_acceptance']
    assert len(functions) == 1
    namespace = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(SOURCE), 'exec'), namespace)
    return namespace['rotation_acceptance'](report)


def isolated_report():
    report = json.loads(OLD.read_text(encoding='utf-8'))
    # The historical report does not prove closure. Only this synthetic criteria
    # fixture supplies those facts; never amend or accept the historical report.
    report['ingress_closure'] = {
        'command_id': report['command_id'], 'run_id': report['run_id'],
        'engine_job_id': report['engine_job_id'], 'closed': True,
        'route_commands': {'valid': 'valid-command', 'rejected': 'rejected-command'},
        'routes': {name: {'command_id': name + '-command', 'run_id': report['run_id'],
                          'closed': True, 'close_id': 'close-' + name}
                   for name in ('valid', 'rejected')}}
    report['original_route_commands'] = dict(report['ingress_closure']['route_commands'])
    return report


class RotationCriteria(unittest.TestCase):
    def test_original_finished_with_unknown_load_is_safely_isolated(self):
        report = isolated_report()
        before = deepcopy(report)
        result = criteria(report)
        self.assertTrue(result['rotation_safety_accepted'])
        self.assertTrue(result['rotation_path_accepted'])
        self.assertTrue(result['load_settlement_pending'])
        self.assertFalse(result['managed_l2_accepted'])
        self.assertEqual(result['rotation_acceptance_scope'], 'safety_isolation_only')
        self.assertEqual(report, before)
        self.assertIsNone(report['load_batches'][0]['receipt'])
        self.assertIsNone(report['supervision']['current_metrics']['sink_bytes'])
        self.assertFalse(report['control']['external_sink']['quiescent'])

    def test_historical_report_missing_closure_is_not_reclassified(self):
        before = OLD.read_bytes()
        report = json.loads(before)
        result = criteria(report)
        self.assertFalse(result['rotation_safety_accepted'])
        self.assertTrue(result['load_settlement_pending'])
        self.assertFalse(result['managed_l2_accepted'])
        self.assertEqual(OLD.read_bytes(), before)

    def test_all_three_original_terminal_states(self):
        for state in ('FINISHED', 'CANCELED', 'FAILED'):
            with self.subTest(state=state):
                report = isolated_report()
                report['control']['termination']['engine_state'] = state
                self.assertTrue(criteria(report)['rotation_safety_accepted'])

    def test_terminal_identity_and_confirmation_are_required(self):
        for field, value in (('engine_job_id', 'replacement'), ('job_name', 'replacement'),
                             ('confirmed', False), ('basis', 'stop_requested'),
                             ('engine_state', 'RUNNING')):
            with self.subTest(field=field):
                report = isolated_report()
                report['control']['termination'][field] = value
                self.assertFalse(criteria(report)['rotation_safety_accepted'])
        report = isolated_report()
        report['engine_job_id'] = 'replacement'
        self.assertFalse(criteria(report)['rotation_safety_accepted'])

    def test_each_route_requires_persisted_closure_and_original_binding(self):
        for name in ('valid', 'rejected'):
            for field, value in (('closed', False), ('close_id', None),
                                 ('command_id', 'replacement'), ('run_id', 'replacement')):
                with self.subTest(route=name, field=field):
                    report = isolated_report()
                    report['ingress_closure']['routes'][name][field] = value
                    self.assertFalse(criteria(report)['rotation_safety_accepted'])
            report = isolated_report()
            del report['ingress_closure']['routes'][name]
            self.assertFalse(criteria(report)['rotation_safety_accepted'])
        report = isolated_report()
        report['original_route_commands']['valid'] = 'replacement'
        self.assertFalse(criteria(report)['rotation_safety_accepted'])

    def test_ingress_binding_must_match_original_run_and_job(self):
        for field, value in (('closed', False), ('command_id', 'replacement'),
                             ('run_id', 'replacement'), ('engine_job_id', 'replacement')):
            with self.subTest(field=field):
                report = isolated_report()
                report['ingress_closure'][field] = value
                self.assertFalse(criteria(report)['rotation_safety_accepted'])

    def test_rotation_authorization_facts_are_all_required(self):
        failures = {'old_revoked': False, 'new_reference_id': None, 'new_version': 1,
            'old_version': None, 'fresh_preparation_reference_id': 'old',
            'old_preparation_state': 'ready', 'original_signed_writer_unchanged': False,
            'cleanup_denied': False, 'run_status_before_rotation': 'final_metrics_pending'}
        for field, value in failures.items():
            with self.subTest(field=field):
                report = isolated_report()
                report['rotation_public_api'][field] = value
                self.assertFalse(criteria(report)['rotation_safety_accepted'])
                del report['rotation_public_api'][field]
                self.assertFalse(criteria(report)['rotation_safety_accepted'])
        report = isolated_report()
        report['rotation_public_api']['new_reference_id'] = report['rotation_public_api']['old_reference_id']
        self.assertFalse(criteria(report)['rotation_safety_accepted'])

    def test_veto_and_owner_retention_are_required(self):
        report = isolated_report()
        report['supervision']['publication_veto'] = False
        self.assertFalse(criteria(report)['rotation_safety_accepted'])
        report = isolated_report()
        report['ownership_retained'] = False
        self.assertFalse(criteria(report)['rotation_safety_accepted'])

    def test_pending_load_is_independent_from_safety(self):
        for phase, receipt in (('dispatching', None), ('unknown', None),
                               ('receipt', {'Status': 'Publish Timeout'}),
                               ('receipt', {'Status': 'Label Already Exists'})):
            with self.subTest(phase=phase, receipt=receipt):
                report = isolated_report()
                report['load_batches'] = [{'phase': phase, 'receipt': receipt}]
                result = criteria(report)
                self.assertTrue(result['rotation_safety_accepted'])
                self.assertTrue(result['load_settlement_pending'])
                self.assertFalse(result['managed_l2_accepted'])
        report = isolated_report()
        report['load_batches'] = [{'phase': 'receipt', 'receipt': {'Status': 'Success', 'LoadBytes': 9}}]
        self.assertFalse(criteria(report)['load_settlement_pending'])
        self.assertFalse(criteria(report)['managed_l2_accepted'])
        del report['load_batches']
        self.assertTrue(criteria(report)['load_settlement_pending'])


if __name__ == '__main__':
    unittest.main()
