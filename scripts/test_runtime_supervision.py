"""Small behavior tracer for the frozen supervision contract (no database)."""
import unittest
from datetime import datetime, timezone
from app.runtime_supervision import RuntimeSupervisionContract, evaluate_observation


class SupervisionContractTest(unittest.TestCase):
    def test_rejection_sample_boundary_and_final_small_sample(self):
        contract = RuntimeSupervisionContract().model_dump()
        now = datetime.now(timezone.utc)
        sample = dict(contract_version=contract['version'], source='l1.fixture', byte_kind='measured',
                      counter_epoch='job-1', observed_at=now.isoformat(), state='running',
                      source_read=99, evaluated=99, valid=0, rejected=99, final_output=0,
                      sink_bytes=0, elapsed_seconds=1)
        self.assertEqual(evaluate_observation(contract, sample, None, now)['reasons'], [])
        sample.update(source_read=100, evaluated=100, rejected=100)
        self.assertIn('reject_ratio_exceeded', evaluate_observation(contract, sample, None, now)['reasons'])
        sample.update(source_read=2, evaluated=2, rejected=2, state='finished')
        self.assertIn('reject_ratio_exceeded', evaluate_observation(contract, sample, None, now)['reasons'])
        sample.update(state='running', sink_bytes=268435457)
        self.assertIn('bytes_exceeded', evaluate_observation(contract, sample, None, now)['reasons'])

    def test_unknown_stale_reset_and_unmeasured_values_cannot_pass(self):
        contract = RuntimeSupervisionContract().model_dump()
        now = datetime.now(timezone.utc)
        sample = dict(contract_version=contract['version'], source='l1.fixture', byte_kind='measured',
                      counter_epoch='job-1', observed_at=now.isoformat(), state='running',
                      source_read=0, evaluated=0, valid=0, rejected=0, final_output=0,
                      sink_bytes=0, elapsed_seconds=0)
        self.assertIsNone(evaluate_observation(contract, sample, None, now)['metrics']['reject_ratio'])
        cases = [(None, 'metrics_missing'), ({**sample, 'sink_bytes': None}, 'metrics_missing_or_invalid'),
                 ({**sample, 'byte_kind': 'estimated'}, 'bytes_not_measured'),
                 ({**sample, 'observed_at': '2020-01-01T00:00:00+00:00'}, 'metrics_stale'),
                 ({**sample, 'counter_epoch': 'new-epoch'}, 'metrics_reset')]
        for current, reason in cases:
            result = evaluate_observation(contract, current, sample, now)
            self.assertIn(reason, result['reasons'])
            self.assertTrue(all(value is None for value in result['metrics'].values()))


if __name__ == '__main__':
    unittest.main()
