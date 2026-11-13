"""Two public CLI behavior checks; no backend/database imports."""
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

CLI = Path(__file__).with_name('verify_evidence.py')


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


class OfflineVerification(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.addCleanup(self.directory.cleanup)
        self.key = Ed25519PrivateKey.generate()
        public = self.key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        self.key_id = hashlib.sha256(public).hexdigest()
        (self.root / 'key.json').write_text(json.dumps({'key_id': self.key_id, 'public_key': base64.b64encode(public).decode()}))

    def bundle(self, count):
        events = []
        previous = '0' * 64
        for sequence in range(1, count + 1):
            event = {'id': str(sequence), 'tenant_id': 'tenant', 'project_id': 'project', 'sequence': sequence,
                     'occurred_at': '2026-11-13T00:00:00+00:00', 'actor_id': 'actor', 'action': 'membership.assigned',
                     'facts': {'roles': ['auditor']}, 'version_id': None, 'preparation_id': None, 'run_id': None,
                     'artifact_hashes': {}, 'previous_hash': previous}
            event['event_hash'] = hashlib.sha256(canonical(event)).hexdigest()
            previous = event['event_hash']
            events.append(event)
        checkpoint = {'format': 'etl-evidence-checkpoint-v1', 'tenant_id': 'tenant', 'project_id': 'project',
                      'sequence': count, 'event_hash': previous, 'issued_at': '2026-11-13T00:00:00+00:00', 'key_id': self.key_id}
        checkpoint['signature'] = base64.b64encode(self.key.sign(canonical(checkpoint))).decode()
        return {'format': 'etl-evidence-v1', 'events': events, 'checkpoint': checkpoint}

    def invoke(self, bundle, external=None):
        path = self.root / 'bundle.json'
        path.write_text(json.dumps(bundle), encoding='utf-8')
        command = [sys.executable, str(CLI), str(path), '--public-key', str(self.root / 'key.json')]
        if external:
            saved = self.root / 'saved.json'
            saved.write_text(json.dumps(external))
            command += ['--checkpoint', str(saved)]
        return subprocess.run(command, capture_output=True, text=True)

    def test_signed_export_and_located_mutation(self):
        bundle = self.bundle(3)
        self.assertEqual(self.invoke(bundle).returncode, 0)
        bundle['events'][1]['facts']['roles'] = ['maker']
        result = self.invoke(bundle)
        self.assertEqual(result.returncode, 1)
        self.assertIn('event 2', result.stdout)

    def test_external_checkpoint_rejects_self_consistent_old_prefix(self):
        current = self.bundle(3)
        old = self.bundle(2)
        self.assertEqual(self.invoke(old).returncode, 0)
        result = self.invoke(old, current['checkpoint'])
        self.assertEqual(result.returncode, 1)
        self.assertIn('truncation', result.stdout)


if __name__ == '__main__':
    unittest.main()
