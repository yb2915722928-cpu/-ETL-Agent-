"""Deployment supplied Ed25519 signer; no development fallback or DB-stored private key."""
import base64
from datetime import datetime, timezone
import hashlib
import os

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from .evidence import canonical


class EvidenceSigner:
    def __init__(self):
        try:
            self.key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(os.environ['EVIDENCE_SIGNING_KEY'], validate=True))
        except (KeyError, ValueError) as error:
            raise RuntimeError('EVIDENCE_SIGNING_KEY must contain a deployment supplied base64 Ed25519 32-byte private seed') from None
        public = self.key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
        self.public = {'algorithm': 'Ed25519', 'key_id': hashlib.sha256(public).hexdigest(),
                       'public_key': base64.b64encode(public).decode()}

    def checkpoint(self, tenant_id, project_id, sequence, event_hash):
        value = {'format': 'etl-evidence-checkpoint-v1', 'tenant_id': tenant_id, 'project_id': project_id,
                 'sequence': sequence, 'event_hash': event_hash,
                 'issued_at': datetime.now(timezone.utc).isoformat(), 'key_id': self.public['key_id']}
        value['signature'] = base64.b64encode(self.key.sign(canonical(value))).decode()
        return value
