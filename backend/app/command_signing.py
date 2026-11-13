"""Domain-separated Ed25519 envelopes. Verification needs only a trusted public key.

Workers may import this module without DATABASE_URL, application startup, or any
private signing environment variable. Never trust a key supplied in an envelope.
"""
import base64
import hashlib
import json

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

CAPABILITY_FORMAT = 'etl-capability-v1'
COMMAND_FORMAT = 'etl-managed-command-v1'


def signing_bytes(payload):
    return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def sign_envelope(signer, payload):
    payload = {**payload, 'key_id': signer.public['key_id']}
    return {'payload': payload, 'signature': base64.b64encode(signer.key.sign(signing_bytes(payload))).decode()}


def verify_envelope(envelope, trusted_public_key, expected_format):
    """Return verified payload or raise ValueError; trusted_public_key is raw base64.

    This verifies signature and format, not current roles/resources or execution
    eligibility. The accepting service/worker must enforce those separately.
    ManagedCommand deliberately has no Capability TTL or replay-consumption step.
    """
    try:
        public = base64.b64decode(trusted_public_key, validate=True)
        payload = envelope['payload']
        if payload['format'] != expected_format or payload['key_id'] != hashlib.sha256(public).hexdigest():
            raise ValueError('签名格式或可信密钥不匹配')
        Ed25519PublicKey.from_public_bytes(public).verify(
            base64.b64decode(envelope['signature'], validate=True), signing_bytes(payload))
    except (KeyError, TypeError, ValueError, InvalidSignature):
        raise ValueError('签名、格式或可信密钥核验失败') from None
    return payload
