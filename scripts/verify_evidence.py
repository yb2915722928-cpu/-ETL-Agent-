"""Independent offline verifier. Requires only Python and cryptography, never app imports/network.

Usage: python scripts/verify_evidence.py evidence.json --public-key trusted-key.json
       [--checkpoint independently-saved-checkpoint.json]
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

GENESIS = '0' * 64


class VerificationError(ValueError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def verify_checkpoint(checkpoint, key, key_id):
    if checkpoint['format'] != 'etl-evidence-checkpoint-v1':
        raise VerificationError('unsupported checkpoint format')
    if checkpoint['key_id'] != key_id:
        raise VerificationError('checkpoint signing key does not match trusted key')
    payload = {name: value for name, value in checkpoint.items() if name != 'signature'}
    try:
        key.verify(base64.b64decode(checkpoint['signature'], validate=True), canonical(payload))
    except InvalidSignature:
        raise VerificationError('checkpoint signature invalid') from None


def verify(bundle, trusted, external=None):
    if bundle['format'] != 'etl-evidence-v1':
        raise VerificationError('unsupported export format')
    public = base64.b64decode(trusted['public_key'], validate=True)
    key_id = hashlib.sha256(public).hexdigest()
    if trusted['key_id'] != key_id:
        raise VerificationError('trusted public key fingerprint mismatch')
    key = Ed25519PublicKey.from_public_bytes(public)
    checkpoint = bundle['checkpoint']
    verify_checkpoint(checkpoint, key, key_id)
    previous = GENESIS
    events = bundle['events']
    for position, event in enumerate(events, 1):
        prefix = f'event {position}'
        if event['sequence'] != position:
            raise VerificationError(f'{prefix}: sequence missing or reordered')
        if event['tenant_id'] != checkpoint['tenant_id'] or (checkpoint['project_id'] is not None and event['project_id'] != checkpoint['project_id']):
            raise VerificationError(f'{prefix}: ownership mismatch')
        if event['previous_hash'] != previous:
            raise VerificationError(f'{prefix}: previous hash mismatch')
        payload = {name: value for name, value in event.items() if name != 'event_hash'}
        if hashlib.sha256(canonical(payload)).hexdigest() != event['event_hash']:
            raise VerificationError(f'{prefix}: event hash mismatch')
        previous = event['event_hash']
    if checkpoint['sequence'] != len(events) or checkpoint['event_hash'] != previous:
        raise VerificationError('checkpoint head mismatch: missing events or tail truncation')
    if external is not None:
        verify_checkpoint(external, key, key_id)
        if (external['tenant_id'], external['project_id']) != (checkpoint['tenant_id'], checkpoint['project_id']):
            raise VerificationError('external checkpoint belongs to a different chain')
        if external['sequence'] > len(events):
            raise VerificationError('tail truncation: export ends before independently saved checkpoint')
        external_hash = events[external['sequence'] - 1]['event_hash'] if external['sequence'] else GENESIS
        if external['event_hash'] != external_hash:
            raise VerificationError('external checkpoint prefix mismatch')
    return {'valid': True, 'events': len(events), 'tenant_id': checkpoint['tenant_id'],
            'project_id': checkpoint['project_id'], 'key_id': key_id,
            'external_checkpoint_checked': external is not None,
            'completeness': 'verified through independently saved checkpoint' if external is not None
                            else 'signed prefix only; tail deletion requires an independently saved checkpoint'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('export', type=Path)
    parser.add_argument('--public-key', required=True, type=Path, help='Previously trusted public key JSON; never trust a key bundled with the export')
    parser.add_argument('--checkpoint', type=Path, help='Signed checkpoint previously saved outside platform control')
    args = parser.parse_args()
    try:
        result = verify(json.loads(args.export.read_text(encoding='utf-8')),
                        json.loads(args.public_key.read_text(encoding='utf-8')),
                        json.loads(args.checkpoint.read_text(encoding='utf-8')) if args.checkpoint else None)
    except (ValueError, KeyError, TypeError, OSError, IndexError) as error:
        # Do not echo malformed event content or filesystem paths.
        detail = str(error) if isinstance(error, VerificationError) else 'malformed export, checkpoint or trusted key'
        print(json.dumps({'valid': False, 'error': detail}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
