"""Trusted host observer for the real adapter's network isolation receipt.

The managed worker has no Docker access. This daemon is the only component
that inspects the live containers, and it only reads: it never creates,
restarts or reconfigures anything. It writes the witness file the worker's
default network gate consumes, atomically, once per second.

Usage:
    python scripts/watch_real_network_witness.py \
        --receipt .runtime/real-network-<token>/network-receipt.json \
        --witness .runtime/real-network-<token>/witness.json

Stop with Ctrl-C. If the daemon stops, the witness goes stale within two
seconds and every real submission is denied again.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))

from app.real_network import (LIVE_MAX_AGE_SECONDS, canonical, observe_containers,
                              read_witness, static_receipt, witness_record)  # noqa: E402


def anchor_path_for(witness):
    return witness.with_name(witness.name + '.anchor')


def load_anchor(path, receipt, live, now):
    """The first observation of this receipt becomes the restart baseline."""
    if path.exists():
        try:
            anchor = json.loads(path.read_text(encoding='utf-8'))
            if anchor.get('receipt_sha256') == witness_record(receipt, live, live, now)['receipt_sha256']:
                return {role: {key: proof[key] for key in ('container_id', 'started_at', 'restart_count')}
                        for role, proof in anchor['live'].items()}
        except (OSError, UnicodeDecodeError, ValueError, KeyError, TypeError):
            pass
    anchor = {role: {key: proof[key] for key in ('container_id', 'started_at', 'restart_count')}
              for role, proof in live.items()}
    path.write_text(json.dumps({'receipt_sha256': witness_record(receipt, live, live, now)['receipt_sha256'],
                                'live': anchor}, sort_keys=True), encoding='utf-8')
    return anchor


def observe_once(receipt, witness, anchor_file, now=None):
    now = now or datetime.now(timezone.utc)
    live = observe_containers(receipt)
    anchor = load_anchor(anchor_file, receipt, live, now)
    record = witness_record(receipt, live, anchor, now)
    temporary = witness.with_name(witness.name + '.tmp')
    temporary.write_bytes(canonical(record))
    temporary.replace(witness)  # Readers never see a partial witness.
    read_witness(str(witness), receipt, now)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--witness', type=Path, required=True)
    parser.add_argument('--interval', type=float, default=0.2)
    parser.add_argument('--once', action='store_true')
    arguments = parser.parse_args()
    raw = arguments.receipt.read_bytes()
    entry = {'network_receipt_sha256': __import__('hashlib').sha256(raw).hexdigest()}
    receipt = static_receipt(entry, raw)
    arguments.witness.parent.mkdir(parents=True, exist_ok=True)
    anchor_file = anchor_path_for(arguments.witness)
    while True:
        record = observe_once(receipt, arguments.witness, anchor_file)
        print(json.dumps({'observed_at': record['observed_at'], 'stale_after_seconds': LIVE_MAX_AGE_SECONDS}),
              flush=True)
        if arguments.once:
            return 0
        time.sleep(max(arguments.interval, 0.2))


if __name__ == '__main__':
    raise SystemExit(main())
