"""Generate local deployment secrets once; never print their values."""
import base64
import os
from pathlib import Path
import secrets

root = Path(__file__).resolve().parents[1]
runtime = root / '.runtime'
runtime.mkdir(exist_ok=True, mode=0o700)
env = root / '.env'
if not env.exists():
    with env.open('x', encoding='utf-8') as handle:
        os.chmod(env, 0o600)
        handle.write('POSTGRES_PASSWORD=' + secrets.token_urlsafe(32) + '\nCOOKIE_SECURE=false\n')
# Read only to preserve deployment configuration; never log environment contents.
existing = env.read_text(encoding='utf-8')
if not any(line.startswith('EVIDENCE_SIGNING_KEY=') for line in existing.splitlines()):
    with env.open('a', encoding='utf-8') as handle:
        os.chmod(env, 0o600)
        handle.write(('' if existing.endswith('\n') else '\n') + 'EVIDENCE_SIGNING_KEY='
                     + base64.b64encode(secrets.token_bytes(32)).decode() + '\n')
print('Local .env and private .runtime directory ready; existing secrets and evidence signing key preserved.')
