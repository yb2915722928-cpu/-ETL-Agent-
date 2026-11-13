"""Generate local deployment secrets once; never print their values."""
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
print('Local .env and private .runtime directory ready; existing secrets preserved.')
