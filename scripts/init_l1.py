"""Initialize an isolated L1 deployment; never copy ordinary stack auth keys.

Only synthetic connector read-only credentials/config are reused explicitly.
Existing L1 keys/config are preserved. Private values are never printed.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--connector-secrets', type=Path, required=True)
parser.add_argument('--runtime', type=Path, default=Path('.runtime/l1'))
args = parser.parse_args()
args.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
env = args.runtime / 'deployment.env'
if not env.exists():
    seed = base64.b64encode(secrets.token_bytes(32)).decode()
    public = subprocess.run(['docker', 'run', '--rm', '-i', 'etl-agent-aliyun-api:3.22.2', 'python', '-c',
        'import sys,base64; from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey; '
        'print(base64.b64encode(Ed25519PrivateKey.from_private_bytes(base64.b64decode(sys.stdin.read())).public_key().public_bytes_raw()).decode())'],
        input=seed, text=True, capture_output=True, check=True).stdout.strip()
    env.write_text('POSTGRES_PASSWORD=' + secrets.token_urlsafe(32) + '\nEVIDENCE_SIGNING_KEY=' + seed +
        '\nSECRET_MASTER_KEY=' + base64.b64encode(secrets.token_bytes(32)).decode() + '\nCOMMAND_PUBLIC_KEY=' + public + '\n', encoding='utf-8')
    os.chmod(env, 0o600)
config = json.loads(args.connector_secrets.read_text(encoding='utf-8'))
policy = args.runtime / 'policy'
policy.mkdir(exist_ok=True)
resources = {}
for kind, port in (('mysql', 13306), ('doris', 19030)):
    resource = {key: config[kind][key] for key in ('database', 'username')}
    resource.update(kind=kind, host='host.docker.internal', port=port,
                    tables=config[kind].get('tables', ['customers', 'orders']))
    if kind == 'doris':
        resource['physical_cluster_id'] = 'l1-synthetic-doris'
    resources['acceptance-' + kind] = resource
resources['doris-alias'] = dict(resources['acceptance-doris'])
resources_file = policy / 'resources.json'
if not resources_file.exists():
    resources_file.write_text(json.dumps(resources, indent=2), encoding='utf-8')
fixture = args.runtime / 'fixture'
fixture.mkdir(exist_ok=True)
if not (fixture / 'fixture.json').exists():
    (fixture / 'fixture.json').write_text('{}\n', encoding='utf-8')
(args.runtime / 'faults').mkdir(exist_ok=True)
print('L1 deployment initialized; isolated keys/config under ' + str(args.runtime) + '. No secrets printed.')
