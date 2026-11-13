"""Build the proven genuine Doris runtime and initialize only an EMPTY isolated volume.

This reuses pinned local Ubuntu/PG layers (see infra/connectors/README.md),
fetches Java from official Aliyun OSS and verifies both release checksums.
Does not start/recreate a service or change host sysctls. Never replaces data.
"""
import argparse
import hashlib
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
IMAGE = 'etl-connectors-doris-runtime:3.0.8-jdk17.0.20'
BASE_IMAGE = 'docker-elasticsearch@sha256:611391cdbf688c003bf01df72748a6e655541908ca4ac7e50a1a6c84cd9d4ff2'
PG_IMAGE = 'etl-connectors-postgres:17.11-126f44c116f1'
PG_ID = 'sha256:126f44c116f1541d3c274246bf6193b3247b2cb66d3549b7b11b890d132b4611'
JAVA_SHA256 = '656a2aa3334082a35a222ac1cfabc6979d89257993d2ba6034ad2fa65e26fc9b'
DORIS_SHA512 = 'e4441aaf845d9e95ebefa51159316b6248f039658db2e24d0e378f56cf5b94f433a841f52a3b68c651899e214f49b75cac0e49e7ae6e53ed8a166d51dbb09b1f'
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--runtime', type=Path, default=Path('.runtime/connectors'))
args = parser.parse_args()
base = args.runtime.resolve()
downloads = base / 'downloads'
downloads.mkdir(parents=True, exist_ok=True)
java = downloads / 'dragonwell-17.0.20.tar.gz'
archive = downloads / 'apache-doris-3.0.8-bin-x64.tar.gz'


def digest(path, algorithm):
    result = hashlib.new(algorithm)
    with path.open('rb') as handle:
        while block := handle.read(4 * 1024 * 1024):
            result.update(block)
    return result.hexdigest()


if not archive.exists():
    raise SystemExit('Download the fixed Doris release first: python scripts/download_connector_doris.py')
if digest(archive, 'sha512') != DORIS_SHA512:
    raise SystemExit('Doris release does not match pinned SHA512')
if not java.exists():
    curl = shutil.which('curl.exe') or shutil.which('curl')
    subprocess.run([curl, '-f', '-L', '--retry', '3', '-o', str(java), 'https://dragonwell.oss-cn-shanghai.aliyuncs.com/17.0.20.0.21%2B8/Alibaba_Dragonwell_Standard_17.0.20.0.21.8_x64_linux.tar.gz'], check=True)
if digest(java, 'sha256') != JAVA_SHA256:
    raise SystemExit('Dragonwell release does not match pinned SHA256')
actual_pg = subprocess.check_output(['docker', 'image', 'inspect', PG_IMAGE, '--format', '{{.Id}}'], text=True).strip()
if actual_pg != PG_ID:
    raise SystemExit('Pinned local PostgreSQL layer required for verified timezone files; see infrastructure README')
subprocess.run(['docker', 'image', 'inspect', BASE_IMAGE, '--format', '{{.Id}}'], check=True, stdout=subprocess.DEVNULL)
context = base / 'doris-image'
context.mkdir(exist_ok=True)
shutil.copyfile(java, context / java.name)
subprocess.run(['docker', 'build', '-t', IMAGE, '-f', str(ROOT / 'infra/connectors/doris.Dockerfile'), str(context)], check=True)
subprocess.run(['docker', 'volume', 'create', 'etl-connectors_doris_home'], check=True, stdout=subprocess.DEVNULL)
# The shell checks volume emptiness before any extraction, including hidden files.
# An existing installation is reused; partial/unknown volume contents fail closed.
script = '''set -eu
if test -f /opt/apache-doris/fe/bin/start_fe.sh && test -f /opt/apache-doris/be/bin/start_be.sh; then
 echo 'Existing Doris installation retained; no archive extraction'
elif test -z "$(ls -A /opt/apache-doris)"; then
 tar -xzf /downloads/apache-doris-3.0.8-bin-x64.tar.gz -C /opt/apache-doris --strip-components=1
else
 echo 'Refusing to overwrite nonempty incomplete Doris volume' >&2
 exit 1
fi
'''
subprocess.run(['docker', 'run', '--rm', '--user', '0', '--entrypoint', 'bash',
                '--mount', f'type=bind,src={downloads},dst=/downloads,readonly',
                '--mount', 'type=volume,src=etl-connectors_doris_home,dst=/opt/apache-doris', BASE_IMAGE, '-c', script], check=True)
print('Genuine fixed Doris runtime ready; existing initialized volume was preserved. Start the tracked Compose then run prepare_connector_doris.py.')
