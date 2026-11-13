"""Fetch official fixed Doris 3.0.8 via Aliyun, with resumable ranges and SHA512 check."""
import argparse
import concurrent.futures
import hashlib
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--runtime', type=Path, default=Path('.runtime/connectors'))
args = parser.parse_args()
base = args.runtime.resolve() / 'downloads'
base.mkdir(parents=True, exist_ok=True)
name = 'apache-doris-3.0.8-bin-x64.tar.gz'
url = 'https://apache-doris-releases.oss-accelerate.aliyuncs.com/' + name
size = 4278678794
chunk = 64 * 1024 * 1024
curl = shutil.which('curl.exe') or shutil.which('curl')


def part(index):
    lo, hi = index * chunk, min(size - 1, (index + 1) * chunk - 1)
    target = base / f'{name}.part{index:03d}'
    if target.exists() and target.stat().st_size == hi - lo + 1:
        return target
    subprocess.run([curl, '-f', '-L', '--retry', '3', '--max-time', '180', '--range', f'{lo}-{hi}', '-sS', '-o', str(target), url], check=True)
    if target.stat().st_size != hi - lo + 1:
        raise RuntimeError('Partial Doris release segment')
    return target


with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    parts = list(pool.map(part, range((size + chunk - 1) // chunk)))
archive = base / name
checksum_file = base / (name + '.sha512')
subprocess.run([curl, '-f', '-L', '--retry', '3', '-sS', '-o', str(checksum_file), url + '.sha512'], check=True)
digest = hashlib.sha512()
with archive.open('wb') as output:
    for target in parts:
        with target.open('rb') as source:
            while data := source.read(4 * 1024 * 1024):
                output.write(data)
                digest.update(data)
if digest.hexdigest().lower() not in checksum_file.read_text(encoding='utf-8').lower():
    raise RuntimeError('Doris release SHA512 mismatch')
print('Official Doris 3.0.8 archive assembled and SHA512 verified; no service or existing volume changed.')
