"""Build the local base using downloads exclusively from Aliyun mirrors."""
import hashlib
from pathlib import Path
import subprocess
import urllib.request

root = Path(__file__).resolve().parents[2]
cache = root / '.runtime' / 'aliyun'
cache.mkdir(parents=True, exist_ok=True)
filename = 'alpine-minirootfs-3.22.2-x86_64.tar.gz'
url = 'https://mirrors.aliyun.com/alpine/v3.22/releases/x86_64/' + filename
urllib.request.urlretrieve(url, cache / filename)
with urllib.request.urlopen(url + '.sha256') as response:
    expected = response.read().decode().split()[0]
assert hashlib.sha256((cache / filename).read_bytes()).hexdigest() == expected
subprocess.run(['docker', 'build', '-f', str(root / 'infra/aliyun/base.Dockerfile'), '-t',
                'etl-agent-aliyun-base:3.22.2', str(cache)], check=True)
