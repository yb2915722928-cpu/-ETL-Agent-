#!/bin/bash
set -euo pipefail
export JAVA_HOME=/opt/java
export PATH="$JAVA_HOME/bin:$PATH"
export SKIP_CHECK_ULIMIT=true
unset http_proxy HTTP_PROXY https_proxy HTTPS_PROXY
cd /opt/apache-doris
# Fixed per-container address, no host VM sysctl mutation.
ip=${DORIS_NETWORK_IP:?DORIS_NETWORK_IP must match the stable isolated-network address}
mkdir -p fe/doris-meta be/storage
if ! test -f .connector-configured; then
sed -i -E 's/-Xmx[0-9]+[mMgG]/-Xmx1024m/g; s/-Xms[0-9]+[mMgG]/-Xms512m/g' fe/conf/fe.conf
sed -i -E 's/-Xmx[0-9]+[mMgG]/-Xmx512m/g; s/-Xms[0-9]+[mMgG]/-Xms128m/g' be/conf/be.conf
cat >> fe/conf/fe.conf <<EOF
priority_networks = ${ip}/32
EOF
cat >> be/conf/be.conf <<EOF
priority_networks = ${ip}/32
mem_limit = 1536M
EOF
: > .connector-configured
fi
# PID files belong to the previous container PID namespace after a restart.
rm -f /opt/apache-doris/fe/bin/fe.pid /opt/apache-doris/be/bin/be.pid
fe/bin/start_fe.sh --daemon
# Small synthetic fixture uses official SKIP_CHECK_ULIMIT instead of global host sysctl mutation.
be/bin/start_be.sh --daemon
for attempt in $(seq 1 60); do
  if test -s fe/bin/fe.pid && test -s be/bin/be.pid; then break; fi
  sleep 1
done
trap 'be/bin/stop_be.sh || true; fe/bin/stop_fe.sh || true; exit 0' TERM INT
while true; do
  if ! kill -0 "$(cat fe/bin/fe.pid)" 2>/dev/null; then echo 'FE exited'; exit 1; fi
  if ! kill -0 "$(cat be/bin/be.pid)" 2>/dev/null; then echo 'BE exited'; exit 1; fi
  sleep 5 & wait $!
done
