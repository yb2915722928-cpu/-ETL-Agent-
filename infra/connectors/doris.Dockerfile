FROM docker-elasticsearch@sha256:611391cdbf688c003bf01df72748a6e655541908ca4ac7e50a1a6c84cd9d4ff2
USER root
RUN printf 'Types: deb\nURIs: https://mirrors.aliyun.com/ubuntu/\nSuites: noble noble-updates noble-security\nComponents: main universe\nSigned-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n' > /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get update -qq \
    && apt-get install -y --no-install-recommends gcc-14-base=14.2.0-4ubuntu2~24.04.1 libgcc-s1=14.2.0-4ubuntu2~24.04.1 libstdc++6=14.2.0-4ubuntu2~24.04.1 libgomp1=14.2.0-4ubuntu2~24.04.1 libaio1t64=0.3.113-6build1.1 libnuma1=2.0.18-1ubuntu0.24.04.1 numactl=2.0.18-1ubuntu0.24.04.1 \
    && rm -rf /var/lib/apt/lists/*
COPY dragonwell-17.0.20.tar.gz /tmp/java.tar.gz
RUN mkdir -p /opt/java && tar -xzf /tmp/java.tar.gz -C /opt/java --strip-components=1 && rm /tmp/java.tar.gz
# tzdata 2026f-r0 from the pinned local PostgreSQL image; TZif files are portable.
COPY --from=etl-connectors-postgres:17.11-126f44c116f1 /usr/share/zoneinfo /usr/share/zoneinfo
ENV JAVA_HOME=/opt/java
ENV PATH=/opt/java/bin:$PATH
WORKDIR /opt/apache-doris
ENTRYPOINT ["/bin/bash", "/run/connector/doris-start.sh"]
