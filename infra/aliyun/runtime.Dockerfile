FROM etl-agent-aliyun-base:3.22.2 AS postgres
RUN apk add --no-cache postgresql17 postgresql17-contrib
COPY infra/aliyun/postgres.sh /usr/local/bin/start-postgres
RUN chmod +x /usr/local/bin/start-postgres
ENV PGDATA=/var/lib/postgresql/data
ENTRYPOINT ["/usr/local/bin/start-postgres"]

FROM etl-agent-aliyun-base:3.22.2 AS redis
RUN apk add --no-cache redis
USER redis
CMD ["redis-server", "--appendonly", "yes", "--dir", "/data"]

FROM etl-agent-aliyun-base:3.22.2 AS api
RUN apk add --no-cache python3 py3-pip libpq && python3 -m venv /venv
ENV PATH=/venv/bin:$PATH PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY backend/requirements.txt .
RUN apk add --no-cache --virtual .build-deps gcc musl-dev python3-dev linux-headers \
    && pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ -r requirements.txt \
    && apk del .build-deps
COPY backend/app ./app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

FROM etl-agent-aliyun-base:3.22.2 AS frontend-build
RUN apk add --no-cache nodejs npm
WORKDIR /web
COPY frontend/package*.json ./
RUN npm ci --registry=https://registry.npmmirror.com --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM etl-agent-aliyun-base:3.22.2 AS web
RUN apk add --no-cache nginx
COPY frontend/nginx.conf /etc/nginx/http.d/default.conf
COPY --from=frontend-build /web/dist /usr/share/nginx/html
CMD ["nginx", "-g", "daemon off;"]
