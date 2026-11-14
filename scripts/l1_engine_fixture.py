"""Isolated L1 HTTP engine. SQLite survives worker/fixture restarts.

Not a SeaTunnel emulator or a production API. Startup-only fixture configuration
selects response/discovery faults. Every POST creates a NEW job deliberately:
the application, never this fixture, must prevent duplicate submission.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import sqlite3
import time
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

ledger = os.environ.get('L1_LEDGER', '/data/jobs.sqlite3')
config = json.loads(Path(os.environ.get('L1_FIXTURE_CONFIG', '/config/fixture.json')).read_text(encoding='utf-8'))
with sqlite3.connect(ledger) as db:
    db.execute('CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, correlation_id TEXT, request_sha256 TEXT, request TEXT, created_at REAL)')


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, value):
        data = json.dumps(value).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/health':
            return self.reply({'level': 'L1', 'durable_ledger': True})
        if parsed.path not in ('/jobs', '/ledger'):
            return self.send_error(404)
        with sqlite3.connect(ledger) as db:
            rows = db.execute('SELECT id,correlation_id,request_sha256,request,created_at FROM jobs ORDER BY created_at').fetchall()
        correlation = parse_qs(parsed.query).get('correlation_id', [None])[0]
        jobs = [{'id': r[0], 'correlation_id': r[1], 'request_sha256': r[2],
                 'request': json.loads(r[3]), 'created_at': r[4]} for r in rows if correlation is None or r[1] == correlation]
        if parsed.path == '/jobs':
            jobs = [job for job in jobs if config.get(job['request']['target']['table'], 'normal') != 'unknown']
        self.reply({'jobs': jobs})

    def do_POST(self):
        if self.path != '/jobs':
            return self.send_error(404)
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        request = body['request']
        job = {'id': str(uuid4()), 'correlation_id': request['correlation_id'], 'request_sha256': body['request_sha256']}
        with sqlite3.connect(ledger) as db:
            db.execute('INSERT INTO jobs VALUES (?,?,?,?,?)', (job['id'], job['correlation_id'], job['request_sha256'],
                       json.dumps(request), time.time()))
        mode = config.get(request['target']['table'], 'normal')
        if mode in ('withhold', 'unknown'):
            time.sleep(30)
        if mode in ('withhold', 'unknown', 'lose_response'):
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        self.reply(job)


ThreadingHTTPServer(('0.0.0.0', 8011), Handler).serve_forever()
