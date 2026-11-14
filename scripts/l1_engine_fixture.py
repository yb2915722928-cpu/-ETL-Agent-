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
    db.execute('CREATE TABLE IF NOT EXISTS metric_polls (job_id TEXT PRIMARY KEY, count INTEGER)')
    db.execute('CREATE TABLE IF NOT EXISTS stop_requests (request_id TEXT PRIMARY KEY, job_id TEXT, requested_at REAL)')
    db.execute('CREATE TABLE IF NOT EXISTS control_polls (job_id TEXT PRIMARY KEY, count INTEGER, terminated_at TEXT)')
    db.execute('CREATE TABLE IF NOT EXISTS effects (effect_id TEXT PRIMARY KEY, kind TEXT, result TEXT)')


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

    def current_state(self, job_id):
        """State of the job's current observation, without consuming a metrics poll."""
        with sqlite3.connect(ledger) as db:
            jobs = db.execute('SELECT id FROM jobs ORDER BY created_at').fetchall()
            index = next((i for i, job in enumerate(jobs) if job[0] == job_id), None)
            if index is None:
                return 'running'
            poll = db.execute('SELECT count FROM metric_polls WHERE job_id=?', (job_id,)).fetchone()
        sequence = config.get('__metric_scenarios', [])
        if index >= len(sequence):
            return 'running'
        sample = sequence[index][min(poll[0] if poll else 0, len(sequence[index]) - 1)]
        return sample.get('state', 'running') if isinstance(sample, dict) else 'running'

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/health':
            return self.reply({'level': 'L1', 'durable_ledger': True})
        if parsed.path == '/effects':
            with sqlite3.connect(ledger) as db:
                rows = db.execute('SELECT kind,result FROM effects').fetchall()
            return self.reply({kind: [json.loads(r[1]) for r in rows if r[0] == kind] for kind in ('stop', 'cleanup')})
        if parsed.path.startswith('/effects/'):
            with sqlite3.connect(ledger) as db:
                row = db.execute('SELECT result FROM effects WHERE effect_id=?', (parsed.path.split('/')[2],)).fetchone()
            return self.reply(json.loads(row[0])) if row else self.send_error(404)
        if parsed.path.startswith('/jobs/') and parsed.path.endswith('/status'):
            from datetime import datetime, timezone
            job_id = parsed.path.split('/')[2]
            with sqlite3.connect(ledger) as db:
                row = db.execute('SELECT correlation_id,request_sha256,request FROM jobs WHERE id=?', (job_id,)).fetchone()
                if not row:
                    return self.send_error(404)
                stopped = db.execute('SELECT 1 FROM stop_requests WHERE job_id=?', (job_id,)).fetchone()
                # A job whose current observation already reports finished is terminal on
                # its own, like a real engine job-info state; a stopped job becomes
                # terminal only after the configured stop polls.
                finished = self.current_state(job_id) == 'finished'
                db.execute('INSERT INTO control_polls VALUES (?,0,NULL) ON CONFLICT DO NOTHING', (job_id,))
                if stopped or finished:
                    db.execute('UPDATE control_polls SET count=count+1 WHERE job_id=?', (job_id,))
                count, terminated = db.execute('SELECT count,terminated_at FROM control_polls WHERE job_id=?', (job_id,)).fetchone()
                threshold = config.get('__control', {}).get('terminal_after_stop_polls', 2)
                terminal = bool((stopped or finished) and count >= threshold)
                if terminal and not terminated:
                    terminated = datetime.now(timezone.utc).isoformat()
                    db.execute('UPDATE control_polls SET terminated_at=? WHERE job_id=?', (terminated, job_id))
            request = json.loads(row[2])
            return self.reply({'id': job_id, 'correlation_id': row[0], 'request_sha256': row[1],
                'state': 'finished' if finished else ('stopped' if terminal else 'running'),
                'inflight_writes': 0 if terminal else 1,
                'terminated_at': terminated, 'unpublished_object': request.get('unpublished_object'), 'publication': 'unpublished'})
        if parsed.path.startswith('/jobs/') and parsed.path.endswith('/metrics'):
            job_id = parsed.path.split('/')[2]
            with sqlite3.connect(ledger) as db:
                jobs = db.execute('SELECT id,created_at FROM jobs ORDER BY created_at').fetchall()
                index = next((i for i, job in enumerate(jobs) if job[0] == job_id), None)
                if index is None:
                    return self.send_error(404)
                db.execute('INSERT INTO metric_polls VALUES (?,0) ON CONFLICT DO NOTHING', (job_id,))
                poll = db.execute('SELECT count FROM metric_polls WHERE job_id=?', (job_id,)).fetchone()[0]
                db.execute('UPDATE metric_polls SET count=count+1 WHERE job_id=?', (job_id,))
            scenarios = config.get('__metric_scenarios', [])
            sequence = scenarios[index] if index < len(scenarios) else [{}]
            sample = sequence[min(poll, len(sequence) - 1)]
            if sample is None:
                return self.send_error(503)
            from datetime import datetime, timezone
            result = dict(contract_version='runtime-supervision-v1', source='l1.fixture.measured', byte_kind='measured',
                counter_epoch=job_id, observed_at=datetime.now(timezone.utc).isoformat(), state='running',
                source_read=0, evaluated=0, valid=0, rejected=0, final_output=0, sink_bytes=0,
                elapsed_seconds=time.time() - jobs[index][1])
            result.update(sample)
            if '__control' in config:
                with sqlite3.connect(ledger) as db:
                    control = db.execute('SELECT count,terminated_at FROM control_polls WHERE job_id=?', (job_id,)).fetchone()
                final_at = config['__control'].get('terminal_after_stop_polls', 2) + config['__control'].get('final_after_terminal_polls', 2)
                result['state'] = 'finished' if control and control[1] and control[0] >= final_at else 'running'
            return self.reply(result)
        if parsed.path == '/stop-requests':
            with sqlite3.connect(ledger) as db:
                rows = db.execute('SELECT request_id,job_id,requested_at FROM stop_requests').fetchall()
            return self.reply({'requests': [{'request_id': r[0], 'job_id': r[1], 'requested_at': r[2]} for r in rows]})
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
        if self.path.startswith('/jobs/') and self.path.split('/')[-1] in ('stop', 'cleanup'):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            job_id, kind = self.path.split('/')[2], self.path.split('/')[-1]
            with sqlite3.connect(ledger) as db:
                if kind == 'stop':
                    db.execute('INSERT INTO stop_requests VALUES (?,?,?) ON CONFLICT DO NOTHING',
                               (body['request_id'], job_id, time.time()))
                if 'effect_id' not in body:  # Legacy #11 fixture contract.
                    return self.reply({'request_id': body['request_id'], 'accepted': True})
                row = db.execute('SELECT correlation_id,request_sha256,request FROM jobs WHERE id=?', (job_id,)).fetchone()
                if not row or body['correlation_id'] != row[0] or body['request_sha256'] != row[1] or body['unpublished_object'] != json.loads(row[2]).get('unpublished_object'):
                    return self.send_error(409)
                if kind == 'cleanup' and not db.execute('SELECT terminated_at FROM control_polls WHERE job_id=? AND terminated_at IS NOT NULL', (job_id,)).fetchone():
                    return self.send_error(409)
                result = {**body, 'accepted' if kind == 'stop' else 'cleaned': True}
                db.execute('INSERT INTO effects VALUES (?,?,?) ON CONFLICT DO NOTHING',
                           (body['effect_id'], kind, json.dumps(result)))
            mode = config.get('__control', {}).get(kind + '_response', 'normal')
            if mode == 'withhold':
                time.sleep(30)
            if mode in ('lose_response', 'withhold'):
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            return self.reply(result)
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
