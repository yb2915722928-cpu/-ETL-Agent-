"""Real PG scheduler/migration evidence; broker calls are spies, never L2."""
import json
from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import func, select, text

from app import main, migrations, outbox_publisher as publisher
from app.execution import ExecutionRun, ManagedCommand, Outbox
from app.models import SchemaMigration
from app.publication import PublicationIntent
from app.reconciliation import ReconciliationRecord
from app.run_control import RunControl
from verify_real_runtime_fixtures import database, seed


@contextmanager
def lock_spy(key):
    # The dispatch transaction remains real PG. Only advisory routing is replaced
    # to avoid consulting the public schema through the module's global engine.
    yield True


def verify():
    with database() as (sessions, signer):
        with sessions() as db:
            engine = db.get_bind()
        # Bootstrap baseline migrations before application events are projected.
        with patch.object(migrations, 'engine', engine):
            migrations.initialize_database()
        command_id, *_ = seed(sessions, signer, b'fixture-receipt')
        intent_id, record_id = str(uuid4()), str(uuid4())
        with sessions.begin() as db:
            command = db.get(ManagedCommand, command_id)
            now = db.scalar(select(func.clock_timestamp()))
            control = db.get(RunControl, command_id)
            control.wake_at = control.reconciliation_wake_at = now - timedelta(seconds=1)
            db.add(Outbox(id=str(uuid4()), tenant_id=command.tenant_id, project_id=command.project_id,
                command_id=command_id, status='sent', attempts=1, created_at=now))
            db.add(ReconciliationRecord(id=record_id, command_id=command_id, actor_id='operator',
                idempotency_key='fixture', request_sha256='a' * 64, envelope={}, phase='observing',
                result={}, created_at=now, updated_at=now))
            db.add(PublicationIntent(id=intent_id, command_id=command_id, run_id=command.run_id,
                idempotency_key='fixture', target_key='b' * 64, inputs={}, input_sha256='c' * 64,
                phase='unknown', created_at=now, updated_at=now, recovery_at=now - timedelta(seconds=1)))
        calls = []

        def observe_dispatch(kind, args):
            # A separate connection sees the reservation committed BEFORE broker I/O.
            with sessions() as db:
                now = db.scalar(select(func.clock_timestamp()))
                control = db.get(RunControl, command_id)
                deadline = (control.wake_at if kind == 'execute' else control.reconciliation_wake_at
                            if kind == 'reconcile' else db.get(PublicationIntent, intent_id).recovery_at)
                assert deadline > now
            calls.append((kind, args))

        with patch.object(publisher, 'Session', sessions), patch.object(publisher, 'command_lock', lock_spy), \
                patch.object(publisher.execute, 'apply_async', side_effect=lambda *, args: observe_dispatch('execute', args)), \
                patch.object(publisher.reconcile, 'apply_async', side_effect=lambda *, args: observe_dispatch('reconcile', args)), \
                patch.object(publisher.publish_snapshot, 'apply_async', side_effect=lambda *, args: observe_dispatch('publication', args)):
            assert publisher.publish_once() == 3
            assert calls == [('execute', [command_id]), ('reconcile', [command_id]),
                             ('publication', [command_id, intent_id])]
            assert publisher.publish_once() == 0
            # Lost publication broker acknowledgment: durable reservation survives;
            # once its deadline is due it can be delivered again, with the SAME IDs.
            with sessions.begin() as db:
                db.get(PublicationIntent, intent_id).recovery_at = db.scalar(select(func.clock_timestamp()))
            with patch.object(publisher.publish_snapshot, 'apply_async', side_effect=OSError('fixture broker lost ack')):
                try:
                    publisher.publish_once()
                except OSError:
                    pass
                else:
                    raise AssertionError('broker failure not exercised')
            assert publisher.publish_once() == 0
            with sessions.begin() as db:
                db.get(PublicationIntent, intent_id).recovery_at = db.scalar(select(func.clock_timestamp()))
            publisher.publish_once()
            assert calls[-1] == ('publication', [command_id, intent_id])

        # Upgrade an old schema with live pending intents; no public data is changed.
        with sessions() as db:
            engine = db.get_bind()
        with engine.begin() as db:
            db.execute(text("DELETE FROM schema_migrations WHERE name IN "
                            "('v14-reconciliation-recovery-deadline','v16-publication-recovery-deadline')"))
            db.execute(text('ALTER TABLE run_controls DROP COLUMN reconciliation_wake_at'))
            db.execute(text('ALTER TABLE publication_intents DROP COLUMN recovery_at'))
        with patch.object(migrations, 'engine', engine):
            migrations.initialize_database()
            migrations.initialize_database()
        with sessions() as db:
            assert db.get(RunControl, command_id).reconciliation_wake_at is not None
            assert db.get(PublicationIntent, intent_id).recovery_at is not None
            for name in ('v14-reconciliation-recovery-deadline', 'v16-publication-recovery-deadline'):
                assert db.scalar(select(func.count()).select_from(SchemaMigration).where(SchemaMigration.name == name)) == 1
        print(json.dumps({'level': 'postgresql_recovery_dispatch_fixture', 'real_l2_qualified': False,
            'passed': ['one command dispatches execute/reconcile/publication independently',
                       'reservations visible before broker calls; immediate repeats suppressed',
                       'lost broker acknowledgment recovers only original command/intent IDs',
                       'old-schema additive migration backfills pending intents and is idempotent']}))


if __name__ == '__main__':
    verify()
