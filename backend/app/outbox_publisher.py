"""Standalone polling Outbox publisher; no Beat and no private signing key."""
import time
from sqlalchemy import or_, select
from .celery_app import execute, publish_snapshot, reconcile
from .db import Session
from .evidence import lock_tenant
from .execution import ExecutionRun, ManagedCommand, Outbox
from .execution_runtime import command_lock, record


def publish_once(after_publish=None):
    from sqlalchemy import func
    from .run_control import RunControl
    from .execution_runtime import EngineAttempt
    from .reconciliation import ReconciliationRecord
    from .publication import PublicationIntent
    with Session() as db:
        pending = list(db.scalars(select(Outbox.command_id).where(Outbox.status == 'pending').order_by(Outbox.created_at).limit(100)))
        # The driver selects each attempt's original protocol before taking the
        # tenant lock. Reconciliation also retains its separate read-only task.
        recovery = list(db.scalars(select(RunControl.command_id).outerjoin(EngineAttempt,
            EngineAttempt.command_id == RunControl.command_id).where(RunControl.wake_at <= func.clock_timestamp(),
                or_(EngineAttempt.command_id.is_(None), EngineAttempt.request['adapter'].as_string().in_(('l1', 'real')))).limit(100)))
        reconciliation_ids = list(db.scalars(select(ReconciliationRecord.command_id).join(RunControl,
            RunControl.command_id == ReconciliationRecord.command_id).where(
                ReconciliationRecord.phase.in_(('accepted', 'observing')),
                RunControl.reconciliation_wake_at <= func.clock_timestamp()).distinct().limit(100)))
        publication_rows = list(db.execute(select(PublicationIntent.id, PublicationIntent.command_id).where(
            PublicationIntent.phase.in_(('prepared', 'dispatching', 'unknown')),
            PublicationIntent.recovery_at <= func.clock_timestamp()).order_by(PublicationIntent.created_at).limit(100)))
        ids = list(dict.fromkeys([*pending, *recovery]))
    for command_id in ids:
        # Publisher and driver deliberately use separate advisory lock domains.
        with command_lock('outbox:' + command_id) as acquired:
            if not acquired:
                continue
            with Session.begin() as db:
                command = db.get(ManagedCommand, command_id)
                lock_tenant(db, command.tenant_id)
                outbox = db.scalar(select(Outbox).where(Outbox.command_id == command_id).with_for_update())
                control = db.get(RunControl, command_id)
                now = db.scalar(select(func.clock_timestamp()))
                due = control and control.wake_at and control.wake_at <= now
                if outbox.status != 'pending' and not due:
                    continue
                if control:
                    from datetime import timedelta
                    control.wake_at = now + timedelta(seconds=15)
                outbox.attempts += 1
                run = db.get(ExecutionRun, command.run_id)
                record(db, run, 'outbox.publication_requested', {'command_id': command_id, 'attempt': outbox.attempts})
            execute.apply_async(args=[command_id])
            if after_publish:
                after_publish(command_id)
            with Session.begin() as db:
                lock_tenant(db, command.tenant_id)
                outbox = db.scalar(select(Outbox).where(Outbox.command_id == command_id).with_for_update())
                outbox.status = 'sent'
                record(db, db.get(ExecutionRun, command.run_id), 'outbox.sent', {'command_id': command_id})
    for command_id in reconciliation_ids:
        with command_lock('reconciliation-outbox:' + command_id) as acquired:
            if not acquired:
                continue
            with Session.begin() as db:
                command = db.get(ManagedCommand, command_id)
                lock_tenant(db, command.tenant_id)
                control = db.get(RunControl, command_id)
                now = db.scalar(select(func.clock_timestamp()))
                if control.reconciliation_wake_at is None or control.reconciliation_wake_at > now:
                    continue
                active = db.scalar(select(ReconciliationRecord.id).where(
                    ReconciliationRecord.command_id == command_id,
                    ReconciliationRecord.phase.in_(('accepted', 'observing'))).limit(1))
                if active is None:
                    control.reconciliation_wake_at = None
                    continue
                from datetime import timedelta
                control.reconciliation_wake_at = now + timedelta(seconds=15)
                record(db, db.get(ExecutionRun, command.run_id), 'reconciliation.recovery_queued',
                       {'command_id': command_id, 'read_only': True})
            # Repeated delivery can only repeat the original GET. A lost broker
            # acknowledgement leaves the durable intent due again after 15s.
            reconcile.apply_async(args=[command_id])
    for intent_id, command_id in publication_rows:
        with command_lock('publication-outbox:' + command_id) as acquired:
            if not acquired:
                continue
            with Session.begin() as db:
                command = db.get(ManagedCommand, command_id)
                lock_tenant(db, command.tenant_id)
                intent = db.scalar(select(PublicationIntent).where(
                    PublicationIntent.id == intent_id,
                    PublicationIntent.command_id == command_id).with_for_update())
                now = db.scalar(select(func.clock_timestamp()))
                if (intent is None or intent.phase not in ('prepared', 'dispatching', 'unknown')
                        or intent.recovery_at is None or intent.recovery_at > now):
                    continue
                from datetime import timedelta
                intent.recovery_at = now + timedelta(seconds=15)
                record(db, db.get(ExecutionRun, command.run_id), 'publication.recovery_queued',
                       {'intent_id': intent_id, 'read_only': intent.phase != 'prepared'})
            publish_snapshot.apply_async(args=[command_id, intent_id])
    return len(ids) + len(reconciliation_ids) + len(publication_rows)


def main():
    while True:
        publish_once()
        time.sleep(1)


if __name__ == '__main__':
    main()
