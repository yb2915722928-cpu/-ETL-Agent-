"""Standalone polling Outbox publisher; no Beat and no private signing key."""
import time
from sqlalchemy import select
from .celery_app import execute
from .db import Session
from .evidence import lock_tenant
from .execution import ExecutionRun, ManagedCommand, Outbox
from .execution_runtime import command_lock, record


def publish_once(after_publish=None):
    with Session() as db:
        ids = list(db.scalars(select(Outbox.command_id).where(Outbox.status == 'pending').order_by(Outbox.created_at).limit(100)))
    for command_id in ids:
        # Publisher and driver deliberately use separate advisory lock domains.
        with command_lock('outbox:' + command_id) as acquired:
            if not acquired:
                continue
            with Session.begin() as db:
                command = db.get(ManagedCommand, command_id)
                lock_tenant(db, command.tenant_id)
                outbox = db.scalar(select(Outbox).where(Outbox.command_id == command_id).with_for_update())
                if outbox.status != 'pending':
                    continue
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
    return len(ids)


def main():
    while True:
        publish_once()
        time.sleep(1)


if __name__ == '__main__':
    main()
