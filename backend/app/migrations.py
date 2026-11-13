"""Additive V02 migration: existing identity rows, IDs and sequence remain intact."""
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .db import Base, engine
from .evidence import lock_tenant, project_identity_event
from .models import IdentityEvent, SchemaMigration, Tenant


def initialize_database():
    with engine.begin() as connection:
        # Serialize API workers/bootstrap; DDL and backfill commit atomically.
        connection.execute(text('SELECT pg_advisory_xact_lock(31702003)'))
        Base.metadata.create_all(connection)
        with Session(bind=connection) as db:
            if db.get(SchemaMigration, 'v02-evidence-chains') is None:
                for tenant_id in db.scalars(select(Tenant.id).order_by(Tenant.id)):
                    lock_tenant(db, tenant_id)
                    for event in db.scalars(select(IdentityEvent).where(IdentityEvent.tenant_id == tenant_id)
                                            .order_by(IdentityEvent.sequence)):
                        project_identity_event(db, event)
                db.add(SchemaMigration(name='v02-evidence-chains'))
                db.flush()
