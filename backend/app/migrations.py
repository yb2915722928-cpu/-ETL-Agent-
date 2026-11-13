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
            if db.get(SchemaMigration, 'v04-managed-csv') is None:
                connection.execute(text('ALTER TABLE connections ADD COLUMN IF NOT EXISTS storage_id varchar(36), '
                                        'ADD COLUMN IF NOT EXISTS source_sha256 varchar(64), '
                                        'ADD COLUMN IF NOT EXISTS source_bytes integer'))
                for table in ('connections', 'metadata_profiles'):
                    connection.execute(text(f'ALTER TABLE {table} ALTER COLUMN secret_reference_id DROP NOT NULL, '
                                            'ALTER COLUMN secret_version DROP NOT NULL'))
                exists = connection.scalar(text("SELECT 1 FROM pg_constraint WHERE conname='connection_source_shape' AND conrelid='connections'::regclass"))
                if not exists:
                    connection.execute(text("ALTER TABLE connections ADD CONSTRAINT connection_source_shape CHECK "
                        "((kind='csv' AND secret_reference_id IS NULL AND secret_version IS NULL AND storage_id IS NOT NULL "
                        "AND source_sha256 IS NOT NULL AND source_bytes IS NOT NULL) OR (kind<>'csv' "
                        "AND secret_reference_id IS NOT NULL AND secret_version IS NOT NULL AND storage_id IS NULL))"))
                db.add(SchemaMigration(name='v04-managed-csv'))
                db.flush()
