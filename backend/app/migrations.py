"""Additive V02 migration: existing identity rows, IDs and sequence remain intact."""
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .db import Base, engine
from .evidence import lock_tenant, project_identity_event
from .models import IdentityEvent, SchemaMigration, Tenant


def initialize_database():
    from . import credential_rotation, managed_load, publication, quality_runtime, real_ingress, real_runtime, real_cleanup, reconciliation  # Register additive runtime facts.
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
            if db.get(SchemaMigration, 'v13-execution-writer-slot') is None:
                connection.execute(text('ALTER TABLE connections '
                    'ADD COLUMN IF NOT EXISTS writer_secret_reference_id varchar(36), '
                    'ADD COLUMN IF NOT EXISTS writer_secret_version integer'))
                constraints = {
                    'connection_writer_secret_fk': 'FOREIGN KEY '
                        '(writer_secret_reference_id, tenant_id, project_id, writer_secret_version) '
                        'REFERENCES secret_references (id, tenant_id, project_id, version)',
                    'connection_writer_shape': "CHECK ((writer_secret_reference_id IS NULL AND writer_secret_version IS NULL) OR "
                        "(kind='doris' AND direction='target' AND writer_secret_reference_id IS NOT NULL "
                        "AND writer_secret_version IS NOT NULL AND writer_secret_reference_id<>secret_reference_id))",
                }
                for name, definition in constraints.items():
                    exists = connection.scalar(text('SELECT 1 FROM pg_constraint WHERE conname=:name '
                                                     "AND conrelid='connections'::regclass"), {'name': name})
                    if not exists:
                        connection.execute(text(f'ALTER TABLE connections ADD CONSTRAINT {name} {definition}'))
                db.add(SchemaMigration(name='v13-execution-writer-slot'))
                db.flush()
            if db.get(SchemaMigration, 'v20-credential-rotation') is None:
                connection.execute(text('ALTER TABLE secret_references ADD COLUMN IF NOT EXISTS revoked_at timestamptz'))
                connection.execute(text('ALTER TABLE preparations ALTER COLUMN version_id DROP NOT NULL'))
                exists = connection.scalar(text("SELECT 1 FROM pg_constraint WHERE conname='preparation_version_shape' "
                                                 "AND conrelid='preparations'::regclass"))
                if not exists:
                    connection.execute(text("ALTER TABLE preparations ADD CONSTRAINT preparation_version_shape CHECK "
                        "((tool='credential.rotate' AND version_id IS NULL) OR "
                        "(tool<>'credential.rotate' AND version_id IS NOT NULL))"))
                db.add(SchemaMigration(name='v20-credential-rotation'))
                db.flush()
            if db.get(SchemaMigration, 'v13-ingress-observation-admission') is None:
                connection.execute(text('ALTER TABLE real_ingress_bindings '
                                        'ADD COLUMN IF NOT EXISTS last_complete_observed_at timestamptz'))
                db.add(SchemaMigration(name='v13-ingress-observation-admission'))
                db.flush()
            if db.get(SchemaMigration, 'v14-reconciliation-recovery-deadline') is None:
                connection.execute(text('ALTER TABLE run_controls '
                                        'ADD COLUMN IF NOT EXISTS reconciliation_wake_at timestamptz'))
                connection.execute(text("UPDATE run_controls SET reconciliation_wake_at=clock_timestamp() "
                    "WHERE EXISTS (SELECT 1 FROM reconciliation_records r WHERE r.command_id=run_controls.command_id "
                    "AND r.phase IN ('accepted','observing'))"))
                db.add(SchemaMigration(name='v14-reconciliation-recovery-deadline'))
                db.flush()
            if db.get(SchemaMigration, 'v16-publication-recovery-deadline') is None:
                connection.execute(text('ALTER TABLE publication_intents '
                                        'ADD COLUMN IF NOT EXISTS recovery_at timestamptz'))
                connection.execute(text("UPDATE publication_intents SET recovery_at=clock_timestamp() "
                                        "WHERE phase IN ('prepared','dispatching','unknown') AND recovery_at IS NULL"))
                db.add(SchemaMigration(name='v16-publication-recovery-deadline'))
                db.flush()
