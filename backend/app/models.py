import uuid
from datetime import datetime

from sqlalchemy import (
    ARRAY, BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey,
    ForeignKeyConstraint, Integer, JSON, LargeBinary, String, UniqueConstraint, func,
)
from sqlalchemy.orm import Mapped, mapped_column

from .db import Base


def new_id():
    return str(uuid.uuid4())


class Tenant(Base):
    __tablename__ = 'tenants'
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    slug: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(100))
    evidence_seq: Mapped[int] = mapped_column(BigInteger, default=0)


class Account(Base):
    __tablename__ = 'accounts'
    __table_args__ = (
        UniqueConstraint('tenant_id', 'username'),
        UniqueConstraint('id', 'tenant_id'),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(ForeignKey('tenants.id'))
    username: Mapped[str] = mapped_column(String(64))
    display_name: Mapped[str] = mapped_column(String(100))
    password_hash: Mapped[str] = mapped_column(String(256))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    tenant_admin: Mapped[bool] = mapped_column(Boolean, default=False)


class Project(Base):
    __tablename__ = 'projects'
    __table_args__ = (UniqueConstraint('tenant_id', 'slug'), UniqueConstraint('id', 'tenant_id'))
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(ForeignKey('tenants.id'))
    slug: Mapped[str] = mapped_column(String(64))
    name: Mapped[str] = mapped_column(String(100))


class Membership(Base):
    __tablename__ = 'project_memberships'
    __table_args__ = (
        ForeignKeyConstraint(['account_id', 'tenant_id'], ['accounts.id', 'accounts.tenant_id']),
        ForeignKeyConstraint(['project_id', 'tenant_id'], ['projects.id', 'projects.tenant_id']),
        CheckConstraint("roles <@ ARRAY['project_admin','maker','data_checker','security_checker','operator','auditor']::varchar[]"),
    )
    project_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    account_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(36))
    roles: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    revision: Mapped[int] = mapped_column(Integer, default=1)


class IdentityEvent(Base):
    __tablename__ = 'identity_events'
    __table_args__ = (
        UniqueConstraint('tenant_id', 'sequence'),
        ForeignKeyConstraint(['actor_id', 'tenant_id'], ['accounts.id', 'accounts.tenant_id']),
        ForeignKeyConstraint(['project_id', 'tenant_id'], ['projects.id', 'projects.tenant_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(ForeignKey('tenants.id'))
    project_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    sequence: Mapped[int] = mapped_column(BigInteger)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    actor_id: Mapped[str] = mapped_column(String(36))
    action: Mapped[str] = mapped_column(String(64))
    facts: Mapped[dict] = mapped_column(JSON)


class EvidenceHead(Base):
    __tablename__ = 'evidence_heads'
    tenant_id: Mapped[str] = mapped_column(ForeignKey('tenants.id'), primary_key=True)
    scope: Mapped[str] = mapped_column(String(36), primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, default=0)
    event_hash: Mapped[str] = mapped_column(String(64), default='0' * 64)


class EvidenceEvent(Base):
    __tablename__ = 'evidence_events'
    __table_args__ = (
        UniqueConstraint('tenant_id', 'scope', 'sequence'),
        UniqueConstraint('tenant_id', 'scope', 'source_event_id'),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(ForeignKey('tenants.id'))
    scope: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str | None] = mapped_column(ForeignKey('projects.id'), nullable=True)
    source_event_id: Mapped[str | None] = mapped_column(ForeignKey('identity_events.id'), nullable=True)
    sequence: Mapped[int] = mapped_column(BigInteger)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    actor_id: Mapped[str] = mapped_column(ForeignKey('accounts.id'))
    action: Mapped[str] = mapped_column(String(64))
    facts: Mapped[dict] = mapped_column(JSON)
    version_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    preparation_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    run_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    artifact_hashes: Mapped[dict] = mapped_column(JSON, default=dict)
    previous_hash: Mapped[str] = mapped_column(String(64))
    event_hash: Mapped[str] = mapped_column(String(64))


class SecretReference(Base):
    __tablename__ = 'secret_references'
    __table_args__ = (
        UniqueConstraint('id', 'tenant_id', 'project_id', 'version'),
        ForeignKeyConstraint(['project_id', 'tenant_id'], ['projects.id', 'projects.tenant_id']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    version: Mapped[int] = mapped_column(Integer, default=1)
    nonce: Mapped[bytes] = mapped_column(LargeBinary)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Connection(Base):
    __tablename__ = 'connections'
    __table_args__ = (
        UniqueConstraint('id', 'tenant_id', 'project_id'),
        CheckConstraint("(kind='csv' AND secret_reference_id IS NULL AND secret_version IS NULL AND storage_id IS NOT NULL AND source_sha256 IS NOT NULL AND source_bytes IS NOT NULL) OR (kind<>'csv' AND secret_reference_id IS NOT NULL AND secret_version IS NOT NULL AND storage_id IS NULL)", name='connection_source_shape'),
        ForeignKeyConstraint(['project_id', 'tenant_id'], ['projects.id', 'projects.tenant_id']),
        ForeignKeyConstraint(['secret_reference_id', 'tenant_id', 'project_id', 'secret_version'],
                             ['secret_references.id', 'secret_references.tenant_id', 'secret_references.project_id', 'secret_references.version']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    name: Mapped[str] = mapped_column(String(100))
    resource_id: Mapped[str] = mapped_column(String(64))
    kind: Mapped[str] = mapped_column(String(32))
    direction: Mapped[str] = mapped_column(String(16))
    resource_fingerprint: Mapped[str] = mapped_column(String(64))
    secret_reference_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    secret_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    storage_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    source_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default='registered')
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class MetadataProfile(Base):
    __tablename__ = 'metadata_profiles'
    __table_args__ = (
        ForeignKeyConstraint(['connection_id', 'tenant_id', 'project_id'],
                             ['connections.id', 'connections.tenant_id', 'connections.project_id']),
        ForeignKeyConstraint(['secret_reference_id', 'tenant_id', 'project_id', 'secret_version'],
                             ['secret_references.id', 'secret_references.tenant_id', 'secret_references.project_id', 'secret_references.version']),
    )
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36))
    project_id: Mapped[str] = mapped_column(String(36))
    connection_id: Mapped[str] = mapped_column(String(36))
    secret_reference_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    secret_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    resource_id: Mapped[str] = mapped_column(String(64))
    resource_fingerprint: Mapped[str] = mapped_column(String(64))
    schema_hash: Mapped[str] = mapped_column(String(64))
    content: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SchemaMigration(Base):
    __tablename__ = 'schema_migrations'
    name: Mapped[str] = mapped_column(String(100), primary_key=True)
