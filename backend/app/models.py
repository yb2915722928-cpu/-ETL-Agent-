import uuid
from datetime import datetime

from sqlalchemy import (
    ARRAY, BigInteger, Boolean, CheckConstraint, DateTime, ForeignKey,
    ForeignKeyConstraint, Integer, JSON, String, UniqueConstraint, func,
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


class SchemaMigration(Base):
    __tablename__ = 'schema_migrations'
    name: Mapped[str] = mapped_column(String(100), primary_key=True)
