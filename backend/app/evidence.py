"""Transactional EvidenceEvent append and complete, independently scoped chains."""
from datetime import timezone
import hashlib
import json

from sqlalchemy import select

from .models import EvidenceEvent, EvidenceHead, IdentityEvent, Tenant, new_id

GENESIS = '0' * 64


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def event_view(event):
    return {'id': event.id, 'tenant_id': event.tenant_id, 'project_id': event.project_id,
            'sequence': event.sequence, 'occurred_at': event.occurred_at.astimezone(timezone.utc).isoformat(),
            'actor_id': event.actor_id, 'action': event.action, 'facts': event.facts,
            'version_id': event.version_id, 'preparation_id': event.preparation_id, 'run_id': event.run_id,
            'artifact_hashes': event.artifact_hashes, 'previous_hash': event.previous_hash, 'event_hash': event.event_hash}


def lock_tenant(db, tenant_id):
    # One lock order for identity allocation, project projections and exports.
    return db.scalar(select(Tenant).where(Tenant.id == tenant_id).with_for_update().execution_options(populate_existing=True))


def project_identity_event(db, source, *, version_id=None, preparation_id=None, run_id=None, artifact_hashes=None):
    """Caller holds tenant lock. Tenant chain contains all facts; project chain only its own."""
    for scope in ['tenant'] + ([source.project_id] if source.project_id else []):
        head = db.get(EvidenceHead, (source.tenant_id, scope))
        if head is None:
            head = EvidenceHead(tenant_id=source.tenant_id, scope=scope, sequence=0, event_hash=GENESIS)
            db.add(head)
        event = EvidenceEvent(id=new_id(), tenant_id=source.tenant_id, scope=scope,
                              project_id=source.project_id, source_event_id=source.id,
                              sequence=head.sequence + 1, occurred_at=source.occurred_at,
                              actor_id=source.actor_id, action=source.action, facts=source.facts,
                              version_id=version_id, preparation_id=preparation_id, run_id=run_id,
                              artifact_hashes=artifact_hashes or {}, previous_hash=head.event_hash, event_hash='')
        payload = event_view(event)
        del payload['event_hash']
        event.event_hash = hashlib.sha256(canonical(payload)).hexdigest()
        head.sequence = event.sequence
        head.event_hash = event.event_hash
        db.add(event)
        db.flush()


def append_event(db, actor, action: str, facts: dict, project_id: str | None = None, *,
                 version_id: str | None = None, preparation_id: str | None = None,
                 run_id: str | None = None, artifact_hashes: dict | None = None):
    """Append within caller's transaction; never commit. Rollback removes business and evidence facts.

    facts/artifact_hashes are explicit safe metadata, never request bodies, secrets,
    connection strings or raw samples. Project must belong to actor's tenant;
    callers authorize the operation before calling this seam.
    """
    tenant = lock_tenant(db, actor.tenant_id)
    if project_id:
        from .models import Project
        project = db.get(Project, project_id)
        if project is None or project.tenant_id != tenant.id:
            raise ValueError('Evidence project must belong to actor tenant')
    tenant.evidence_seq += 1
    source = IdentityEvent(tenant_id=tenant.id, project_id=project_id, sequence=tenant.evidence_seq,
                           actor_id=actor.id, action=action, facts=facts)
    db.add(source)
    db.flush()
    project_identity_event(db, source, version_id=version_id, preparation_id=preparation_id,
                           run_id=run_id, artifact_hashes=artifact_hashes)
    return source


def complete_chain(db, tenant_id, project_id=None):
    lock_tenant(db, tenant_id)
    scope = project_id or 'tenant'
    events = list(db.scalars(select(EvidenceEvent).where(EvidenceEvent.tenant_id == tenant_id,
                           EvidenceEvent.scope == scope).order_by(EvidenceEvent.sequence)))
    head = db.get(EvidenceHead, (tenant_id, scope))
    return [event_view(event) for event in events], head.sequence if head else 0, head.event_hash if head else GENESIS
