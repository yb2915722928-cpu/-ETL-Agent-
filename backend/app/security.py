import base64
import hashlib
import hmac
import os

import redis
from sqlalchemy import select

from .models import IdentityEvent, Tenant

sessions = redis.Redis.from_url(os.environ['REDIS_URL'], decode_responses=True, socket_connect_timeout=3, socket_timeout=3)
COOKIE = 'etl_session'
SESSION_TTL = 8 * 60 * 60


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1)
    return 'scrypt$16384$8$1$' + base64.b64encode(salt).decode() + '$' + base64.b64encode(digest).decode()


def verify_password(password: str, encoded: str) -> bool:
    _, n, r, p, salt, expected = encoded.split('$')
    actual = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt), n=int(n), r=int(r), p=int(p))
    return hmac.compare_digest(actual, base64.b64decode(expected))


def session_key(token: str) -> str:
    return 'etl:session:' + hashlib.sha256(token.encode()).hexdigest()


def append_event(db, actor, action: str, facts: dict, project_id: str | None = None):
    """Append ordered identity facts within the caller's PostgreSQL transaction.

    The tenant row serializes sequence allocation. Callers pass explicit safe
    facts, never request bodies, password hashes, cookies or credentials.
    V02 can extend this one append seam with chain/checkpoint fields.
    """
    tenant = db.scalar(select(Tenant).where(Tenant.id == actor.tenant_id).with_for_update().execution_options(populate_existing=True))
    tenant.evidence_seq += 1
    event = IdentityEvent(tenant_id=tenant.id, project_id=project_id,
                          sequence=tenant.evidence_seq, actor_id=actor.id,
                          action=action, facts=facts)
    db.add(event)
    db.flush()
    return event
