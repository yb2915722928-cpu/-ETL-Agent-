import base64
import hashlib
import hmac
import os

import redis
from .evidence import append_event  # compatibility for existing identity callers

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
