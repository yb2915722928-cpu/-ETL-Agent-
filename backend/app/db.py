import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker


class Base(DeclarativeBase):
    pass


# A real PostgreSQL connection is required. There is deliberately no fallback DB.
engine = create_engine(os.environ['DATABASE_URL'], pool_pre_ping=True, hide_parameters=True)
if engine.dialect.name != 'postgresql':
    raise RuntimeError('DATABASE_URL 必须使用 PostgreSQL')
Session = sessionmaker(engine, expire_on_commit=False)
