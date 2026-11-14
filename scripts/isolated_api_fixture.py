"""Disposable real PostgreSQL clone and HTTP app lifecycle for acceptance fixtures."""
from contextlib import contextmanager
import threading
import time
from uuid import uuid4

import uvicorn
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app import main
from app.connector_policy import ResourcePolicy
from app.db import Base, engine
from app.evidence_signing import EvidenceSigner
from app.managed_secrets import SecretProvider


@contextmanager
def isolated_api(prefix, port):
    schema = prefix + '_' + uuid4().hex
    isolated = create_engine(engine.url, pool_pre_ping=True, connect_args={'options': f'-csearch_path={schema}'})
    session = sessionmaker(isolated, expire_on_commit=False)
    server = thread = None
    previous_overrides = main.app.dependency_overrides.copy()
    previous_state = {key: getattr(main.app.state, key, None) for key in (
        'secret_provider', 'evidence_signer', 'connector_policy')}
    try:
        with engine.begin() as db:
            db.execute(text(f'CREATE SCHEMA {schema}'))
            Base.metadata.create_all(db.execution_options(schema_translate_map={None: schema}))
            for table in Base.metadata.sorted_tables:
                if db.scalar(text('SELECT to_regclass(:name)'), {'name': 'public.' + table.name}) is None:
                    continue  # Additive feature tables need not exist in the deployed baseline.
                columns = ','.join(f'"{column.name}"' for column in table.columns)
                db.execute(text(f'INSERT INTO {schema}."{table.name}" ({columns}) SELECT {columns} FROM public."{table.name}"'))
        with session() as db:
            assert db.scalar(text('SELECT current_schema()')) == schema
        def fixture_database():
            with session.begin() as db:
                yield db
        main.app.dependency_overrides[main.database] = fixture_database
        main.app.state.secret_provider = SecretProvider()
        main.app.state.evidence_signer = EvidenceSigner()
        main.app.state.connector_policy = ResourcePolicy()
        server = uvicorn.Server(uvicorn.Config(main.app, host='127.0.0.1', port=port,
                                lifespan='off', log_level='critical'))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(100):
            if server.started:
                break
            time.sleep(.05)
        assert server.started, 'isolated API did not start'
        yield session, schema
    finally:
        if server:
            server.should_exit = True
        if thread:
            thread.join(timeout=5)
        main.app.dependency_overrides.clear()
        main.app.dependency_overrides.update(previous_overrides)
        for key, value in previous_state.items():
            setattr(main.app.state, key, value)
        isolated.dispose()
        with engine.begin() as db:
            db.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
