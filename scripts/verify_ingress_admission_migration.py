"""Exercise additive observation admission migration on an old real PG schema."""
import json
import os
import uuid
from unittest.mock import patch

from sqlalchemy import create_engine, text

from app import main, migrations
from app.db import Base


def verify():
    name = 'ingress_admission_migration_' + uuid.uuid4().hex
    admin = create_engine(os.environ['DATABASE_URL'], hide_parameters=True)
    with admin.begin() as db:
        db.execute(text('CREATE SCHEMA ' + name))
    engine = create_engine(os.environ['DATABASE_URL'], hide_parameters=True,
                           connect_args={'options': '-csearch_path=' + name})
    try:
        with engine.begin() as db:
            Base.metadata.create_all(db)
            db.execute(text('ALTER TABLE real_ingress_bindings DROP COLUMN last_complete_observed_at'))
        with patch.object(migrations, 'engine', engine):
            migrations.initialize_database()
            migrations.initialize_database()
        with engine.connect() as db:
            assert db.scalar(text("SELECT count(*) FROM information_schema.columns WHERE table_schema=:s "
                                  "AND table_name='real_ingress_bindings' AND column_name='last_complete_observed_at'"),
                             {'s': name}) == 1
            assert db.scalar(text("SELECT count(*) FROM schema_migrations "
                                  "WHERE name='v13-ingress-observation-admission'")) == 1
        print(json.dumps({'level': 'postgresql_additive_migration', 'idempotent': True,
                          'real_l2_qualified': False}))
    finally:
        engine.dispose()
        with admin.begin() as db:
            db.execute(text('DROP SCHEMA ' + name + ' CASCADE'))
        admin.dispose()


if __name__ == '__main__':
    verify()
