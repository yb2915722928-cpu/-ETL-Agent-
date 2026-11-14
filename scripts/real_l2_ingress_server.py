"""Private per-run gateway process; started by the owned namespace helper only."""
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, '/app')
private = Path('/app/l2')
environment = json.loads((private / 'environment.json').read_text(encoding='utf-8'))
os.environ.update(environment)
schema = json.loads((private / 'schema.json').read_text(encoding='utf-8'))['schema']
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.real_runtime import ingress_service
from app.real_ingress import create_ingress_app
import uvicorn
engine = create_engine(os.environ['DATABASE_URL'], hide_parameters=True,
                       connect_args={'options': '-csearch_path=' + schema})
sessions = sessionmaker(engine, expire_on_commit=False)
service = ingress_service(sessions=sessions)
app = create_ingress_app(service)
(private / 'ingress-ready').write_text('ready', encoding='ascii')
uvicorn.run(app, host='0.0.0.0', port=8001, access_log=False, log_level='critical')
