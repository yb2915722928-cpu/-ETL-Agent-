"""Generate password-free deployment resources from private demo connector config."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--secrets', type=Path, default=Path('.runtime/connectors/secrets.json'))
parser.add_argument('--output', type=Path, default=Path('.runtime/connectors/resources.json'))
parser.add_argument('--kinds', default='mysql,doris,postgresql')
parser.add_argument('--csv-project', action='append', default=[], help='Explicit authorized project UUID; repeatable')
parser.add_argument('--csv-root', default='/run/identity/managed-csv', help='Absolute API-side persistent managed root')
args = parser.parse_args()
config = json.loads(args.secrets.read_text(encoding='utf-8'))
resources = {}
for kind in args.kinds.split(','):
    if kind not in ('mysql', 'doris', 'postgresql'):
        raise SystemExit('Only implemented MySQL/Doris/PostgreSQL resources are supported')
    source = config[kind]
    resources['acceptance-' + kind] = {key: source[key] for key in ('host', 'port', 'database', 'username')}
    resources['acceptance-' + kind].update(kind=kind, tables=source.get('tables', ['customers', 'orders']))
    if kind == 'postgresql':
        resources['acceptance-' + kind]['schema'] = source['schema']
if args.csv_project:
    resources['acceptance-csv'] = {'kind': 'csv', 'root': args.csv_root, 'projects': args.csv_project}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(resources, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print('Password-free connector resource allowlist generated.')
