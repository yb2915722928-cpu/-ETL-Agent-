"""Real connector acceptance at the managed read-only tool boundary; no credentials printed."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from app.connector_probe import inspect_resource, ProbeFailure


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--secrets', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--kind', choices=['mysql', 'doris', 'postgresql'], default='mysql')
    args = parser.parse_args()
    config = json.loads(args.secrets.read_text(encoding='utf-8'))[args.kind]
    resource = {**config, 'host': '127.0.0.1', 'port': config['host_port'],
                'kind': args.kind, 'tables': config.get('tables', ['customers', 'orders'])}
    profile = inspect_resource(resource, config['password'])
    encoded = json.dumps(profile, ensure_ascii=False)
    assert config['password'] not in encoded
    assert 'alice@example.test' not in encoded and 'Alice Example' not in encoded
    assert {t['name'] for t in profile['tables']} == set(resource['tables'])
    assert all(t['columns'] for t in profile['tables'])
    if args.kind in ('mysql', 'postgresql'):
        customer = next(t for t in profile['tables'] if t['name'] == 'customers')
        assert next(c for c in customer['columns'] if c['name'] == 'balance')['type'] == ('numeric(18,4)' if args.kind == 'postgresql' else 'decimal(18,4)')
        assert customer['samples'] and customer['samples'][0]['email'] == '[已脱敏]'
    try:
        inspect_resource(resource, 'sentinel-wrong-password-never-echo')
    except ProbeFailure as error:
        assert config['password'] not in str(error) and 'sentinel' not in str(error)
    else:
        raise AssertionError('invalid credential unexpectedly accepted')
    elevated = {**resource, 'username': config.get('admin_username', 'root')}
    try:
        inspect_resource(elevated, config['admin_password'])
    except ProbeFailure as error:
        assert error.code == 'readonly_required'
    else:
        raise AssertionError('writable account unexpectedly accepted')
    report = json.dumps({'kind': args.kind, 'server_version': profile['server_version'],
                         'tables': [t['name'] for t in profile['tables']],
                         'passed': ['real schema/types/statistics/masked samples', 'safe credential failure',
                                    'real writable admin account rejected']}, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding='utf-8')
    print(report)


if __name__ == '__main__':
    main()
