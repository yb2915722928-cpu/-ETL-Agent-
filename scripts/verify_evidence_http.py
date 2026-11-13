"""V02 real HTTP + independent offline CLI acceptance; never reads database internals.

Run against deployed services after V01 bootstrap (demo and isolation). Exports
and saved checkpoints remain under ignored .runtime unless --directory is given.
Run again with --recheck after restarting services to verify persisted prefixes.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import uuid

from verify_identity import Client


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('--credentials', type=Path, default=Path('.runtime/bootstrap-demo.json'))
    parser.add_argument('--other-credentials', type=Path, default=Path('.runtime/bootstrap-isolation.json'))
    parser.add_argument('--directory', type=Path, default=Path('.runtime/evidence-verification'))
    parser.add_argument('--output', type=Path)
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    credentials = json.loads(args.credentials.read_text(encoding='utf-8'))
    other = json.loads(args.other_credentials.read_text(encoding='utf-8'))
    plaintext = [account['password'] for source in (credentials, other) for account in source['accounts'].values()]
    args.directory.mkdir(parents=True, exist_ok=True)
    admin = Client(args.url, plaintext)
    admin.login(credentials, 'admin')
    base = f"/api/tenants/{credentials['tenant_id']}"
    project_base = f"{base}/projects/{credentials['project_id']}"
    project_path = project_base + '/evidence'
    key_path = args.directory / 'trusted-key.json'
    cli = Path(__file__).with_name('verify_evidence.py')
    passed = []

    def offline(name, bundle, checkpoint=None, valid=True, error=None):
        path = args.directory / (name + '.json')
        save(path, bundle)
        command = [sys.executable, str(cli), str(path), '--public-key', str(key_path)]
        if checkpoint:
            command += ['--checkpoint', str(checkpoint)]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        assert result.returncode == (0 if valid else 1), f'{name}: CLI returned {result.returncode}: {result.stdout}'
        report = json.loads(result.stdout)
        assert report['valid'] is valid
        if error:
            assert error in report['error'], f'{name}: expected located error {error}'
        return path

    if args.recheck:
        current = admin.call('GET', project_path + '/export')
        offline('after-restart', current, args.directory / 'checkpoint.json')
        original = json.loads((args.directory / 'original.json').read_text(encoding='utf-8'))
        assert current['events'][:len(original['events'])] == original['events']
        passed.append('project chain IDs/hashes/order and independently saved checkpoint survive restart')
    else:
        save(key_path, admin.call('GET', '/api/evidence/public-key'))
        tenant_export = admin.call('GET', base + '/evidence/export')
        offline('tenant', tenant_export)
        legacy = admin.call('GET', base + '/identity-events')
        assert len(legacy) == len(tenant_export['events'])
        assert [(e['actor_id'], e['action'], e['facts']) for e in legacy] == [(e['actor_id'], e['action'], e['facts']) for e in tenant_export['events']]
        passed.append('all historical V01 facts preserved in ordered signed tenant chain')
        auditor = Client(args.url, plaintext)
        auditor.login(credentials, 'auditor')
        bundle = auditor.call('GET', project_path + '/export')
        assert bundle['events'] and all(e['project_id'] == credentials['project_id'] for e in bundle['events'])
        assert [e['sequence'] for e in bundle['events']] == list(range(1, len(bundle['events']) + 1))
        offline('original', bundle)
        checkpoint_path = args.directory / 'checkpoint.json'
        save(checkpoint_path, auditor.call('GET', project_path + '/checkpoint'))
        offline('with-external', bundle, checkpoint_path)
        passed.append('auditor exports complete project-only chain; independent CLI verifies signatures and external checkpoint')

        maker = Client(args.url, plaintext)
        maker.login(credentials, 'maker')
        for suffix in ('', '/export', '/checkpoint'):
            maker.call('GET', project_path + suffix, expected=403)
            auditor.call('GET', base + '/evidence' + suffix, expected=403)
            admin.call('GET', f"{base}/projects/{other['project_id']}/evidence{suffix}", expected=403)
            admin.call('GET', f"/api/tenants/{other['tenant_id']}/projects/{other['project_id']}/evidence{suffix}", expected=403)
        private = admin.call('POST', base + '/projects', {'slug': 'audit_' + uuid.uuid4().hex[:10], 'name': '审计隔离项目'}, expected=201)
        auditor.call('GET', f"{base}/projects/{private['id']}/evidence/export", expected=403)
        private_bundle = admin.call('GET', f"{base}/projects/{private['id']}/evidence/export")
        assert all(e['project_id'] == private['id'] for e in private_bundle['events'])
        offline('private', private_bundle)
        passed.append('maker, other tenant, foreign project ID and unjoined project export/checkpoint requests rejected')

        changed = deepcopy(bundle)
        changed['events'][0]['action'] = 'tampered'
        offline('modified', changed, valid=False, error='event 1')
        deleted = deepcopy(bundle)
        del deleted['events'][1]
        offline('deleted', deleted, valid=False, error='event 2')
        reordered = deepcopy(bundle)
        reordered['events'][0], reordered['events'][1] = reordered['events'][1], reordered['events'][0]
        offline('reordered', reordered, valid=False, error='event 1')
        unsigned = deepcopy(bundle)
        unsigned['checkpoint']['event_hash'] = 'f' * 64
        offline('forged-checkpoint', unsigned, valid=False, error='signature')
        truncated = deepcopy(bundle)
        truncated['events'].pop()
        offline('truncated', truncated, valid=False, error='checkpoint head mismatch')
        passed.append('actual modification, deletion, reordering, signature forgery and tail removal rejected with located errors')

        # Obtain a later authentic checkpoint, then replay the older self-consistent
        # export. No forged signatures or backend internals are involved.
        auditor_id = credentials['accounts']['auditor']['id']
        admin.call('PUT', project_base + '/members/' + auditor_id, {'roles': ['auditor']})
        later = auditor.call('GET', project_path + '/export')
        save(args.directory / 'later-checkpoint.json', later['checkpoint'])
        offline('old-prefix-without-external', bundle)
        offline('old-prefix-with-external', bundle, args.directory / 'later-checkpoint.json', valid=False, error='truncation')
        offline('later', later, checkpoint_path)
        passed.append('replayed valid old prefix passes alone but fails against separately retained later checkpoint; later export preserves older checkpoint')

        before = auditor.call('GET', project_path)
        admin.call('PUT', project_base + '/members/' + other['accounts']['maker']['id'], {'roles': ['maker']}, expected=404)
        after = auditor.call('GET', project_path)
        assert before['events'] == after['events']
        admin.call('DELETE', f"{base}/projects/{private['id']}/members/{credentials['accounts']['admin']['id']}")
        admin.call('GET', f"{base}/projects/{private['id']}/evidence/export", expected=403)
        passed.append('rejected transaction produces no success event; project access revoked for existing admin session')
        # Scan JSON exports as well as HTTP responses for known plaintext secrets.
        assert not any(secret in json.dumps(later) for secret in plaintext)

    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'phase': 'persistence' if args.recheck else 'evidence', 'passed': passed}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        save(args.output, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
