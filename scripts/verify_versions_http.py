"""V06 public HTTP acceptance using real persisted profiles, PostgreSQL and Redis.

Run verify_studio_http.py first. --policy-file may point to an isolated API's
resource policy copy: temporarily removes one resource and always restores it.
No database is accessed directly. Credentials and version state stay private.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import urllib.request
import uuid

from verify_identity import Client, private_write


def digest(content):
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


def object_keys(value):
    if isinstance(value, dict):
        for key, child in value.items():
            yield key
            yield from object_keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from object_keys(child)


def check_artifacts(client, path, version):
    facts = version['artifacts']['facts.json']
    assert digest(facts) == version['execution_sha256']
    assert digest(version['artifacts']['manifest.json']) == version['bundle_sha256']
    manifest = json.loads(version['artifacts']['manifest.json'])
    for artifact, expected in version['artifacts'].items():
        request = urllib.request.Request(client.url + path + '/' + version['id'] + '/artifacts/' + artifact)
        with client.opener.open(request, timeout=15) as response:
            raw = response.read().decode('utf-8')
            assert raw == expected
            assert not any(value and value in raw for value in client.secrets)
            assert 'attachment' in response.headers['Content-Disposition']
            assert response.headers['Cache-Control'] == 'no-store'
            assert response.headers['X-Content-SHA256'] == version['artifact_hashes'][artifact] == digest(raw)
            if artifact != 'manifest.json':
                assert manifest['artifacts'][artifact] == digest(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    parser.add_argument('--credentials', type=Path, default=Path('.runtime/bootstrap-demo.json'))
    parser.add_argument('--other-credentials', type=Path, default=Path('.runtime/bootstrap-isolation.json'))
    parser.add_argument('--secrets', type=Path, default=Path('.runtime/connectors/secrets.json'))
    parser.add_argument('--studio-state', type=Path, default=Path('.runtime/studio-verification.json'))
    parser.add_argument('--state', type=Path, default=Path('.runtime/versions-verification.json'))
    parser.add_argument('--policy-file', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--recheck', action='store_true')
    args = parser.parse_args()
    credentials = json.loads(args.credentials.read_text(encoding='utf-8'))
    other = json.loads(args.other_credentials.read_text(encoding='utf-8'))
    config = json.loads(args.secrets.read_text(encoding='utf-8'))
    forbidden = [a['password'] for c in (credentials, other) for a in c['accounts'].values()]
    forbidden += [value for entry in config.values() for key, value in entry.items() if 'password' in key]
    maker, auditor, admin, outsider = [Client(args.url, forbidden) for _ in range(4)]
    for client, role in ((maker, 'maker'), (auditor, 'auditor'), (admin, 'admin')):
        client.login(credentials, role)
    outsider.login(other, 'admin')
    base = f"/api/tenants/{credentials['tenant_id']}/projects/{credentials['project_id']}"
    path = base + '/versions'
    candidates = base + '/studio/candidates'
    passed = []
    if args.recheck:
        state = json.loads(args.state.read_text(encoding='utf-8'))
        for version in state['versions']:
            assert maker.call('GET', path + '/' + version['id']) == version
            check_artifacts(maker, path, version)
        passed.append('all frozen bytes, facts, revisions, profile references and digests survive API restart')
    else:
        candidate = json.loads(args.studio_state.read_text(encoding='utf-8'))['candidate']
        freeze = lambda saved: maker.call('POST', path, {'candidate_id': saved['id'], 'expected_revision': saved['revision']}, expected=201)
        version = freeze(candidate)
        assert version['candidate_revision'] == candidate['revision']
        with ThreadPoolExecutor(max_workers=2) as pool:
            repeated = list(pool.map(lambda _: freeze(candidate), range(2)))
        assert repeated == [version, version]
        check_artifacts(maker, path, version)
        assert maker.call('GET', path + '/' + version['id']) == version
        assert auditor.call('GET', path + '/' + version['id']) == version
        passed.append('current Candidate freezes; concurrent repeats reuse the same immutable version; every saved artifact and manifest digest recomputes from exact download bytes')

        # A second real probe changes Profile IDs and collection times, not execution facts.
        plan = deepcopy(candidate['plan'])
        for side in ('source', 'target'):
            profile = maker.call('POST', base + '/connections/' + plan[side]['connection_id'] + '/probe', {}, expected=201)
            assert profile['id'] != plan[side]['profile_id']
            plan[side]['profile_id'] = profile['id']
        plan['mappings'].reverse()
        plan['tools'].reverse()
        reprobed = maker.call('POST', candidates, {'name': 'V06 重探查与顺序稳定性', 'plan': plan}, expected=201)
        stable = freeze(reprobed)
        assert stable['id'] != version['id']
        assert stable['execution_sha256'] == version['execution_sha256']
        for artifact in ('review.conf', 'dag.json', 'facts.json'):
            assert stable['artifacts'][artifact] == version['artifacts'][artifact]
        assert stable['frozen']['profile_refs'] != version['frozen']['profile_refs']
        assert stable['bundle_sha256'] != version['bundle_sha256']
        facts = json.loads(stable['artifacts']['facts.json'])
        assert not {'profile_id', 'created_at', 'samples', 'approx_rows', 'elapsed'}.intersection(object_keys(facts))
        assert facts['source']['secret_version'] and facts['target']['resource_fingerprint']
        passed.append('real re-probe, name, timestamps and input-array order leave execution SHA256/HOCON/DAG unchanged; Profile references stay separate and bundle digest binds them')

        changed = deepcopy(plan)
        # Select a real profiled textual source for the fixture's email projection.
        alternative = next(c['name'] for c in facts['source']['schema']['columns']
                           if c['name'] != 'email' and c['type'].lower().startswith('varchar'))
        next(m for m in changed['mappings'] if m['target'] == 'email')['source'] = alternative
        changed['quality'].append({'kind': 'not_null', 'field': 'email'})
        changed['budget']['max_rows'] -= 1
        revised = maker.call('PUT', candidates + '/' + reprobed['id'], {'name': reprobed['name'], 'plan': changed,
                            'expected_revision': reprobed['revision']})
        assert revised['assessment']['passed'], revised['assessment']['gates']
        business = freeze(revised)
        assert business['execution_sha256'] != stable['execution_sha256']
        comparison = maker.call('GET', path + '/' + stable['id'] + '/diff/' + business['id'])
        assert comparison['execution_changed']
        assert all(comparison['sections'][key]['changed'] for key in ('mapping', 'hocon', 'quality'))
        assert not comparison['sections']['schema']['changed']
        assert comparison['sections']['hocon']['unified']
        maker.call('POST', path, {'candidate_id': revised['id'], 'expected_revision': 1}, expected=409)
        for method in ('PUT', 'PATCH', 'DELETE'):
            maker.call(method, path + '/' + version['id'], {'name': 'rewrite', 'artifacts': {}}, expected=409)
        assert maker.call('GET', path + '/' + version['id']) == version
        assert maker.call('GET', path + '/' + stable['id']) == stable
        passed.append('mapping/quality/budget revisions change execution digest and named Diff sections; stale revision and PUT/PATCH/DELETE rejected; earlier frozen bytes remain identical')

        if args.policy_file:
            policy_bytes = args.policy_file.read_bytes()
            resources = json.loads(policy_bytes)
            resource_id = version['frozen']['execution_facts']['source']['resource_id']
            del resources[resource_id]
            try:
                args.policy_file.write_text(json.dumps(resources), encoding='utf-8')
                maker.call('POST', path, {'candidate_id': candidate['id'], 'expected_revision': candidate['revision']}, expected=409)
                assert maker.call('GET', candidates + '/' + candidate['id'])['assessment']['passed']
                assert maker.call('GET', path + '/' + version['id']) == version
            finally:
                args.policy_file.write_bytes(policy_bytes)
            assert freeze(candidate) == version
            passed.append('freeze rechecks current deployed whitelist even for an already-frozen, saved-passed candidate; removal blocks reuse without altering history; isolated policy restored')

        auditor.call('POST', path, {'candidate_id': revised['id'], 'expected_revision': revised['revision']}, expected=403)
        private = admin.call('POST', f"/api/tenants/{credentials['tenant_id']}/projects", {
            'slug': 'versions_' + uuid.uuid4().hex[:8], 'name': 'V06 版本隔离验收'}, expected=201)
        private_base = f"/api/tenants/{credentials['tenant_id']}/projects/{private['id']}"
        admin.call('PUT', private_base + '/members/' + credentials['accounts']['maker']['id'], {'roles': ['maker']})
        for suffix in ('/' + version['id'], '/' + version['id'] + '/artifacts/review.conf',
                       '/' + version['id'] + '/diff/' + business['id']):
            maker.call('GET', private_base + '/versions' + suffix, expected=404)
            outsider.call('GET', path + suffix, expected=403)
        maker.call('GET', path + '/' + version['id'] + '/diff/' + str(uuid.uuid4()), expected=404)
        member_path = base + '/members/' + credentials['accounts']['maker']['id']
        roles = next(m['roles'] for m in admin.call('GET', base + '/members') if m['id'] == credentials['accounts']['maker']['id'])
        try:
            admin.call('PUT', member_path, {'roles': ['auditor']})
            maker.call('POST', path, {'candidate_id': revised['id'], 'expected_revision': revised['revision']}, expected=403)
            admin.call('DELETE', member_path)
            maker.call('GET', path + '/' + version['id'] + '/artifacts/review.conf', expected=403)
            maker.call('GET', path + '/' + version['id'] + '/diff/' + business['id'], expected=403)
        finally:
            admin.call('PUT', member_path, {'roles': roles})
        evidence = auditor.call('GET', base + '/evidence')['events']
        frozen_event = next(e for e in evidence if e['action'] == 'version.frozen' and e['version_id'] == version['id'])
        assert frozen_event['artifact_hashes'] == version['artifact_hashes']
        assert any(e['action'] == 'version.mutation_denied' and e['version_id'] == version['id'] for e in evidence)
        assert any(e['action'] == 'version.freeze_denied' for e in evidence)
        private_write(args.state, {'versions': [version, stable, business], 'candidate': revised})
        passed.append('read/download/Diff enforce tenant and project scope; current Maker and membership revocation take effect; audit evidence retains version/artifact bindings and denied mutations/freezes; no known plaintext secrets returned')
    report = {'checked_at': datetime.now(timezone.utc).isoformat(), 'phase': 'versions-persistence' if args.recheck else 'versions',
              'passed': passed, 'limits': 'immutable manual review artifacts only; no engine, model, preparation or publication'}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
