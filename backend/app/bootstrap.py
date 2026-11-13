"""Idempotent local initialization; secrets only in an ignored private file."""
import argparse
import json
import os
from pathlib import Path
import secrets

from sqlalchemy import select

from .db import Base, Session, engine
from .models import Account, Membership, Project, Tenant
from .security import append_event, hash_password

IDENTITIES = [
    ('admin', '租户与项目管理员', ['project_admin']),
    ('maker', '申请人', ['maker']),
    ('data_checker', '数据审批人', ['data_checker']),
    ('security_checker', '安全审批人', ['security_checker']),
    ('operator', '操作员', ['operator']),
    ('auditor', '审计人员', ['auditor']),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', default='demo')
    parser.add_argument('--project', default='demo')
    args = parser.parse_args()
    if not all(value and len(value) <= 64 and all(c.isascii() and (c.isalnum() or c in '_-') for c in value)
               for value in (args.tenant, args.project)):
        parser.error('tenant/project must use ASCII letters, digits, _ or - (max 64)')
    directory = Path(os.environ.get('BOOTSTRAP_DIR', Path(__file__).resolve().parents[2] / '.runtime'))
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    output = directory / f'bootstrap-{args.tenant}.json'
    credentials = json.loads(output.read_text(encoding='utf-8')) if output.exists() else {'tenant': args.tenant, 'accounts': {}}
    Base.metadata.create_all(engine)
    with Session.begin() as db:
        tenant = db.scalar(select(Tenant).where(Tenant.slug == args.tenant))
        if tenant is None:
            tenant = Tenant(slug=args.tenant, name=f'{args.tenant} 协作租户')
            db.add(tenant)
            db.flush()
        project = db.scalar(select(Project).where(Project.tenant_id == tenant.id, Project.slug == args.project))
        new_project = project is None
        if new_project:
            project = Project(tenant_id=tenant.id, slug=args.project, name=f'{args.project} 数据集成项目')
            db.add(project)
            db.flush()
        admin = None
        for username, display_name, roles in IDENTITIES:
            account = db.scalar(select(Account).where(Account.tenant_id == tenant.id, Account.username == username))
            new_account = account is None
            if new_account:
                password = secrets.token_urlsafe(24)
                account = Account(tenant_id=tenant.id, username=username, display_name=display_name,
                                  password_hash=hash_password(password), tenant_admin=username == 'admin')
                db.add(account)
                db.flush()
                credentials['accounts'][username] = {'password': password, 'id': account.id}
            if username == 'admin':
                admin = account
            if new_account:
                append_event(db, admin, 'account.bootstrapped', {'target_account_id': account.id, 'tenant_admin': account.tenant_admin})
            # An existing project's revoked membership is never restored by bootstrap.
            if new_project:
                db.add(Membership(tenant_id=tenant.id, project_id=project.id, account_id=account.id, roles=roles))
                append_event(db, admin, 'membership.bootstrapped', {'target_account_id': account.id, 'roles': roles}, project.id)
        if new_project:
            append_event(db, admin, 'project.bootstrapped', {'name': project.name}, project.id)
        credentials['tenant_id'] = tenant.id
        credentials['project_id'] = project.id
    with output.open('w', encoding='utf-8') as handle:
        os.chmod(output, 0o600)
        json.dump(credentials, handle, ensure_ascii=False, indent=2)
    print(f'Bootstrap complete for tenant {args.tenant}; private credentials at {output}. Existing passwords/status/roles preserved.')


if __name__ == '__main__':
    main()
