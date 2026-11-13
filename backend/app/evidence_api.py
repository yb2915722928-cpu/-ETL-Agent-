"""Audit HTTP routes, registered with the application's identity dependencies."""
from fastapi import HTTPException
from fastapi.responses import JSONResponse

from .evidence import complete_chain


def register_evidence_routes(app, Db, Actor, tenant_admin, project_scope):
    def export(db, tenant_id, project_id=None):
        events, sequence, event_hash = complete_chain(db, tenant_id, project_id)
        return {'format': 'etl-evidence-v1', 'events': events,
                'checkpoint': app.state.evidence_signer.checkpoint(tenant_id, project_id, sequence, event_hash)}

    def authorize_project(db, actor, tenant_id, project_id):
        member = project_scope(db, actor, tenant_id, project_id)
        if not {'auditor', 'project_admin'}.intersection(member.roles):
            raise HTTPException(403, '仅本项目审计人员或项目管理员可查看和导出证据')

    def download(value):
        return JSONResponse(value, headers={'Content-Disposition': 'attachment; filename="evidence.json"',
                                             'Cache-Control': 'no-store'})

    @app.get('/api/evidence/public-key')
    def public_key(actor: Actor):
        return app.state.evidence_signer.public

    @app.get('/api/tenants/{tenant_id}/evidence')
    def tenant_evidence(tenant_id: str, db: Db, actor: Actor):
        tenant_admin(actor, tenant_id)
        return export(db, tenant_id)

    @app.get('/api/tenants/{tenant_id}/evidence/export')
    def tenant_export(tenant_id: str, db: Db, actor: Actor):
        tenant_admin(actor, tenant_id)
        return download(export(db, tenant_id))

    @app.get('/api/tenants/{tenant_id}/evidence/checkpoint')
    def tenant_checkpoint(tenant_id: str, db: Db, actor: Actor):
        tenant_admin(actor, tenant_id)
        return export(db, tenant_id)['checkpoint']

    @app.get('/api/tenants/{tenant_id}/projects/{project_id}/evidence')
    def project_evidence(tenant_id: str, project_id: str, db: Db, actor: Actor):
        authorize_project(db, actor, tenant_id, project_id)
        return export(db, tenant_id, project_id)

    @app.get('/api/tenants/{tenant_id}/projects/{project_id}/evidence/export')
    def project_export(tenant_id: str, project_id: str, db: Db, actor: Actor):
        authorize_project(db, actor, tenant_id, project_id)
        return download(export(db, tenant_id, project_id))

    @app.get('/api/tenants/{tenant_id}/projects/{project_id}/evidence/checkpoint')
    def project_checkpoint(tenant_id: str, project_id: str, db: Db, actor: Actor):
        authorize_project(db, actor, tenant_id, project_id)
        return export(db, tenant_id, project_id)['checkpoint']
