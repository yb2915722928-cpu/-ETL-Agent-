"""Shared project read/Maker write authorization with transactional denial evidence."""
from fastapi import HTTPException
from fastapi.responses import JSONResponse

from .evidence import append_event


def operation_authorizer(project_scope, denied_event, maker_message):
    def authorize(db, actor, tenant_id, project_id, write=False):
        try:
            member = project_scope(db, actor, tenant_id, project_id)
            if write and 'maker' not in member.roles:
                raise HTTPException(403, maker_message)
        except HTTPException as error:
            # Denials belong to the actor's tenant, never the untrusted target scope.
            append_event(db, actor, denied_event, {'reason': 'scope_or_role_denied', 'write': write})
            return JSONResponse(status_code=error.status_code, content={'detail': error.detail})
    return authorize
