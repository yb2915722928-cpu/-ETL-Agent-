"""Finite manual-plan compiler: deterministic review artifacts, no execution adapter.

Public seams for versioning: normalize_plan, assess_plan, freeze_facts. Callers
must authorize the project before invoking assess_plan; references are scoped.
"""
import json
import os
import re
from typing import Annotated, Literal

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .connector_policy import fingerprint
from .connector_probe import POLICY_VERSION as CONNECTOR_POLICY_VERSION
from .connectors import get_connection, get_profile
from .models import SecretReference
from .runtime_supervision import supervision_contract

VERSIONS = {'canonicalization': 'sorted-json-v1', 'compiler': 'studio-plan-v1',
            'renderer': 'review-hocon-v1', 'capability': 'mysql-doris-projection-v1',
            'policy': 'studio-policy-v1', 'connector_policy': CONNECTOR_POLICY_VERSION}
Identifier = Annotated[str, Field(pattern=r'^[a-zA-Z_][a-zA-Z0-9_]{0,63}$')]


class PlanModel(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, allow_inf_nan=False)


class AssetRef(PlanModel):
    connection_id: str = Field(min_length=1, max_length=36)
    profile_id: str = Field(min_length=1, max_length=36)
    table: Identifier


class Mapping(PlanModel):
    source: Identifier
    target: Identifier
    operation: Literal['direct'] = 'direct'


class QualityRule(PlanModel):
    kind: Literal['not_null', 'numeric_range']
    field: Identifier
    minimum: float | None = None
    maximum: float | None = None


class Cleaning(PlanModel):
    # Cleaning changes mapped payload only, never source record identity.
    field: Identifier
    kind: Literal['trim']


class Filter(PlanModel):
    # Matches the original source value; a match is a rejected ErrorRecord.
    field: Identifier
    equals: str | int | float | bool | None


class Budget(PlanModel):
    max_rows: int = 100000
    max_bytes: int = 268435456
    max_seconds: int = 600
    max_reject_ratio: float = 0.2
    max_amplification: float = 2.0


class EtlPlan(PlanModel):
    version: Literal['etl-plan-v1'] = 'etl-plan-v1'
    mode: Literal['bounded_full'] = 'bounded_full'
    # Empty replacement is opt-in and remains visible in the frozen plan.
    allow_empty: bool = False
    source: AssetRef
    target: AssetRef
    mappings: list[Mapping] = Field(min_length=1, max_length=64)
    quality: list[QualityRule] = Field(default_factory=list, max_length=64)
    cleaning: list[Cleaning] = Field(default_factory=list, max_length=64)
    filters: list[Filter] = Field(default_factory=list, max_length=64)
    budget: Budget = Field(default_factory=Budget)
    tools: list[str] = Field(default_factory=lambda: ['mysql.read', 'doris.write'], max_length=8)


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def normalize_plan(raw) -> EtlPlan:
    plan = EtlPlan.model_validate(raw)
    plan.mappings.sort(key=lambda mapping: (mapping.target, mapping.source))
    plan.quality.sort(key=lambda rule: canonical_json(rule.model_dump()))
    plan.cleaning.sort(key=lambda operation: canonical_json(operation.model_dump()))
    plan.filters.sort(key=lambda condition: canonical_json(condition.model_dump()))
    plan.tools.sort()
    return plan


def column_type(value):
    """Conservative supported SQL types; no implicit lossy or textual casts."""
    value = value.lower().strip()
    match = re.fullmatch(r'(tinyint|smallint|int|integer|bigint)(?:\(\d+\))?', value)
    if match:
        return ('integer', {'tinyint': 8, 'smallint': 16, 'int': 32, 'integer': 32, 'bigint': 64}[match[1]])
    match = re.fullmatch(r'(?:decimal|decimalv3)\((\d+),\s*(\d+)\)', value)
    if match:
        return ('decimal', int(match[1]), int(match[2]))
    match = re.fullmatch(r'(?:varchar|char)\((\d+)\)', value)
    if match:
        return ('text', int(match[1]))
    if value in ('tinytext', 'text', 'mediumtext', 'longtext', 'string'):
        return ('text', {'tinytext': 255, 'text': 65535, 'mediumtext': 16777215,
                         'longtext': 4294967295, 'string': 2147483647}[value])
    if value in ('date', 'datev2'):
        return ('date',)
    match = re.fullmatch(r'datetime(?:v2)?(?:\(([0-6])\))?', value)
    if match:
        return ('datetime', int(match[1] or 0))
    if value in ('float', 'double', 'boolean', 'json'):
        return (value,)
    return None


def quality_configuration(plan, source_schema, target_schema):
    """Validate the finite cleaning/filter dialect against approved schemas.

    Trim uses mapped target names. Filters use original source names and reject
    matches, so an unmapped filter column still belongs to the source scan.
    """
    source = {column['name']: column for column in source_schema['columns']}
    target = {column['name']: column for column in target_schema['columns']}
    mappings = {item['target']: item['source'] for item in plan['mappings']}
    primary = {name for name, column in source.items() if column.get('key') == 'PRI'}
    cleaning, filters = plan.get('cleaning', []), plan.get('filters', [])
    if not isinstance(cleaning, list) or not isinstance(filters, list):
        raise ValueError('quality_configuration_invalid')
    if len({operation.get('field') for operation in cleaning}) != len(cleaning):
        raise ValueError('duplicate_cleaning_field')
    for operation in cleaning:
        field = operation.get('field')
        if set(operation) != {'field', 'kind'} or operation.get('kind') != 'trim' or field not in mappings:
            raise ValueError('unsupported_cleaning')
        source_field = mappings[field]
        if source_field not in source or field not in target:
            raise ValueError('cleaning_field_missing')
        if source_field in primary:
            raise ValueError('source_primary_key_cleaning_forbidden')
        if any(column_type(column['type']) is None or column_type(column['type'])[0] != 'text'
               for column in (source[source_field], target[field])):
            raise ValueError('cleaning_requires_text_field')
    for condition in filters:
        field = condition.get('field')
        if set(condition) != {'field', 'equals'} or field not in source:
            raise ValueError('unsupported_filter')
        datatype = column_type(source[field]['type'])
        value = condition['equals']
        if datatype is None or datatype[0] not in ('integer', 'text'):
            raise ValueError('filter_source_wire_type_unverified')
        if value is not None and ((datatype[0] == 'text' and type(value) is not str)
                                  or (datatype[0] == 'integer' and type(value) is not int)):
            raise ValueError('filter_value_type_mismatch')
        if datatype[0] == 'integer' and type(value) is int:
            bits = datatype[1]
            lower, upper = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
            if not lower <= value <= upper:
                raise ValueError('filter_integer_out_of_range')
            if abs(value) > 9007199254740991:
                raise ValueError('filter_integer_not_json_safe')
    # Detach contract values from caller mutation; finite JSON validation rejects NaN.
    return json.loads(canonical_json(cleaning)), json.loads(canonical_json(filters))


def compatible(source, target):
    a, b = column_type(source), column_type(target)
    if a is None or b is None or a[0] != b[0]:
        return False
    if a[0] in ('integer', 'text', 'datetime'):
        return a[1] <= b[1]
    if a[0] == 'decimal':
        return a[2] <= b[2] and a[1] - a[2] <= b[1] - b[2]
    return a == b


def assess_plan(db, policy, tenant_id, project_id, raw) -> dict:
    gates = []

    def gate(name, errors):
        gates.append({'gate': name, 'passed': not errors, 'errors': errors})

    result = {'passed': False, 'gates': gates, 'normalized_plan': None,
              'hocon': None, 'dag': None, 'execution_facts': None, 'profile_refs': None,
              'versions': VERSIONS, 'artifact_kind': 'review_only',
              'limitations': ['手工单表有界全量映射审查', '质量规则与预算为静态契约，尚未执行验证',
                              '无模型、引擎执行、正式发布；HOCON为安全引用审查格式，不能直接提交引擎']}
    try:
        plan = normalize_plan(raw)
    except ValidationError as error:
        # Only schema paths and fixed error kinds, never echo rejected input.
        gate('syntax', [{'path': '.'.join(str(p) for p in e['loc']), 'code': e['type'],
                         'message': '字段缺失、类型/语法不合法或操作尚未支持'} for e in error.errors()])
        return result
    gate('syntax', [])
    normalized = plan.model_dump()
    # An absent new default must not change the digest of a legacy frozen plan.
    # Explicit defaults remain frozen and reviewed, including empty operation lists.
    for key in ('allow_empty', 'cleaning', 'filters'):
        if key not in plan.model_fields_set:
            normalized.pop(key)
    result['normalized_plan'] = normalized
    assets, resource_errors = {}, []
    writer_binding = None
    for side, kind, direction in (('source', 'mysql', 'source'), ('target', 'doris', 'target')):
        ref = getattr(plan, side)
        connection = get_connection(db, tenant_id, project_id, ref.connection_id)
        profile = get_profile(db, tenant_id, project_id, ref.profile_id)
        resource = policy.get(connection.resource_id)
        secret = db.get(SecretReference, connection.secret_reference_id) if connection.secret_reference_id else None
        errors = []
        if connection.kind != kind or connection.direction != direction:
            errors.append('仅支持 MySQL 源与 Doris 目标')
        if connection.status != 'ready':
            errors.append('连接须完成成功的受管探查')
        if resource is None or fingerprint(resource) != connection.resource_fingerprint:
            errors.append('部署白名单已变更，需重新登记连接与探查')
        elif ref.table not in resource['tables']:
            errors.append('表不在当前资源白名单内')
        if profile.connection_id != connection.id or any(getattr(profile, key) != getattr(connection, key) for key in (
                'secret_reference_id', 'secret_version', 'resource_id', 'resource_fingerprint')):
            errors.append('档案与连接、资源或凭据版本不一致')
        if secret is None or (secret.tenant_id, secret.project_id, secret.version) != (tenant_id, project_id, connection.secret_version):
            errors.append('凭据引用及版本无效')
        if side == 'target' and os.environ.get('EXECUTION_ADAPTER', 'disabled') == 'real':
            from .writer_credentials import execution_writer_binding
            try:
                writer_binding = execution_writer_binding(db, resource, connection)
            except HTTPException as error:
                errors.append(error.detail)
        schema = [{'name': table['name'], 'columns': table['columns']} for table in profile.content['tables']]
        if fingerprint(schema) != profile.schema_hash:
            errors.append('档案 Schema 指纹不一致')
        table = next((t for t in schema if t['name'] == ref.table), None)
        if table is None:
            errors.append('档案中不存在选定表')
        resource_errors.extend({'path': side, 'code': 'resource_rejected', 'message': message} for message in errors)
        assets[side] = (connection, profile, table)
    gate('resources', resource_errors)
    gate('tools', [] if plan.tools == ['doris.write', 'mysql.read'] else [
        {'path': 'tools', 'code': 'tool_not_allowed', 'message': '只允许 mysql.read 与 doris.write 计划意图；不授予执行权限'}])
    budget = plan.budget
    budget_errors = []
    for key, maximum in (('max_rows', 100000), ('max_bytes', 268435456), ('max_seconds', 600), ('max_amplification', 2)):
        if not 0 < getattr(budget, key) <= maximum:
            budget_errors.append({'path': 'budget.' + key, 'code': 'budget_exceeded', 'message': f'必须大于0且不超过{maximum}'})
    if not 0 <= budget.max_reject_ratio <= 0.2:
        budget_errors.append({'path': 'budget.max_reject_ratio', 'code': 'budget_exceeded', 'message': '最大拒绝率必须在0至0.2之间'})
    gate('budget', budget_errors)
    mapping_errors, schema_errors, quality_errors = [], [], []
    source_table, target_table = assets['source'][2], assets['target'][2]
    if source_table is not None and target_table is not None:
        source = {c['name']: c for c in source_table['columns']}
        target = {c['name']: c for c in target_table['columns']}
        mapped = [m.target for m in plan.mappings]
        if len(set(mapped)) != len(mapped):
            mapping_errors.append({'path': 'mappings', 'code': 'duplicate_target', 'message': '目标字段不能重复映射'})
        # Full target coverage avoids relying on unprofiled defaults/generated columns.
        if set(mapped) != set(target):
            mapping_errors.append({'path': 'mappings', 'code': 'target_coverage', 'message': '本能力要求目标每个字段恰好映射一次'})
        nonnull = {r.field for r in plan.quality if r.kind == 'not_null'}
        for m in plan.mappings:
            if m.source not in source or m.target not in target:
                mapping_errors.append({'path': 'mappings.' + m.target, 'code': 'unknown_field', 'message': '源或目标字段不存在于所引用 Schema'})
                continue
            if not compatible(source[m.source]['type'], target[m.target]['type']):
                schema_errors.append({'path': 'mappings.' + m.target, 'code': 'type_mismatch', 'message': '类型未支持、精度损失或类型不兼容，禁止隐式转换'})
            if source[m.source]['nullable'] and not target[m.target]['nullable'] and m.target not in nonnull:
                schema_errors.append({'path': 'mappings.' + m.target, 'code': 'nullability', 'message': '可空源映射至非空目标须显式非空质量规则'})
        try:
            quality_configuration(normalized, source_table, target_table)
        except ValueError as error:
            quality_errors.append({'path': 'cleaning/filters', 'code': str(error),
                'message': '清洗仅支持非主键文本目标字段去空白；过滤须引用已批准的整型/文本源字段且值类型一致'})
        for rule in plan.quality:
            reason = None
            if rule.field not in mapped or rule.field not in target:
                reason = '质量规则必须引用已映射的目标字段'
            elif rule.kind == 'numeric_range':
                m = next(m for m in plan.mappings if m.target == rule.field)
                datatype = column_type(source[m.source]['type']) if m.source in source else None
                if datatype is None or datatype[0] not in ('integer', 'decimal', 'float', 'double'):
                    reason = '范围规则仅支持数值字段'
                elif rule.minimum is None and rule.maximum is None:
                    reason = '范围规则至少指定一个边界'
                elif rule.minimum is not None and rule.maximum is not None and rule.minimum > rule.maximum:
                    reason = '最小值不得大于最大值'
            elif rule.minimum is not None or rule.maximum is not None:
                reason = '非空规则不能携带数值边界'
            if reason:
                quality_errors.append({'path': 'quality.' + rule.field, 'code': 'quality_invalid', 'message': reason})
    else:
        mapping_errors.append({'path': 'mappings', 'code': 'missing_schema', 'message': '必须先解决资源档案门禁'})
    gate('mapping', mapping_errors)
    gate('schema', schema_errors)
    gate('quality', quality_errors)
    real_contract = None
    if os.environ.get('EXECUTION_ADAPTER', 'disabled') == 'real':
        from .real_contract import real_execution_contract
        real_errors = []
        if source_table is not None and target_table is not None and not mapping_errors:
            try:
                real_contract = real_execution_contract(normalized, source_table, target_table)
            except ValueError as error:
                real_errors.append({'path': 'source', 'code': str(error),
                                    'message': '真实路径需要非空稳定主键及已验证的整型/文本传输契约'})
        else:
            real_errors.append({'path': 'source', 'code': 'real_schema_unavailable',
                                'message': '真实传输契约需要完整且有效的源目标结构与映射'})
        gate('real_transport_contract', real_errors)
    if not all(g['passed'] for g in gates):
        return result
    facts = {'plan': {**normalized, 'source': {'table': plan.source.table}, 'target': {'table': plan.target.table}},
             'runtime_supervision_contract': supervision_contract(normalized['budget'], allow_empty=plan.allow_empty), 'versions': VERSIONS}
    refs = {}
    for side, (connection, profile, table) in assets.items():
        facts[side] = {'connection_id': connection.id, 'resource_id': connection.resource_id,
                       'resource_fingerprint': connection.resource_fingerprint, 'schema_hash': profile.schema_hash,
                       'schema': table, 'secret_reference_id': connection.secret_reference_id, 'secret_version': connection.secret_version}
        refs[side] = {'profile_id': profile.id, 'schema_hash': profile.schema_hash}
    if writer_binding is not None:
        facts['target']['execution_writer'] = writer_binding
    if real_contract is not None:
        facts['real_execution_contract'] = real_contract
    optional_quality = {key: normalized[key] for key in ('cleaning', 'filters') if key in normalized}
    dag = {'nodes': [{'id': 'source', 'label': 'MySQL / ' + plan.source.table},
                     {'id': 'mapping', 'label': '字段映射', 'mappings': normalized['mappings']},
                     {'id': 'quality', 'label': ('清洗、必需规则与显式过滤（未执行）' if optional_quality
                                                else '质量契约（未执行）'),
                      'rules': normalized['quality'], **optional_quality},
                     {'id': 'target', 'label': 'Doris / ' + plan.target.table}],
           'edges': [{'from': a, 'to': b} for a, b in (('source', 'mapping'), ('mapping', 'quality'), ('quality', 'target'))]}
    # JSON is a HOCON subset. This explicitly versioned review dialect is not
    # presented as executable SeaTunnel configuration or runtime enforcement.
    hocon = json.dumps({'artifact_kind': 'review_only', 'renderer': VERSIONS['renderer'],
                        'mode': plan.mode, 'source': {**facts['source'], 'table': plan.source.table,
                            'credential': f'secret://{assets["source"][0].secret_reference_id}/versions/{assets["source"][0].secret_version}'},
                        'target': {**facts['target'], 'table': plan.target.table,
                            'credential': f'secret://{assets["target"][0].secret_reference_id}/versions/{assets["target"][0].secret_version}'},
                        'mappings': normalized['mappings'], **optional_quality,
                         'quality': normalized['quality'], 'budget': normalized['budget'],
                         **({'real_execution_contract': real_contract} if real_contract is not None else {}),
                         'runtime_supervision_contract': facts['runtime_supervision_contract']},
                       ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + '\n'
    result.update(passed=True, execution_facts=facts, profile_refs=refs, dag=dag, hocon=hocon)
    return result


def freeze_facts(assessment) -> dict:
    """Return deterministic facts only after a fresh assess_plan by the caller."""
    if not assessment['passed'] or not all(g['passed'] for g in assessment['gates']) or assessment['execution_facts'] is None:
        raise HTTPException(409, '候选未通过确定性门禁，不能冻结')
    return {'execution_facts': assessment['execution_facts'], 'profile_refs': assessment['profile_refs'],
            'normalized_plan': assessment['normalized_plan'], 'hocon': assessment['hocon'],
            'dag': assessment['dag'], 'versions': assessment['versions']}
