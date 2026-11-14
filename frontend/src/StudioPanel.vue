<script setup>
import { computed, ref, watch } from 'vue'

const props = defineProps({ api: Function, tenantId: String, accountId: String, projects: Array })
const projectId = ref('')
const candidates = ref([])
const connections = ref([])
const profiles = ref([])
const selected = ref(null)
const name = ref('手工 MySQL → Doris 候选')
const plan = ref(blankPlan())
const assessment = ref(null)
const assessedPlan = ref('')
const error = ref('')
const message = ref('')
const busy = ref(false)
const base = computed(() => `/api/tenants/${props.tenantId}/projects/${projectId.value}`)
const project = computed(() => props.projects.find(p => p.id === projectId.value))
const canMake = computed(() => project.value?.roles.includes('maker'))
const canEdit = computed(() => canMake.value && (!selected.value || selected.value.owner_id === props.accountId))
const planText = computed(() => JSON.stringify(plan.value, null, 2))
const currentReview = computed(() => assessment.value && assessedPlan.value === JSON.stringify(plan.value) ? assessment.value : null)
const formReady = computed(() => plan.value.source && plan.value.target && plan.value.budget && Array.isArray(plan.value.mappings) && Array.isArray(plan.value.quality) && Array.isArray(plan.value.cleaning) && Array.isArray(plan.value.filters))
const unsaved = computed(() => !selected.value || JSON.stringify(plan.value) !== JSON.stringify(selected.value.plan) || name.value !== selected.value.name)
const gateNames = { syntax: '计划语法与能力', resources: '资源、档案与凭据', tools: '工具白名单', budget: '安全预算', mapping: '字段映射', schema: 'Schema 与类型', quality: '质量规则' }
const budgetLabels = { max_rows: '最大读取行数（≤ 100000）', max_bytes: '最大读取字节（≤ 268435456）', max_seconds: '最长时间 / 秒（≤ 600）', max_reject_ratio: '最大拒绝率（0–0.2）', max_amplification: '最大放大倍数（≤ 2）' }

function blankPlan() {
  return { version: 'etl-plan-v1', mode: 'bounded_full', allow_empty: false,
    source: { connection_id: '', profile_id: '', table: '' }, target: { connection_id: '', profile_id: '', table: '' },
    mappings: [], cleaning: [], quality: [], filters: [], tools: ['mysql.read', 'doris.write'],
    budget: { max_rows: 100000, max_bytes: 268435456, max_seconds: 600, max_reject_ratio: 0.2, max_amplification: 2 } }
}
function available(side) {
  const kind = side === 'source' ? 'mysql' : 'doris'
  return profiles.value.filter(p => connections.value.some(c => c.id === p.connection_id && c.kind === kind && c.direction === side))
}
function profile(side) { return profiles.value.find(p => p.id === plan.value[side].profile_id) }
function table(side) { return profile(side)?.content.tables.find(t => t.name === plan.value[side].table) }
function columns(side) { return table(side)?.columns || [] }
function cleanableMappings() {
  return plan.value.mappings.filter(mapping => {
    const source = columns('source').find(column => column.name === mapping.source)
    const target = columns('target').find(column => column.name === mapping.target)
    return source?.key !== 'PRI' && /^(varchar|char|tinytext|text|mediumtext|longtext|string)/i.test(source?.type || '') && /^(varchar|char|tinytext|text|mediumtext|longtext|string)/i.test(target?.type || '')
  })
}
function filterColumns() { return columns('source').filter(column => /^(tinyint|smallint|int|integer|bigint|varchar|char|tinytext|text|mediumtext|longtext|string)(\b|\()/i.test(column.type)) }
function filterIsNumeric(field) { return /^(tinyint|smallint|int|integer|bigint)(\b|\()/i.test(columns('source').find(column => column.name === field)?.type || '') }
function setFilterValue(condition, value) {
  if (!filterIsNumeric(condition.field)) { condition.equals = value; return }
  // Validate the decimal spelling before conversion: Number() rounds fractions and accepts blanks.
  const isInteger = /^-?(0|[1-9][0-9]*)$/.test(value)
  const parsed = isInteger ? Number(value) : NaN
  condition.equals = Number.isSafeInteger(parsed) ? parsed : value
}
function filterValueError(condition) {
  if (!filterIsNumeric(condition.field) || condition.equals === null || Number.isSafeInteger(condition.equals)) return ''
  return '请输入完整的十进制整数，范围为 -9007199254740991 至 9007199254740991（不接受小数、指数或空白）。当前原文已保留，服务端将拒绝此值。'
}
function resetFilterValue(condition) { condition.equals = filterIsNumeric(condition.field) ? 0 : '' }
function optionalConfiguration(saved) {
  return { ...structuredClone(saved), allow_empty: saved.allow_empty === true,
    cleaning: structuredClone(saved.cleaning || []), filters: structuredClone(saved.filters || []) }
}
function profileLabel(p) {
  const connection = connections.value.find(c => c.id === p.connection_id)
  return `${connection?.name} · ${new Date(p.created_at).toLocaleString('zh-CN')} · ${p.id.slice(0, 8)}`
}
async function run(action) {
  busy.value = true; error.value = ''; message.value = ''
  try { await action() } catch (cause) { error.value = cause.message }
  finally { busy.value = false }
}
function fresh() {
  selected.value = null; name.value = '手工 MySQL → Doris 候选'; plan.value = blankPlan()
  assessment.value = null; assessedPlan.value = ''
}
async function load() {
  fresh(); candidates.value = []; connections.value = []; profiles.value = []
  if (!projectId.value) return
  const [saved, managed] = await Promise.all([props.api(`${base.value}/studio/candidates`), props.api(`${base.value}/connections`)])
  candidates.value = saved; connections.value = managed
  const entries = await Promise.all(managed.filter(c => c.status === 'ready' && ['mysql', 'doris'].includes(c.kind))
    .map(c => props.api(`${base.value}/connections/${c.id}/profiles`)))
  profiles.value = entries.flat()
}
function chooseProfile(side) {
  plan.value[side].connection_id = profile(side)?.connection_id || ''
  plan.value[side].table = ''
  plan.value.mappings = []; plan.value.cleaning = []; plan.value.quality = []; plan.value.filters = []
}
function suggestMappings() {
  plan.value.mappings = columns('target').map(c => ({ source: columns('source').some(s => s.name === c.name) ? c.name : '', target: c.name, operation: 'direct' }))
  plan.value.cleaning = []; plan.value.quality = []; plan.value.filters = []
}
async function open(id) {
  await run(async () => {
    const saved = await props.api(`${base.value}/studio/candidates/${id}`)
    selected.value = saved; name.value = saved.name; plan.value = optionalConfiguration(saved.plan)
    assessment.value = saved.assessment; assessedPlan.value = JSON.stringify(plan.value)
    message.value = '已读取保存时的审查结果；重新校验会核对当前资源白名单与凭据版本。'
  })
}
async function validate() {
  await run(async () => {
    const reviewed = await props.api(`${base.value}/studio/validate`, { method: 'POST', body: JSON.stringify({ plan: plan.value }) })
    assessment.value = reviewed; assessedPlan.value = JSON.stringify(plan.value)
    message.value = reviewed.passed ? '静态门禁通过，审查制品已生成。尚未执行数据读取、质量规则或写入。' : '门禁未通过，请按下方定位修复；仍可保存为失败候选。'
  })
}
async function save() {
  await run(async () => {
    const url = `${base.value}/studio/candidates${selected.value ? '/' + selected.value.id : ''}`
    const saved = await props.api(url, { method: selected.value ? 'PUT' : 'POST', body: JSON.stringify({ name: name.value, plan: plan.value,
      ...(selected.value ? { expected_revision: selected.value.revision } : {}) }) })
    selected.value = saved; plan.value = optionalConfiguration(saved.plan); assessment.value = saved.assessment
    assessedPlan.value = JSON.stringify(plan.value)
    candidates.value = await props.api(`${base.value}/studio/candidates`)
    message.value = `已保存候选修订 ${saved.revision} · ${saved.assessment.passed ? '静态门禁通过' : '门禁失败，等待修复'}。`
  })
}
function clone() { selected.value = null; name.value += ' · 副本' }
function artifactText(kind) {
  return kind === 'plan.json' ? planText.value : kind === 'review.conf' ? currentReview.value?.hocon : JSON.stringify(currentReview.value?.dag, null, 2)
}
async function copy(kind) {
  await run(async () => { await navigator.clipboard.writeText(artifactText(kind)); message.value = '已复制审查内容。' })
}
async function download(kind) {
  await run(async () => {
    const response = await fetch(`${base.value}/studio/candidates/${selected.value.id}/artifacts/${kind}`, { credentials: 'same-origin' })
    if (!response.ok) throw new Error((await response.json()).detail || '下载失败')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a'); link.href = url; link.download = `${selected.value.id}-${kind}`; link.click()
    URL.revokeObjectURL(url); message.value = '已下载已保存修订的审查制品。'
  })
}
watch(projectId, () => run(load))
watch(() => props.projects, () => {
  if (!props.projects.some(p => p.id === projectId.value)) projectId.value = ''
  else if (projectId.value) run(load)
})
</script>

<template>
  <div class="studio-panel">
    <p class="muted">手工模式 · 从元数据档案设计 MySQL → Doris 单表有界全量计划。此阶段仅作确定性审查；无模型调用、引擎执行或正式发布。</p>
    <label class="project-picker">所属项目<select v-model="projectId" :disabled="busy"><option value="">选择项目</option><option v-for="p in projects" :key="p.id" :value="p.id">{{ p.name }}</option></select></label>
    <p v-if="error" class="notice error" role="alert">{{ error }}</p>
    <p v-if="message" class="notice success" role="status">{{ message }}</p>
    <template v-if="projectId">
      <section class="panel">
        <div class="panel-title"><div><p class="eyebrow">MANUAL STUDIO</p><h2>候选工作区</h2></div><button v-if="canMake" class="secondary" :disabled="busy" @click="fresh">＋ 新建手工计划</button></div>
        <div class="candidate-list"><button v-for="candidate in candidates" :key="candidate.id" class="candidate-button" :class="{ active: selected?.id === candidate.id }" :disabled="busy" @click="open(candidate.id)"><strong>{{ candidate.name }}</strong><small>修订 {{ candidate.revision }} · {{ candidate.assessment.passed ? '保存时门禁通过' : '门禁未通过' }}</small></button><p v-if="!candidates.length" class="muted">暂无保存候选。选择源与目标档案，开始设计计划。</p></div>
      </section>
      <section v-if="canMake || selected" class="panel">
        <div class="panel-title"><h2>结构化 EtlPlan</h2><span class="badge accent">{{ selected ? `候选 · 修订 ${selected.revision}` : '未保存' }} · bounded_full</span></div>
        <form class="editor" @submit.prevent="validate">
          <p v-if="!canEdit" class="notice">当前为只读审查。仅本项目 Maker 可创建，原创建者可修改候选。</p>
          <p v-if="!formReady" class="notice">此候选语法不完整，可先查看下方原始计划与门禁定位。<button v-if="canEdit" class="text-button" type="button" @click="plan = blankPlan()">用表单重建此修订的计划</button></p>
          <fieldset v-if="formReady" :disabled="busy || !canEdit" class="plan-fields">
            <label>候选名称<input v-model="name" required maxlength="100"></label>
            <div class="asset-grid"><div v-for="side in ['source', 'target']" :key="side" class="asset-card"><h3>{{ side === 'source' ? '01 / MySQL 源' : '02 / Doris 目标' }}</h3>
              <label>只读探查档案<select v-model="plan[side].profile_id" required @change="chooseProfile(side)"><option value="" disabled>选择已成功探查的档案</option><option v-for="p in available(side)" :key="p.id" :value="p.id">{{ profileLabel(p) }}</option></select></label>
              <label>单表<select v-model="plan[side].table" required :disabled="!profile(side)" @change="suggestMappings"><option value="" disabled>选择一张表</option><option v-for="t in profile(side)?.content.tables || []" :key="t.name" :value="t.name">{{ t.name }}</option></select></label>
              <p v-if="profile(side)" class="small muted">Schema <code>{{ profile(side).schema_hash }}</code></p>
              <p v-if="!available(side).length" class="muted">暂无可用档案，请先在“连接与资产”完成登记与探查。</p>
            </div></div>
            <h3>03 / 字段映射</h3><p class="muted small">仅支持直接投影与重命名，目标每个字段须映射一次。类型只允许已支持的无损兼容；不接受 SQL、表达式、多表或 CDC。</p>
            <div v-if="plan.mappings?.length" class="table-scroll"><table><thead><tr><th>目标字段 / 类型</th><th>源字段 / 类型</th><th>操作</th></tr></thead><tbody><tr v-for="mapping in plan.mappings" :key="mapping.target"><td><strong>{{ mapping.target }}</strong><small>{{ columns('target').find(c => c.name === mapping.target)?.type }}</small></td><td><select v-model="mapping.source" required><option value="" disabled>选择源字段</option><option v-for="column in columns('source')" :key="column.name" :value="column.name">{{ column.name }} / {{ column.type }}{{ column.nullable ? ' · 可空' : '' }}</option></select></td><td>直接映射</td></tr></tbody></table></div>
            <p v-else class="muted">选择源与目标表后生成映射行，可逐项调整源字段。</p>
            <h3>04 / 清洗</h3><p class="muted small">先对映射后的文本目标字段去除首尾空白，再执行必需规则。NULL 保持 NULL；源主键及其映射字段不能清洗，记录身份始终取原始源主键。</p>
            <div v-for="(operation, index) in plan.cleaning || []" :key="index" class="quality-row"><label>文本目标字段<select v-model="operation.field" required><option value="" disabled>选择字段</option><option v-for="mapping in cleanableMappings()" :key="mapping.target" :value="mapping.target">{{ mapping.target }}</option></select></label><span>去除首尾空白</span><button class="text-button danger" type="button" @click="plan.cleaning.splice(index, 1)">移除</button></div>
            <button class="secondary" type="button" :disabled="!cleanableMappings().length" @click="plan.cleaning.push({ kind: 'trim', field: cleanableMappings()[0].target })">＋ 添加文本清洗</button>
            <h3>05 / 必需质量规则</h3><p class="muted small">规则引用清洗后的目标字段。全部适用规则通过才进入有效分支；一行可有多个错误码，但只拒绝、路由一次。</p>
            <div v-for="(rule, index) in plan.quality || []" :key="index" class="quality-row"><label>目标字段<select v-model="rule.field" required><option value="" disabled>选择字段</option><option v-for="m in plan.mappings" :key="m.target" :value="m.target">{{ m.target }}</option></select></label><label>规则<select v-model="rule.kind" @change="rule.minimum = null; rule.maximum = null"><option value="not_null">非空</option><option value="numeric_range">数值范围</option></select></label><template v-if="rule.kind === 'numeric_range'"><label>最小值<input v-model.number="rule.minimum" type="number" step="any"></label><label>最大值<input v-model.number="rule.maximum" type="number" step="any"></label></template><button class="text-button danger" type="button" @click="plan.quality.splice(index, 1)">移除</button></div>
            <button class="secondary" type="button" :disabled="!plan.mappings?.length" @click="plan.quality.push({ kind: 'not_null', field: plan.mappings[0].target, minimum: null, maximum: null })">＋ 添加规则</button>
            <h3>06 / 显式过滤</h3><p class="muted small">引用原始源字段，等于指定值的记录进入错误分支并携带 FILTERED；不会静默丢弃。整型与文本按类型比较，NULL 只匹配 NULL。过滤字段即使未映射也会加入批准的源读取列。</p>
            <div v-for="(condition, index) in plan.filters || []" :key="index" class="quality-row"><label>源字段<select v-model="condition.field" required @change="resetFilterValue(condition)"><option value="" disabled>选择字段</option><option v-for="column in filterColumns()" :key="column.name" :value="column.name">{{ column.name }} / {{ column.type }}</option></select></label><label v-if="condition.equals !== null">等于此值<input :value="condition.equals" type="text" :inputmode="filterIsNumeric(condition.field) ? 'numeric' : undefined" :aria-invalid="filterValueError(condition) ? 'true' : undefined" :aria-describedby="filterValueError(condition) ? 'filter-error-' + index : undefined" @input="setFilterValue(condition, $event.target.value)"><small v-if="filterValueError(condition)" :id="'filter-error-' + index" role="alert">{{ filterValueError(condition) }}</small></label><label><span>匹配 NULL</span><input type="checkbox" :checked="condition.equals === null" @change="$event.target.checked ? condition.equals = null : resetFilterValue(condition)"></label><button class="text-button danger" type="button" @click="plan.filters.splice(index, 1)">移除</button></div>
            <button class="secondary" type="button" :disabled="!filterColumns().length" @click="plan.filters.push({ field: filterColumns()[0].name, equals: filterIsNumeric(filterColumns()[0].name) ? 0 : '' })">＋ 添加拒绝条件</button>
            <label class="check-row"><input v-model="plan.allow_empty" type="checkbox">显式批准空表替换（须随版本重新双人审批）</label>
            <h3>07 / 安全预算与工具意图</h3><div class="budget-grid"><label v-for="(label, key) in budgetLabels" :key="key">{{ label }}<input v-model.number="plan.budget[key]" type="number" :step="['max_reject_ratio', 'max_amplification'].includes(key) ? 'any' : '1'" required></label></div>
            <p class="muted small">工具意图固定为 mysql.read / doris.write。预算是未来受控执行上限，当前未授予任何读写执行权限。</p>
          </fieldset>
          <div class="actions studio-actions"><button v-if="canEdit" class="primary" type="submit" :disabled="busy">确定性校验与审查</button><button v-if="canEdit" class="secondary" type="button" :disabled="busy || !name" @click="save">{{ selected ? '保存修订（含失败候选）' : '保存候选（含失败候选）' }}</button><button v-if="canMake && selected" class="secondary" type="button" :disabled="busy" @click="clone">另存为新候选</button></div>
        </form>
      </section>
      <section v-if="currentReview" class="panel"><div class="panel-title"><h2>确定性门禁</h2><span :class="['badge', currentReview.passed ? 'accent' : '']">{{ currentReview.passed ? '静态审查通过' : '未通过 · 不能冻结' }}</span></div><div class="review-body"><p class="muted small">版本 {{ currentReview.versions.compiler }} / {{ currentReview.versions.policy }} · 修改计划后需重新校验。保存的结果代表保存时状态。</p><article v-for="gate in currentReview.gates" :key="gate.gate" class="gate"><strong>{{ gate.passed ? '✓' : '×' }} {{ gateNames[gate.gate] || gate.gate }}</strong><span>{{ gate.passed ? '通过' : '阻断' }}</span><ul v-if="gate.errors.length"><li v-for="(item, index) in gate.errors" :key="index"><code>{{ item.path }}</code> · {{ item.message }} <small>({{ item.code }})</small></li></ul></article><p class="muted small">{{ currentReview.limitations.join('；') }}。</p></div></section>
      <p v-else-if="projectId && (canMake || selected)" class="notice">{{ assessment ? '计划已变更，原审查制品已隐藏。请重新校验或保存。' : '尚未校验。服务端会重新核对当前项目角色、资源与档案，再生成审查制品。' }}</p>
      <section v-if="currentReview?.passed" class="panel"><div class="panel-title"><h2>同源 DAG · 只读审查</h2><div class="actions"><button class="text-button" :disabled="busy" @click="copy('dag.json')">复制 DAG</button><button class="text-button" :disabled="busy || unsaved" @click="download('dag.json')">下载 DAG</button></div></div><div class="review-body"><div class="dag" role="img" aria-label="MySQL源 → 字段映射 → 质量契约 → Doris目标"><template v-for="(node, index) in currentReview.dag.nodes" :key="node.id"><span v-if="index" aria-hidden="true">→</span><div class="dag-node"><small>{{ node.id }}</small><strong>{{ node.label }}</strong><small v-if="node.mappings">{{ node.mappings.length }} 个映射</small><small v-if="node.rules">{{ node.rules.length }} 条规则 · 未执行</small></div></template></div><details><summary>查看 DAG JSON</summary><pre>{{ artifactText('dag.json') }}</pre></details></div></section>
      <section v-if="currentReview?.passed" class="panel"><div class="panel-title"><h2>HOCON · 只读审查配置</h2><div class="actions"><button class="text-button" :disabled="busy" @click="copy('review.conf')">复制 HOCON</button><button class="text-button" :disabled="busy || unsaved" @click="download('review.conf')">下载 HOCON</button></div></div><div class="review-body"><p class="notice">此为 JSON 兼容 HOCON 的审查格式，仅含安全凭据引用；尚非可直接提交引擎的配置。保存当前计划后可下载。</p><pre tabindex="0">{{ currentReview.hocon }}</pre></div></section>
      <section v-if="canMake || selected" class="panel"><div class="panel-title"><h2>EtlPlan JSON · 只读</h2><div class="actions"><button class="text-button" :disabled="busy" @click="copy('plan.json')">复制计划</button><button class="text-button" :disabled="busy || unsaved" @click="download('plan.json')">下载计划</button></div></div><div class="review-body"><p class="muted small">表单是唯一编辑入口。HOCON 与 DAG 均由这份结构化计划确定性派生，不支持独立编辑。</p><pre tabindex="0">{{ planText }}</pre></div></section>
    </template>
    <div v-else class="choose-project">选择一个项目，设计或审查手工候选。</div>
  </div>
</template>

<style scoped>
.project-picker{max-width:460px;margin-bottom:24px}.candidate-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:12px;padding:20px 24px}.candidate-button{text-align:left;padding:16px;background:#f7fafb;border:1px solid #e3eaf0;border-radius:10px;color:#253f50}.candidate-button.active{border-color:#1c7180;background:#eef8f8}.candidate-button small{display:block;margin-top:8px;color:#657783}.plan-fields{display:block;border:0;padding:0;margin:0;min-width:0}.plan-fields h3{margin:28px 0 10px}.asset-grid{display:grid;grid-template-columns:1fr 1fr;gap:20px}.asset-card{min-width:0;background:#f7fafb;padding:18px;border-radius:12px;margin-top:18px}.asset-card h3{margin-top:0}.asset-card label+label{margin-top:12px}code{overflow-wrap:anywhere}.budget-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:16px}.quality-row{display:flex;align-items:end;gap:12px;margin:12px 0;flex-wrap:wrap}.quality-row label{flex:1;min-width:130px}.studio-actions{margin-top:24px;flex-wrap:wrap}.review-body{padding:20px 24px}.gate{padding:12px 0;border-bottom:1px solid #e5ecf0}.gate>span{float:right;color:#557780}.gate li{margin:8px 0;font-size:13px}.dag{display:flex;align-items:center;gap:12px;overflow-x:auto;padding:10px 0 24px}.dag-node{min-width:150px;display:flex;flex-direction:column;gap:8px;border:1px solid #bad2da;padding:18px;border-radius:12px;background:#f5fafb}.dag-node small{color:#667f8c}pre{white-space:pre;overflow:auto;max-height:480px;padding:20px;background:#f3f7fa;border-radius:10px;font-size:12px;line-height:1.7}.review-body summary{cursor:pointer;color:#366c7e}@media(max-width:1000px){.asset-grid{grid-template-columns:1fr}.panel-title{flex-wrap:wrap;gap:12px}}
</style>
