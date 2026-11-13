<script setup>
import { computed, ref, watch } from 'vue'

const props = defineProps({ api: Function, tenantId: String, accountId: String, projects: Array })
const projectId = ref('')
const versionId = ref('')
const tool = ref('pipeline.publish')
const versions = ref([])
const rows = ref([])
const selected = ref(null)
const busy = ref(false)
const error = ref('')
const message = ref('')
const base = computed(() => `/api/tenants/${props.tenantId}/projects/${projectId.value}`)
const roles = computed(() => props.projects.find(p => p.id === projectId.value)?.roles || [])
const states = { pending: '待独立双审批', approved: '双审批通过', rejected: '已拒绝', denied: '策略禁止', expired: '已过期', invalidated: '已失效', auto_authorized: '受限只读自动授权' }
const slots = { data: '数据审批', security: '安全审批' }
const roleForSlot = { data: 'data_checker', security: 'security_checker' }
function pretty(value) { return JSON.stringify(value, null, 2) }
function time(value) { return new Date(value).toLocaleString('zh-CN') }
async function run(action) {
  busy.value = true; error.value = ''; message.value = ''
  try { await action() } catch (cause) { error.value = cause.message }
  finally { busy.value = false }
}
async function load() {
  versions.value = []; rows.value = []; selected.value = null; versionId.value = ''
  if (!projectId.value) return
  const [v, r] = await Promise.all([props.api(`${base.value}/versions`), props.api(`${base.value}/preparations`)])
  versions.value = v; rows.value = r
}
async function open(id) { selected.value = await props.api(`${base.value}/preparations/${id}`) }
async function prepare() {
  await run(async () => {
    selected.value = await props.api(`${base.value}/preparations`, { method: 'POST', body: JSON.stringify({ version_id: versionId.value, tool: tool.value, environment: 'local' }) })
    rows.value = await props.api(`${base.value}/preparations`)
    message.value = '已冻结本次操作。有效期固定24小时，批准不会延长；失效后须重新准备与审批。'
  })
}
function canDecide(slot) {
  return selected.value?.validation.state === 'pending' && roles.value.includes(roleForSlot[slot]) &&
    props.accountId !== selected.value.maker_id && !selected.value.decisions.some(d => d.slot === slot || d.checker_id === props.accountId)
}
async function decide(slot, decision) {
  await run(async () => {
    const id = selected.value.id
    try {
      selected.value = await props.api(`${base.value}/preparations/${id}/decisions`, { method: 'POST', body: JSON.stringify({ slot, decision }) })
      rows.value = await props.api(`${base.value}/preparations`)
      message.value = `${slots[slot]}已具名${decision === 'approve' ? '批准' : '拒绝'}，决定不可修改。`
    } catch (cause) {
      await open(id)
      throw cause
    }
  })
}
watch(projectId, () => run(load))
watch(() => props.projects, () => {
  if (!props.projects.some(p => p.id === projectId.value)) projectId.value = ''
  else if (projectId.value) run(load)
})
</script>

<template>
  <div class="preparations-panel">
    <p class="muted">Maker 冻结操作事实；独立数据与安全 Checker 使用各自账号审查。复用最新探查档案，并通过受管只读出口实时核对外部结构与权限。结构变化后请在连接与资产中重新探查并冻结版本。</p>
    <label class="project-picker">所属项目<select v-model="projectId" :disabled="busy"><option value="">选择项目</option><option v-for="p in projects" :key="p.id" :value="p.id">{{ p.name }}</option></select></label>
    <p v-if="error" class="notice error" role="alert">{{ error }}</p>
    <p v-if="message" class="notice success" role="status">{{ message }}</p>
    <template v-if="projectId">
      <section v-if="roles.includes('maker')" class="panel">
        <div class="panel-title"><h2>生成冻结准备单</h2><span class="badge accent">Maker</span></div>
        <form class="prepare-form" @submit.prevent="prepare">
          <label>冻结版本<select v-model="versionId" :disabled="busy" required><option value="">选择已审查版本</option><option v-for="v in versions" :key="v.id" :value="v.id">{{ v.name }} · {{ v.id.slice(0, 8) }}</option></select></label>
          <label>本次操作<select v-model="tool" :disabled="busy"><option value="pipeline.publish">P1 · 正式发布意图</option><option value="pipeline.trial">P2 · 受限试运行意图</option><option value="metadata.read">P3 · 仅受限只读元数据</option></select></label>
          <button class="primary" :disabled="busy || !versionId">冻结24小时准备单</button>
          <p class="muted small">准备阶段只保存本地事实。P1/P2 均须两名不同审批人，申请人禁止自批。试运行预算最多1万行、16MiB、120秒。</p>
        </form>
      </section>
      <section class="panel">
        <div class="panel-title"><h2>项目准备单</h2><button class="secondary" :disabled="busy" @click="run(load)">刷新状态与资格</button></div>
        <div class="preparation-list"><button v-for="row in rows" :key="row.id" :class="['preparation-button', { active: row.id === selected?.id }]" :disabled="busy" @click="run(() => open(row.id))"><strong>{{ row.risk_level }} · {{ states[row.validation.state] }}</strong><small>{{ row.tool }} · {{ row.id.slice(0, 8) }}</small><small>截止 {{ time(row.expires_at) }}</small></button><p v-if="!rows.length" class="muted">尚无准备单，请先冻结版本后由 Maker 生成。</p></div>
      </section>
      <template v-if="selected">
        <section class="panel"><div class="panel-title"><h2>冻结事实与当前有效性</h2><span class="badge accent">{{ states[selected.validation.state] }}</span></div><div class="body">
          <p v-if="selected.validation.reasons.length" class="notice error">{{ selected.validation.reasons.join('；') }}</p>
          <dl><dt>风险与原因</dt><dd>{{ selected.risk_level }} · {{ selected.frozen.risk_reason }}</dd><dt>申请人</dt><dd><code>{{ selected.maker_id }}</code> · 当前资格{{ selected.validation.current_eligibility.maker ? '有效' : '失效' }}</dd><dt>有效期</dt><dd>{{ time(selected.created_at) }} → {{ time(selected.expires_at) }}（不续期）</dd><dt>冻结指纹</dt><dd><code>{{ selected.frozen_sha256 }}</code></dd><dt>版本</dt><dd><code>{{ selected.version_id }}</code></dd><dt>检查方式</dt><dd>{{ selected.frozen.checks }}</dd><dt>数据分级</dt><dd>{{ selected.frozen.classification }}</dd></dl>
          <details><summary>资源、Schema 与凭据版本</summary><pre>{{ pretty(selected.frozen.impact) }}</pre></details>
          <details><summary>质量规则、指标口径与预算</summary><pre>{{ pretty({ quality: selected.frozen.quality, metrics: selected.frozen.metrics, budget: selected.frozen.budget }) }}</pre></details>
          <details><summary>发布 / 恢复方案</summary><pre>{{ pretty(selected.frozen.recovery) }}</pre></details>
          <details><summary>授权范围、计划、HOCON 与策略版本</summary><pre>{{ pretty({ operation: selected.frozen.authorized_operation, plan: selected.frozen.plan, hocon: selected.frozen.hocon, policy: selected.frozen.policy_version, compiler: selected.frozen.compiler_versions }) }}</pre></details>
        </div></section>
        <section class="panel"><div class="panel-title"><h2>独立具名审批</h2><span class="muted small">批准不会续期 · 决定不可修改</span></div><div class="body">
          <p v-if="!selected.frozen.approval_slots.length" class="muted">{{ selected.risk_level === 'P3' ? '受限只读操作按当前权限、白名单和预算自动授权，不授予写入能力。' : 'P0 操作被禁止，人工审批不能提升权限。' }}</p>
          <div v-for="slot in selected.frozen.approval_slots" :key="slot" class="decision">
            <h3>{{ slots[slot] }}</h3>
            <template v-for="d in selected.decisions.filter(d => d.slot === slot)" :key="d.id"><p>{{ d.checker_name }} · {{ d.decision === 'approve' ? '已批准' : '已拒绝' }} · {{ time(d.decided_at) }}</p><small class="muted">当前职责{{ selected.validation.current_eligibility[slot] ? '有效' : '失效' }} · 账号 {{ d.checker_id }}</small></template>
            <p v-if="!selected.decisions.some(d => d.slot === slot)" class="muted">等待独立{{ slots[slot] }}人决定</p>
            <div v-if="canDecide(slot)" class="actions"><button class="primary" :disabled="busy" @click="decide(slot, 'approve')">具名批准</button><button class="secondary" :disabled="busy" @click="decide(slot, 'reject')">具名拒绝</button></div>
          </div>
          <p class="muted small">当前账号、Maker 与审批人资格、策略及冻结事实在服务端重新核验。过期、撤权、拒绝或事实变化后，须重新准备与审批。</p>
        </div></section>
      </template>
    </template>
    <div v-else class="choose-project">选择项目，审查准备单与独立审批。</div>
  </div>
</template>

<style scoped>
.project-picker{max-width:460px;margin-bottom:24px}.prepare-form,.body{padding:20px 24px}.prepare-form{display:flex;align-items:end;gap:16px;flex-wrap:wrap}.prepare-form label{flex:1;min-width:220px}.prepare-form p{width:100%;margin:0}.preparation-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px;padding:20px 24px}.preparation-button{text-align:left;padding:16px;background:#f7fafb;border:1px solid #e3eaf0;border-radius:10px;color:#253f50}.preparation-button.active{border-color:#1c7180;background:#eef8f8}.preparation-button small{display:block;margin-top:8px;color:#657783}dl{display:grid;grid-template-columns:150px minmax(0,1fr);gap:12px}dt{color:#657783}dd{margin:0;overflow-wrap:anywhere}summary{cursor:pointer;color:#366c7e;margin:16px 0}pre{white-space:pre;overflow:auto;max-height:420px;padding:20px;background:#f3f7fa;border-radius:10px;font-size:12px;line-height:1.7}.decision{border-bottom:1px solid #e3eaf0;padding:4px 0 20px;margin-bottom:20px}.decision small{display:block;overflow-wrap:anywhere}.actions{margin-top:16px}@media(max-width:900px){dl{grid-template-columns:1fr;gap:8px}dd{margin-bottom:12px}.panel-title{flex-wrap:wrap}}
</style>
