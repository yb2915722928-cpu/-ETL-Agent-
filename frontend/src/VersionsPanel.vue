<script setup>
import { computed, ref, watch } from 'vue'

const props = defineProps({ api: Function, tenantId: String, projects: Array })
const projectId = ref('')
const candidates = ref([])
const versions = ref([])
const candidateId = ref('')
const selected = ref(null)
const otherId = ref('')
const comparison = ref(null)
const artifact = ref('review.conf')
const busy = ref(false)
const error = ref('')
const message = ref('')
const base = computed(() => `/api/tenants/${props.tenantId}/projects/${projectId.value}`)
const project = computed(() => props.projects.find(p => p.id === projectId.value))
const canMake = computed(() => project.value?.roles.includes('maker'))
const candidate = computed(() => candidates.value.find(c => c.id === candidateId.value))
const artifactNames = { 'review.conf': 'HOCON 审查配置', 'plan.json': '结构化计划', 'dag.json': 'DAG', 'facts.json': '执行事实', 'manifest.json': '制品摘要清单' }
const sectionNames = { mapping: '字段映射', schema: '源 / 目标 Schema', quality: '质量规则', hocon: 'HOCON' }
const text = computed(() => selected.value?.artifacts[artifact.value] || '')
function pretty(value) { return JSON.stringify(value, null, 2) }
function label(row) { return `${row.name} · 候选修订 ${row.candidate_revision} · ${row.id.slice(0, 8)}` }
async function run(action) {
  busy.value = true; error.value = ''; message.value = ''
  try { await action() } catch (cause) { error.value = cause.message }
  finally { busy.value = false }
}
async function load() {
  candidates.value = []; versions.value = []; selected.value = null; candidateId.value = ''; otherId.value = ''; comparison.value = null
  if (!projectId.value) return
  const [saved, frozen] = await Promise.all([props.api(`${base.value}/studio/candidates`), props.api(`${base.value}/versions`)])
  candidates.value = saved; versions.value = frozen
}
async function open(id) {
  selected.value = null; comparison.value = null; otherId.value = ''
  selected.value = await props.api(`${base.value}/versions/${id}`)
}
async function freeze() {
  await run(async () => {
    const frozen = await props.api(`${base.value}/versions`, { method: 'POST', body: JSON.stringify({
      candidate_id: candidate.value.id, expected_revision: candidate.value.revision,
    }) })
    versions.value = await props.api(`${base.value}/versions`)
    await open(frozen.id)
    message.value = `已冻结候选修订 ${frozen.candidate_revision}。再次冻结相同修订与执行事实会返回同一版本。`
  })
}
async function compare() {
  await run(async () => {
    comparison.value = null
    comparison.value = await props.api(`${base.value}/versions/${selected.value.id}/diff/${otherId.value}`)
  })
}
async function readArtifact() {
  const response = await fetch(`${base.value}/versions/${selected.value.id}/artifacts/${artifact.value}`, { credentials: 'same-origin', cache: 'no-store' })
  if (!response.ok) throw new Error((await response.json()).detail || '读取制品失败')
  return response
}
async function copy() {
  await run(async () => {
    const response = await readArtifact()
    await navigator.clipboard.writeText(await response.text())
    message.value = '已核验当前项目权限并复制冻结制品。'
  })
}
async function download() {
  await run(async () => {
    const response = await readArtifact()
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a'); link.href = url; link.download = `${selected.value.id}-${artifact.value}`; link.click()
    URL.revokeObjectURL(url); message.value = '已下载冻结制品原始字节，可按 SHA256 重算验证。'
  })
}
watch(projectId, () => run(load))
watch(otherId, () => { comparison.value = null })
watch(() => props.projects, () => {
  if (!props.projects.some(p => p.id === projectId.value)) projectId.value = ''
  else if (projectId.value) run(load)
})
</script>

<template>
  <div class="versions-panel">
    <p class="muted">冻结通过当前门禁的候选，保留修订、事实与制品原始字节。修正计划后创建新版本；此阶段仅保存审查制品。</p>
    <label class="project-picker">所属项目<select v-model="projectId" :disabled="busy"><option value="">选择项目</option><option v-for="p in projects" :key="p.id" :value="p.id">{{ p.name }}</option></select></label>
    <p v-if="error" class="notice error" role="alert">{{ error }}</p>
    <p v-if="message" class="notice success" role="status">{{ message }}</p>
    <template v-if="projectId">
      <section v-if="canMake" class="panel">
        <div class="panel-title"><div><p class="eyebrow">FREEZE CANDIDATE</p><h2>冻结当前修订</h2></div><span class="badge accent">Maker</span></div>
        <form class="freeze-form" @submit.prevent="freeze">
          <label>已保存候选<select v-model="candidateId" :disabled="busy" required><option value="">选择候选</option><option v-for="c in candidates" :key="c.id" :value="c.id">{{ c.name }} · 修订 {{ c.revision }} · {{ c.assessment.passed ? '保存时通过' : '保存时未通过' }}</option></select></label>
          <button class="primary" :disabled="busy || !candidate" type="submit">冻结当前修订</button>
          <p class="muted small">冻结会重新核对当前资源、凭据和安全门禁。若修订变化或门禁不再通过，请返回手工集成 Studio 修复并保存。</p>
        </form>
      </section>
      <section class="panel">
        <div class="panel-title"><div><p class="eyebrow">IMMUTABLE VERSIONS</p><h2>不可变版本</h2></div><button class="secondary" :disabled="busy" @click="run(load)">刷新版本</button></div>
        <div class="version-list"><button v-for="v in versions" :key="v.id" :class="['version-button', { active: selected?.id === v.id }]" :disabled="busy" @click="run(() => open(v.id))"><strong>{{ v.name }}</strong><small>修订 {{ v.candidate_revision }} · {{ new Date(v.created_at).toLocaleString('zh-CN') }}</small><code>{{ v.execution_sha256.slice(0, 20) }}…</code><small>{{ v.id }}</small></button><p v-if="!versions.length" class="muted">尚无冻结版本。Maker 可选择已保存候选，重新审查并冻结。</p></div>
      </section>
      <template v-if="selected">
        <section class="panel"><div class="panel-title"><h2>冻结事实与摘要</h2><span class="badge accent">只读 · 不可变</span></div><div class="version-body">
          <dl><dt>版本标识</dt><dd><code>{{ selected.id }}</code></dd><dt>候选 / 修订</dt><dd><code>{{ selected.candidate_id }}</code> / {{ selected.candidate_revision }}</dd><dt>执行事实 SHA256</dt><dd><code>{{ selected.execution_sha256 }}</code></dd><dt>制品清单 SHA256</dt><dd><code>{{ selected.bundle_sha256 }}</code></dd></dl>
          <p class="muted small">执行摘要排除采集时间、统计、样本与 Profile 随机标识；制品清单同时绑定计划中保留的档案引用。HOCON 为 review_only 审查格式。</p>
          <details><summary>查看冻结档案引用与编译 / 策略版本</summary><pre>{{ pretty({ profile_refs: selected.frozen.profile_refs, versions: selected.frozen.versions }) }}</pre></details>
        </div></section>
        <section class="panel"><div class="panel-title"><h2>冻结制品 · 只读</h2><div class="actions"><button class="text-button" :disabled="busy" @click="copy">复制制品</button><button class="text-button" :disabled="busy" @click="download">下载制品</button></div></div><div class="version-body">
          <label>制品<select v-model="artifact" :disabled="busy"><option v-for="(name, key) in artifactNames" :key="key" :value="key">{{ name }} · {{ key }}</option></select></label>
          <p class="digest small">SHA256 <code>{{ selected.artifact_hashes[artifact] }}</code></p><pre tabindex="0">{{ text }}</pre>
        </div></section>
        <section class="panel"><div class="panel-title"><h2>版本 Diff</h2></div><div class="version-body">
          <p class="muted small">从当前版本比较至另一冻结版本，展示映射、脱敏结构、HOCON 与质量规则变化。</p>
          <form class="compare-form" @submit.prevent="compare"><label>比较至<select v-model="otherId" :disabled="busy" required><option value="">选择另一版本</option><option v-for="v in versions.filter(v => v.id !== selected.id)" :key="v.id" :value="v.id">{{ label(v) }}</option></select></label><button class="secondary" :disabled="busy || !otherId">比较版本</button></form>
          <template v-if="comparison"><p class="notice">{{ comparison.execution_changed ? '执行事实有变化，请审查下方差异。' : '执行事实一致。档案引用或冻结时间可不同。' }}</p>
            <article v-for="(section, key) in comparison.sections" :key="key" class="diff-section"><h3>{{ sectionNames[key] }} <span class="badge" :class="section.changed ? 'accent' : ''">{{ section.changed ? '有变化' : '一致' }}</span></h3>
              <template v-if="section.changed"><pre v-if="key === 'hocon'" tabindex="0">{{ section.unified }}</pre><div v-else class="diff-grid"><div><h4>原版本</h4><pre tabindex="0">{{ pretty(section.before) }}</pre></div><div><h4>比较版本</h4><pre tabindex="0">{{ pretty(section.after) }}</pre></div></div></template>
            </article>
          </template>
        </div></section>
      </template>
    </template>
    <div v-else class="choose-project">选择一个项目，冻结或审查不可变版本。</div>
  </div>
</template>

<style scoped>
.project-picker{max-width:460px;margin-bottom:24px}.freeze-form,.version-body{padding:20px 24px}.freeze-form{display:flex;align-items:end;gap:16px;flex-wrap:wrap}.freeze-form label{flex:1;min-width:240px}.freeze-form p{width:100%;margin:0}.version-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px;padding:20px 24px}.version-button{text-align:left;padding:16px;background:#f7fafb;border:1px solid #e3eaf0;border-radius:10px;color:#253f50;min-width:0}.version-button.active{border-color:#1c7180;background:#eef8f8}.version-button small,.version-button code{display:block;margin-top:8px;color:#657783}.version-button small{overflow-wrap:anywhere}code{overflow-wrap:anywhere}dl{display:grid;grid-template-columns:160px minmax(0,1fr);gap:12px}dt{color:#657783}dd{margin:0}.digest{overflow-wrap:anywhere;color:#657783}pre{white-space:pre;overflow:auto;max-height:480px;padding:20px;background:#f3f7fa;border-radius:10px;font-size:12px;line-height:1.7}.version-body summary{cursor:pointer;color:#366c7e}.compare-form{display:flex;align-items:end;gap:16px}.compare-form label{flex:1;min-width:0}.diff-grid{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:16px}.diff-section{border-top:1px solid #e3eaf0;margin-top:24px;padding-top:12px}.diff-section h3{display:flex;align-items:center;gap:12px}.diff-grid h4{font-weight:500;color:#657783;margin-bottom:0}@media(max-width:1000px){.diff-grid{grid-template-columns:minmax(0,1fr)}dl{grid-template-columns:1fr;gap:8px}dd{margin-bottom:12px}.compare-form,.panel-title{flex-wrap:wrap}}
</style>
