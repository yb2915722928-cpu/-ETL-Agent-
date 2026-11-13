<script setup>
import { computed, ref, reactive, watch } from 'vue'
const props = defineProps({ api: Function, tenantId: String, projects: Array })
const projectId = ref('')
const connections = ref([])
const resources = ref([])
const profiles = ref([])
const active = ref(null)
const error = ref('')
const message = ref('')
const busy = ref(false)
const form = reactive({ name: '', resource_id: '', password: '' })
const project = computed(() => props.projects.find(p => p.id === projectId.value))
const canProbe = computed(() => project.value?.roles.includes('maker'))
const base = computed(() => `/api/tenants/${props.tenantId}/projects/${projectId.value}`)
const resource = computed(() => resources.value.find(r => r.id === form.resource_id))
const statusNames = { registered: '待探查', ready: '探查成功', failed: '探查失败' }
async function run(action) {
  busy.value = true; error.value = ''; message.value = ''
  try { await action() } catch (cause) { error.value = cause.message }
  finally { busy.value = false }
}
async function load() {
  connections.value = []; resources.value = []; profiles.value = []; active.value = null
  form.password = ''; form.resource_id = ''; form.name = ''
  if (!projectId.value) return
  resources.value = await props.api(`${base.value}/connector-resources`)
  connections.value = await props.api(`${base.value}/connections`)
}
async function select(connection) {
  active.value = connection; profiles.value = []
  profiles.value = await props.api(`${base.value}/connections/${connection.id}/profiles`)
}
async function create() {
  await run(async () => {
    let created
    try { created = await props.api(`${base.value}/connections`, { method: 'POST', body: JSON.stringify(form) }) }
    finally { form.password = '' }
    connections.value = await props.api(`${base.value}/connections`)
    await select(created)
    form.name = ''; message.value = '新连接和加密凭据引用已保存，可以开始只读探查。'
  })
}
async function probe(connection) {
  await run(async () => {
    try { await props.api(`${base.value}/connections/${connection.id}/probe`, { method: 'POST', body: '{}' }) }
    finally {
      connections.value = await props.api(`${base.value}/connections`)
      await select(connections.value.find(c => c.id === connection.id))
    }
    message.value = 'P3 只读探查完成，元数据档案已保存。'
  })
}
watch(projectId, () => run(load))
watch(() => props.projects, () => {
  if (!props.projects.some(p => p.id === projectId.value)) projectId.value = ''
  else run(load)
})
</script>

<template>
  <div class="connections-panel">
    <p class="muted">登记 MySQL 数据源与 Doris 目标，查看受管只读探查得到的结构、近似统计和脱敏样本。</p>
    <label class="project-picker">所属项目<select v-model="projectId" :disabled="busy"><option value="">选择项目</option><option v-for="p in projects" :key="p.id" :value="p.id">{{ p.name }}</option></select></label>
    <p v-if="error" class="notice error" role="alert">{{ error }}</p>
    <p v-if="message" class="notice success" role="status">{{ message }}</p>
    <template v-if="projectId">
      <section class="panel">
        <div class="panel-title"><h2>受管连接</h2><span class="count">{{ connections.length }}</span></div>
        <div v-if="connections.length" class="table-scroll"><table><thead><tr><th>连接 / 资源</th><th>类型与用途</th><th>探查状态</th><th>操作</th></tr></thead><tbody>
          <tr v-for="connection in connections" :key="connection.id"><td><strong>{{ connection.name }}</strong><small>{{ connection.resource_id }}</small></td><td>{{ connection.kind === 'mysql' ? 'MySQL · 源端' : 'Doris · 目标端' }}</td><td>{{ statusNames[connection.status] }}</td><td><div class="actions"><button class="text-button" :disabled="busy" @click="run(() => select(connection))">查看档案</button><button v-if="canProbe" class="text-button" :disabled="busy" @click="probe(connection)">{{ busy ? '探查中…' : '测试连接与探查' }}</button></div></td></tr>
        </tbody></table></div>
        <p v-else class="panel-note">暂无连接。Maker 可在下方登记部署者已授权的资源。</p>
      </section>
      <section v-if="canProbe" class="panel"><div class="panel-title"><h2>登记新连接</h2><span class="badge accent">Maker · P3</span></div>
        <form class="editor" @submit.prevent="create"><div class="form-grid"><label>连接名称<input v-model="form.name" required maxlength="100" placeholder="例如：客户资料源"></label><label>授权资源<select v-model="form.resource_id" required :disabled="busy"><option value="" disabled>选择白名单资源</option><option v-for="r in resources" :key="r.id" :value="r.id">{{ r.kind }} / {{ r.database }} / {{ r.id }}</option></select></label><label>受管只读账号密码<input v-model="form.password" type="password" required maxlength="512" autocomplete="new-password" placeholder="保存后不再显示"></label></div>
          <p v-if="resource" class="muted">授权表：{{ resource.tables.join('、') }}。每表最多 5 条脱敏样本；每条查询最多 3 秒。</p>
          <p v-if="!resources.length" class="notice">暂无已授权资源，请部署者配置连接白名单。</p>
          <button class="primary" :disabled="busy || !resources.length">保存新连接</button><p class="muted small">凭据使用部署主密钥加密保存。此页登记新凭据引用及版本，已有连接的凭据不能覆盖。</p>
        </form>
      </section>
      <p v-else class="panel-note">当前角色可读取本项目的脱敏档案；登记和探查需要 Maker 角色。</p>
      <section v-if="active" class="panel"><div class="panel-title"><div><p class="eyebrow">METADATA PROFILE</p><h2>{{ active.name }} · 元数据档案</h2></div></div>
        <div class="profile-details"><span>凭据版本 {{ active.secret_version }}</span><code>引用 {{ active.secret_reference_id }}</code></div>
        <p v-if="!profiles.length" class="panel-note">暂无成功档案。完成一次只读探查后，此处展示实际元数据。</p>
        <article v-for="profile in profiles" :key="profile.id" class="metadata-profile"><p class="muted">{{ new Date(profile.created_at).toLocaleString('zh-CN') }} · {{ profile.content.version_kind === 'mysql_protocol_compatibility' ? 'MySQL 协议兼容版本' : '服务版本' }} {{ profile.content.server_version }} · {{ profile.content.policy_version }}</p><code>档案 {{ profile.id }}</code>
          <div v-for="table in profile.content.tables" :key="table.name" class="metadata-table"><h3>{{ profile.content.database }}.{{ table.name }}</h3><p class="muted">近似行数：{{ table.approx_rows ?? '暂无估计' }} · 近似字节：{{ table.approx_bytes ?? '暂无估计' }}（服务端估计，非精确扫描计数）</p>
            <div class="table-scroll"><table><thead><tr><th>字段</th><th>实际类型</th><th>可空</th><th>键</th></tr></thead><tbody><tr v-for="column in table.columns" :key="column.name"><td>{{ column.name }}</td><td><code>{{ column.type }}</code></td><td>{{ column.nullable ? '是' : '否' }}</td><td>{{ column.key || '—' }}</td></tr></tbody></table></div>
            <details><summary>脱敏样本（{{ table.samples.length }} 条）</summary><div v-if="table.samples.length" class="table-scroll"><table><thead><tr><th v-for="column in table.columns" :key="column.name">{{ column.name }}</th></tr></thead><tbody><tr v-for="(row, index) in table.samples" :key="index"><td v-for="column in table.columns" :key="column.name">{{ row[column.name] ?? 'NULL' }}</td></tr></tbody></table></div><p v-else class="muted">该表暂无样本。</p></details>
          </div>
        </article>
      </section>
    </template>
    <div v-else class="choose-project">选择一个项目，管理连接与元数据档案。</div>
  </div>
</template>

<style scoped>
.project-picker{max-width:460px;margin-bottom:24px}.profile-details{display:flex;gap:18px;flex-wrap:wrap;padding:12px 24px}.metadata-profile{padding:24px;border-top:1px solid #e6ebf1}.metadata-table{margin-top:24px}.metadata-table h3{margin-bottom:4px}.metadata-table details{margin-top:16px}.metadata-table summary{cursor:pointer;color:#366c7e}.metadata-table .table-scroll{margin-top:12px}code{overflow-wrap:anywhere}
</style>
