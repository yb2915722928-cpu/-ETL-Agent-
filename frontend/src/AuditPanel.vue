<script setup>
import { computed, ref, watch } from 'vue'

const props = defineProps({ api: Function, tenantId: String, tenantAdmin: Boolean, projects: Array })
const scope = ref('')
const bundle = ref(null)
const key = ref(null)
const error = ref('')
const busy = ref(false)
const allowed = computed(() => props.projects.filter(p => p.roles.some(r => ['auditor', 'project_admin'].includes(r))))
const path = computed(() => `/api/tenants/${props.tenantId}${scope.value === 'tenant' ? '' : `/projects/${scope.value}`}/evidence`)
const events = computed(() => [...(bundle.value?.events || [])].reverse())
const actionNames = {
  'account.bootstrapped': '初始化账号', 'account.created': '创建账号', 'account.updated': '变更账号',
  'project.bootstrapped': '初始化项目', 'project.created': '创建项目', 'membership.bootstrapped': '初始化成员',
  'membership.assigned': '分配项目角色', 'membership.removed': '移除项目成员',
  'session.login': '登录', 'session.logout': '退出登录',
}

async function load() {
  bundle.value = null
  if (!scope.value) return
  busy.value = true
  error.value = ''
  try {
    bundle.value = await props.api(path.value)
    key.value = await props.api('/api/evidence/public-key')
  } catch (cause) { error.value = cause.message }
  finally { busy.value = false }
}

function save(value, name) {
  const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], { type: 'application/json' }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = name
  anchor.click()
  URL.revokeObjectURL(url)
}

async function download(kind) {
  busy.value = true
  error.value = ''
  try {
    const value = await props.api(kind === 'key' ? '/api/evidence/public-key' : `${path.value}/${kind}`)
    save(value, kind === 'key' ? 'trusted-key.json' : kind === 'checkpoint' ? 'checkpoint.json' : 'evidence.json')
  } catch (cause) { error.value = cause.message }
  finally { busy.value = false }
}

watch(() => [props.tenantId, props.tenantAdmin, props.projects], () => {
  if (scope.value === 'tenant' && props.tenantAdmin) return
  if (allowed.value.some(p => p.id === scope.value)) return
  scope.value = props.tenantAdmin ? 'tenant' : allowed.value[0]?.id || ''
}, { immediate: true })
watch(scope, load, { immediate: true })
</script>

<template>
  <section class="audit-view">
    <p class="muted">查看真实身份操作历史，导出完整哈希链与 Ed25519 签名检查点。项目证据仅向本项目审计人员及项目管理员开放。</p>
    <div class="audit-toolbar">
      <label>审计范围<select v-model="scope" :disabled="busy"><option v-if="tenantAdmin" value="tenant">租户完整账本 · 含账号与登录事件</option><option v-for="project in allowed" :key="project.id" :value="project.id">{{ project.name }} · 项目完整账本</option></select></label>
      <button class="secondary" :disabled="busy || !scope" @click="load">刷新审计</button>
    </div>
    <p v-if="!scope" class="empty">暂无可审计范围，请联系项目管理员分配审计角色。</p>
    <p v-if="error" class="notice error" role="alert">{{ error }}</p>
    <section v-if="bundle" class="panel">
      <div class="panel-title"><div><p class="eyebrow">可验证证据</p><h2>连续事件 {{ bundle.events.length }} 条</h2></div><span class="badge accent">Ed25519 签名检查点</span></div>
      <div class="audit-actions"><button class="primary" :disabled="busy" @click="download('export')">导出完整证据</button><button class="secondary" :disabled="busy" @click="download('checkpoint')">单独保存检查点</button><button class="secondary" :disabled="busy" @click="download('key')">下载公钥</button></div>
      <div class="audit-checkpoint"><p>检查点序号 <strong>{{ bundle.checkpoint.sequence }}</strong> · {{ new Date(bundle.checkpoint.issued_at).toLocaleString() }}</p><p>链头哈希 <code>{{ bundle.checkpoint.event_hash }}</code></p><p v-if="key">签名公钥指纹 <code>{{ key.key_id }}</code></p></div>
      <p class="panel-note">请通过部署者确认公钥指纹，把公钥和检查点保存在平台之外的独立位置。离线验签可发现修改、删除和重排；识别完整尾部删除需要事先独立保留的检查点。签名有效不代表对所有特权管理员绝对防篡改。</p>
      <div class="table-scroll"><table><thead><tr><th>序号 / 时间</th><th>操作与主体</th><th>安全事实与制品关联</th><th>事件哈希</th></tr></thead><tbody><tr v-for="event in events" :key="event.id"><td><strong>#{{ event.sequence }}</strong><small>{{ new Date(event.occurred_at).toLocaleString() }}</small></td><td><strong>{{ actionNames[event.action] || event.action }}</strong><small>{{ event.actor_id }}</small></td><td><details><summary>查看事实</summary><pre>{{ JSON.stringify({ facts: event.facts, version_id: event.version_id, preparation_id: event.preparation_id, run_id: event.run_id, artifact_hashes: event.artifact_hashes }, null, 2) }}</pre></details></td><td><code :title="event.event_hash">{{ event.event_hash.slice(0, 16) }}…</code></td></tr></tbody></table></div>
      <p v-if="!events.length" class="empty">当前范围尚无证据事件，空链检查点仍可独立验签。</p>
    </section>
  </section>
</template>

<style scoped>
.audit-toolbar, .audit-actions { display: flex; align-items: end; flex-wrap: wrap; gap: 12px; margin: 22px 0; }
.audit-toolbar label { flex: 1; max-width: 520px; }
.audit-actions, .audit-checkpoint { padding: 0 24px; }
.audit-checkpoint { font-size: 13px; color: #617070; overflow-wrap: anywhere; }
.audit-checkpoint code { font-size: 11px; }
pre { white-space: pre-wrap; min-width: 220px; font-size: 12px; }
</style>
