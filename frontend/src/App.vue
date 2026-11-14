<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import AuditPanel from './AuditPanel.vue'
import ConnectionsPanel from './ConnectionsPanel.vue'
import StudioPanel from './StudioPanel.vue'
import VersionsPanel from './VersionsPanel.vue'
import PreparationsPanel from './PreparationsPanel.vue'
import RunsPanel from './RunsPanel.vue'

const roleNames = {
  project_admin: '项目管理员', maker: 'Maker · 申请人', data_checker: '数据 Checker',
  security_checker: '安全 Checker', operator: 'Operator · 操作员', auditor: 'Auditor · 审计人员',
}
const identity = ref(null)
const projects = ref([])
const selected = ref(null)
const members = ref([])
const accounts = ref([])
const eligible = ref([])
const tab = ref('projects')
const error = ref('')
const message = ref('')
const busy = ref(false)
const health = ref(null)
const login = reactive({ tenant: 'demo', username: '', password: '' })
const newProject = reactive({ slug: '', name: '' })
const newAccount = reactive({ username: '', display_name: '', password: '', tenant_admin: false })
const memberEdit = reactive({ account_id: '', roles: [] })
const accountEdit = reactive({ id: '', enabled: true, tenant_admin: false, password: '' })
const tenantPath = computed(() => `/api/tenants/${identity.value?.tenant.id}`)
const memberPath = computed(() => `${tenantPath.value}/projects/${selected.value?.id}`)
const isTenantAdmin = computed(() => identity.value?.account.tenant_admin)
const canManage = computed(() => selected.value?.can_manage_members === true)

async function api(path, options = {}) {
  const response = await fetch(path, { credentials: 'same-origin', ...options,
    headers: { 'Content-Type': 'application/json', ...options.headers } })
  const result = await response.json()
  if (!response.ok) {
    if (response.status === 401) {
      identity.value = null
      selected.value = null
      members.value = []
      accounts.value = []
    }
    throw new Error(result.detail || '请求失败，请重试')
  }
  return result
}

async function run(action) {
  busy.value = true
  error.value = ''
  message.value = ''
  try { await action() } catch (cause) { error.value = cause.message }
  finally { busy.value = false }
}

async function loadHealth() {
  try {
    const response = await fetch('/api/health')
    health.value = await response.json()
  } catch { health.value = { status: 'unavailable' } }
}

async function loadProject(project) {
  selected.value = project
  memberEdit.account_id = ''
  memberEdit.roles = []
  eligible.value = []
  members.value = await api(`${memberPath.value}/members`)
  if (canManage.value) eligible.value = await api(`${memberPath.value}/eligible-accounts`)
}

async function refresh() {
  identity.value = await api('/api/me')
  projects.value = await api(`${tenantPath.value}/projects`)
  if (!isTenantAdmin.value && tab.value === 'accounts') tab.value = 'projects'
  accounts.value = isTenantAdmin.value ? await api(`${tenantPath.value}/accounts`) : []
  const previous = projects.value.find(p => p.id === selected.value?.id)
  if (previous) await loadProject(previous)
  else { selected.value = null; members.value = []; eligible.value = [] }
  await loadHealth()
}

async function signIn() {
  await run(async () => {
    try { await api('/api/login', { method: 'POST', body: JSON.stringify(login) }) }
    finally { login.password = '' }
    await refresh()
    message.value = '登录成功。权限由服务器按当前账号与成员关系核验。'
  })
}

async function signOut() {
  await run(async () => {
    await api('/api/logout', { method: 'POST' })
    identity.value = null
    projects.value = []
    selected.value = null
    members.value = []
    accounts.value = []
    tab.value = 'projects'
  })
}

async function createProject() {
  await run(async () => {
    await api(`${tenantPath.value}/projects`, { method: 'POST', body: JSON.stringify(newProject) })
    newProject.slug = ''; newProject.name = ''
    await refresh()
    message.value = '项目已创建，你已成为该项目管理员。'
  })
}

function editMember(member) {
  memberEdit.account_id = member.id
  memberEdit.roles = [...member.roles]
}

async function saveMember() {
  await run(async () => {
    await api(`${memberPath.value}/members/${memberEdit.account_id}`, {
      method: 'PUT', body: JSON.stringify({ roles: memberEdit.roles }),
    })
    await refresh()
    message.value = '项目角色已保存，后续请求立即按新权限执行。'
  })
}

async function removeMember(member) {
  await run(async () => {
    await api(`${memberPath.value}/members/${member.id}`, { method: 'DELETE' })
    await refresh()
    message.value = '成员已移除，原登录会话不再获得该项目权限。'
  })
}

async function createAccount() {
  await run(async () => {
    try { await api(`${tenantPath.value}/accounts`, { method: 'POST', body: JSON.stringify(newAccount) }) }
    finally { newAccount.password = '' }
    newAccount.username = ''; newAccount.display_name = ''; newAccount.tenant_admin = false
    await refresh()
    message.value = '本租户账号已创建，请在项目中分配角色。'
  })
}

function editAccount(account) {
  Object.assign(accountEdit, { id: account.id, enabled: account.enabled, tenant_admin: account.tenant_admin, password: '' })
}

async function saveAccount() {
  await run(async () => {
    const patch = { enabled: accountEdit.enabled, tenant_admin: accountEdit.tenant_admin }
    if (accountEdit.password) patch.password = accountEdit.password
    try { await api(`${tenantPath.value}/accounts/${accountEdit.id}`, { method: 'PATCH', body: JSON.stringify(patch) }) }
    finally { accountEdit.password = '' }
    accountEdit.id = ''
    await refresh()
    message.value = '账号状态已保存，服务端权限立即生效。'
  })
}

onMounted(async () => {
  await loadHealth()
  try { await refresh() } catch { /* A new browser has no session. */ }
})
</script>

<template>
  <main v-if="!identity" class="login-layout">
    <section class="welcome">
      <a class="brand" href="/">E<span>↗</span> ETL-Agent</a>
      <div class="welcome-copy"><p class="eyebrow">数据集成 · 从可信协作开始</p>
        <h1>每个项目，<br>清晰的身份与边界。</h1>
        <p>使用你的独立账号登录，在所属项目中协作。角色与访问范围由服务端实时核验。</p>
        <div class="welcome-lines"><span>01 / 独立身份</span><span>02 / 项目隔离</span><span>03 / 变更留痕</span></div>
      </div>
      <small>V01 · 身份与项目协作</small>
    </section>
    <section class="login-panel">
      <form class="login-form" @submit.prevent="signIn">
        <p class="eyebrow">欢迎回来</p><h2>登录工作空间</h2><p class="muted">输入租户标识和你的本地账号。</p>
        <label>租户标识<input v-model="login.tenant" name="tenant" required autocomplete="organization" placeholder="例如 demo"></label>
        <label>账号<input v-model="login.username" name="username" required autocomplete="username" placeholder="输入账号"></label>
        <label>密码<input v-model="login.password" name="password" type="password" required minlength="12" maxlength="128" autocomplete="current-password" placeholder="输入密码"></label>
        <p v-if="error" class="notice error" role="alert">{{ error }}</p>
        <button class="primary wide" :disabled="busy" type="submit">{{ busy ? '正在登录…' : '登录 →' }}</button>
        <p class="login-note">初始化凭据由部署者安全提供。请勿共享独立审批账号。</p>
      </form>
      <div class="service-line"><i :class="health?.status === 'ok' ? 'dot' : 'dot offline'"></i>
        {{ health?.status === 'ok' ? `运行正常 · PostgreSQL ${health.postgresql.version} / Redis ${health.redis.version}` : '运行条件未就绪，请联系部署者' }}</div>
    </section>
  </main>
  <div v-else class="workspace">
    <aside class="sidebar">
      <a class="brand" href="/">E<span>↗</span> ETL-Agent</a>
      <div class="tenant-label"><small>当前租户</small><strong>{{ identity.tenant.name }}</strong><code>{{ identity.tenant.slug }}</code></div>
      <p class="nav-label">工作空间</p>
      <button :class="['nav', { active: tab === 'projects' }]" @click="tab = 'projects'">▦ <span>项目与成员</span></button>
      <button v-if="isTenantAdmin" :class="['nav', { active: tab === 'accounts' }]" @click="tab = 'accounts'">◎ <span>账号管理</span></button>
      <button :class="['nav', { active: tab === 'connections' }]" @click="tab = 'connections'">⇄ <span>连接与资产</span></button>
      <button :class="['nav', { active: tab === 'studio' }]" @click="tab = 'studio'">◇ <span>手工集成 Studio</span></button>
      <button :class="['nav', { active: tab === 'versions' }]" @click="tab = 'versions'">▤ <span>冻结版本与 Diff</span></button>
      <button :class="['nav', { active: tab === 'preparations' }]" @click="tab = 'preparations'">✓ <span>准备单与独立审批</span></button>
      <button :class="['nav', { active: tab === 'runs' }]" @click="tab = 'runs'">▷ <span>运行中心</span></button>
      <button :class="['nav', { active: tab === 'audit' }]" @click="tab = 'audit'">≡ <span>审计与证据</span></button>
      <div class="scope-note"><span>当前交付 / #2–#10</span><p>准备、双审批与单次 Commit；隔离 L1 异步执行验证。普通部署执行关闭，L1 不计入 L2。</p></div>
      <div class="user-card"><div class="avatar">{{ identity.account.display_name.slice(0, 1) }}</div>
        <div><strong>{{ identity.account.display_name }}</strong><small>{{ isTenantAdmin ? '租户管理员' : identity.account.username }}</small></div>
      </div>
      <button class="logout" :disabled="busy" @click="signOut">退出登录 ↗</button>
    </aside>
    <section class="content">
      <header class="topbar"><div><span class="breadcrumb">工作空间 / {{ tab === 'projects' ? '项目协作' : tab === 'audit' ? '审计证据' : tab === 'connections' ? '连接与资产' : tab === 'studio' ? '手工集成 Studio' : tab === 'versions' ? '冻结版本与 Diff' : tab === 'preparations' ? '准备单与独立审批' : tab === 'runs' ? '运行中心' : '身份管理' }}</span>
        <h1>{{ tab === 'projects' ? '项目与成员' : tab === 'audit' ? '审计与证据' : tab === 'connections' ? '连接与资产' : tab === 'studio' ? '手工集成 Studio' : tab === 'versions' ? '冻结版本与 Diff' : tab === 'preparations' ? '准备单与独立审批' : tab === 'runs' ? '运行中心' : '账号管理' }}</h1></div>
        <button class="secondary" :disabled="busy" @click="run(refresh)">↻ 刷新权限</button>
      </header>
      <p v-if="error" class="notice error" role="alert">{{ error }}</p>
      <p v-if="message" class="notice success" role="status">{{ message }}</p>
      <template v-if="tab === 'projects'">
        <div class="section-intro"><p>你所属的项目 <span class="count">{{ projects.length }}</span></p><span class="muted">仅展示当前账号的有效项目成员关系</span></div>
        <div class="project-grid">
          <button v-for="project in projects" :key="project.id" :class="['project-card', { selected: selected?.id === project.id }]" :disabled="busy" @click="run(() => loadProject(project))">
            <span class="project-icon">▦</span><span class="project-heading">{{ project.name }}</span><code>{{ project.slug }}</code>
            <div class="badges"><span v-for="role in project.roles" :key="role" class="badge">{{ roleNames[role] }}</span></div><span class="project-arrow">查看成员 ↗</span>
          </button>
          <p v-if="!projects.length" class="empty">暂无所属项目，请联系项目管理员分配角色。</p>
        </div>
        <details v-if="isTenantAdmin" class="create-project"><summary>＋ 创建项目</summary>
          <form class="inline-form" @submit.prevent="createProject"><label>项目标识<input v-model="newProject.slug" required pattern="[a-zA-Z0-9_-]+" maxlength="64"></label><label>项目名称<input v-model="newProject.name" required maxlength="100"></label><button class="primary" :disabled="busy">创建项目</button></form>
        </details>
        <section v-if="selected" class="panel">
          <div class="panel-title"><div><p class="eyebrow">PROJECT MEMBERS</p><h2>{{ selected.name }} · 成员</h2></div><span class="badge" :class="canManage ? 'accent' : ''">{{ canManage ? '可管理成员' : '只读成员信息' }}</span></div>
          <div class="table-scroll"><table><thead><tr><th>成员 / 账号</th><th>项目角色</th><th>账号状态</th><th v-if="canManage">操作</th></tr></thead><tbody><tr v-for="member in members" :key="member.id">
            <td><strong>{{ member.display_name }}</strong><small>{{ member.username }}</small></td>
            <td><div class="badges"><span v-for="role in member.roles" :key="role" class="badge">{{ roleNames[role] }}</span></div></td>
            <td><span :class="['status', { disabled: !member.enabled }]">{{ member.enabled ? '已启用' : '已禁用' }}</span></td>
            <td v-if="canManage"><div class="actions"><button class="text-button" :disabled="busy" @click="editMember(member)">编辑角色</button><button class="text-button danger" :disabled="busy" @click="removeMember(member)">移除</button></div></td>
          </tr></tbody></table></div>
          <form v-if="canManage" class="editor" @submit.prevent="saveMember"><h3>添加成员 / 调整角色</h3><label>本租户账号<select v-model="memberEdit.account_id" required><option disabled value="">选择账号</option><option v-for="account in eligible" :key="account.id" :value="account.id">{{ account.display_name }}（{{ account.username }}{{ account.enabled ? '' : ' · 已禁用' }}）</option></select></label>
            <fieldset><legend>项目角色（至少一项）</legend><label v-for="(label, role) in roleNames" :key="role" class="checkbox"><input v-model="memberEdit.roles" type="checkbox" :value="role">{{ label }}</label></fieldset>
            <button class="primary" :disabled="busy || !memberEdit.account_id || !memberEdit.roles.length">保存项目角色</button><p class="muted small">项目管理员可管理本项目成员。租户账号状态与租户管理员资格由账号管理维护。</p>
          </form>
          <p v-else class="panel-note">你可以查看本项目成员。当前角色没有成员管理权限。</p>
        </section>
        <div v-else class="choose-project">选择一个项目，查看成员与当前角色权限。</div>
      </template>
      <ConnectionsPanel v-else-if="tab === 'connections'" :api="api" :tenant-id="identity.tenant.id" :projects="projects" />
      <StudioPanel v-else-if="tab === 'studio'" :api="api" :tenant-id="identity.tenant.id" :account-id="identity.account.id" :projects="projects" />
      <VersionsPanel v-else-if="tab === 'versions'" :api="api" :tenant-id="identity.tenant.id" :projects="projects" />
      <PreparationsPanel v-else-if="tab === 'preparations'" :api="api" :tenant-id="identity.tenant.id" :account-id="identity.account.id" :projects="projects" />
      <RunsPanel v-else-if="tab === 'runs'" :api="api" :tenant-id="identity.tenant.id" :projects="projects" />
      <AuditPanel v-else-if="tab === 'audit'" :api="api" :tenant-id="identity.tenant.id" :tenant-admin="isTenantAdmin" :projects="projects" />
      <template v-else-if="isTenantAdmin">
        <p class="muted">管理本租户的独立账号。禁用账号或撤销管理资格后，现有会话在下一次请求立即受限。</p>
        <section class="panel"><div class="panel-title"><h2>租户账号</h2><span class="count">{{ accounts.length }}</span></div>
          <div class="table-scroll"><table><thead><tr><th>姓名 / 账号</th><th>租户资格</th><th>状态</th><th>操作</th></tr></thead><tbody><tr v-for="account in accounts" :key="account.id"><td><strong>{{ account.display_name }}</strong><small>{{ account.username }}</small></td><td>{{ account.tenant_admin ? '租户管理员' : '普通成员' }}</td><td><span :class="['status', { disabled: !account.enabled }]">{{ account.enabled ? '已启用' : '已禁用' }}</span></td><td><button class="text-button" :disabled="busy" @click="editAccount(account)">管理账号</button></td></tr></tbody></table></div>
          <form v-if="accountEdit.id" class="editor" @submit.prevent="saveAccount"><h3>管理账号状态与密码</h3><div class="check-row"><label class="checkbox"><input v-model="accountEdit.enabled" type="checkbox">启用账号</label><label class="checkbox"><input v-model="accountEdit.tenant_admin" type="checkbox">租户管理员</label></div><label>重设密码（留空表示保留）<input v-model="accountEdit.password" type="password" minlength="12" maxlength="128" autocomplete="new-password"></label><button class="primary" :disabled="busy">保存账号</button></form>
        </section>
        <section class="panel"><div class="panel-title"><h2>创建独立账号</h2></div><form class="editor" @submit.prevent="createAccount"><div class="form-grid"><label>账号标识<input v-model="newAccount.username" required pattern="[a-zA-Z0-9_-]+" maxlength="64" autocomplete="off"></label><label>显示名称<input v-model="newAccount.display_name" required maxlength="100"></label><label>初始密码<input v-model="newAccount.password" type="password" required minlength="12" maxlength="128" autocomplete="new-password" placeholder="12–128 位"></label></div><label class="checkbox"><input v-model="newAccount.tenant_admin" type="checkbox">授予租户管理员资格</label><button class="primary" :disabled="busy">创建账号</button></form></section>
      </template>
      <footer class="runtime-footer"><i :class="health?.status === 'ok' ? 'dot' : 'dot offline'"></i><span v-if="health?.status === 'ok'">依赖已连接 · PostgreSQL {{ health.postgresql.version }} · Redis {{ health.redis.version }} · API {{ health.application }}</span><span v-else>依赖服务不可用或尚未验证</span><span class="runtime-tag">真实服务检查</span></footer>
    </section>
  </div>
</template>
