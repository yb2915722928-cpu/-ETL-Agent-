<script setup>
import { computed, ref, watch, onMounted, onUnmounted } from 'vue'

const props = defineProps({ api: Function, tenantId: String, projects: Array })
const projectId = ref('')
const preparationId = ref('')
const preparations = ref([])
const runs = ref([])
const selected = ref(null)
const pending = ref(null)
const busy = ref(false)
const error = ref('')
const message = ref('')
const base = computed(() => `/api/tenants/${props.tenantId}/projects/${projectId.value}`)
const operator = computed(() => props.projects.find(p => p.id === projectId.value)?.roles.includes('operator'))
const eligible = computed(() => preparations.value.filter(p => p.validation.valid && !runs.value.some(r => r.preparation_id === p.id)))
const supervision = computed(() => selected.value?.execution.supervision)
const metrics = [['source_read', '累计源读取记录'], ['evaluated', '已判定记录'], ['valid', '有效记录'], ['rejected', '拒绝记录'], ['final_output', '最终输出记录'], ['sink_bytes', '实测写入字节（byte）'], ['elapsed_seconds', '累计时长（秒）'], ['throughput_rows_per_second', '输出吞吐（条/秒）'], ['amplification', '放大比'], ['reject_ratio', '拒绝率']]
const metricValue = (key, value) => value == null ? '暂无数据' : key === 'reject_ratio' ? `${(value * 100).toFixed(2)}%` : Number.isInteger(value) ? value.toLocaleString('zh-CN') : value.toFixed(3)
const supervisionStatus = value => ({ awaiting_metrics: '等待可靠指标', observing: '持续观测，发布闭锁', unsafe: '指标不安全', stop_requested: '已请求停止，等待终止确认', finished: '最终指标通过，仍需发布流程' }[value] || value)
const reason = value => ({ rows_exceeded: '源读取行数超过预算', bytes_exceeded: '实测写入字节超过预算', duration_exceeded: '运行时长超过预算', amplification_exceeded: '输出放大比超过预算', reject_ratio_exceeded: '拒绝率超过预算', empty_result: '空结果禁止发布', metrics_missing: '指标缺失或无法观测', metrics_missing_or_invalid: '指标字段缺失或无效', metrics_stale: '指标陈旧或时钟不可信', metrics_reset: '指标重置、回退或来源变化', bytes_not_measured: '写入字节未经实测', metric_contract_mismatch: '指标来源或契约不匹配', metrics_inconsistent: '指标阶段或计数不一致', metrics_timestamp_invalid: '指标时间无效' }[value] || value)
let refreshTimer
onMounted(() => { refreshTimer = setInterval(() => { if (projectId.value && !busy.value) act(refresh) }, 2000) })
onUnmounted(() => clearInterval(refreshTimer))
const pretty = value => JSON.stringify(value, null, 2)
const time = value => new Date(value).toLocaleString('zh-CN')
const status = value => ({ queued: '已接受，待调度', submitting: '提交中，关联已持久化', submission_unknown: '提交结果未知，保持目标隔离', submitted: 'L1 作业已关联', denied: '执行前核验拒绝', blocked_target: '物理目标已被其他运行占有', metadata_checked: '只读检查完成', stop_requested: '监督闭锁，已请求停止', supervision_passed: '最终监督通过（尚未发布）', supervision_unavailable: '监督契约缺失，禁止发布' }[value] || value)
async function act(action) {
  busy.value = true; error.value = ''; message.value = ''
  try { await action() } catch (cause) { error.value = cause.message }
  finally { busy.value = false }
}
async function refresh() {
  if (!projectId.value) return
  const [p, r] = await Promise.all([props.api(`${base.value}/preparations`), props.api(`${base.value}/runs`)])
  preparations.value = p; runs.value = r
  if (selected.value) selected.value = r.find(row => row.id === selected.value.id) || null
}
async function issue() {
  await act(async () => {
    const path = `${base.value}/preparations/${preparationId.value}`
    const result = await props.api(`${path}/capabilities`, { method: 'POST', body: '{}' })
    pending.value = { path, preparationId: preparationId.value, capability: result.capability, idempotency_key: crypto.randomUUID() }
    message.value = '已签发绑定当前操作员的五分钟授权。确认后提交一次 Commit。'
  })
}
async function commit() {
  await act(async () => {
    const body = { capability: pending.value.capability, idempotency_key: pending.value.idempotency_key }
    selected.value = await props.api(`${pending.value.path}/commit`, { method: 'POST', body: JSON.stringify(body) })
    pending.value = null
    message.value = 'Commit 已接受唯一运行。当前队列与作业状态请查看运行事实。'
    await refresh()
  })
}
watch(projectId, () => {
  preparations.value = []; runs.value = []; selected.value = null; preparationId.value = ''; pending.value = null
  act(refresh)
})
watch(() => props.projects, () => {
  if (!props.projects.some(p => p.id === projectId.value)) projectId.value = ''
  else act(refresh)
})
</script>

<template>
  <div>
    <p class="muted">Operator 接受已批准的冻结准备单。每份准备单只能生成一个运行。普通部署执行关闭；隔离 L1 展示真实 Redis/Celery 控制事实，不执行真实 ETL、发布或清理，不能计入 L2。</p>
    <label class="project-picker">所属项目<select v-model="projectId" :disabled="busy || !!pending"><option value="">选择项目</option><option v-for="p in projects" :key="p.id" :value="p.id">{{ p.name }}</option></select></label>
    <p v-if="error" class="notice error" role="alert">{{ error }}<span v-if="pending"> 授权与幂等键已保留。若响应不明，请用下方按钮重试同一请求；也可刷新运行列表核对。</span></p>
    <p v-if="message" class="notice success" role="status">{{ message }}</p>
    <template v-if="projectId">
      <section v-if="operator" class="panel">
        <div class="panel-title"><h2>授权与单次 Commit</h2><span class="badge accent">Operator</span></div>
        <div class="body">
          <template v-if="!pending"><label>可接受的准备单<select v-model="preparationId" :disabled="busy"><option value="">选择有效准备单</option><option v-for="p in eligible" :key="p.id" :value="p.id">{{ p.risk_level }} · {{ p.tool }} · {{ p.id.slice(0, 8) }}</option></select></label><button class="primary" :disabled="busy || !preparationId" @click="issue">签发五分钟授权</button></template>
          <template v-else><dl><dt>准备单</dt><dd><code>{{ pending.preparationId }}</code></dd><dt>授权截止</dt><dd>{{ time(pending.capability.payload.expires_at * 1000) }}</dd><dt>本次幂等键</dt><dd><code>{{ pending.idempotency_key }}</code></dd></dl><p class="muted">提交时重新核验当前职责、审批和受管资源。重复提交返回原运行；同一准备单不可创建第二个运行。</p><div class="actions"><button class="primary" :disabled="busy" @click="commit">提交 / 重试同一 Commit</button><button class="secondary" :disabled="busy" @click="pending = null">放弃本次授权</button></div></template>
        </div>
      </section>
      <section class="panel"><div class="panel-title"><h2>项目运行</h2><button class="secondary" :disabled="busy" @click="act(refresh)">刷新运行事实</button></div><div class="run-list"><button v-for="r in runs" :key="r.id" class="run-row" :disabled="busy" @click="selected = r"><strong>{{ status(r.status) }}</strong><code>{{ r.id }}</code><small>{{ time(r.accepted_at) }}</small></button><p v-if="!runs.length" class="muted">尚无已接受运行。</p></div></section>
      <section v-if="supervision" class="panel"><div class="panel-title"><h2>运行监督与预算</h2><span class="badge accent">{{ supervisionStatus(supervision.status) }}</span></div><div class="body"><p class="muted">每约两秒观测一次。监督预算会有检测及停止延迟，不代表物理资源上限，也不保证零超调。运行中拒绝率至少 100 条判定记录才检查，最终小样本仍检查；其他预算始终生效。</p><dl><dt>发布门禁</dt><dd>{{ supervision.publication_veto ? '闭锁：禁止推进发布' : '最终监督指标通过；尚未执行发布' }}</dd><dt>指标来源</dt><dd>{{ supervision.latest_observation?.source || '暂无可靠来源' }}</dd><dt>观测时间</dt><dd>{{ supervision.latest_observation ? time(supervision.latest_observation.received_at) : '暂无数据' }}</dd><template v-for="[key, label] in metrics" :key="key"><dt>{{ label }}</dt><dd>{{ metricValue(key, supervision.current_metrics[key]) }}</dd></template><dt>闭锁原因</dt><dd>{{ supervision.reasons.length ? supervision.reasons.map(reason).join('；') : '暂无异常；等待最终判定' }}</dd><dt>停止请求</dt><dd>{{ supervision.stop_request ? `${supervision.stop_request.delivery_status === 'acknowledged' ? '引擎已接收请求' : '请求已持久化，投递待核对'} · ${time(supervision.stop_request.requested_at)}` : '尚无停止请求' }}</dd><dt>终止与隔离</dt><dd>{{ supervision.termination }}</dd></dl><details><summary>冻结监督契约、阶段与字节单位</summary><pre>{{ pretty(supervision.contract) }}</pre></details><details><summary>持久观测与停止事实</summary><pre>{{ pretty({ latest_observation: supervision.latest_observation, stop_request: supervision.stop_request }) }}</pre></details></div></section>
      <section v-if="selected" class="panel"><div class="panel-title"><h2>运行与控制事实</h2><span class="badge accent">{{ status(selected.status) }}</span></div><div class="body"><dl><dt>唯一运行 ID</dt><dd><code>{{ selected.id }}</code></dd><dt>准备单</dt><dd><code>{{ selected.preparation_id }}</code></dd><dt>原始操作员</dt><dd><code>{{ selected.subject }}</code></dd><dt>接受时间</dt><dd>{{ time(selected.accepted_at) }}</dd><dt>执行级别</dt><dd>{{ selected.execution.level === 'L1' ? '隔离 L1 故障 Engine' : '执行关闭' }}</dd><dt>引擎任务</dt><dd>{{ selected.engine_job_id || (selected.status === 'submission_unknown' || selected.status === 'submitting' ? '尚未确认，不代表作业不存在' : '暂无已确认任务') }}</dd><dt>提交关联</dt><dd><code>{{ selected.execution.attempt?.correlation_id || '尚未申请提交' }}</code></dd><dt>提交阶段</dt><dd>{{ selected.execution.attempt ? status(selected.execution.attempt.phase) : '无 Attempt' }}</dd><dt>核对说明</dt><dd>{{ selected.execution.attempt?.reason || '暂无异常' }}</dd><dt>目标隔离</dt><dd>{{ selected.execution.ownership?.retained ? '持续保留；未知结果不因超时释放' : '本运行未占有目标' }}</dd><dt>发布与清理</dt><dd>{{ selected.execution.publication }}</dd><dt>投递状态</dt><dd>{{ selected.outbox.status === 'pending' ? '待投递' : selected.outbox.status }} · 已尝试 {{ selected.outbox.attempts }} 次</dd></dl><details><summary>已接受命令、授权范围与冻结预算</summary><pre>{{ pretty(selected.managed_command) }}</pre></details><details open><summary>持久化队列、提交与目标所有权</summary><pre>{{ pretty({ outbox: selected.outbox, attempt: selected.execution.attempt, ownership: selected.execution.ownership }) }}</pre></details><details><summary>同事务控制证据</summary><pre>{{ pretty(selected.execution.control_facts) }}</pre></details></div></section>
    </template>
    <div v-else class="choose-project">选择项目，查看已接受运行与待调度事实。</div>
  </div>
</template>

<style scoped>
.project-picker{max-width:460px;margin-bottom:24px}.body,.run-list{padding:20px 24px}.body>label{max-width:620px;margin-bottom:16px}.run-list{display:grid;gap:12px}.run-row{display:flex;align-items:center;gap:20px;flex-wrap:wrap;text-align:left;padding:16px;border:1px solid #dce7ed;border-radius:10px;background:#f7fafb;color:#253f50}.run-row code{flex:1}.run-row small{color:#657783}dl{display:grid;grid-template-columns:140px minmax(0,1fr);gap:12px}dt{color:#657783}dd{margin:0;overflow-wrap:anywhere}summary{cursor:pointer;color:#366c7e;margin:16px 0}pre{overflow:auto;max-height:460px;padding:20px;background:#f3f7fa;border-radius:10px;font-size:12px;line-height:1.7}@media(max-width:900px){dl{grid-template-columns:1fr;gap:8px}dd{margin-bottom:12px}}
</style>
