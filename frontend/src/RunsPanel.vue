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
const control = computed(() => selected.value?.execution.control)
const quality = computed(() => selected.value?.execution.quality)
const publication = computed(() => selected.value?.execution.publication_state)
const publicationPending = ref(null)
const canPublish = computed(() => operator.value && selected.value?.status === 'supervision_passed' && publication.value?.approved_contract && !publication.value?.intent)
const publicationPhase = value => ({ prepared: '发布意图已持久化', dispatching: '交换已预约，结果待核对', unknown: '交换结果未知；禁止再次交换', confirmed: '实际物理身份已确认发布' }[value] || value)
async function sendPublication() {
  await act(async () => {
    if (!publicationPending.value) publicationPending.value = { runId: selected.value.id, path: `${base.value}/runs/${selected.value.id}/publish`, body: { idempotency_key: crypto.randomUUID() } }
    if (publicationPending.value.runId !== selected.value.id) throw new Error('请先选择原发布请求绑定的运行，再重试同一意图。')
    const result = await props.api(publicationPending.value.path, { method: 'POST', body: JSON.stringify(publicationPending.value.body) })
    publicationPending.value = null
    await refresh()
    message.value = `发布意图 ${result.intent_id} 已持久接受。单条交换结果及旧表保留期限以发布事实为准。`
  })
}
const qualityReport = computed(() => {
  const report = quality.value?.report
  return report && Object.keys(report).length ? report : null
})
const qualityCount = value => Number.isSafeInteger(value) && value >= 0 ? value.toLocaleString('zh-CN') : '未知（尚无可靠指标）'
const qualityReason = value => ({ source_parse_failure: '源记录解析失败', record_payload_conflict: '同一记录的内容发生冲突', source_counter_reset: '源读取计数回退', source_report_incomplete: '源读取报告不完整', source_parse_or_evaluation_incomplete: '源解析或判定未完整覆盖', route_receipts_incomplete: '双路写入回执不完整', route_receipt_reused: '写入回执被重复关联', route_receipt_unverified: '写入回执尚未核验', route_not_closed_or_bound: '写入通道尚未关闭或绑定不符', route_supervision_veto: '写入通道监督闭锁', route_extra_or_failed_batch: '存在额外批次或失败批次' }[value] || `待核验原因代码：${value}`)
const controlReason = ref('Operator 核对运行并请求安全控制')
const controlPending = ref(null)
const reconciliation = computed(() => selected.value?.execution.reconciliation || [])
const reconcileReasons = { submission_unknown: '提交结果未知', worker_lost: '工作进程失联', lease_expired: '调度租约已过期', stop_unconfirmed: '停止请求尚未确认', operator_requested: '操作员主动核对' }
const reconcileReason = ref('operator_requested')
const reconcilePending = ref(null)
const reconciliationBasis = value => ({ intent_persisted: '核对意图已持久化，等待原作业只读查询', original_id_name_status: '原作业 ID、命令名称与引擎状态一致', original_job_id_missing: '缺少原作业 ID，不能证明作业不存在', original_job_unconfirmed: '原作业查询失败或关联事实不完整', authorization_or_binding_unavailable: '当前职责或签名绑定不可核验' }[value] || value)
async function sendReconcile() {
  await act(async () => {
    if (!reconcilePending.value) reconcilePending.value = {
      path: `${base.value}/runs/${selected.value.id}/reconcile`,
      body: { idempotency_key: crypto.randomUUID(), reason: reconcileReason.value }
    }
    const result = await props.api(reconcilePending.value.path, { method: 'POST', body: JSON.stringify(reconcilePending.value.body) })
    reconcilePending.value = null
    await refresh()
    message.value = `核对记录 ${result.record_id} 已持久接受。只读原作业，不新增运行或引擎任务；引擎终态仍需独立写入静止证据。`
  })
}
const canStop = computed(() => selected.value && !['denied', 'blocked_target', 'metadata_checked', 'cancelled', 'cleaned', 'terminated'].includes(selected.value.status))
const canCleanup = computed(() => control.value?.termination.confirmed && control.value?.final_metrics.status === 'known' && !control.value?.inflight && selected.value?.execution.ownership?.retained)
async function sendControl(kind) {
  await act(async () => {
    if (!controlPending.value) controlPending.value = { path: `${base.value}/runs/${selected.value.id}/${kind}`, kind,
      body: { idempotency_key: crypto.randomUUID(), reason: controlReason.value } }
    const request = controlPending.value
    await props.api(request.path, { method: 'POST', body: JSON.stringify(request.body) })
    controlPending.value = null
    await refresh()
    message.value = request.kind === 'stop' ? '停止意图已持久接受。引擎接收 ACK 后仍持续核对终止和最终指标。' : '独立清理请求已接受。只有本运行未发布对象的清理收据确认后才释放物理目标。'
  })
}
const metrics = [['source_read', '累计源读取记录'], ['evaluated', '已判定记录'], ['valid', '有效记录'], ['rejected', '拒绝记录'], ['final_output', '最终输出记录'], ['sink_bytes', '实测写入字节（byte）'], ['elapsed_seconds', '累计时长（秒）'], ['throughput_rows_per_second', '输出吞吐（条/秒）'], ['amplification', '放大比'], ['reject_ratio', '拒绝率']]
const metricValue = (key, value) => value == null ? '暂无数据' : key === 'reject_ratio' ? `${(value * 100).toFixed(2)}%` : Number.isInteger(value) ? value.toLocaleString('zh-CN') : value.toFixed(3)
const supervisionStatus = value => ({ awaiting_metrics: '等待可靠指标', observing: '持续观测，发布闭锁', unsafe: '指标不安全', stop_requested: '已请求停止，等待终止确认', finished: '最终指标通过，仍需发布流程' }[value] || value)
const reason = value => ({ rows_exceeded: '源读取行数超过预算', bytes_exceeded: '实测写入字节超过预算', duration_exceeded: '运行时长超过预算', amplification_exceeded: '输出放大比超过预算', reject_ratio_exceeded: '拒绝率超过预算', empty_result: '空结果禁止发布', metrics_missing: '指标缺失或无法观测', metrics_missing_or_invalid: '指标字段缺失或无效', metrics_stale: '指标陈旧或时钟不可信', metrics_reset: '指标重置、回退或来源变化', bytes_not_measured: '写入字节未经实测', metric_contract_mismatch: '指标来源或契约不匹配', metrics_inconsistent: '指标阶段或计数不一致', metrics_timestamp_invalid: '指标时间无效' }[value] || value)
let refreshTimer
onMounted(() => { refreshTimer = setInterval(() => { if (projectId.value && !busy.value) act(refresh) }, 2000) })
onUnmounted(() => clearInterval(refreshTimer))
const pretty = value => JSON.stringify(value, null, 2)
const time = value => new Date(value).toLocaleString('zh-CN')
const status = value => ({ queued: '已接受，待调度', submitting: '提交中，关联已持久化', submission_unknown: '提交结果未知，保持目标隔离', submitted: 'L1 作业已关联', denied: '执行前核验拒绝', blocked_target: '物理目标已被其他运行占有', metadata_checked: '只读检查完成', stop_requested: '监督闭锁，已请求停止', termination_pending: '终止确认中，目标所有权保持', final_metrics_pending: '等待可靠最终指标，发布闭锁', terminated: '终止已确认，最终指标已知', supervision_passed: '最终监督通过（尚未发布）', cleanup_unknown: '清理结果未知，目标所有权保持', cleaned: '本运行未发布对象已清理', cancelled: '已取消，未提交', supervision_unavailable: '监督契约缺失，禁止发布' }[value] || value)
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
    <label class="project-picker">所属项目<select v-model="projectId" :disabled="busy || !!pending || !!reconcilePending"><option value="">选择项目</option><option v-for="p in projects" :key="p.id" :value="p.id">{{ p.name }}</option></select></label>
    <p v-if="error" class="notice error" role="alert">{{ error }}<span v-if="pending"> 授权与幂等键已保留。若响应不明，请用下方按钮重试同一请求；也可刷新运行列表核对。</span></p>
    <p v-if="message" class="notice success" role="status">{{ message }}</p>
    <template v-if="projectId">
      <section v-if="operator" class="panel">
        <div class="panel-title"><h2>授权与单次 Commit</h2><span class="badge accent">Operator</span></div>
        <div class="body">
          <template v-if="!pending"><label>可接受的准备单<select v-model="preparationId" :disabled="busy"><option value="">选择有效准备单</option><option v-for="p in eligible" :key="p.id" :value="p.id">{{ p.risk_level }} · {{ p.tool }} · {{ p.id.slice(0, 8) }}</option></select></label><button class="primary" :disabled="busy || !preparationId || !!reconcilePending" @click="issue">签发五分钟授权</button></template>
          <template v-else><dl><dt>准备单</dt><dd><code>{{ pending.preparationId }}</code></dd><dt>授权截止</dt><dd>{{ time(pending.capability.payload.expires_at * 1000) }}</dd><dt>本次幂等键</dt><dd><code>{{ pending.idempotency_key }}</code></dd></dl><p class="muted">提交时重新核验当前职责、审批和受管资源。重复提交返回原运行；同一准备单不可创建第二个运行。</p><div class="actions"><button class="primary" :disabled="busy || !!reconcilePending" @click="commit">提交 / 重试同一 Commit</button><button class="secondary" :disabled="busy" @click="pending = null">放弃本次授权</button></div></template>
        </div>
      </section>
      <section class="panel"><div class="panel-title"><h2>项目运行</h2><button class="secondary" :disabled="busy" @click="act(refresh)">刷新运行事实</button></div><div class="run-list"><button v-for="r in runs" :key="r.id" class="run-row" :disabled="busy || !!reconcilePending" @click="selected = r"><strong>{{ status(r.status) }}</strong><code>{{ r.id }}</code><small>{{ time(r.accepted_at) }}</small></button><p v-if="!runs.length" class="muted">尚无已接受运行。</p></div></section>
      <section v-if="supervision" class="panel"><div class="panel-title"><h2>运行监督与预算</h2><span class="badge accent">{{ supervisionStatus(supervision.status) }}</span></div><div class="body"><p class="muted">每约两秒观测一次。监督预算会有检测及停止延迟，不代表物理资源上限，也不保证零超调。运行中拒绝率至少 100 条判定记录才检查，最终小样本仍检查；其他预算始终生效。</p><dl><dt>发布门禁</dt><dd>{{ supervision.publication_veto ? '闭锁：禁止推进发布' : '最终监督指标通过；尚未执行发布' }}</dd><dt>指标来源</dt><dd>{{ supervision.latest_observation?.source || '暂无可靠来源' }}</dd><dt>观测时间</dt><dd>{{ supervision.latest_observation ? time(supervision.latest_observation.received_at) : '暂无数据' }}</dd><template v-for="[key, label] in metrics" :key="key"><dt>{{ label }}</dt><dd>{{ metricValue(key, supervision.current_metrics[key]) }}</dd></template><dt>闭锁原因</dt><dd>{{ supervision.reasons.length ? supervision.reasons.map(reason).join('；') : '暂无异常；等待最终判定' }}</dd><dt>停止请求</dt><dd>{{ supervision.stop_request ? `${supervision.stop_request.delivery_status === 'acknowledged' ? '引擎已接收请求' : '请求已持久化，投递待核对'} · ${time(supervision.stop_request.requested_at)}` : '尚无停止请求' }}</dd><dt>终止与隔离</dt><dd>{{ supervision.termination }}</dd></dl><details><summary>冻结监督契约、阶段与字节单位</summary><pre>{{ pretty(supervision.contract) }}</pre></details><details><summary>持久观测与停止事实</summary><pre>{{ pretty({ latest_observation: supervision.latest_observation, stop_request: supervision.stop_request }) }}</pre></details></div></section>
      <section v-if="selected" class="panel">
        <div class="panel-title"><h2>真实质量事实</h2><span class="badge accent">{{ qualityReport ? '已形成质量报告' : '报告待完整核验' }}</span></div>
        <div class="body">
          <p class="muted">源读取、解析、质量判定和双路最终写入分别计数；缺失指标不代表零。错误码频次按字段与规则累计，同一记录可能命中多条规则，不等于拒绝记录数。此处事实不构成真实受管 L2 资格。</p>
          <p v-if="!qualityReport" class="notice">未形成完整质量报告。</p>
          <dl><dt>质量契约哈希</dt><dd><code>{{ quality?.contract_sha256 || '尚无绑定的质量契约' }}</code></dd><dt>报告汇总状态</dt><dd>{{ quality?.finalized === true ? '已完成汇总；仍需核验闭锁原因' : quality?.finalized === false ? '尚未完成汇总' : '未知' }}</dd><dt>发布与清理门禁</dt><dd>{{ selected.execution.publication }}</dd><dt>真实受管 L2</dt><dd>尚未取得资格</dd></dl>
          <h3>源读取与解析</h3>
          <dl><dt>源读取记录数</dt><dd>{{ qualityCount(quality?.source_facts?.source_read) }}</dd><dt>成功解析记录数</dt><dd>{{ qualityCount(quality?.source_facts?.parsed) }}</dd><dt>源报告完整性</dt><dd>{{ quality?.source_facts?.complete === true ? '源报告完整' : quality?.source_facts?.complete === false ? '源报告尚不完整' : '未知' }}</dd><dt>已记录异常</dt><dd>{{ quality?.failures?.length ? quality.failures.map(qualityReason).join('；') : Array.isArray(quality?.failures) ? '当前未记录异常；不代表完整性已确认' : '尚无异常事实' }}</dd></dl>
          <h3>质量判定计数</h3>
          <dl><dt>已判定记录数</dt><dd>{{ qualityCount(qualityReport?.evaluated) }}</dd><dt>有效记录数</dt><dd>{{ qualityCount(qualityReport?.valid) }}</dd><dt>拒绝记录数</dt><dd>{{ qualityCount(qualityReport?.rejected) }}</dd></dl>
          <h3>双路最终写入</h3>
          <dl><dt>有效记录通道</dt><dd>{{ qualityCount(qualityReport?.final_written?.valid) }}</dd><dt>拒绝记录通道</dt><dd>{{ qualityCount(qualityReport?.final_written?.rejected) }}</dd></dl>
          <h3>字段 / 错误码命中频次</h3>
          <p v-if="!qualityReport?.rule_hits" class="muted">尚无完整的规则命中统计。</p>
          <p v-else-if="!Object.keys(qualityReport.rule_hits).length" class="muted">报告未记录规则命中；不代表写入或发布已获准。</p>
          <dl v-else><template v-for="(count, code) in qualityReport.rule_hits" :key="code"><dt><code>{{ code }}</code></dt><dd>{{ qualityCount(count) }}</dd></template></dl>
          <h3>质量发布闭锁</h3>
          <dl><dt>质量报告门禁</dt><dd>{{ qualityReport?.publication_veto === true ? '闭锁：质量事实未满足发布条件' : qualityReport?.publication_veto === false ? '本报告未设置质量闭锁；仍受运行发布门禁约束' : '未知：保持发布闭锁' }}</dd><dt>报告原因</dt><dd>{{ qualityReport?.reasons?.length ? qualityReport.reasons.map(qualityReason).join('；') : Array.isArray(qualityReport?.reasons) ? '报告未记录质量闭锁原因' : '尚无完整原因事实' }}</dd></dl>
        </div>
      </section>
      <section v-if="selected" class="panel">
        <div class="panel-title"><h2>完整全量结果发布</h2><span class="badge accent">{{ publication?.published ? '已确认发布' : '尚未确认发布' }}</span></div>
        <div class="body">
          <p class="muted">引擎完成与正式发布是两个独立事实。仅原 P1 发布准备单的双审批、完整质量报告、监督预算、引擎终止、双路写入静止与物理表身份全部通过后，才允许单条交换。PostgreSQL 与 Doris 不构成跨库原子事务；未知结果保持目标隔离，禁止再次交换。</p>
          <dl><dt>批准的发布契约</dt><dd>{{ publication?.approved_contract ? '已冻结既有正式表身份与完整全量范围' : '无有效发布契约；需重新准备 P1 发布与双审批' }}</dd><dt>发布阶段</dt><dd>{{ publication?.intent ? publicationPhase(publication.intent.phase) : '尚无持久发布意图' }}</dd><dt>结果依据</dt><dd>{{ publication?.intent?.reason || '等待实际物理身份核验' }}</dd><dt>旧正式表保留至</dt><dd>{{ publication?.snapshot ? time(publication.snapshot.retain_until) : '尚无已确认快照；成功后至少保留七天' }}</dd></dl>
          <button v-if="operator" class="primary" :disabled="busy || (!canPublish && !publicationPending)" @click="sendPublication">{{ publicationPending ? '重试同一发布请求' : '发布已批准的完整结果' }}</button>
          <p v-if="publicationPending" class="notice">响应未知，已保留运行 <code>{{ publicationPending.runId }}</code> 的原幂等键。请选回该运行重试同一意图；不能重新交换。</p>
          <details v-if="publication"><summary>发布契约、对象身份与保留快照</summary><pre>{{ pretty(publication) }}</pre></details>
        </div>
      </section>
      <section v-if="selected" class="panel">
        <div class="panel-title"><h2>原作业状态核对</h2><span class="badge accent">只读恢复</span></div>
        <div class="body">
          <p class="muted">只查询原 Attempt 已持久绑定的引擎和任务 ID，核对命令名称与状态。缺少 ID、查询未知或租约过期均保持目标隔离；引擎终态不等于写入已静止，不能据此发布或清理。真实受管 L2 门禁仍关闭。</p>
          <template v-if="operator">
            <label>核对原因<select v-model="reconcileReason" :disabled="busy || !!reconcilePending"><option v-for="(label, code) in reconcileReasons" :key="code" :value="code">{{ label }}</option></select></label>
            <button class="primary" :disabled="busy || !reconcileReason.trim()" @click="sendReconcile">{{ reconcilePending ? '重试同一核对请求' : '核对原作业' }}</button>
            <p v-if="reconcilePending" class="notice">响应尚未确认，原幂等键与理由已保留：<code>{{ reconcilePending.body.idempotency_key }}</code>。重试将返回同一核对记录。</p>
          </template>
          <p v-if="!reconciliation.length" class="muted">尚无持久核对记录。</p>
          <article v-for="fact in reconciliation" :key="fact.record_id" class="reconciliation-fact">
            <dl><dt>核对记录 ID</dt><dd><code>{{ fact.record_id }}</code></dd><dt>核对原因</dt><dd>{{ reconcileReasons[fact.reason] || '原因代码待核验' }}</dd><dt>处理阶段</dt><dd>{{ ({ accepted: '意图已接受', observing: '只读查询中，重启后仍仅查询原 ID', observed: '原作业事实已关联', unknown: '事实未知，继续隔离' })[fact.phase] || fact.phase }}</dd><dt>核对依据</dt><dd>{{ reconciliationBasis(fact.basis) }}</dd><dt>原作业 ID</dt><dd>{{ fact.result?.job_id || '未可靠确认' }}</dd><dt>引擎状态</dt><dd>{{ fact.result?.engine_state || '未知' }}</dd><dt>引擎终态</dt><dd>{{ fact.result?.engine_terminal === true ? '已观测终态；尚不证明写入静止' : fact.result?.engine_terminal === false ? '仍在运行' : '未知' }}</dd><dt>写入静止 / 清理</dt><dd>独立证据待核验，核对不授予清理许可</dd><dt>更新时间</dt><dd>{{ time(fact.updated_at) }}</dd></dl>
            <details><summary>签名意图与持久证据</summary><pre>{{ pretty({ evidence: fact.evidence, result: fact.result }) }}</pre></details>
          </article>
        </div>
      </section>
      <section v-if="operator && selected" class="panel">
        <div class="panel-title"><h2>安全控制</h2><span class="badge accent">Operator</span></div>
        <div class="body">
          <p class="muted">停止与清理是两个独立的持久控制效果：停止只提交意图，引擎 ACK 不等于终止确认；清理只在终止确认、最终指标已知且无在途调用后，按固定签名对象范围释放本运行未发布对象。两者都会因当前职责或原绑定凭据不可用而被拒绝。</p>
          <label>控制理由<textarea v-model="controlReason" rows="2" :disabled="busy"></textarea></label>
          <div class="actions">
            <button class="primary" :disabled="busy || !canStop" @click="sendControl('stop')">请求安全停止</button>
            <button class="secondary" :disabled="busy || !canCleanup" @click="sendControl('cleanup')">请求独立清理</button>
          </div>
          <dl><dt>可停止</dt><dd>{{ canStop ? '当前状态允许提交停止意图' : '当前状态不接受停止意图' }}</dd><dt>可清理</dt><dd>{{ canCleanup ? '终止与最终指标已确认，且无在途调用' : '尚未满足清理前置；目标所有权保持隔离' }}</dd></dl>
        </div>
      </section>
      <section v-if="control" class="panel"><div class="panel-title"><h2>控制阶段与最终指标</h2><span class="badge accent">{{ control.termination.confirmed ? '终止已确认' : '终止未确认' }}</span></div><div class="body"><p class="muted">最终指标与终态相互独立：终止确认后仍可能缺少可靠指标，此时发布保持闭锁。超调按冻结契约逐项计算，指标缺失时为 null，不代表零超调。</p><dl><dt>终止依据</dt><dd>{{ control.termination.confirmed ? control.termination.basis : '未确认；ACK 与超时都不构成终止' }}</dd><dt>终态</dt><dd>{{ control.termination.confirmed ? control.termination.state : '暂无数据' }}</dd><dt>在途调用</dt><dd>{{ control.inflight ? `${control.inflight.kind} · ${control.inflight.status}` : '无' }}</dd><dt>最终指标</dt><dd>{{ control.final_metrics.status === 'known' ? '已取得可靠最终指标' : '待可靠指标；发布闭锁' }}</dd><template v-if="control.final_metrics.status === 'known'"><template v-for="(value, key) in control.final_metrics.overshoot" :key="key"><dt>{{ key }} 超调</dt><dd>{{ value == null ? '指标缺失，不计零' : value }}</dd></template><dt>检测后增量</dt><dd><pre>{{ pretty(control.final_metrics.post_detection_delta) }}</pre></dd></template></dl><details open><summary>持久控制效果</summary><pre>{{ pretty(control.effects) }}</pre></details><details><summary>租约与唤醒</summary><pre>{{ pretty(control.lease) }}</pre></details></div></section>
      <section v-if="selected" class="panel"><div class="panel-title"><h2>运行与控制事实</h2><span class="badge accent">{{ status(selected.status) }}</span></div><div class="body"><dl><dt>唯一运行 ID</dt><dd><code>{{ selected.id }}</code></dd><dt>准备单</dt><dd><code>{{ selected.preparation_id }}</code></dd><dt>原始操作员</dt><dd><code>{{ selected.subject }}</code></dd><dt>接受时间</dt><dd>{{ time(selected.accepted_at) }}</dd><dt>执行级别</dt><dd>{{ selected.execution.level === 'L1' ? '隔离 L1 故障 Engine' : '执行关闭' }}</dd><dt>引擎任务</dt><dd>{{ selected.engine_job_id || (selected.status === 'submission_unknown' || selected.status === 'submitting' ? '尚未确认，不代表作业不存在' : '暂无已确认任务') }}</dd><dt>提交关联</dt><dd><code>{{ selected.execution.attempt?.correlation_id || '尚未申请提交' }}</code></dd><dt>提交阶段</dt><dd>{{ selected.execution.attempt ? status(selected.execution.attempt.phase) : '无 Attempt' }}</dd><dt>核对说明</dt><dd>{{ selected.execution.attempt?.reason || '暂无异常' }}</dd><dt>目标隔离</dt><dd>{{ selected.execution.ownership?.retained ? '持续保留；未知结果不因超时释放' : '本运行未占有目标' }}</dd><dt>发布与清理</dt><dd>{{ selected.execution.publication }}</dd><dt>投递状态</dt><dd>{{ selected.outbox.status === 'pending' ? '待投递' : selected.outbox.status }} · 已尝试 {{ selected.outbox.attempts }} 次</dd></dl><details><summary>已接受命令、授权范围与冻结预算</summary><pre>{{ pretty(selected.managed_command) }}</pre></details><details open><summary>持久化队列、提交与目标所有权</summary><pre>{{ pretty({ outbox: selected.outbox, attempt: selected.execution.attempt, ownership: selected.execution.ownership }) }}</pre></details><details><summary>同事务控制证据</summary><pre>{{ pretty(selected.execution.control_facts) }}</pre></details></div></section>
    </template>
    <div v-else class="choose-project">选择项目，查看已接受运行与待调度事实。</div>
  </div>
</template>

<style scoped>
.project-picker{max-width:460px;margin-bottom:24px}.body,.run-list{padding:20px 24px}.body>label{max-width:620px;margin-bottom:16px}.run-list{display:grid;gap:12px}.run-row{display:flex;align-items:center;gap:20px;flex-wrap:wrap;text-align:left;padding:16px;border:1px solid #dce7ed;border-radius:10px;background:#f7fafb;color:#253f50}.run-row code{flex:1}.run-row small{color:#657783}dl{display:grid;grid-template-columns:140px minmax(0,1fr);gap:12px}dt{color:#657783}dd{margin:0;overflow-wrap:anywhere}summary{cursor:pointer;color:#366c7e;margin:16px 0}pre{overflow:auto;max-height:460px;padding:20px;background:#f3f7fa;border-radius:10px;font-size:12px;line-height:1.7}@media(max-width:900px){dl{grid-template-columns:1fr;gap:8px}dd{margin-bottom:12px}}
</style>
