import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import * as Vue from 'vue'
import { compileScript, parse } from 'vue/compiler-sfc'

// Compile and render the real SFC, exercising its input events and API payload.
const source = readFileSync(new URL('./StudioPanel.vue', import.meta.url), 'utf8')
const { descriptor } = parse(source)
const compiled = compileScript(descriptor, { id: 'studio-input-test', inlineTemplate: true })
const component = new Function('Vue', compiled.content
  .replace(/import \{([\s\S]*?)\} from ['"]vue['"]/g, (_, bindings) =>
    'const {' + bindings.replace(/ as /g, ': ') + '} = Vue')
  .replace('export default', 'return'))(Vue)

globalThis.document = { activeElement: null }
function element(type, text = '') {
  return {
    type, text, children: [], props: {}, tagName: type.toUpperCase(),
    addEventListener() {}, removeEventListener() {},
    get options() { return this.children.filter(child => child.type === 'option') },
    setAttribute(name, value) { this.props[name] = value },
    removeAttribute(name) { delete this.props[name] },
  }
}
const renderer = Vue.createRenderer({
  createElement: element,
  createText: text => element('#text', text),
  createComment: text => element('#comment', text),
  setText: (node, text) => { node.text = text },
  setElementText: (node, text) => { node.text = text; node.children = [] },
  parentNode: node => node.parent,
  nextSibling: node => node.parent?.children[node.parent.children.indexOf(node) + 1],
  patchProp: (node, key, _previous, value) => { node.props[key] = value; if (['value', 'checked'].includes(key)) node[key] = value },
  insert(node, parent, anchor = null) {
    if (node.parent) node.parent.children.splice(node.parent.children.indexOf(node), 1)
    node.parent = parent
    const index = anchor ? parent.children.indexOf(anchor) : -1
    if (index < 0) parent.children.push(node)
    else parent.children.splice(index, 0, node)
  },
  remove(node) { node.parent.children.splice(node.parent.children.indexOf(node), 1) },
})
function nodes(root) { return [root, ...root.children.flatMap(nodes)] }
function text(node) { return node.text + node.children.map(text).join('') }
async function settle() {
  for (let i = 0; i < 5; i++) { await Promise.resolve(); await Vue.nextTick() }
}
async function studio() {
  const calls = []
  const plan = {
    version: 'etl-plan-v1', mode: 'bounded_full', allow_empty: false,
    source: { connection_id: 'mysql', profile_id: 'profile', table: 'source' },
    target: { connection_id: '', profile_id: '', table: '' },
    mappings: [], cleaning: [], quality: [], filters: [{ field: 'id', equals: 7 }],
    tools: ['mysql.read', 'doris.write'], budget: {},
  }
  const candidate = { id: 'candidate', name: 'precision fixture', owner_id: 'maker', revision: 1, plan, assessment: { passed: false, versions: { compiler: 'fixture', policy: 'fixture' }, gates: [], limitations: [] } }
  const api = async (url, options) => {
    if (options) {
      calls.push({ url, ...JSON.parse(options.body) })
      throw new Error('fixture: server rejects invalid filter')
    }
    if (url.endsWith('/connections')) return [{ id: 'mysql', kind: 'mysql', direction: 'source', status: 'ready' }]
    if (url.endsWith('/profiles')) return [{ id: 'profile', connection_id: 'mysql', content: { tables: [{ name: 'source', columns: [{ name: 'id', type: 'BIGINT' }, { name: 'label', type: 'VARCHAR(50)' }] }] } }]
    if (url.endsWith('/studio/candidates/candidate')) return structuredClone(candidate)
    if (url.endsWith('/studio/candidates')) return [candidate]
    throw new Error('unexpected fixture API: ' + url)
  }
  const root = element('root')
  const app = renderer.createApp(component, { api, tenantId: 'tenant', accountId: 'maker', projects: [{ id: 'project', name: 'Project', roles: ['maker'] }] })
  app.mount(root)
  nodes(root).find(node => node.type === 'select').props['onUpdate:modelValue']('project')
  await settle()
  nodes(root).find(node => node.type === 'button' && text(node).includes('precision fixture')).props.onClick()
  await settle()
  const input = () => nodes(root).find(node => node.type === 'input' && node.props.onInput)
  return {
    root, calls, app, input,
    async enter(value) { input().props.onInput({ target: { value } }); await settle() },
    async validate() { nodes(root).find(node => node.type === 'form').props.onSubmit({ preventDefault() {} }); await settle() },
    async save() { nodes(root).find(node => node.type === 'button' && text(node).includes('保存修订')).props.onClick(); await settle() },
    plan() { return JSON.parse(nodes(root).find(node => node.type === 'pre').text) },
  }
}

test('fractional BIGINT input is preserved in the current plan and submitted without rounding', async () => {
  const page = await studio()
  try {
    await page.enter('9007199254740991.1')
    assert.equal(page.plan().filters[0].equals, '9007199254740991.1')
    assert.equal(page.input().props.type, 'text')
    assert.equal(page.input().props['aria-invalid'], 'true')
    assert.match(text(page.root), /当前原文已保留，服务端将拒绝此值/)
    await page.validate()
    assert.equal(page.calls.at(-1).plan.filters[0].equals, '9007199254740991.1')
    await page.save()
    assert.equal(page.calls.at(-1).plan.filters[0].equals, '9007199254740991.1')
    assert.equal(page.calls.at(-1).expected_revision, 1)
  } finally { page.app.unmount() }
})

test('blank, fractional, unsafe and nondecimal input replace the old value without coercion', async () => {
  const page = await studio()
  try {
    for (const raw of ['', ' ', '9007199254740992', '-9007199254740992', '9223372036854775807', '-9007199254740991.1', '1.0', '1e3', '0x10', 'NaN', 'Infinity', '-', '+1', '01', '1\n']) {
      await page.enter('42')
      await page.enter(raw)
      assert.equal(page.plan().filters[0].equals, raw, JSON.stringify(raw))
      assert.equal(page.input().props.value, raw)
      assert.equal(page.input().props['aria-invalid'], 'true')
      await page.validate()
      assert.equal(page.calls.at(-1).plan.filters[0].equals, raw)
      await page.save()
      assert.equal(page.calls.at(-1).plan.filters[0].equals, raw)
    }
  } finally { page.app.unmount() }
})

test('safe decimal integers submit as exact numbers and clear the previous error', async () => {
  const page = await studio()
  try {
    await page.enter('9007199254740992')
    for (const [raw, expected] of [['0', 0], ['42', 42], ['-42', -42], ['9007199254740991', 9007199254740991], ['-9007199254740991', -9007199254740991]]) {
      await page.enter(raw)
      assert.equal(page.plan().filters[0].equals, expected)
      assert.equal(page.input().props['aria-invalid'], undefined)
      assert.doesNotMatch(text(page.root), /当前原文已保留/)
      await page.validate()
      assert.equal(page.calls.at(-1).plan.filters[0].equals, expected)
    }
  } finally { page.app.unmount() }
})

test('NULL and text filters preserve their existing semantics', async () => {
  const page = await studio()
  try {
    const row = () => nodes(page.root).find(node => node.props.class === 'quality-row')
    const nullInput = () => nodes(row()).find(node => node.type === 'input' && node.props.type === 'checkbox')
    await page.enter('invalid')
    nullInput().props.onChange({ target: { checked: true } })
    await settle()
    assert.equal(page.plan().filters[0].equals, null)
    assert.equal(page.input(), undefined)
    await page.validate()
    assert.equal(page.calls.at(-1).plan.filters[0].equals, null)
    nullInput().props.onChange({ target: { checked: false } })
    await settle()
    assert.equal(page.plan().filters[0].equals, 0)
    const field = nodes(row()).find(node => node.type === 'select')
    field.props['onUpdate:modelValue']('label')
    field.props.onChange()
    await settle()
    for (const raw of ['', '9007199254740991.1', '  label  ']) {
      await page.enter(raw)
      assert.equal(page.plan().filters[0].equals, raw)
      assert.equal(page.input().props['aria-invalid'], undefined)
      await page.validate()
      assert.equal(page.calls.at(-1).plan.filters[0].equals, raw)
    }
  } finally { page.app.unmount() }
})
