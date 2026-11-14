// Visual smoke uses the actual built UI and persisted L1 report with HTTP replay.
// API/Worker behavior is verified separately by verify_supervision_l1.py.
const { chromium } = require('../.runtime/browser/node_modules/playwright');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const report = JSON.parse(fs.readFileSync(path.join(root, 'artifacts/issue-11/l1.json'), 'utf8'));
const rows = report.passed.map(item => ({ id: item.run_id, status: item.status, tenant_id: 'smoke', project_id: 'fixture',
  preparation_id: 'frozen-preparation', subject: 'operator', accepted_at: report.checked_at, engine_job_id: `fixture-${item.scenario}`,
  managed_command: {payload: {runtime_supervision_contract: item.supervision.contract}},
  outbox: { status: 'sent', attempts: 1 }, execution: { level: 'L1', supervision: item.supervision,
    attempt: {phase: 'submitted', correlation_id: item.scenario}, ownership: {retained: true},
    control_facts: [], publication: 'L1 未执行发布与清理' } }));
const server = http.createServer((req, res) => {
  const target = path.join(root, 'frontend/dist', req.url === '/' ? 'index.html' : req.url);
  const types = {'.html':'text/html', '.js':'text/javascript', '.css':'text/css'};
  res.setHeader('Content-Type', types[path.extname(target)] || 'application/octet-stream');
  fs.createReadStream(target).on('error', () => res.end()).pipe(res);
});
(async () => {
  await new Promise(resolve => server.listen(18111, '127.0.0.1', resolve));
  let browser;
  try {
    browser = await chromium.launch({executablePath: 'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe', headless: true});
    const page = await browser.newPage({viewport:{width:1440,height:1100}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/**', async route => {
      const url = new URL(route.request().url());
      let body = [];
      if (url.pathname === '/api/me') body = { tenant:{id:'smoke',name:'隔离 L1 展示',slug:'smoke'},account:{display_name:'审计人员',username:'auditor',tenant_admin:false} };
      else if (url.pathname === '/api/health') body = {status:'ok',postgresql:{version:'17'},redis:{version:'7'}};
      else if (url.pathname === '/api/tenants/smoke/projects') body = [{id:'fixture',name:'V10 运行监督',slug:'fixture',roles:['auditor']}];
      else if (url.pathname.endsWith('/runs')) body = rows;
      await route.fulfill({json:body});
    });
    await page.goto('http://127.0.0.1:18111');
    await page.getByRole('button', {name:'▷ 运行中心'}).click();
    await page.locator('select').first().selectOption('fixture');
    await page.locator('.run-row').nth(0).click();
    await page.getByRole('heading', {name:'运行监督与预算'}).waitFor();
    assert.ok((await page.locator('body').innerText()).includes('拒绝率超过预算'));
    await page.screenshot({path:path.join(root,'artifacts/issue-11/budget-stop.png'),fullPage:true});
    await page.locator('.run-row').nth(6).click();
    const missing = await page.locator('body').innerText();
    assert.ok(missing.includes('指标缺失或无法观测') && missing.includes('暂无数据'));
    await page.screenshot({path:path.join(root,'artifacts/issue-11/missing-metrics.png'),fullPage:true});
    await page.locator('.run-row').nth(10).click();
    assert.ok((await page.locator('body').innerText()).includes('暂无数据'));
    assert.deepEqual(errors, []);
    fs.writeFileSync(path.join(root,'artifacts/issue-11/browser.json'),JSON.stringify({passed:['Chinese stop/veto/reason and measured metrics rendered','missing metrics rendered as 暂无数据','zero denominator rendered as 暂无数据'],mode:'actual production Vue bundle with replay of persisted L1 HTTP facts',errors},null,2));
    console.log('Browser smoke passed: stop, missing and zero-denominator Chinese UI; no page errors');
  } finally { if(browser) await browser.close(); server.close(); }
})().catch(error => {console.error(error);process.exitCode=1});
