/* Offline DOM and real SQLite/WSGI browser regression; never sends external messages. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {spawn} = require('node:child_process');
const root = path.resolve(__dirname, '..');
const ui = path.join(root, 'src/pc28touzhu/ui');
function dependency(name) {
    for (const base of ['', process.env.PC28_UI_NODE_MODULES, '/tmp/pc28-cycle-ui-tools/node_modules', '/tmp/pc28-draw-trigger-ui-check/node_modules'].filter(v => v !== undefined)) {
        try { return require(base ? path.join(base, name) : name); }
        catch (error) {if (error.code !== 'MODULE_NOT_FOUND') throw error;}
    }
    throw new Error('Existing UI dependency unavailable: ' + name);
}
const {JSDOM} = dependency('jsdom');
const tick = () => new Promise(resolve => setTimeout(resolve, 10));
async function until(check, description) {
    for (let i = 0; i < 250; i++) {if (check()) return; await tick();}
    throw new Error('Timed out: ' + description);
}
function mount(fetcher, url = 'http://127.0.0.1/auto-triggers') {
    const dom = new JSDOM(fs.readFileSync(path.join(ui, 'auto_triggers.html'), 'utf8'), {url, runScripts: 'outside-only'});
    dom.window.scrollTo = () => {};
    dom.window.confirm = () => true;
    dom.window.fetch = fetcher;
    dom.window.eval(fs.readFileSync(path.join(ui, 'auto_triggers.js'), 'utf8'));
    return dom;
}
const day = () => new Intl.DateTimeFormat('en-CA', {timeZone: 'Asia/Shanghai'}).format(new Date());
const sub = {id: 21, source_id: 20, name: '短记忆 <img src=x onerror=alert(1)>', status: 'standby', strategy_v2: {staking_policy: {mode: 'win_streak_roll'}}};
function rule(runtime) {
    return {id: 13, name: '四时段规则', status: 'active', scope_mode: 'selected_subscriptions', subscription_ids: [21],
        trigger_mode: 'schedule', conditions: [], action: {}, routes: [{id: 13, status: 'active'}],
        cycle_policy: {mode: 'rule', max_starts_per_day: 4, max_starts_per_window: 1, on_target: 'wait_condition', on_bust: 'wait_condition'},
        cycle_runtime: [{subscription_id: 21, subscription_name: sub.name, stat_date: day(), starts_today: 0, max_starts_per_day: 4,
            status: 'waiting', stop_reason: 'delivery_uncertain_expired', unresolved_run_count: 1, reserved_stake: 13,
            pending_capital_credit: 0, last_interrupted_run_id: 1937, last_interrupted_at: '2026-11-26T02:07:48+08:00',
            routes: [{route_id: 13, route_name: '原目标', balance: 9.39, cycle_stake: 13.93, win_streak: 1, target_streak: 3,
                pending_issue: '', reserved_stake: 13, pending_capital_credit: 0, current_top_up_amount: 0.61}], ...runtime}]};
}
function fixture(fetchHistory, getRule = () => rule()) {
    const calls = [];
    const fetcher = async (url, options = {}) => {
        calls.push({url, method: options.method || 'GET'});
        if (url.includes('/cycle-recoveries')) return fetchHistory(url, options);
        let payload = {items: []};
        if (url === '/api/auth/me') payload = {user: {id: 1}};
        else if (url === '/api/platform/sources') payload = {items: [{id: 20, status: 'active'}]};
        else if (url === '/api/platform/subscriptions/summary') payload = {items: [sub], launch_workflow: 'explicit_start_v1'};
        else if (url.startsWith('/api/platform/auto-trigger-rules')) payload = {items: [getRule()], cycle_window_limits: true};
        return {ok: true, status: 200, json: async () => payload};
    };
    return {calls, dom: mount(fetcher)};
}
const response = payload => ({ok: true, status: 200, json: async () => payload});
function archive(id, fields = {}) {
    return {id, rule_id: 13, subscription_id: 21, route_id: 13, rule_run_id: 1900 + id, stat_date: '2026-11-26',
        issue_no: '3510978', status: 'awaiting_reconciliation', reason: 'delivery_uncertain_expired',
        initial_reserved_stake: 13, reserved_stake: 13, capital_credit: null,
        capital_disposition: 'deferred_chain_credit', ...fields};
}
async function domChecks() {
    let resolveFirst;
    let failMore = true;
    const first = new Promise(resolve => {resolveFirst = resolve;});
    const f = fixture(async url => {
        if (!url.includes('before_id=')) return first;
        assert.match(url, /before_id=2/);
        if (failMore) return {ok: false, status: 503, json: async () => ({error: 'internal failure'})};
        return response({items: [archive(1, {status: 'resolved', capital_credit: 0, reserved_stake: 0,
            capital_disposition: 'independent_cycle_return'})], next_cursor: null});
    });
    const {document: d} = f.dom.window;
    try {
        await until(() => d.querySelector('.cycle-runtime-card'), 'rule loaded');
        const card = d.querySelector('.cycle-runtime-card');
        assert.equal(card.querySelector('.cycle-state').textContent, '等待触发');
        assert.ok(card.querySelector('.cycle-recovery-summary'), 'expired old run needs a separate history summary');
        assert.match(card.textContent, /保留本金\s*13\.00/);
        assert.match(card.textContent, /策略余额\s*9\.39/);
        assert.match(card.textContent, /本轮补充策略本金\s*0\.61/);
        assert.doesNotMatch(card.textContent, /当前暂不可启动|必须人工|人工处理才能/);
        assert.equal(card.querySelectorAll('img').length, 0, 'subscription name must be escaped');
        const toggle = card.querySelector('.cycle-history-toggle');
        assert.ok(toggle, 'history must be reachable without manual recovery');
        toggle.click();
        assert.match(card.textContent, /正在加载历史账务/);
        assert.equal(toggle.getAttribute('aria-expanded'), 'true');
        resolveFirst(response({items: [archive(3, {issue_no: '<img src=x onerror=alert(1)>'}), archive(2, {status: 'future_status'})], next_cursor: 2}));
        await until(() => card.querySelectorAll('.cycle-history-item').length === 2, 'first history page');
        assert.match(card.textContent, /历史结果待核对/);
        assert.match(card.textContent, /尚未确定/);
        assert.match(card.textContent, /状态未知/);
        assert.equal(card.querySelectorAll('img').length, 0, 'history must be escaped');
        assert.ok(f.calls.find(call => /cycle-recoveries\?subscription_id=21&limit=20$/.test(call.url)));
        card.querySelector('.cycle-history-more').click();
        await until(() => card.querySelector('.cycle-history-retry'), 'history retry visible');
        assert.equal(card.querySelectorAll('.cycle-history-item').length, 2, 'failed next page keeps previous rows');
        assert.doesNotMatch(card.textContent, /internal failure/);
        failMore = false;
        card.querySelector('.cycle-history-retry').click();
        await until(() => card.querySelectorAll('.cycle-history-item').length === 3, 'next page recovered');
        assert.match(card.querySelector('.cycle-history-item:last-child').textContent, /独立周期返还\s*0\.00/);
        assert.equal(card.querySelectorAll('.cycle-history-more').length, 0);
        assert.equal(card.querySelectorAll('.retry-job-btn,.resume-cycle-btn').length, 0);
        toggle.click(); assert.equal(toggle.getAttribute('aria-expanded'), 'false');
        const reads = f.calls.filter(call => call.url.includes('/cycle-recoveries')).length;
        toggle.click(); await tick();
        assert.equal(f.calls.filter(call => call.url.includes('/cycle-recoveries')).length, reads, 'reopen uses loaded history');
        assert.ok(f.calls.every(call => call.method === 'GET'), 'history never writes or resends');
    } finally {f.dom.window.close();}

    for (const current of [
        {status: 'running', stop_reason: '', starts_today: 1, active_run_id: 2000},
        {status: 'blocked', stop_reason: 'manual_stop'},
        {status: 'waiting', stop_reason: '', unresolved_run_count: undefined, reserved_stake: undefined,
            pending_capital_credit: undefined, last_interrupted_run_id: undefined},
    ]) {
        const x = fixture(async () => response({items: [], next_cursor: null}), () => rule(current));
        try {
            const doc = x.dom.window.document;
            await until(() => doc.querySelector('.cycle-runtime-card'), 'variant rule');
            const card = doc.querySelector('.cycle-runtime-card');
            assert.equal(card.querySelector('.cycle-state').textContent, {running: '进行中', blocked: '暂停，需核对', waiting: '等待触发'}[current.status]);
            if (current.status === 'blocked') assert.match(card.textContent, /已人工结束本轮/);
            if (current.last_interrupted_run_id === undefined && current.status === 'waiting') {
                assert.equal(card.querySelector('.cycle-recovery-summary'), null, 'legacy response stays compatible');
            } else {
                card.querySelector('.cycle-history-toggle').click();
                await until(() => /暂无中断轮次账务/.test(card.textContent), 'empty history');
            }
        } finally {x.dom.window.close();}
    }
    const invalid = fixture(async () => response({items: [archive(1)], next_cursor: -1}));
    try {
        const d = invalid.dom.window.document;
        await until(() => d.querySelector('.cycle-history-toggle'), 'invalid history fixture');
        d.querySelector('.cycle-history-toggle').click();
        await until(() => d.querySelector('.cycle-history-retry'), 'invalid cursor rejected');
        assert.equal(d.querySelectorAll('.cycle-history-item').length, 0, 'malformed page is not partially appended');
    } finally {invalid.dom.window.close();}
    let finishStale;
    const staleResponse = new Promise(resolve => {finishStale = resolve;});
    const stale = fixture(async () => staleResponse);
    try {
        const w = stale.dom.window;
        const d = w.document;
        await until(() => d.querySelector('.cycle-history-toggle'), 'auth history fixture');
        d.querySelector('.cycle-history-toggle').click();
        d.dispatchEvent(new w.CustomEvent('platform-auth:changed', {detail: {user: null}}));
        finishStale(response({items: [archive(1, {issue_no: 'stale-private-history'})], next_cursor: null}));
        await tick();
        assert.doesNotMatch(d.body.textContent, /stale-private-history/);
        assert.equal(d.querySelectorAll('.cycle-history-item').length, 0);
    } finally {stale.dom.window.close();}
    console.log('PASS recovery DOM: independent current state, reserve, read-only history, paging/retry/empty, null/zero, escaping, legacy fields.');
}

async function realChecks() {
    const child = spawn(process.env.PYTHON || 'python3', ['tests/cycle_recovery_ui_server.py'], {
        cwd: root, env: {...process.env, PYTHONPATH: path.join(root, 'src')}, stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stderr = '';
    child.stderr.on('data', chunk => {stderr += chunk.toString();});
    const exited = new Promise(resolve => child.once('exit', code => resolve(code)));
    const info = await new Promise((resolve, reject) => {
        let output = '';
        child.stdout.on('data', chunk => {
            output += chunk.toString();
            if (output.includes('\n')) {
                try {resolve(JSON.parse(output.split('\n')[0]));} catch (error) {reject(error);}
            }
        });
        child.once('error', reject);
        child.once('exit', code => {if (!output.includes('\n')) reject(new Error('Fixture failed ' + code + ': ' + stderr));});
    });
    try {
        const base = '/api/platform/auto-trigger-rules/' + info.waiting_rule_id + '/cycle-recoveries';
        const http = (url, options = {}) => fetch(new URL(url, info.url), {...options, headers: {...options.headers, Cookie: info.cookie}});
        const unauthenticated = await fetch(info.url + base);
        assert.equal(unauthenticated.status, 401);
        const first = await (await http(base + '?limit=1')).json();
        assert.equal(first.items.length, 1);
        assert.ok(first.next_cursor > 0);
        const second = await (await http(base + '?limit=1&before_id=' + first.next_cursor)).json();
        assert.ok(second.items[0].id < first.items[0].id);
        assert.equal(first.items[0].state, undefined);
        assert.equal(first.items[0].pending_plan, undefined);
        const dom = mount(http, info.url + '/auto-triggers');
        try {
            const d = dom.window.document;
            await until(() => d.querySelector('.cycle-recovery-summary'), 'real API history summary');
            const detail = d.querySelector('[data-cycle-rule="' + info.waiting_rule_id + '"]');
            assert.equal(detail.querySelector('.cycle-state').textContent, '等待触发');
            detail.querySelector('.cycle-history-toggle').click();
            await until(() => detail.querySelectorAll('.cycle-history-item').length === 20, 'real first page');
            detail.querySelector('.cycle-history-more').click();
            await until(() => detail.querySelectorAll('.cycle-history-item').length === info.history_count, 'real second page');
            const active = d.querySelector('[data-cycle-rule="' + info.running_rule_id + '"]');
            assert.equal(active.querySelector('.cycle-state').textContent, '进行中');
            assert.match(active.textContent, /未决旧轮\s*1/);
            assert.match(active.textContent, /保留本金\s*10\.00/);
            assert.equal(active.querySelectorAll('.resume-cycle-btn,.retry-job-btn').length, 0);
        } finally {dom.window.close();}
        if (process.argv.includes('--browser')) await browserChecks(info);
        console.log('PASS recovery real WSGI/SQLite: authenticated history, cursor pages, new active run with archived old principal, no writes.');
    } finally {
        child.stdin.end();
        const code = await exited;
        assert.equal(code, 0, stderr);
    }
}

async function browserChecks(info) {
    const {chromium} = dependency('playwright-core');
    const executablePath = process.env.PC28_UI_CHROMIUM || '/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome';
    const browser = await chromium.launch({executablePath, headless: true, args: ['--no-sandbox']});
    const errors = [];
    const writes = [];
    try {
        const context = await browser.newContext();
        const split = info.cookie.indexOf('=');
        await context.addCookies([{name: info.cookie.slice(0, split), value: info.cookie.slice(split + 1), url: info.url}]);
        await context.route('**/*', route => new URL(route.request().url()).origin === info.url ? route.continue() : route.abort());
        const page = await context.newPage();
        page.on('pageerror', error => errors.push(error.message));
        page.on('request', req => {if (req.method() !== 'GET') writes.push(req.method() + ' ' + req.url());});
        for (const viewport of [{width: 1440, height: 1000}, {width: 390, height: 844}]) {
            await page.setViewportSize(viewport);
            await page.goto(info.url + '/auto-triggers');
            const waiting = page.locator('[data-cycle-rule="' + info.waiting_rule_id + '"]');
            await waiting.locator('summary').first().click();
            await waiting.getByRole('button', {name: '查看历史账务', exact: true}).click();
            await waiting.locator('.cycle-history-item').first().waitFor();
            assert.equal(await waiting.locator('.cycle-history-item').count(), 20);
            await waiting.getByRole('button', {name: '加载更早账务'}).click();
            await page.waitForFunction(({id, count}) => document.querySelector('[data-cycle-rule="' + id + '"]').querySelectorAll('.cycle-history-item').length === count,
                {id: info.waiting_rule_id, count: info.history_count});
            assert.equal(await waiting.locator('.cycle-state').innerText(), '等待触发');
            const active = page.locator('[data-cycle-rule="' + info.running_rule_id + '"]');
            await active.locator('summary').first().click();
            assert.equal(await active.locator('.cycle-state').innerText(), '进行中');
            assert.match(await active.innerText(), /保留本金\s*10\.00/);
            assert.equal(await active.locator('.retry-job-btn,.resume-cycle-btn').count(), 0);
            const dimensions = await page.evaluate(() => ({width: document.documentElement.scrollWidth, viewport: innerWidth}));
            assert.ok(dimensions.width <= dimensions.viewport + 1, JSON.stringify(dimensions));
            const evidence = process.env.PC28_UI_EVIDENCE_DIR || path.join(root, '.boss/cycle-recovery/evidence');
            fs.mkdirSync(evidence, {recursive: true});
            await active.scrollIntoViewIfNeeded();
            await page.screenshot({path: path.join(evidence, 'recovery-' + viewport.width + '.png'), fullPage: true});
        }
        assert.deepEqual(errors, []);
        assert.deepEqual(writes, []);
        console.log('PASS recovery Chromium: desktop 1440/mobile 390, real history pages, independent active state, no overflow/errors/writes.');
    } finally {await browser.close();}
}

async function main() {await domChecks(); if (!process.argv.includes('--dom-only')) await realChecks();}
main().catch(error => {console.error(error.stack || error); process.exitCode = 1;});
