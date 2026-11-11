/* Run: node tests/test_cycle_ui.cjs. Optional test dependency: jsdom (NODE_PATH or PC28_UI_NODE_MODULES).
 * No worker or external messaging is started; API checks use a temporary SQLite database.
 */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {spawn} = require('node:child_process');

function loadTestDependency(name) {
    const candidates = [name];
    if (process.env.PC28_UI_NODE_MODULES) candidates.push(path.join(process.env.PC28_UI_NODE_MODULES, name));
    candidates.push(path.join('/tmp/pc28-draw-trigger-ui-check/node_modules', name));
    candidates.push(path.join('/tmp/pc28-cycle-ui-tools/node_modules', name));
    for (const candidate of candidates) {
        try { return require(candidate); } catch (error) { if (error.code !== 'MODULE_NOT_FOUND') throw error; }
    }
    throw new Error('Install ' + name + ' in a temporary test directory and set PC28_UI_NODE_MODULES; no dependencies are installed automatically.');
}
const {JSDOM} = loadTestDependency('jsdom');
const root = path.resolve(__dirname, '..');
const ui = path.join(root, 'src/pc28touzhu/ui');
const tick = () => new Promise(resolve => setTimeout(resolve, 10));
async function until(predicate, description) {
    for (let attempt = 0; attempt < 200; attempt++) {
        if (predicate()) return;
        await tick();
    }
    throw new Error('Timed out: ' + description);
}
function localDate() {
    return new Intl.DateTimeFormat('en-CA', {timeZone: 'Asia/Shanghai'}).format(new Date());
}
const policy = {mode: 'rule', max_starts_per_day: 3, max_starts_per_window: 1, on_target: 'wait_condition', on_bust: 'close_day'};
const subscription = {id: 18, source_id: 17, name: '过关方案', status: 'standby',
    strategy: {win_streak_roll: {structure: 'B', base_amount: 10, target_streak: 3}},
    strategy_v2: {staking_policy: {mode: 'win_streak_roll', structure: 'B', base_amount: 10, target_streak: 3}},
    manual_cycle_state: null};
function fixtureRule(id, values = {}) {
    return {id, name: '规则 ' + id, status: 'active', scope_mode: 'selected_subscriptions', subscription_ids: [18],
        conditions: [{type: 'draw_streak', metric: 'combo', bet_value: '大双', threshold: 2, operator: 'gte'}],
        action: {play_filter_action: 'keep', dispatch_latest_signal: true}, guard_groups: [], routes: [],
        cooldown_issues: 10, cycle_policy: {...policy}, cycle_runtime: [], ...values};
}
function mountRulePage(fetcher, url = 'http://127.0.0.1/auto-triggers') {
    const dom = new JSDOM(fs.readFileSync(path.join(ui, 'auto_triggers.html'), 'utf8'), {url, runScripts: 'outside-only'});
    const styles = dom.window.document.createElement('style');
    styles.textContent = fs.readFileSync(path.join(ui, 'auto_triggers.css'), 'utf8');
    dom.window.document.head.append(styles);
    dom.window.scrollTo = () => {};
    dom.window.confirm = () => true;
    dom.window.fetch = fetcher;
    dom.window.eval(fs.readFileSync(path.join(ui, 'auto_triggers.js'), 'utf8'));
    return dom;
}
function change(w, input, value) {
    assert.ok(input, 'requested form field exists');
    input.value = value;
    input.dispatchEvent(new w.Event('change', {bubbles: true}));
}
function submit(w) {
    w.document.getElementById('ruleForm').dispatchEvent(new w.Event('submit', {bubbles: true, cancelable: true}));
}
async function domChecks() {
    const date = localDate();
    const runtime = {subscription_id: 18, subscription_name: '过关方案 <img src=x onerror=alert(1)>', stat_date: date, starts_today: 1, max_starts_per_day: 3,
        status: 'day_closed', start_stat_date: date, stop_reason: 'cycle_balance_depleted', routes: [
            {route_id: 11, route_name: '测试群', status: 'stopped', balance: 0, cycle_stake: 0, win_streak: 0, target_streak: 3, pending_issue: ''},
        ]};
    let rules = [fixtureRule(11, {cycle_runtime: [runtime]}), fixtureRule(12, {cycle_policy: {mode: 'legacy'}})];
    const writes = [];
    let rejectResume = false;
    let windowLimitsSupported = true;
    const fetcher = async (requestPath, opts = {}) => {
        let data = {items: []};
        let ok = true;
        if (requestPath === '/api/auth/me') data = {user: {id: 1, username: 'local-test'}};
        else if (requestPath === '/api/platform/sources') data = {items: [{id: 17, name: '来源', status: 'active'}]};
        else if (requestPath === '/api/platform/subscriptions/summary') data = {items: [subscription], launch_workflow: 'explicit_start_v1'};
        else if (requestPath.startsWith('/api/platform/auto-trigger-rules')) {
            if (opts.method === 'POST') {
                const body = JSON.parse(opts.body);
                writes.push({path: requestPath, body});
                if (requestPath.endsWith('/resume-cycle')) {
                    if (rejectResume) {ok = false; data = {error: '该方案仍有待结算投注'};}
                    else {runtime.status = 'waiting'; data = {resumed: true, item: rules[0], cycle_runtime: runtime};}
                } else {
                    const id = Number(requestPath.split('/').pop());
                    const existing = rules.find(item => item.id === id);
                    const next = {...(existing || fixtureRule(99)), ...body};
                    if (existing) rules = rules.map(item => item.id === id ? next : item);
                    else rules.push(next);
                    data = {item: next};
                }
            } else data = {items: rules, cycle_window_limits: windowLimitsSupported};
        }
        return {ok, status: ok ? 200 : 400, json: async () => data};
    };
    const dom = mountRulePage(fetcher);
    const w = dom.window;
    const d = w.document;
    try {
        await until(() => d.querySelector('.edit-rule-btn'), 'rule list loaded');
        const form = d.getElementById('ruleForm');
        assert.ok(form.elements.cycle_mode, 'automatic rules expose independent cycle management');
        assert.equal(form.elements.cycle_mode.value, 'rule');
        assert.equal(form.elements.cycle_max_starts.value, '1');
        assert.equal(form.elements.cycle_on_target.value, 'wait_condition');
        assert.equal(form.elements.cycle_on_bust.value, 'wait_condition');
        assert.equal(w.getComputedStyle(d.getElementById('fixedMetricField')).display, 'none', 'keeping the original play hides the fixed metric field');
        change(w, form.elements.play_filter_action, 'fixed_metric');
        assert.notEqual(w.getComputedStyle(d.getElementById('fixedMetricField')).display, 'none');
        change(w, form.elements.play_filter_action, 'keep');
        assert.equal(w.getComputedStyle(d.getElementById('fixedMetricField')).display, 'none');
        assert.deepEqual([...form.elements.cycle_mode.options].map(o => o.value), ['rule']);
        d.querySelector('.edit-rule-btn[data-id="11"]').click();
        assert.equal(form.elements.cycle_max_starts.value, '3');
        assert.equal(form.elements.cycle_on_bust.value, 'close_day');
        change(w, form.elements.cycle_max_starts, '4');
        change(w, form.elements.cycle_on_target, 'new_cycle');
        submit(w);
        await until(() => writes.length === 1 && !d.getElementById('saveRuleBtn').disabled, 'rule saved');
        assert.deepEqual(writes[0].body.cycle_policy, {mode: 'rule', max_starts_per_day: 4, max_starts_per_window: 1, on_target: 'new_cycle', on_bust: 'close_day'});
        assert.equal(form.elements.cycle_max_starts.value, '4');
        change(w, form.elements.trigger_mode, 'schedule');
        assert.equal(d.getElementById('cycleWindowQuotaField').hidden, false);
        assert.match(d.getElementById('cycleOnTargetHint').textContent, /直接续开也占时段次数/);
        change(w, form.elements.cycle_on_target, 'wait_condition');
        assert.equal(form.elements.cycle_on_target.selectedOptions[0].textContent, '等待下一个可用时段');
        change(w, form.elements.cycle_window_max_starts, '0');
        assert.match(d.getElementById('scheduleModeHint').textContent, /不限轮数/);
        for (const invalid of ['', '-1', '1.5', '1001']) {
            change(w, form.elements.cycle_window_max_starts, invalid);
            const before = writes.length;
            submit(w);
            await tick();
            assert.equal(writes.length, before, 'invalid window limit must not be submitted');
            assert.equal(form.elements.cycle_window_max_starts.getAttribute('aria-invalid'), 'true');
        }
        change(w, form.elements.cycle_window_max_starts, '1');
        change(w, form.elements.trigger_mode, 'condition');
        assert.equal(d.getElementById('cycleWindowQuotaField').hidden, true);

        for (const invalid of ['', '0', '1.5', '1001']) {
            change(w, form.elements.cycle_max_starts, invalid);
            const before = writes.length;
            submit(w);
            await tick();
            assert.equal(writes.length, before, 'invalid daily limit must not be submitted: ' + invalid);
            assert.equal(form.elements.cycle_max_starts.getAttribute('aria-invalid'), 'true');
        }
        d.querySelector('.edit-rule-btn[data-id="12"]').click();
        assert.equal(form.elements.cycle_mode.value, 'legacy');
        assert.equal(form.elements.cycle_max_starts.disabled, true);
        form.elements.name.value = '兼容规则改名';
        submit(w);
        await until(() => writes.length === 2 && !d.getElementById('saveRuleBtn').disabled, 'legacy saved');
        assert.deepEqual(writes[1].body.cycle_policy, {mode: 'legacy'});
        change(w, form.elements.cycle_mode, 'rule');
        assert.equal(form.elements.cycle_max_starts.disabled, false);
        assert.equal(form.elements.cycle_max_starts.value, '1');
        assert.match(d.getElementById('ruleList').textContent, /所选日已启动 1 \/ 3 轮/);
        assert.match(d.getElementById('ruleList').textContent, /余额 0\.00/);
        assert.match(d.getElementById('ruleList').textContent, /继承方案投递/);
        assert.equal(d.querySelector('.cycle-runtime img'), null, 'names remain plain text');
        const resume = d.querySelector('.resume-cycle-btn[data-id="11"]');
        assert.ok(resume && !resume.disabled, 'only eligible stopped scope can be restored');
        assert.match(resume.getAttribute('aria-label'), /过关方案/);
        rejectResume = true;
        resume.click();
        await until(() => d.getElementById('statusMessage').textContent.includes('待结算投注'), 'resume rejection rendered');
        assert.equal(runtime.status, 'day_closed');
        assert.equal(resume.disabled, false);
        assert.equal(resume.hasAttribute('aria-busy'), false);
        rejectResume = false;
        resume.click();
        await until(() => d.getElementById('statusMessage').textContent.includes('历史盈亏已保留'), 'resume success rendered');
        assert.deepEqual(writes.at(-1).body, {subscription_id: 18});
        assert.equal(runtime.starts_today, 1);
        assert.ok(writes.every(item => !item.path.endsWith('/run-once')), 'resume never starts a round');
        runtime.status = 'day_closed'; runtime.starts_today = 3;
        d.getElementById('refreshBtn').click();
        await until(() => d.getElementById('ruleList').textContent.includes('3 / 3'), 'quota state refresh');
        assert.equal(d.querySelector('.resume-cycle-btn[data-id="11"]').disabled, true);
        runtime.starts_today = 1;
        const scopedRule = rules.find(item => item.id === 11);
        const refresh = async () => {d.getElementById('refreshBtn').click(); await tick(); await tick();};
        for (const reason of ['cycle_target_hit', 'cycle_balance_depleted']) {
            scopedRule.daily_stat = {status: 'stopped', stopped_reason: reason, stat_date: date};
            await refresh();
            assert.equal(d.querySelector('.resume-cycle-btn[data-id="11"]').disabled, false,
                'independent policy allows scoped server validation after old cycle stop: ' + reason);
        }
        scopedRule.cycle_policy = {mode: 'legacy'};
        await refresh();
        assert.equal(d.querySelector('.resume-cycle-btn[data-id="11"]').disabled, true, 'legacy cycle day remains stopped');
        scopedRule.cycle_policy = {...policy};
        scopedRule.daily_stat.stopped_reason = 'loss_limit_hit';
        await refresh();
        assert.equal(d.querySelector('.resume-cycle-btn[data-id="11"]').disabled, true, 'daily risk cannot be bypassed');
        scopedRule.daily_stat.stopped_reason = 'cycle_balance_depleted';
        runtime.outside_scope = true;
        await refresh();
        assert.match(d.getElementById('ruleList').textContent, /已移出规则范围，仅保留运行记录/);
        assert.equal(d.querySelector('.resume-cycle-btn[data-id="11"]').disabled, true, 'removed scopes cannot resume');
        change(w, d.getElementById('statDateInput'), '2020-01-01');
        await tick(); await tick();
        assert.equal(d.querySelector('.resume-cycle-btn'), null, 'historical views cannot restore current cycles');
        assert.match(d.getElementById('ruleList').textContent, /切回今天操作/);
        windowLimitsSupported = false;
        await refresh();
        d.querySelector('.edit-rule-btn[data-id="11"]').click();
        const countBeforeOldBackend = writes.length;
        submit(w);
        await until(() => d.getElementById('statusMessage').textContent.includes('时段启动次数设置尚未在服务端生效'), 'old backend capability message');
        assert.equal(writes.length, countBeforeOldBackend, 'old backend must not silently ignore the window limit');
        console.log('PASS UI forms: defaults, editing, strict limits, legacy preservation, scoped resume, quota and history.');
    } finally { dom.window.close(); }
}

async function manualDomChecks() {
    const item = JSON.parse(JSON.stringify(subscription));
    const writes = [];
    const confirmations = [];
    const dom = new JSDOM(fs.readFileSync(path.join(ui, 'autobet.html'), 'utf8').replace('__WORKSPACE_CONTENT__', fs.readFileSync(path.join(ui, 'workspace_subscriptions.html'), 'utf8')), {url: 'http://127.0.0.1/autobet/subscriptions', runScripts: 'outside-only'});
    const w = dom.window;
    w.scrollTo = () => {};
    w.PlatformUiText = {labelStatus: value => String(value || '—'), labelSourceType: value => String(value || '—')};
    w.PlatformAuthPanel = {sync: () => {}};
    w.HTMLElement.prototype.scrollIntoView = () => {};
    w.confirm = message => {confirmations.push(message); return true;};
    w.fetch = async (url, opts = {}) => {
        let data = {items: []};
        if (url === '/api/auth/me') data = {user: {id: 1, username: 'local-test'}};
        else if (opts.method === 'POST') {writes.push({url, body: JSON.parse(opts.body)}); data = {item};}
        else if (url.startsWith('/api/platform/subscriptions')) data = {items: [item], launch_workflow: 'explicit_start_v1'};
        else if (url.startsWith('/api/platform/sources')) data = {items: [{id: 17, name: '来源', status: 'active', source_type: 'internal_ai'}]};
        return {ok: true, status: 200, json: async () => data};
    };
    try {
        w.eval(fs.readFileSync(path.join(ui, 'autobet.js'), 'utf8'));
        await until(() => w.document.querySelector('.subscription-card-item'), 'manual subscription card').catch(error => {
            throw new Error(error.message + ': ' + w.document.getElementById('statusMessage').textContent);
        });
        let panel = w.document.querySelector('.manual-cycle-state');
        assert.ok(panel, 'manual cycle panel has explicit ownership');
        assert.match(panel.textContent, /尚未手动开始/);
        item.manual_cycle_state = {stat_date: localDate(), status: 'stopped', balance: 0, cycle_stake: 0, win_streak: 0, target_streak: 3,
            cycles_today: 2, day_closed: true, stop_reason: 'balance_depleted', pending_issue: ''};
        w.document.getElementById('refreshAutobetBtn').click();
        await until(() => w.document.querySelector('.manual-cycle-state').textContent.includes('当天已重开 2 次'), 'manual own state refresh');
        panel = w.document.querySelector('.manual-cycle-state');
        assert.match(panel.textContent, /余额 0\.00/);
        assert.match(panel.textContent, /当前关数 0 \/ 3/);
        const restart = w.document.querySelector('.reset-subscription-runtime-btn');
        assert.equal(restart.textContent, '手动开始新一轮');
        restart.click();
        await until(() => writes.length === 1, 'manual restart request');
        assert.equal(writes[0].url, '/api/platform/subscriptions/18/restart');
        assert.match(confirmations[0], /自动规则的过关状态、日次数和历史盈亏保持不变/);
        assert.doesNotMatch(confirmations[0], /定时窗口可以重开/);
        assert.equal(w.document.querySelector('[name="roll_close_scope"]').type, 'hidden');
        console.log('PASS manual UI: owned state, null/zero display, manual restart scope, hidden legacy close scope.');
    } finally {dom.window.close();}
}

async function realApiChecks() {
    const child = spawn(process.env.PYTHON || 'python3', ['tests/cycle_ui_server.py'], {
        cwd: root, env: {...process.env, PYTHONPATH: path.join(root, 'src')}, stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stderr = '';
    child.stderr.on('data', chunk => {stderr += chunk;});
    let output = '';
    const info = await new Promise((resolve, reject) => {
        child.stdout.on('data', chunk => {
            output += chunk;
            if (output.includes('\n')) {
                try {resolve(JSON.parse(output.split('\n')[0]));} catch (error) {reject(error);}
            }
        });
        child.once('error', reject);
        child.once('exit', code => {if (!output.includes('\n')) reject(new Error('Fixture exited ' + code + ': ' + stderr));});
    });
    const requestLog = [];
    const http = async (url, opts = {}) => {
        const result = await fetch(new URL(url, info.url), {...opts, headers: {...(opts.headers || {}), Cookie: info.cookie}});
        if (opts.method === 'POST') requestLog.push({path: String(url), body: JSON.parse(opts.body), status: result.status});
        return result;
    };
    const dom = mountRulePage(http, info.url + '/auto-triggers');
    const w = dom.window;
    const d = w.document;
    try {
        await until(() => d.querySelector('.edit-rule-btn'), 'real API initial data');
        const form = d.getElementById('ruleForm');
        form.elements.name.value = '真实界面创建规则';
        const subOption = [...d.getElementById('subscriptionSelect').options].find(option => Number(option.value) === info.subscription_id);
        assert.ok(subOption);
        subOption.selected = true;
        change(w, form.elements.cycle_max_starts, '3');
        change(w, form.elements.cycle_on_bust, 'new_cycle');
        change(w, form.elements.trigger_mode, 'condition_schedule');
        change(w, form.elements.cycle_window_max_starts, '2');
        form.elements.schedule_allow_pre_window_signal.checked = true;
        assert.equal(d.getElementById('cycleWindowQuotaField').hidden, false);
        assert.equal(d.getElementById('scheduleFields').hidden, false);
        assert.equal(d.getElementById('conditionPanel').hidden, false);
        assert.equal(d.getElementById('guardPanel').hidden, false);
        d.querySelector('.schedule-window-end').value = '';
        d.querySelectorAll('.remove-schedule-window-btn')[1].click();
        const primary = d.querySelector('#conditionRows .condition-row');
        change(w, primary.querySelector('.condition-type'), 'hit_streak');
        change(w, primary.querySelector('.condition-draw-value'), '大');
        primary.querySelector('.condition-threshold').value = '3';
        d.getElementById('addGuardGroupBtn').click();
        const guard = d.querySelector('#guardGroupRows .condition-row');
        change(w, guard.querySelector('.condition-type'), 'draw_absence');
        change(w, guard.querySelector('.condition-metric'), 'combo');
        change(w, guard.querySelector('.condition-draw-value'), '大双');
        guard.querySelector('.condition-threshold').value = '4';
        submit(w);
        await until(() => requestLog.length > 0 && !d.getElementById('saveRuleBtn').disabled, 'real API create');
        assert.equal(requestLog[0].status, 200, d.getElementById('statusMessage').textContent);
        let payload = await (await http('/api/platform/auto-trigger-rules')).json();
        const created = payload.items.find(item => item.name === '真实界面创建规则');
        assert.equal(created.conditions[0].type, 'hit_streak');
        assert.equal(created.conditions[0].prediction_value, '大');
        assert.equal(created.guard_groups[0].conditions[0].type, 'draw_absence');
        assert.equal(created.guard_groups[0].conditions[0].bet_value, '大双');
        assert.deepEqual(created.cycle_policy, {mode: 'rule', max_starts_per_day: 3, max_starts_per_window: 2, on_target: 'wait_condition', on_bust: 'new_cycle'});
        assert.ok(Array.isArray(created.cycle_runtime));
        assert.equal(created.cycle_runtime[0].starts_today, 0);
        assert.equal(created.trigger_mode, 'condition_schedule');
        assert.equal(created.cycle_policy.max_starts_per_window, 2);
        assert.equal(created.schedule.allow_pre_window_signal, true);
        assert.ok(created.conditions.length);
        assert.equal(created.schedule.windows[0].end, '24:00');
        assert.equal(d.querySelector('.schedule-window-end').value, '', '24:00 returns as an empty end field');
        assert.match(d.getElementById('ruleList').textContent, /条件检查时段：18:00-24:00/);
        change(w, form.elements.trigger_mode, 'schedule');
        assert.equal(d.getElementById('conditionPanel').hidden, true);
        change(w, form.elements.trigger_mode, 'condition');
        assert.equal(d.getElementById('scheduleFields').hidden, true);
        change(w, form.elements.trigger_mode, 'condition_schedule');
        assert.equal(d.getElementById('scheduleFields').hidden, false);
        assert.equal(d.getElementById('conditionPanel').hidden, false);
        change(w, form.elements.cycle_max_starts, '4');
        submit(w);
        await until(() => requestLog.length > 1 && !d.getElementById('saveRuleBtn').disabled, 'real API edit');
        payload = await (await http('/api/platform/auto-trigger-rules')).json();
        assert.equal(payload.items.find(item => item.id === created.id).cycle_policy.max_starts_per_day, 4);
        assert.equal(payload.items.find(item => item.id === created.id).schedule.windows[0].end, '24:00');
        d.querySelector('.edit-rule-btn[data-id="' + info.legacy_rule_id + '"]').click();
        assert.equal(form.elements.cycle_mode.value, 'legacy');
        form.elements.name.value = '旧规则只改名';
        submit(w);
        await until(() => requestLog.length > 2 && !d.getElementById('saveRuleBtn').disabled, 'real API legacy edit');
        payload = await (await http('/api/platform/auto-trigger-rules')).json();
        assert.equal(payload.items.find(item => item.id === info.legacy_rule_id).cycle_policy.mode, 'legacy');
        const resume = d.querySelector('.resume-cycle-btn[data-id="' + info.closed_rule_id + '"]');
        assert.ok(resume && !resume.disabled, 'real stopped scope eligible for resume');
        resume.click();
        await until(() => requestLog.length > 3 && !resume.hasAttribute('aria-busy'), 'real scoped resume');
        assert.equal(requestLog.at(-1).status, 200, d.getElementById('statusMessage').textContent);
        payload = await (await http('/api/platform/auto-trigger-rules')).json();
        const restored = payload.items.find(item => item.id === info.closed_rule_id).cycle_runtime[0];
        assert.equal(restored.status, 'waiting');
        assert.equal(restored.starts_today, 1);
        const summary = await (await http('/api/platform/subscriptions/summary')).json();
        assert.equal(summary.items[0].manual_cycle_state, null, 'auto resume does not create manual state');
        const full = await (await http('/api/platform/subscriptions')).json();
        assert.equal(full.items[0].manual_cycle_state, null, 'manual workspace endpoint includes owned state');
        const jobs = await (await http('/api/platform/execution-jobs')).json();
        assert.equal(jobs.items.length, 0, 'configuration and resume must not dispatch');
        assert.ok(requestLog.every(entry => !entry.path.endsWith('/run-once')));
        console.log('PASS real WSGI + SQLite: UI create/edit/legacy save/resume, persisted quota and manual isolation, zero jobs.');
        if (process.argv.includes('--browser')) await browserChecks(info);
    } finally {
        dom.window.close();
        child.stdin.end();
        await new Promise((resolve, reject) => child.once('exit', code => code === 0 ? resolve() : reject(new Error('Fixture cleanup failed: ' + stderr))));
    }
}

async function browserChecks(info) {
    const {chromium} = loadTestDependency('playwright-core');
    const executablePath = process.env.PC28_UI_CHROMIUM || '/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome';
    assert.ok(fs.existsSync(executablePath), 'set PC28_UI_CHROMIUM to a Chromium executable for browser checks');
    const browser = await chromium.launch({executablePath, headless: true, args: ['--no-sandbox']});
    const errors = [];
    try {
        const context = await browser.newContext();
        const cookieIndex = info.cookie.indexOf('=');
        await context.addCookies([{name: info.cookie.slice(0, cookieIndex), value: info.cookie.slice(cookieIndex + 1), url: info.url}]);
        await context.route('**/*', route => new URL(route.request().url()).origin === info.url ? route.continue() : route.abort());
        const page = await context.newPage();
        page.on('pageerror', error => errors.push(error.message));
        page.on('dialog', dialog => dialog.accept());
        for (const viewport of [{width: 1440, height: 1000}, {width: 390, height: 844}]) {
            await page.setViewportSize(viewport);
            await page.goto(info.url + '/auto-triggers');
            await page.getByRole('heading', {name: '单独控制本规则启动的过关轮次'}).waitFor();
            await page.getByRole('button', {name: '编辑', exact: true}).first().waitFor();
            await page.locator('#triggerModeSelect').selectOption('condition_schedule');
            await page.locator('[name="schedule_allow_pre_window_signal"]').check();
            assert.equal(await page.locator('[name="schedule_allow_pre_window_signal"]').isChecked(), true);
            assert.ok(await page.locator('#scheduleFields').isVisible());
            assert.ok(await page.locator('#conditionPanel').isVisible());
            assert.ok(await page.locator('#guardPanel').isVisible());
            await page.locator('.schedule-window-end').first().fill('');
            await page.getByLabel('每个方案每天最多启动（轮）', {exact: true}).fill('5');
            await page.getByLabel('每个方案每个时段最多启动（轮）', {exact: true}).fill('0');
            await page.locator('#cycleOnTargetSelect').selectOption('new_cycle');
            assert.match(await page.locator('#cycleOnTargetHint').innerText(), /每个时段不限轮数/);
            await page.getByLabel('过关成功后', {exact: true}).selectOption('new_cycle');
            await page.getByText(/过关运行 ·/).first().click();
            const conditionRow = page.locator('#conditionRows .condition-row').first();
            for (const type of ['hit_streak', 'miss_streak', 'draw_streak', 'draw_absence']) {
                await conditionRow.locator('.condition-type').selectOption(type);
                await conditionRow.locator('.condition-metric').selectOption('combo');
                assert.ok(await conditionRow.locator('.condition-draw-value-field').isVisible());
                const values = await conditionRow.locator('.condition-draw-value option').evaluateAll(options => options.map(o => o.value));
                assert.equal(values.includes(''), ['hit_streak', 'miss_streak'].includes(type));
                await conditionRow.locator('.condition-draw-value').selectOption('大双');
                const size = await page.evaluate(() => ({viewport: innerWidth, width: document.documentElement.scrollWidth}));
                assert.ok(size.width <= size.viewport + 1, type + ' overflows at ' + viewport.width);
            }
            const dimensions = await page.evaluate(() => ({viewport: innerWidth, width: document.documentElement.scrollWidth}));
            assert.ok(dimensions.width <= dimensions.viewport + 1, 'rule page overflows at ' + viewport.width + ': ' + JSON.stringify(dimensions));
            const evidence = process.env.PC28_UI_EVIDENCE_DIR || path.join(root, '.boss/cycle-isolation/evidence');
            fs.mkdirSync(evidence, {recursive: true});
            await page.screenshot({path: path.join(evidence, 'rules-' + viewport.width + '.png'), fullPage: true});
            await page.goto(info.url + '/autobet/subscriptions');
            await page.getByRole('region', {name: '手动过关状态', exact: true}).waitFor();
            const manualDimensions = await page.evaluate(() => ({viewport: innerWidth, width: document.documentElement.scrollWidth}));
            assert.ok(manualDimensions.width <= manualDimensions.viewport + 1, 'manual page overflows at ' + viewport.width + ': ' + JSON.stringify(manualDimensions));
            await page.screenshot({path: path.join(evidence, 'manual-' + viewport.width + '.png'), fullPage: true});
        }
        assert.deepEqual(errors, []);
        console.log('PASS Chromium: desktop 1440 and mobile 390, keyboard-labelled fields, scoped states, no page errors or horizontal overflow.');
    } finally {await browser.close();}
}

async function streakDomChecks() {
    let rule = fixtureRule(33, {
        conditions: [{type: 'miss_streak', metric: 'big_small', operator: 'lte', threshold: 0}],
        guard_groups: [{name: '附加条件', conditions: [{type: 'hit_streak', metric: 'big_small', prediction_value: '小', operator: 'gte', threshold: 2}]}],
    });
    const writes = [];
    const dom = mountRulePage(async (requestPath, opts = {}) => {
        let data = {items: []};
        if (requestPath === '/api/auth/me') data = {user: {id: 1, username: 'local-test'}};
        else if (requestPath === '/api/platform/sources') data = {items: [{id: 17, name: '来源', status: 'active'}]};
        else if (requestPath === '/api/platform/subscriptions/summary') data = {items: [subscription], launch_workflow: 'explicit_start_v1'};
        else if (requestPath.startsWith('/api/platform/auto-trigger-rules')) {
            if (opts.method === 'POST') {
                writes.push(JSON.parse(opts.body));
                rule = {...rule, ...writes.at(-1)};
                data = {item: rule};
            } else data = {items: [rule], cycle_window_limits: true};
        } else if (requestPath.startsWith('/api/platform/auto-trigger-events')) {
            data = {items: [{status: 'triggered', latest_issue_no: '4000000', matched_conditions: [
                {type: 'hit_streak', metric: 'big_small', prediction_value: '大', operator: 'gte', threshold: 3, actual_hit_streak: 3},
                {type: 'draw_absence', metric: 'combo', bet_value: '大双', operator: 'gte', threshold: 4, actual_draw_absence: 4, streak_complete: false},
            ]}]};
        }
        return {ok: true, status: 200, json: async () => data};
    });
    const w = dom.window;
    const d = w.document;
    try {
        await until(() => d.querySelector('.edit-rule-btn'), 'streak rules loaded');
        d.querySelector('.edit-rule-btn').click();
        let row = d.querySelector('#conditionRows .condition-row');
        let guard = d.querySelector('#guardGroupRows .condition-row');
        assert.equal(row.querySelector('.condition-draw-value').value, '', 'old miss rule stays unrestricted');
        assert.equal(row.querySelector('.condition-threshold').value, '0', 'saved zero threshold is preserved');
        assert.equal(row.querySelector('.condition-value-label').textContent, '预测方向');
        assert.deepEqual([...row.querySelector('.condition-draw-value').options].map(o => o.textContent), ['不限', '大', '小']);
        assert.equal(guard.querySelector('.condition-draw-value').value, '小');
        change(w, row.querySelector('.condition-type'), 'hit_streak');
        change(w, row.querySelector('.condition-draw-value'), '大');
        row.querySelector('.condition-threshold').value = '3';
        change(w, guard.querySelector('.condition-metric'), 'combo');
        assert.equal(guard.querySelector('.condition-draw-value').value, '', 'changing metric resets invalid direction');
        change(w, guard.querySelector('.condition-type'), 'draw_absence');
        assert.equal(guard.querySelector('.condition-value-label').textContent, '开奖结果');
        assert.ok([...guard.querySelector('.condition-draw-value').options].every(o => o.value), 'draw absence requires a result');
        change(w, guard.querySelector('.condition-draw-value'), '大双');
        guard.querySelector('.condition-threshold').value = '4';
        submit(w);
        await until(() => writes.length === 1 && !d.getElementById('saveRuleBtn').disabled, 'new streak conditions saved');
        assert.equal(writes[0].conditions[0].prediction_value, '大');
        assert.equal(writes[0].conditions[0].bet_value, undefined);
        assert.equal(writes[0].guard_groups[0].conditions[0].type, 'draw_absence');
        assert.equal(writes[0].guard_groups[0].conditions[0].bet_value, '大双');
        assert.equal(writes[0].guard_groups[0].conditions[0].prediction_value, undefined);
        d.querySelector('.edit-rule-btn').click();
        row = d.querySelector('#conditionRows .condition-row');
        guard = d.querySelector('#guardGroupRows .condition-row');
        assert.equal(row.querySelector('.condition-draw-value').value, '大');
        assert.equal(guard.querySelector('.condition-draw-value').value, '大双');
        change(w, row.querySelector('.condition-type'), 'miss_streak');
        assert.equal(row.querySelector('.condition-draw-value').value, '大', 'hit/miss retain their shared direction');
        change(w, row.querySelector('.condition-metric'), 'odd_even');
        assert.equal(row.querySelector('.condition-draw-value').value, '');
        change(w, row.querySelector('.condition-draw-value'), '单');
        change(w, guard.querySelector('.condition-type'), 'draw_streak');
        assert.equal(guard.querySelector('.condition-draw-value').value, '大双', 'draw presence/absence retain their shared result');
        submit(w);
        await until(() => writes.length === 2 && !d.getElementById('saveRuleBtn').disabled, 'updated streak conditions saved');
        assert.equal(writes[1].conditions[0].prediction_value, '单');
        assert.equal(writes[1].guard_groups[0].conditions[0].type, 'draw_streak');
        assert.match(d.getElementById('eventList').textContent, /预测大.*当前连中.*当前3期/);
        assert.match(d.getElementById('eventList').textContent, /大双连续不出.*当前至少4期/);
        console.log('PASS streak UI: unrestricted legacy rules, all four conditions, direction changes, guards, save/edit round trips and event evidence.');
    } finally {dom.window.close();}
}

async function main() {
    await domChecks();
    await streakDomChecks();
    await manualDomChecks();
    if (!process.argv.includes('--dom-only')) await realApiChecks();
}
main().catch(error => {console.error(error.stack || error); process.exitCode = 1;});
