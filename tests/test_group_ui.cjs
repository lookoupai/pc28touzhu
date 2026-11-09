/* Offline DOM checks: no worker, credentials, or external API traffic. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
function dependency(name) {
    for (const base of [process.env.PC28_UI_NODE_MODULES, '/tmp/pc28-cycle-ui-tools/node_modules', '/tmp/pc28-draw-trigger-ui-check/node_modules']) {
        if (!base) continue;
        try { return require(path.join(base, name)); } catch (error) { if (error.code !== 'MODULE_NOT_FOUND') throw error; }
    }
    return require(name);
}
const {JSDOM} = dependency('jsdom');
const root = path.resolve(__dirname, '..');
const ui = path.join(root, 'src/pc28touzhu/ui');
const tick = () => new Promise(resolve => setTimeout(resolve, 20));
async function until(predicate) {
    for (let i = 0; i < 150; i++) { if (predicate()) return; await tick(); }
    throw new Error('DOM condition did not resolve');
}
async function subscriptions() {
    const html = fs.readFileSync(path.join(ui, 'autobet.html'), 'utf8')
        .replace('__WORKSPACE_CONTENT__', fs.readFileSync(path.join(ui, 'workspace_subscriptions.html'), 'utf8'))
        .replace('data-autobet-scope="workbench"', 'data-autobet-scope="subscriptions"');
    const dom = new JSDOM(html, {url: 'http://localhost/autobet/subscriptions', runScripts: 'outside-only'});
    const w = dom.window;
    w.scrollTo = () => {};
    w.confirm = () => true;
    w.PlatformAuthPanel = {sync: () => {}};
    w.eval(fs.readFileSync(path.join(ui, 'ui_text.js'), 'utf8'));
    for (const form of w.document.forms) {
        for (const field of form.elements) {
            if (field.name && !form[field.name]) Object.defineProperty(form, field.name, {get: () => form.elements.namedItem(field.name)});
        }
    }
    const writes = [];
    w.fetch = async (url, opts = {}) => {
        let data = {items: []};
        if (url === '/api/auth/me') data = {user: {id: 1, username: 'offline'}};
        if (url.startsWith('/api/platform/sources')) data = {items: [{id: 1, name: '来源', source_type: 'ai_trading_simulator_export', status: 'active'}]};
        if (url.startsWith('/api/platform/delivery-targets')) data = {items: [{id: 1, status: 'active', executor_type: 'telegram_group', target_key: '-1001', target_name: '离线群', last_test_status: 'success'}]};
        if (opts.method && opts.method !== 'GET') { writes.push({url, body: JSON.parse(opts.body)}); data = {item: {id: 1}}; }
        return {ok: true, status: 200, json: async () => data};
    };
    w.eval(fs.readFileSync(path.join(ui, 'autobet.js'), 'utf8'));
    const form = w.document.getElementById('subscriptionForm');
    await until(() => form.elements.source_id.options.length > 1);
    for (const [metric, count] of [['double_group', 6], ['kill_group', 4]]) {
        const preset = w.document.querySelector(`[data-play-filter-preset="${metric}"]`);
        assert.ok(preset, metric + ' preset');
        preset.click();
        const checked = [...form.querySelectorAll('input[name="bet_filter_key"]:checked')];
        assert.equal(checked.length, count);
        assert.ok(checked.every(input => input.value.startsWith(metric + ':')));
        assert.equal(form.elements.bet_filter_mode.value, 'selected');
    }
    form.elements.source_id.value = '1';
    form.elements.subscription_name.value = '杀组测试';
    try { await until(() => form.elements.delivery_target_ids.options.length > 0); }
    catch (error) { throw new Error(error.message + ': ' + w.document.getElementById('statusMessage').textContent); }
    form.elements.delivery_target_ids.options[0].selected = true;
    form.elements.stake_amount.value = '10';
    form.elements.stake_amount.dispatchEvent(new w.Event('input', {bubbles: true}));
    assert.match(w.document.getElementById('subscriptionGroupStakeHint').textContent, /每组合.*10.*30/);
    form.dispatchEvent(new w.Event('submit', {bubbles: true, cancelable: true}));
    try { await until(() => writes.length > 0); }
    catch (error) { throw new Error(error.message + ': ' + w.document.getElementById('statusMessage').textContent); }
    const strategy = writes[0].body.strategy_v2 || writes[0].body.strategy;
    assert.equal(strategy.play_filter.mode, 'selected');
    assert.equal(strategy.play_filter.selected_keys.length, 4);
    assert.equal(strategy.staking_policy.fixed_amount, 10);
    await until(() => !w.document.getElementById('createSubscriptionBtn').disabled);
    form.elements.bet_filter_mode.value = 'all';
    form.elements.bet_filter_mode.dispatchEvent(new w.Event('change', {bubbles: true}));
    assert.match(w.document.getElementById('subscriptionBetFilterHint').textContent, /双组.*杀组.*勾选/);
    assert.equal(w.document.getElementById('subscriptionGroupStakeHint').hidden, true);
    dom.window.close();
}

function sourcePresentation() {
    const sourceRoot = path.resolve(root, '../AITradingSimulator');
    const dom = new JSDOM('<!doctype html><html></html>', {runScripts: 'outside-only'});
    const w = dom.window;
    const code = fs.readFileSync(path.join(sourceRoot, 'static/app.js'), 'utf8').replace('const app = new PredictionApp();', 'window.TestPredictionApp = PredictionApp;');
    w.eval(code);
    const app = Object.create(w.TestPredictionApp.prototype);
    assert.equal(app.deriveDoubleGroup('大单'), '大单＋小双');
    assert.equal(app.derivePairTicket('大单'), '大单＋小双');
    const options = app.getFormProfitMetricOptions('pc28', ['combo']);
    assert.deepEqual(Array.from(options, option => option.key), ['combo', 'double_group', 'kill_group']);
    assert.equal(app.resolveFormProfitMetric('pc28', options, 'kill_group', ''), 'kill_group');
    const snapshot = app.buildPlaySnapshot(null, '大', '单', '大单');
    assert.equal(snapshot.killTicket, '大单＋大双＋小单');
    const result = app.renderProfitResult({result_type: 'miss', result_label: '亏损', prediction_hit: true, per_combo_amount: 10, refund_reason: '13/14 退本金'});
    assert.match(result, /预测命中/);
    assert.match(result, /亏损/);
    dom.window.close();
}

(async () => { await subscriptions(); sourcePresentation(); console.log('双组/杀组 DOM 与来源展示验证通过'); })().catch(error => {console.error(error); process.exitCode = 1;});
