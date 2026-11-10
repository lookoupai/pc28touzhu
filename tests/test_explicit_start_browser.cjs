/* Chromium end-to-end checks against tests/group_ui_server.py only. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {spawn} = require('node:child_process');
function dependency(name) {
    for (const base of [process.env.PC28_UI_NODE_MODULES, '/tmp/pc28-cycle-ui-tools/node_modules', '/tmp/pc28-draw-trigger-ui-check/node_modules']) {
        if (!base) continue;
        try { return require(path.join(base, name)); } catch (error) { if (error.code !== 'MODULE_NOT_FOUND') throw error; }
    }
    return require(name);
}
const {chromium} = dependency('playwright-core');
const root = path.resolve(__dirname, '..');
(async () => {
    const server = spawn(process.env.PYTHON || 'python3', ['-u', '-m', 'tests.group_ui_server'], {cwd: root, stdio: ['pipe', 'pipe', 'pipe']});
    let stderr = '';
    server.stderr.on('data', data => {stderr += data;});
    let browser;
    try {
        const info = await new Promise((resolve, reject) => {
            let output = '';
            const timeout = setTimeout(() => reject(new Error('Fixture timeout: ' + stderr)), 20000);
            server.on('exit', code => {clearTimeout(timeout); reject(new Error('Fixture exit ' + code + ': ' + stderr));});
            server.stdout.on('data', data => {output += data; if (output.includes('\n')) {clearTimeout(timeout); resolve(JSON.parse(output.split('\n')[0]));}});
        });
        browser = await chromium.launch({headless: true, executablePath: process.env.PC28_UI_CHROMIUM || '/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox']});
        const context = await browser.newContext({viewport: {width: 1440, height: 1100}});
        await context.addCookies([{name: info.cookie_name, value: info.cookie, url: info.url}]);
        await context.route('**/*', route => new URL(route.request().url()).origin === info.url ? route.continue() : route.abort());
        const page = await context.newPage();
        const errors = [];
        page.on('pageerror', error => errors.push(String(error)));

        async function saved(pathname, click) {
            const response = page.waitForResponse(r => r.request().method() === 'POST' && new URL(r.url()).pathname === pathname);
            await click();
            const result = await response;
            assert.equal(result.status(), 200, await result.text());
            const payload = await result.json();
            await page.waitForLoadState('networkidle');
            await page.waitForFunction(() => Array.from(document.querySelectorAll('#createSubscriptionBtn, #saveRuleBtn')).every(button => !button.disabled));
            return payload.item;
        }
        async function items(kind) {
            return page.evaluate(async kind => (await (await fetch('/api/platform/' + kind)).json()).items, kind);
        }
        await page.goto(info.url + '/autobet/subscriptions');
        await page.waitForLoadState('networkidle');
        await page.selectOption('#subscriptionForm [name="source_id"]', String(info.source_id));
        await page.selectOption('#subscriptionTargetSelect', String(info.target_id));
        await page.fill('#subscriptionForm [name="subscription_name"]', '保存后不启动');
        await page.selectOption('#subscriptionStrategyModeSelect', 'win_streak_roll');
        await page.selectOption('#subscriptionForm [name="roll_structure"]', 'single_play');
        await page.selectOption('#subscriptionForm [name="roll_play_type"]', 'big_small');
        await page.locator('#toggleSubscriptionAdvancedBtn').click();
        await page.selectOption('#subscriptionSettlementRuleSourceSelect', 'subscription_fixed');
        await page.selectOption('#subscriptionForm [name="settlement_rule_id"]', 'pc28_fullpay_2_8_regular');
        const sub = await saved('/api/platform/subscriptions', () => page.locator('#createSubscriptionBtn').click());
        assert.equal(sub.status, 'inactive');
        assert.equal(sub.launch_mode, 'unstarted');
        await page.locator('#subscriptionSavedActions').waitFor({state:'visible'});
        assert.equal(await page.locator('#subscriptionSavedActions').isVisible(), true);
        assert.match(await page.locator('#subscriptionSavedActions').innerText(), /尚未启动/);
        const createdCard = page.locator('.subscription-card-item').filter({has: page.locator('.edit-subscription-btn[data-subscription-id="'+sub.id+'"]')});
        assert.match(await createdCard.innerText(), /未启动/);
        await page.locator('#subscriptionSavedActions a').click();
        await page.waitForLoadState('networkidle');
        assert.deepEqual(await page.locator('#subscriptionSelect').evaluate(el => Array.from(el.selectedOptions).map(o=>o.value)), [String(sub.id)]);
        await page.fill('#ruleForm [name="name"]', '显式启动规则');
        const rule = await saved('/api/platform/auto-trigger-rules', () => page.locator('#saveRuleBtn').click());
        assert.equal(rule.status, 'inactive');
        assert.match(await page.locator('#ruleLaunchPreview').innerText(), /彩28满赔2.8.*继承方案/);
        assert.equal(await page.locator('#enableSavedRuleBtn').isVisible(), true);
        assert.equal((await items('subscriptions')).find(s=>s.id===sub.id).status, 'inactive');
        await page.fill('#ruleForm [name="name"]', '显式启动规则已修改');
        assert.equal(await page.locator('#enableSavedRuleBtn').isDisabled(), true);
        const edited = await saved('/api/platform/auto-trigger-rules/'+rule.id, () => page.locator('#saveRuleBtn').click());
        assert.equal(edited.status, 'inactive');
        await saved('/api/platform/auto-trigger-rules/'+rule.id+'/status', () => page.locator('#enableSavedRuleBtn').click());
        let current = (await items('subscriptions')).find(s=>s.id===sub.id);
        assert.equal(current.status, 'standby');
        assert.equal(current.launch_mode, 'automatic');
        await page.locator('#enableSavedRuleBtn').waitFor({state:'hidden'});
        assert.equal(await page.locator('#enableSavedRuleBtn').isVisible(), false);
        // Route overrides must be clearly identified in the effective settlement summary.
        await page.locator('#addRouteBtn').click();
        await page.locator('.route-target').selectOption(String(info.target_id));
        await page.locator('.route-settlement-mode').selectOption('override');
        await page.locator('.route-settlement-rule').selectOption('pc28_fullpay_2_0_regular');
        const overridden = await saved('/api/platform/auto-trigger-rules/'+rule.id, () => page.locator('#saveRuleBtn').click());
        assert.equal(overridden.status, 'active');
        assert.match(await page.locator('#ruleLaunchPreview').innerText(), /彩28满赔2.0.*路由覆盖/);
        await page.locator('.copy-rule-btn[data-id="'+rule.id+'"]').click();
        const duplicateRule = await saved('/api/platform/auto-trigger-rules', () => page.locator('#saveRuleBtn').click());
        assert.equal(duplicateRule.status, 'inactive');
        assert.notEqual(duplicateRule.id, rule.id);
        assert.equal((await items('auto-trigger-rules')).find(r=>r.id===rule.id).status, 'active');
        await page.goto(info.url + '/autobet/subscriptions');
        await page.waitForLoadState('networkidle');
        const autoCard = page.locator('.subscription-card-item').filter({has: page.locator('.edit-subscription-btn[data-subscription-id="'+sub.id+'"]')});
        assert.match(await autoCard.innerText(), /自动规则.*等待时间窗口／条件|等待时间窗口／条件.*自动规则/s);
        assert.equal(await autoCard.getByRole('button', {name:'开始直接跟单', exact:true}).count(), 0);
        assert.match(await autoCard.innerText(), /彩28满赔2.0.*路由覆盖/);
        const copy = await saved('/api/platform/subscriptions', () => autoCard.locator('.copy-subscription-btn').click());
        assert.equal(copy.status, 'inactive');
        assert.equal(copy.launch_mode, 'unstarted');
        await saved('/api/platform/subscriptions/'+copy.id+'/status', () => page.locator('#subscriptionSavedActions .saved-subscription-start').click());
        current = (await items('subscriptions')).find(s=>s.id===copy.id);
        assert.equal(current.status, 'active');
        assert.equal(current.launch_mode, 'direct');
        await page.locator('#subscriptionSavedActions').waitFor({state:'hidden'});
        assert.equal(await page.locator('#subscriptionSavedActions').isVisible(), false);
        await page.locator('.edit-subscription-btn[data-subscription-id="'+copy.id+'"]').click();
        await page.fill('#subscriptionForm [name="subscription_name"]', '直接方案编辑仍运行');
        const savedDirect = await saved('/api/platform/subscriptions/'+copy.id, () => page.locator('#createSubscriptionBtn').click());
        assert.equal(savedDirect.status, 'active');
        assert.equal(savedDirect.launch_mode, 'direct');
        assert.equal((await items('execution-jobs')).length, 1, 'configuration/start changes should create no jobs without new signals');
        await page.setViewportSize({width:390,height:844});
        await page.reload();
        await page.waitForLoadState('networkidle');
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'subscription mobile overflow');
        await page.screenshot({path:'/tmp/explicit-start-subscriptions-mobile.png',fullPage:true});
        await page.screenshot({path:'/tmp/explicit-start-subscriptions-preview.png'});
        await page.goto(info.url + '/auto-triggers');
        await page.waitForLoadState('networkidle');
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'rules mobile overflow');
        await page.screenshot({path:'/tmp/explicit-start-rules-mobile.png',fullPage:true});
        await page.screenshot({path:'/tmp/explicit-start-rules-preview.png'});

        // An old running backend must never interpret the new Save button as Start.
        await page.route('**/api/platform/subscriptions**', async route => {
            if (route.request().method() !== 'GET') return route.continue();
            const response = await route.fetch();
            const data = await response.json();
            delete data.launch_workflow;
            await route.fulfill({response, json:data});
        });
        let unexpectedWrites = 0;
        page.on('request', request => {
            if (request.method() === 'POST' && /\/api\/platform\/(subscriptions|auto-trigger-rules)/.test(request.url())) unexpectedWrites++;
        });
        await page.goto(info.url + '/autobet/subscriptions');
        await page.waitForLoadState('networkidle');
        await page.locator('.copy-subscription-btn[data-subscription-id="'+sub.id+'"]').click();
        await page.getByText('保存与启动分离尚未在服务端生效，请重启跟单服务后刷新页面。', {exact:true}).waitFor();
        await page.goto(info.url + '/auto-triggers');
        await page.waitForLoadState('networkidle');
        await page.locator('.copy-rule-btn[data-id="'+rule.id+'"]').click();
        await page.locator('#saveRuleBtn').click();
        await page.getByText('保存与启动分离尚未在服务端生效，请重启跟单服务后刷新页面。', {exact:true}).waitFor();
        assert.equal(unexpectedWrites, 0);
        assert.deepEqual(errors, []);
        console.log('Explicit start browser flow passed: draft/edit/enable/copy/direct start, odds inheritance and override, mobile layouts; no new jobs.');
    } finally {
        if (browser) await browser.close();
        server.stdin.end();
    }
})().catch(error => {console.error(error); process.exitCode = 1;});
