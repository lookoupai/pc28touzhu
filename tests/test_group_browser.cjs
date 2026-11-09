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
        await page.goto(info.url + '/autobet/subscriptions');
        await page.waitForLoadState('networkidle');
        await page.locator('[data-play-filter-preset="kill_group"]').click();
        assert.equal(await page.locator('input[name="bet_filter_key"]:checked').count(), 4);
        await page.selectOption('#subscriptionForm [name="source_id"]', String(info.source_id));
        await page.selectOption('#subscriptionTargetSelect', String(info.target_id));
        await page.fill('#subscriptionForm [name="subscription_name"]', '浏览器创建杀组');
        await page.fill('#subscriptionForm [name="stake_amount"]', '12');
        assert.match(await page.locator('#subscriptionGroupStakeHint').innerText(), /每组合 12.*36/);
        const savedResponse = page.waitForResponse(response => response.request().method() === 'POST' && new URL(response.url()).pathname === '/api/platform/subscriptions');
        await page.locator('#createSubscriptionBtn').click();
        const response = await savedResponse;
        assert.equal(response.status(), 200, await response.text());
        const created = (await response.json()).item;
        assert.equal(created.strategy_v2.staking_policy.fixed_amount, 12);
        assert.equal(created.strategy_v2.play_filter.selected_keys.length, 4);
        await page.waitForLoadState('networkidle');
        await page.reload();
        await page.waitForLoadState('networkidle');
        const stored = await page.evaluate(async () => (await (await fetch('/api/platform/subscriptions')).json()).items);
        const selected = stored.find(item => item.id === created.id);
        assert.equal(selected.strategy_v2.play_filter.mode, 'selected');
        assert.ok(selected.strategy_v2.play_filter.selected_keys.every(key => key.startsWith('kill_group:')));
        await page.locator('[data-play-filter-preset="double_group"]').click();
        assert.equal(await page.locator('input[name="bet_filter_key"]:checked').count(), 6);
        await page.selectOption('#subscriptionBetFilterModeSelect', 'all');
        assert.match(await page.locator('#subscriptionBetFilterHint').innerText(), /双组.*杀组.*勾选/);
        await page.goto(info.url + '/auto-triggers');
        await page.waitForLoadState('networkidle');
        await page.locator('.condition-metric').first().selectOption('kill_group');
        await page.locator('.condition-type').first().selectOption('hit_streak');
        assert.equal(await page.locator('.condition-draw-value').first().locator('option').count(), 5);
        assert.match(await page.locator('.condition-draw-value').first().innerText(), /杀小双/);
        await page.goto(info.url + '/autobet/templates');
        await page.waitForLoadState('networkidle');
        await page.locator('[data-template-sample="kill_group"]').click();
        assert.match(await page.locator('#templatePreviewOutput').innerText(), /大单10.*大双10.*小单10/);
        await page.goto(info.url + '/records');
        await page.waitForLoadState('networkidle');
        assert.match(await page.locator('#recordsTableBody').innerText(), /每组合 10.00.*合计 30.00/);
        assert.match(await page.locator('#recordsTableBody').innerText(), /预测命中.*-20.00/);
        assert.match(await page.locator('#recordsTableBody').innerText(), /小单 退本/);
        const evidence = process.env.PC28_UI_EVIDENCE_DIR || path.join(root, '.boss/double-kill-group/evidence');
        fs.mkdirSync(evidence, {recursive: true});
        await page.screenshot({path: path.join(evidence, 'group-records-desktop.png'), fullPage: true});
        await page.setViewportSize({width: 390, height: 844});
        await page.goto(info.url + '/autobet/subscriptions');
        await page.waitForLoadState('networkidle');
        await page.locator('[data-play-filter-preset="kill_group"]').click();
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'mobile layout should not overflow');
        await page.screenshot({path: path.join(evidence, 'group-subscriptions-mobile.png'), fullPage: true});
        assert.deepEqual(errors, []);
        console.log('双组/杀组 Chromium 创建、回读、规则、模板、资金记录与移动布局验证通过');
    } finally {
        if (browser) await browser.close();
        server.stdin.end();
    }
})().catch(error => {console.error(error); process.exitCode = 1;});
