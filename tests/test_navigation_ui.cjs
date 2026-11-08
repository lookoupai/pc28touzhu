/* Local Chromium checks; uses a temporary SQLite fixture and blocks external requests. */
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
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
const evidence = process.env.PC28_UI_EVIDENCE_DIR || '/tmp/pc28-navigation-evidence';
const userPaths = ['/', '/autobet/subscriptions', '/auto-triggers', '/autobet/sources', '/autobet/accounts', '/autobet/targets', '/autobet/templates', '/autobet/bot', '/records', '/alerts'];
const adminPaths = ['/admin', '/admin/sources', '/admin/signals', '/admin/execution', '/admin/alerts', '/admin/telegram', '/admin/support'];
async function fixture() {
    const server = spawn(process.env.PYTHON || 'python3', ['-u', '-m', 'tests.navigation_ui_server'], {cwd: root, stdio: ['pipe', 'pipe', 'pipe']});
    let stderr = '';
    server.stderr.on('data', data => { stderr += data; });
    const info = await new Promise((resolve, reject) => {
        let output = '';
        const timeout = setTimeout(() => reject(new Error('Fixture timeout: ' + stderr)), 20000);
        server.on('exit', code => { clearTimeout(timeout); reject(new Error('Fixture exited ' + code + ': ' + stderr)); });
        server.stdout.on('data', data => { output += data; if (output.includes('\n')) { clearTimeout(timeout); resolve(JSON.parse(output.split('\n')[0])); } });
    });
    return {server, info, errors: () => stderr};
}
(async function () {
    fs.mkdirSync(evidence, {recursive: true});
    const {server, info, errors: serverErrors} = await fixture();
    const browser = await chromium.launch({headless: true, executablePath: process.env.PC28_UI_CHROMIUM || '/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox']});
    const errors = [];
    async function context(role) {
        const ctx = await browser.newContext();
        if (role) await ctx.addCookies([{name: info.cookie_name, value: info.cookies[role], url: info.url}]);
        await ctx.route('**/*', route => new URL(route.request().url()).origin === info.url ? route.continue() : route.abort());
        return ctx;
    }
    async function loaded(page, pathname) {
        const response = await page.goto(info.url + pathname);
        assert.equal(response.status(), 200, pathname);
        await page.waitForLoadState('networkidle');
        const route = new URL(page.url()).pathname;
        assert.equal(await page.evaluate(() => typeof window.PlatformNavigation), 'object', pathname + ': shared behavior loaded');
        const selected = page.locator('.app-navigation [aria-current="page"]');
        assert.equal(await selected.count(), 1, pathname + ': selected item');
        assert.equal(await selected.getAttribute('href'), route);
        const dimensions = await page.evaluate(() => ({width: document.documentElement.scrollWidth, viewport: innerWidth}));
        assert.ok(dimensions.width <= dimensions.viewport + 1, pathname + ': overflow ' + JSON.stringify(dimensions));
    }
    try {
        const user = await context('user');
        const page = await user.newPage();
        page.on('pageerror', error => errors.push(error.message));
        for (const width of [1440, 390]) {
            await page.setViewportSize({width, height: 900});
            for (const pathname of userPaths) {
                await loaded(page, pathname);
                assert.equal(await page.locator('.app-nav-group[data-admin-only]').isVisible(), false);
                assert.equal(await page.locator('.status-banner.is-error, .status-message.is-error').count(), 0, pathname + ': page load should succeed');
                const selectors = {'/autobet/sources': '#sourceList .config-list-item', '/autobet/accounts': '#accountList .config-list-item', '/autobet/targets': '#targetList .config-list-item', '/autobet/templates': '#messageTemplateList .config-list-item', '/autobet/subscriptions': '#subscriptionList .config-list-item'};
                if (selectors[pathname]) assert.ok(await page.locator(selectors[pathname]).count(), pathname + ': configuration data loaded');
                if (pathname === '/') {
                    assert.equal(await page.locator('#workbenchSubscriptions').innerText(), '1');
                    assert.match(await page.locator('#workbenchChains').innerText(), /等待触发/);
                    assert.equal(await page.locator('#setupGuide').getAttribute('open'), null);
                }
                if (['/', '/auto-triggers', '/autobet/subscriptions'].includes(pathname)) {
                    await page.screenshot({path: path.join(evidence, (pathname === '/' ? 'workbench' : pathname.split('/').pop()) + '-' + width + '.png'), fullPage: true});
                }
            }
            if (width === 390) {
                await page.locator('#navigationToggle').click();
                assert.equal(await page.locator('#navigationToggle').getAttribute('aria-expanded'), 'true');
                assert.equal(await page.locator('#appSidebar').getAttribute('aria-modal'), 'true');
                assert.equal(await page.locator('#mainContent').evaluate(el => el.inert), true);
                await page.keyboard.press('Escape');
                assert.equal(await page.locator('#navigationToggle').getAttribute('aria-expanded'), 'false');
                assert.equal(await page.evaluate(() => document.activeElement.id), 'navigationToggle');
                await page.locator('#navigationToggle').click();
                await page.locator('.app-navigation a[href="/auto-triggers"]').click();
                await page.waitForURL(info.url + '/auto-triggers');
                assert.equal(await page.locator('#navigationToggle').getAttribute('aria-expanded'), 'false');
            }
        }
        console.log('PASS 20 user page/viewport checks, shared navigation, responsive layout and keyboard drawer.');
        await page.setViewportSize({width: 1440, height: 1000});
        for (const [old, expected] of [
            ['/autobet', '/'], ['/autobet#botBindingSection', '/autobet/bot'],
            ['/#importSection', '/autobet/sources#sourceForm'], ['/autobet#accountsSection', '/autobet/accounts'],
            ['/autobet?from=bookmark#autobetQuickstartSection', '/?from=bookmark#setupGuide'],
            ['/records/?status=failed', '/records?status=failed'],
        ]) {
            await page.goto(info.url + old);
            await page.waitForURL(info.url + expected);
            await page.waitForLoadState('networkidle');
        }
        await loaded(page, '/records?status=failed&signal_id=42#recordsTableSection');
        assert.equal(await page.locator('#recordsFilterForm [name=status]').inputValue(), 'failed');
        assert.equal(await page.locator('#recordsFilterForm [name=signal_id]').inputValue(), '42');
        assert.match(page.url(), /#recordsTableSection$/);
        await page.reload();
        await page.waitForLoadState('networkidle');
        assert.equal(await page.locator('#recordsFilterForm [name=status]').inputValue(), 'failed');
        for (const [pathname, selector] of [['/autobet/sources', '#sourceNameInput'], ['/autobet/accounts', '#accountForm [name=label]'], ['/autobet/targets', '#targetForm [name=target_name]'], ['/autobet/templates', '#messageTemplateForm [name=name]'], ['/autobet/subscriptions', '#subscriptionForm [name=subscription_name]']]) {
            await loaded(page, pathname);
            await page.locator(selector).fill('未保存修改');
            const draftForm = await page.locator(selector).evaluate(el => el.form.id);
            assert.equal(await page.evaluate(id => window.PlatformNavigation.isDirty(document.getElementById(id)), draftForm), true, pathname);
            page.once('dialog', dialog => dialog.dismiss());
            await page.locator('.app-navigation a[href="/records"]').click();
            assert.equal(new URL(page.url()).pathname, pathname);
            await page.evaluate(id => window.PlatformNavigation.markClean(document.getElementById(id)), draftForm);
        }
        await loaded(page, '/autobet/sources');
        await page.locator('.source-subscription-link').first().click();
        await page.waitForURL('**/autobet/subscriptions?subscription_id=*');
        await page.locator('.subscription-detail-panel').waitFor();
        assert.equal(new URL(page.url()).searchParams.get('subscription_id'), String(info.subscription_id));
        // Exercise the isolated binding UI without calling external services or writing the fixture.
        await loaded(page, '/autobet/bot');
        await page.route('**/api/platform/telegram-binding/token', route => route.fulfill({status: 200, contentType: 'application/json', body: JSON.stringify({item: {is_bound: false, bind_token: 'LOCAL123', bind_token_expire_at: new Date(Date.now() + 60000).toISOString()}})}));
        await page.locator('#generateBindCodeBtn').click();
        await page.waitForFunction(() => document.getElementById('botBindingCommand').textContent === '/bind LOCAL123');
        assert.equal(await page.locator('#copyBindCommandBtn').isEnabled(), true);
        await page.route('**/api/platform/telegram-binding/unbind', route => route.fulfill({status: 200, contentType: 'application/json', body: '{"item":{"is_bound":false}}'}));
        page.once('dialog', dialog => dialog.accept());
        await page.locator('#clearTelegramBindingBtn').click();
        await page.waitForFunction(() => document.getElementById('botBindingCommand').textContent === '/bind <绑定码>');
        assert.equal(await page.locator('#copyBindCommandBtn').isDisabled(), true);
        console.log('PASS isolated configuration forms and Bot binding generate/clear behavior.');
        await loaded(page, '/autobet/subscriptions?subscription_id=' + info.subscription_id);
        await page.locator('.subscription-detail-panel').waitFor();
        await page.locator('a[href="/auto-triggers?subscription_id=' + info.subscription_id + '"]').first().click();
        await page.waitForLoadState('networkidle');
        assert.equal(await page.locator('#ruleSubscriptionFilter').inputValue(), String(info.subscription_id));
        await loaded(page, '/auto-triggers?rule_id=' + info.rule_id + '&stat_date=2026-11-07');
        assert.equal(await page.locator('#ruleForm [name=rule_id]').inputValue(), String(info.rule_id));
        assert.equal(await page.locator('#statDateInput').inputValue(), '2026-11-07');
        await page.locator('#ruleForm [name=name]').fill('尚未保存的编辑');
        assert.deepEqual(errors, [], 'page scripts loaded successfully');
        assert.equal(await page.evaluate(() => window.PlatformNavigation.isDirty(document.getElementById('ruleForm'))), true, 'editor detects a changed draft');
        let prompts = 0;
        const decline = async dialog => { prompts++; await dialog.dismiss(); };
        page.on('dialog', decline);
        await page.locator('.app-navigation a[href="/records"]').click();
        assert.equal(prompts, 1);
        assert.equal(new URL(page.url()).pathname, '/auto-triggers');
        assert.equal(await page.locator('#ruleForm [name=name]').inputValue(), '尚未保存的编辑');
        await page.locator('#refreshBtn').click();
        await page.waitForLoadState('networkidle');
        assert.equal(await page.locator('#ruleForm [name=name]').inputValue(), '尚未保存的编辑', 'refresh preserves editor draft');
        page.off('dialog', decline);
        page.once('dialog', dialog => dialog.accept());
        await page.locator('.app-navigation a[href="/records"]').click();
        await page.waitForURL(info.url + '/records');
        await page.goBack();
        await page.waitForLoadState('networkidle');
        assert.match(page.url(), /rule_id=/);
        console.log('PASS old links, canonical paths, query/hash persistence, browser back and unsaved-change guard.');
        await page.goto(info.url + '/');
        await page.waitForLoadState('networkidle');
        await page.route('**/api/platform/telegram-accounts', route => route.fulfill({status: 503, contentType: 'application/json', body: '{"error":"fixture unavailable"}'}));
        await page.locator('#refreshHomeBtn').click();
        await page.waitForFunction(() => document.getElementById('workbenchAttention').textContent.includes('加载失败'));
        assert.equal(await page.locator('#batchPauseBtn').isDisabled(), true);
        assert.equal(await page.locator('#workbenchSubscriptions').innerText(), '1');
        await page.unroute('**/api/platform/telegram-accounts');
        await page.reload(); await page.waitForLoadState('networkidle');
        await page.locator('#batchOperations summary').click();
        page.once('dialog', dialog => dialog.accept());
        await page.locator('#batchResumeBtn').click();
        await page.waitForFunction(() => document.getElementById('batchResult').textContent.includes('失败'));
        assert.match(await page.locator('#batchResult').innerText(), /已启用 0 项/);
        console.log('PASS partial loading failures and honest partial batch-operation feedback.');

        const admin = await context('admin');
        const adminPage = await admin.newPage();
        adminPage.on('pageerror', error => errors.push(error.message));
        for (const width of [1440, 390]) {
            await adminPage.setViewportSize({width, height: 900});
            for (const pathname of adminPaths) await loaded(adminPage, pathname);
        }
        await adminPage.goto(info.url + '/admin#executionJobsSection');
        await adminPage.waitForURL(info.url + '/admin/execution');
        await adminPage.waitForLoadState('networkidle');
        assert.equal(await adminPage.locator('.app-nav-group[data-admin-only]').getAttribute('hidden'), null);
        const forbidden = await page.goto(info.url + '/admin');
        assert.equal(forbidden.status(), 403);
        const anon = await context(null);
        const anonPage = await anon.newPage();
        anonPage.on('pageerror', error => errors.push(error.message));
        await loaded(anonPage, '/');
        assert.equal(await anonPage.locator('#workbenchGuest').isVisible(), true);
        assert.equal(await anonPage.locator('#workbenchContent').isVisible(), false);
        await loaded(anonPage, '/autobet/accounts');
        await anonPage.locator('#accountMenuBtn').click();
        await anonPage.locator('#authUsername').fill('navigation-user');
        await anonPage.locator('#authPassword').fill('local-pass');
        await anonPage.locator('#loginBtn').click();
        await anonPage.waitForFunction(() => document.documentElement.getAttribute('data-authenticated') === 'true');
        assert.equal(new URL(anonPage.url()).pathname, '/autobet/accounts');
        // Direct admin links return to the exact target after an administrator signs in.
        const direct = await context(null);
        const directPage = await direct.newPage();
        directPage.on('pageerror', error => errors.push(error.message));
        const denied = await directPage.goto(info.url + '/admin/execution');
        assert.equal(denied.status(), 401);
        await directPage.locator('[data-account-open]').click();
        await directPage.locator('#authUsername').fill('navigation-admin');
        await directPage.locator('#authPassword').fill('local-pass');
        await directPage.locator('#loginBtn').click();
        await directPage.locator('#adminContent').waitFor();
        assert.equal(new URL(directPage.url()).pathname, '/admin/execution');
        await directPage.waitForLoadState('networkidle');
        assert.deepEqual(errors, []);
        assert.equal(serverErrors(), '');
        console.log('PASS 14 admin page/viewport checks, role visibility, server authorization and login in place.');
    } finally {
        await browser.close();
        const exited = new Promise(resolve => server.once('exit', resolve));
        server.stdin.end();
        await exited;
    }
})().catch(error => { console.error(error); process.exitCode = 1; });
