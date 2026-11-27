/* Real local WSGI/SQLite browser flow for continuation. No production connections. */
const assert = require('node:assert/strict');
const path = require('node:path');
const {spawn} = require('node:child_process');
const {chromium} = require(path.join(process.env.PC28_UI_NODE_MODULES || '/tmp/pc28-cycle-ui-tools/node_modules', 'playwright-core'));
const root = path.resolve(__dirname, '..');

(async () => {
    const server = spawn(process.env.PYTHON || 'python3', ['-u', '-m', 'tests.rule_lab_continuation_ui_server'],
        {cwd: root, stdio: ['pipe', 'pipe', 'pipe']});
    let browser, stderr = '';
    server.stderr.on('data', data => {stderr += data;});
    try {
        const info = await new Promise((resolve, reject) => {
            let output = '';
            const timeout = setTimeout(() => reject(new Error('Fixture timeout: ' + stderr)), 20000);
            server.on('exit', code => {clearTimeout(timeout); reject(new Error('Fixture exited ' + code + ': ' + stderr));});
            server.stdout.on('data', data => {
                output += data;
                if (output.includes('\n')) {clearTimeout(timeout); resolve(JSON.parse(output.split('\n')[0]));}
            });
        });
        browser = await chromium.launch({headless: true,
            executablePath: process.env.PC28_UI_CHROMIUM || '/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',
            args: ['--no-sandbox']});
        const context = await browser.newContext({viewport: {width: 1440, height: 1000}});
        await context.addCookies([{name: info.cookie_name, value: info.cookie, url: info.url}]);
        await context.route('**/*', route => new URL(route.request().url()).origin === info.url ? route.continue() : route.abort());
        const page = await context.newPage(), errors = [], writes = [];
        page.on('pageerror', error => errors.push(String(error)));
        page.on('request', request => {
            if (request.method() === 'POST' && new URL(request.url()).pathname.startsWith('/api/platform/rule-lab/')) writes.push(request.url());
        });
        await page.goto(info.url + '/rule-lab');
        await page.waitForFunction(() => !document.getElementById('labRefreshButton').disabled);
        assert.match(await page.locator('#labResultTitle').innerText(), /原26个版本/);
        assert.match(await page.locator('#labResultPageInfo').innerText(), /共 26 个/);
        assert.equal(writes.length, 0, 'Opening results never enables capture or compute');
        assert.equal(await page.locator('#labContinuationButton').count(), 1,
            'Completed historical batch offers an incremental continuation entry');
        await page.locator('#labContinuationButton').click();
        await page.locator('#labContinuationPanel').waitFor({state: 'visible'});
        await page.locator('#labContinuationSummary').filter({hasText: '未启用'}).waitFor();
        assert.equal(await page.locator('#labContinuationCapture').isChecked(), false);
        assert.equal(await page.locator('#labContinuationCompute').isChecked(), false);
        assert.match(await page.locator('#labContinuationBasis').innerText(), /26.*1104|1104.*26/);
        assert.equal(writes.length, 0, 'Preview is read-only');
        const api = async (suffix, payload) => page.evaluate(async ({suffix, payload}) => {
            const response = await fetch(suffix, payload === undefined ? {} : {
                method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload),
            });
            const body = await response.json();
            if (!response.ok) throw new Error(JSON.stringify(body));
            return body;
        }, {suffix, payload});
        const batchPath = '/api/platform/rule-lab/batches/' + info.batch_id;
        const refresh = async () => {
            await page.locator('#labContinuationRefresh').click();
            await page.waitForFunction(() => !document.getElementById('labContinuationRefresh').disabled);
        };
        const saved = async (suffix, click) => {
            const pending = page.waitForResponse(response => response.request().method() === 'POST' && new URL(response.url()).pathname === batchPath + suffix);
            await click();
            const response = await pending;
            assert.ok([200, 201, 202].includes(response.status()), await response.text());
            const result = await response.json();
            await page.waitForFunction(() => !document.getElementById('labRefreshButton').disabled);
            return result;
        };
        await page.locator('#labContinuationCapture').check();
        assert.equal(await page.locator('#labContinuationCompute').isChecked(), false);
        const enabled = await saved('/continuation', () => page.locator('#labContinuationSave').click());
        assert.equal(enabled.continuation.capture_enabled, true);
        assert.equal(enabled.continuation.compute_enabled, false);
        assert.equal(await page.locator('#labContinuationCompute').isChecked(), false);
        assert.match(await page.locator('#labContinuationSegments').innerText(), /原回测基线/);
        const baseline = await api(batchPath + '/segments?limit=20');
        assert.equal(baseline.segments.length, 1);
        const originalSegment = JSON.stringify(baseline.segments[0]);
        const fixtureBefore = await api('/__fixture/state');
        assert.equal(fixtureBefore.experiments.length, 26);
        assert.ok(fixtureBefore.experiments.some(exp => exp.settled_count > 0), 'Baseline has actual settled returns');
        // Persisted time and switches survive closing/reopening the actual page.
        await page.locator('#labContinuationCompute').check();
        await page.locator('#labContinuationDaily').fill('03:25');
        await saved('/continuation', () => page.locator('#labContinuationSave').click());
        await page.reload();await page.waitForFunction(() => !document.getElementById('labRefreshButton').disabled);
        await page.locator('#labContinuationButton').click();
        await page.waitForFunction(() => !document.getElementById('labContinuationSave').disabled);
        assert.equal(await page.locator('#labContinuationDaily').inputValue(), '03:25');
        assert.equal(await page.locator('#labContinuationCompute').isChecked(), true);
        await page.locator('#labContinuationCompute').uncheck();
        await saved('/continuation', () => page.locator('#labContinuationSave').click());
        // A request that never reached the server can only be explicitly retried with its original ID.
        const submittedIds = [];
        await page.route('**/batches/*/continuation', async route => {
            submittedIds.push(route.request().postDataJSON().request_id);
            await route.abort();
        });
        await page.locator('#labContinuationCompute').check();
        await page.locator('#labContinuationSave').click();
        await page.locator('#labContinuationRetry').waitFor({state: 'visible'});
        assert.equal((await api(batchPath + '/continuation-preview')).continuation.compute_enabled, false);
        await page.unroute('**/batches/*/continuation');
        await page.route('**/batches/*/continuation', async route => {
            submittedIds.push(route.request().postDataJSON().request_id);
            await route.continue();
        });
        await saved('/continuation', () => page.locator('#labContinuationRetry').click());
        assert.equal(submittedIds.length, 2);
        assert.equal(submittedIds[0], submittedIds[1]);
        await page.unroute('**/batches/*/continuation');
        await page.locator('#labContinuationCompute').uncheck();
        await saved('/continuation', () => page.locator('#labContinuationSave').click());
        await api('/__fixture/append-source', {});
        await page.locator('#labContinuationEndDate').fill(info.today);
        const appended = await saved('/append', () => page.locator('#labContinuationAppend').click());
        assert.equal(appended.continuation.compute_enabled, false, 'Manual append never enables daily automatic runs');
        assert.match(await page.locator('#labContinuationFeedback').innerText(), /已保存/);
        let current;
        for (let attempt = 0; attempt < 60; attempt++) {
            await api('/__fixture/tick', {budget: .02, page_size: 1});
            current = await api(batchPath + '/continuation-preview');
            if (current.continuation.active_segment_id) break;
        }
        assert.ok(current.continuation.active_segment_id, 'Bounded worker creates the appended segment: ' + JSON.stringify({preview: current, coverage: await api(batchPath + '/input-coverage')}));
        await refresh();
        await saved('/state', () => page.locator('#labContinuationPause').click());
        assert.match(await page.locator('#labContinuationPause').innerText(), /恢复本段/);
        assert.match(await page.locator('#labContinuationPauseHelp').innerText(), /仍保留后续数据/);
        const paused = await api('/__fixture/state');
        await api('/__fixture/tick', {});
        const pausedAfter = await api('/__fixture/state');
        assert.equal(pausedAfter.batch.processed_issues, paused.batch.processed_issues, 'Paused computation remains paused');
        assert.equal(pausedAfter.batch.continuation.capture_enabled, true, 'Pausing the current segment keeps capture enabled');
        await saved('/state', () => page.locator('#labContinuationPause').click());
        for (let attempt = 0; attempt < 60; attempt++) {
            await api('/__fixture/tick', {});
            current = await api(batchPath + '/continuation-preview');
            if (!current.continuation.active_segment_id && current.continuation.calculated_through_issue === '1111') break;
        }
        assert.equal(current.continuation.calculated_through_issue, '1111');
        assert.equal(current.continuation.compute_enabled, false);
        await refresh();
        const after = await api('/__fixture/state');
        assert.deepEqual(after.experiments.map(exp => exp.id), fixtureBefore.original_ids);
        assert.equal(after.batch.end_date, info.baseline_day, 'Original baseline interval remains frozen');
        const segments = await api(batchPath + '/segments?limit=20');
        assert.equal(segments.segments.length, 2);
        assert.equal(JSON.stringify(segments.segments.find(segment => segment.kind === 'baseline')), originalSegment);
        await page.locator('#labContinuationSegments details').first().evaluate(node => {node.open = true;});
        assert.equal(await page.locator('#labContinuationSegments details').first().locator('tbody tr').count(), 26);
        assert.match(await page.locator('#labContinuationSummary').innerText(), /1111/);
        await page.locator('#labContinuationPanel').evaluate(node => node.scrollIntoView({block: 'start', behavior: 'instant'}));
        await page.screenshot({path: '/tmp/pc28-continuation-desktop.png'});
        await page.setViewportSize({width: 390, height: 844});
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'Mobile has no page-wide overflow');
        await page.locator('#labContinuationPanel').evaluate(node => node.scrollIntoView({block: 'start', behavior: 'instant'}));
        assert.ok(await page.locator('#labContinuationTitle').evaluate(node => node.getBoundingClientRect().top >= 60 && node.getBoundingClientRect().top < 300), 'Mobile screenshot shows continuation controls below the sticky header');
        await page.screenshot({path: '/tmp/pc28-continuation-mobile.png'});
        // Lost response after actual DB commit: GET receipt confirms it, never a second POST.
        let appendPosts = 0;
        await page.route('**/batches/*/append', async route => {appendPosts++;await route.fetch();await route.abort();});
        await page.locator('#labContinuationAppend').click();
        await page.locator('#labContinuationFeedback').filter({hasText: '已重新核对'}).waitFor();
        assert.equal(appendPosts, 1);
        await page.unroute('**/batches/*/append');
        await api('/__fixture/tick', {});await refresh();
        assert.equal((await api(batchPath + '/segments?limit=20')).segments.length, 2, 'Already caught up creates no empty segment');
        const monthHistory = await api('/__fixture/monthly-revisions', {});
        await page.locator('#labContinuationClose').click();
        assert.equal(await page.locator('#labContinuationPanel').isVisible(), false);
        await page.locator('#labResultsBody tr').first().getByRole('button', {name: '月度成绩', exact: true}).click();
        await page.locator('#labMonthlyBody tr').first().waitFor();
        assert.match(await page.locator('#labReportScope').innerText(), /待结算|累计统计至/);
        await page.locator('#labMonthlySnapshots').evaluate(node => {node.open = true;});
        assert.equal(await page.locator('#labMonthlySnapshotList details').count(), 2);
        const versions = page.locator('#labMonthlySnapshotList details');
        await versions.first().evaluate(node => {node.open = true;});
        await versions.nth(1).evaluate(node => {node.open = true;});
        assert.match(await versions.first().innerText(), new RegExp(monthHistory.month + '.*第 2 版'));
        assert.match(await versions.first().innerText(), /\+13\.34/);
        assert.match(await versions.nth(1).innerText(), /\+12\.34/);
        const group = await api('/__fixture/create-study', {});
        await page.locator('#labRefreshButton').click();
        await page.waitForFunction(() => !document.getElementById('labRefreshButton').disabled);
        await page.locator('#labStudyList button[data-study-id="' + group.study_id + '"]').click();
        await page.waitForFunction(() => !document.getElementById('labRefreshButton').disabled);
        await page.locator('#labStudyContinuationButton').click();
        await page.waitForFunction(() => !document.getElementById('labContinuationSave').disabled);
        assert.match(await page.locator('#labContinuationTitle').innerText(), /比较组增量续算/);
        assert.match(await page.locator('#labContinuationOwner').innerText(), /全部风控方案统一追加/);
        const postsBeforeGroup = writes.length;
        await page.locator('#labContinuationClose').click();
        await page.locator('#labStudyProgress button[data-child-batch="' + group.child_id + '"]').click();
        await page.waitForFunction(() => !document.getElementById('labRefreshButton').disabled);
        assert.equal(await page.locator('#labContinuationButton').innerText(), '比较组增量续算');
        const groupPreview = page.waitForResponse(response => new URL(response.url()).pathname === '/api/platform/rule-lab/studies/' + group.study_id + '/continuation-preview');
        await page.locator('#labContinuationButton').click();
        assert.equal((await groupPreview).status(), 200);
        await page.waitForFunction(() => !document.getElementById('labRefreshButton').disabled);
        assert.match(await page.locator('#labContinuationOwner').innerText(), /全部风控方案统一追加/);
        assert.equal(writes.length, postsBeforeGroup, 'Group and child continuation previews never enable anything');
        assert.deepEqual(errors, []);
        console.log('Continuation UI passed: read-only preview, disabled defaults, manual/auto separation, saved time, page-1 capture and real append, pause/capture, stable 26 IDs, immutable baseline, segment and monthly revision reports, lost-response receipt/original-ID retry, no empty append, study/child owner routing, desktop/mobile.');
    } catch (error) {
        console.error(stderr);
        throw error;
    } finally {
        if (browser) await browser.close();
        server.stdin.end();
    }
})().catch(error => {console.error(error); process.exitCode = 1;});
