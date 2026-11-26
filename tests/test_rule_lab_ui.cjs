/* Real WSGI + local temporary databases; never reaches production or external hosts. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {spawn} = require('node:child_process');
const {chromium} = require(path.join(process.env.PC28_UI_NODE_MODULES || '/tmp/pc28-cycle-ui-tools/node_modules', 'playwright-core'));
const root = path.resolve(__dirname, '..');

(async () => {
    const server = spawn(process.env.PYTHON || 'python3', ['-u', '-m', 'tests.rule_lab_ui_server'], {cwd:root, stdio:['pipe','pipe','pipe']});
    let stderr = '', browser;
    server.stderr.on('data', data => {stderr += data;});
    try {
        const info = await new Promise((resolve, reject) => {
            let output = '';
            const timeout = setTimeout(() => reject(new Error('Fixture timeout: ' + stderr)), 20000);
            server.on('exit', code => {clearTimeout(timeout); reject(new Error('Fixture exited '+code+': '+stderr));});
            server.stdout.on('data', data => {output += data; if(output.includes('\n')) {clearTimeout(timeout); resolve(JSON.parse(output.split('\n')[0]));}});
        });
        browser = await chromium.launch({headless:true, executablePath:process.env.PC28_UI_CHROMIUM || '/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome', args:['--no-sandbox']});
        const context = await browser.newContext({viewport:{width:1440,height:1000}, acceptDownloads:true});
        await context.addCookies([{name:info.cookie_name, value:info.cookie, url:info.url}]);
        await context.route('**/*', route => new URL(route.request().url()).origin === info.url ? route.continue() : route.abort());
        const page = await context.newPage();
        const errors = [];
        page.on('pageerror', error => errors.push(String(error)));
        const response = await page.goto(info.url+'/rule-lab');
        assert.equal(response.status(),200,'administrator can open the rule lab page');
        await page.getByRole('heading',{name:'规则实验室',exact:true}).waitFor();
        await page.locator('#labPredictor option[value="1"]').waitFor({state:'attached'});
        await page.getByText('还没有实验批次。先选择来源与玩法生成候选。',{exact:true}).waitFor();
        assert.equal(await page.locator('#labGenerator').getAttribute('open'),'');
        assert.match(await page.locator('#labCandidatePreview').innerText(),/50/);
        await page.getByLabel('投注玩法',{exact:true}).selectOption('combo');
        assert.match(await page.locator('#labCandidatePreview').innerText(),/78/);
        await page.getByLabel('投注玩法',{exact:true}).selectOption('odd_even');
        await page.locator('#labSettlementDetails').evaluate(el=>el.open=true);
        assert.match(await page.locator('#labSettlementSummary').innerText(),/2\.846/);
        assert.doesNotMatch(await page.locator('#labSettlementSummary').innerText(),/彩28/);
        assert.equal(await page.locator('#labSettlementSummary img').count(),0);
        await page.getByLabel('预测来源',{exact:true}).selectOption('2');
        assert.equal(await page.locator('#labPredictor img').count(),0);
        assert.equal(await page.evaluate(()=>window.labXss),undefined);
        await page.getByLabel('预测来源',{exact:true}).selectOption('1');

        let creates = 0;
        page.on('request', request => {if(request.method()==='POST' && new URL(request.url()).pathname==='/api/platform/rule-lab/batches')creates++;});
        async function saved(pathname, click) {
            const pending=page.waitForResponse(r=>r.request().method()==='POST' && new URL(r.url()).pathname===pathname);
            await click();
            const r=await pending;
            assert.ok([200,201].includes(r.status()),await r.text());
            return r.json();
        }
        const malicious='<img src=x onerror="window.labXss=2"> 测试批次';
        await page.getByLabel('批次名称',{exact:true}).fill(malicious);
        assert.equal(await page.locator('#labRiskMode').inputValue(),'round');
        await page.getByRole('button',{name:'生成并保存',exact:true}).click();
        await page.locator('#labFormFeedback').filter({hasText:'尚未设置退出条件'}).waitFor();
        assert.equal(creates,0);
        await page.locator('#labRiskMode').selectOption('legacy');
        const created = await saved('/api/platform/rule-lab/batches',()=>page.getByRole('button',{name:'生成并保存',exact:true}).click());
        assert.equal(created.generated_count,50);
        assert.equal(created.batch.status,'paused');
        const id=created.batch.id;
        await page.locator('#labResultTitle').filter({hasText:malicious}).waitFor();
        await page.locator('#labResultsBody tr').first().waitFor();
        assert.equal(await page.locator('#labResultsBody tr').count(),20);
        assert.match(await page.locator('#labResultPageInfo').innerText(),/共 50/);
        await page.locator('#labResultNext').click();
        assert.match(await page.locator('#labResultPageInfo').innerText(),/21–40/);
        await page.locator('#labResultNext').click();
        assert.equal(await page.locator('#labResultsBody tr').count(),10);
        assert.equal(await page.locator('#labResultNext').isDisabled(),true);
        await page.locator('#labResultPrevious').click();
        await page.locator('#labResultPrevious').click();
        assert.equal(await page.locator('#labResultPrevious').isDisabled(),true);
        assert.equal(await page.locator('#labBatchList img').count(),0);
        assert.equal(await page.evaluate(()=>window.labXss),undefined);
        assert.equal(creates,1);
        assert.match(await page.locator('#labBatchMeta').innerText(),/已暂停/);
        assert.match(await page.locator('#labResultEmpty').innerText(),/尚无已结算/);
        await page.locator('#labEditRiskButton').click();
        await page.locator('#labEditRiskPreset').selectOption('6:9:7:odd_even');
        assert.equal(await page.locator('#labEditRiskRoundProfit').inputValue(),'10');
        assert.equal(await page.locator('#labEditRiskDayProfit').inputValue(),'20');
        assert.equal(await page.locator('#labEditRiskThreshold').inputValue(),'25');
        assert.equal(await page.locator('#labEditRiskCooldown').inputValue(),'10');
        await page.locator('#labEditRiskRoundMax').fill('3');
        const riskSaved=await saved('/api/platform/rule-lab/batches/'+id+'/risk-policy',()=>page.locator('#labSaveRiskButton').click());
        assert.equal(riskSaved.batch.predictor_id,created.batch.predictor_id);
        assert.equal(riskSaved.batch.stake_amount,1);
        assert.equal(riskSaved.batch.risk_policy.source.rule_id,6);
        assert.equal(riskSaved.batch.risk_policy.source.modified,true);
        await page.locator('#labBatchRiskSummary').filter({hasText:'最多下注 3'}).waitFor();
        let changed = await saved('/api/platform/rule-lab/batches/'+id+'/state',()=>page.locator('#labBatchStateButton').click());
        assert.equal(changed.batch.status,'running');
        await page.waitForFunction(()=>document.getElementById('labBatchStateButton').textContent==='暂停模拟');
        assert.equal(await page.locator('#labEditRiskButton').isVisible(),false);
        const seeded=await page.evaluate(async id=>(await fetch('/__fixture/settle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({batch_id:id})})).status,id);
        assert.equal(seeded,200);
        await page.locator('#labRefreshButton').click();
        await page.waitForFunction(()=>document.querySelector('#labResultsBody tr').textContent.includes('+0.96'));
        const first=page.locator('#labResultsBody tr').first();
        assert.match(await first.innerText(),/0\.96/);
        assert.match(await page.locator('#labResultsBody tr').nth(1).innerText(),/0\.85/);
        await first.getByRole('button',{name:'详情',exact:true}).click();
        await page.locator('#labDailyBody tr').first().waitFor();
        assert.equal(await page.locator('#labRecentBody tr').count(),3);
        assert.match(await page.locator('#labRoundsBody').innerText(),/单轮下注上限/);
        assert.equal(await first.getByRole('button',{name:'详情',exact:true}).getAttribute('aria-expanded'),'true');
        await page.locator('#labCloseDetailButton').click();
        assert.equal(await first.getByRole('button',{name:'详情',exact:true}).evaluate(el=>el===document.activeElement),true);
        const downloaded=page.waitForEvent('download');
        await page.locator('#labExportButton').click();
        const download=await downloaded;
        const csv=fs.readFileSync(await download.path(),'utf8');
        assert.match(csv,/experiment_id/);
        assert.match(csv,/risk_policy/);
        assert.match(csv,/0\.96/);
        assert.equal(csv.split('\n').filter(Boolean).length,51);
        assert.match(download.suggestedFilename(),/\.csv$/);
        changed=await saved('/api/platform/rule-lab/batches/'+id+'/state',()=>page.locator('#labBatchStateButton').click());
        assert.equal(changed.batch.status,'paused');
        await page.waitForFunction(()=>document.getElementById('labBatchStateButton').textContent==='启动模拟');
        await page.locator('#labSort').selectOption('drawdown');
        assert.match(await page.locator('#labResultPageInfo').innerText(),/1–20/);
        assert.doesNotMatch(await page.locator('#labResultsBody tr').first().innerText(),/无条件跟随/);
        await page.locator('#labSort').selectOption('paired');

        await page.route('**/export.csv', route=>route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({error:'模拟账本正忙',code:'store_busy'})}));
        await page.locator('#labExportButton').click();
        await page.locator('#statusMessage').filter({hasText:'导出失败'}).waitFor();
        assert.equal(await page.locator('#labExportButton').isDisabled(),false);
        await page.unroute('**/export.csv');

        // Failed reads retain previously loaded results and offer a manual retry.
        let failOverview=true;
        await page.route('**/api/platform/rule-lab',async route=> {
            if(failOverview){failOverview=false;return route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({error:'暂时不可用',code:'store_busy'})});}
            return route.continue();
        });
        await page.locator('#labRefreshButton').click();
        await page.locator('#statusMessage').filter({hasText:'上次读取结果'}).waitFor();
        assert.equal(await page.locator('#labResultsBody tr').count(),20);
        await page.locator('#labRefreshButton').click();
        await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
        await page.unroute('**/api/platform/rule-lab');

        // A successful create with a lost response must not be repeated automatically.
        await page.locator('#labGenerator').evaluate(el=>el.open=true);
        await page.getByLabel('批次名称',{exact:true}).fill('响应丢失仍只创建一次');
        const beforeCreates=creates;
        let loseOnce=true;
        await page.route('**/api/platform/rule-lab/batches',async route=> {
            if(route.request().method()==='POST' && loseOnce){loseOnce=false;await route.fetch();return route.abort('failed');}
            return route.continue();
        });
        await page.getByRole('button',{name:'生成并保存',exact:true}).click();
        await page.locator('#labFormFeedback').filter({hasText:'核对'}).waitFor();
        await page.locator('#labBatchList').getByText('响应丢失仍只创建一次',{exact:true}).waitFor();
        assert.equal(creates,beforeCreates+1);
        assert.equal(await page.getByLabel('批次名称',{exact:true}).inputValue(),'响应丢失仍只创建一次');
        await page.unroute('**/api/platform/rule-lab/batches');

        // Backtest form validates locally and saves the full 78-version combo batch paused.
        await page.getByLabel('批次名称',{exact:true}).fill('组合历史条件观察');
        await page.getByLabel('投注玩法',{exact:true}).selectOption('combo');
        await page.getByRole('radio',{name:'历史回测',exact:true}).check();
        const beforeInvalid=creates;
        await page.getByRole('button',{name:'生成并保存',exact:true}).click();
        assert.equal(await page.getByLabel('开始日期',{exact:true}).getAttribute('aria-invalid'),'true');
        assert.equal(creates,beforeInvalid);
        const date=await page.evaluate(()=>new Date(Date.now()+8*3600000).toISOString().slice(0,10));
        await page.getByLabel('开始日期',{exact:true}).fill(date);
        await page.getByLabel('结束日期',{exact:true}).fill(date);
        const historical=await saved('/api/platform/rule-lab/batches',()=>page.getByRole('button',{name:'生成并保存',exact:true}).click());
        assert.equal(historical.generated_count,78);
        assert.equal(historical.batch.mode,'backtest');
        assert.equal(historical.batch.status,'paused');
        await page.locator('#labResultTitle').filter({hasText:'组合历史条件观察'}).waitFor();
        assert.match(await page.locator('#labResultPageInfo').innerText(),/共 78/);
        assert.match(await page.locator('#labBatchMeta').innerText(),/历史回测/);
        assert.equal(await page.locator('#labBatchNotice').isVisible(),true);
        assert.match(await page.locator('#labResultsBody tr').first().innerText(),/—/);

        await page.evaluate(()=>{document.activeElement.blur();window.scrollTo({top:0,behavior:'instant'});});
        await page.screenshot({path:'/tmp/pc28-rule-lab-ui-desktop.png',fullPage:true});
        await page.screenshot({path:'/tmp/pc28-rule-lab-ui-desktop-viewport.png'});
        await page.setViewportSize({width:390,height:844});
        await page.reload();
        await page.locator('#labBatchList button[data-batch-id]').first().waitFor();
        assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'no page-level mobile horizontal overflow');
        await page.locator('#labGenerator').evaluate(el=>el.open=true);
        await page.getByLabel('批次名称',{exact:true}).focus();
        await page.keyboard.press('Tab');
        assert.equal(await page.getByLabel('预测来源',{exact:true}).evaluate(el=>el===document.activeElement),true);
        assert.notEqual(await page.getByLabel('预测来源',{exact:true}).evaluate(el=>getComputedStyle(el).outlineStyle),'none');
        await page.evaluate(()=>{document.activeElement.blur();window.scrollTo({top:0,behavior:'instant'});});
        await page.screenshot({path:'/tmp/pc28-rule-lab-ui-mobile.png',fullPage:true});
        await page.screenshot({path:'/tmp/pc28-rule-lab-ui-mobile-viewport.png'});

        // Auth checks exercise the real server, not merely hidden navigation.
        const anonymous=await browser.newContext();
        assert.equal((await anonymous.request.get(info.url+'/api/platform/rule-lab')).status(),401);
        await anonymous.addCookies([{name:info.cookie_name,value:info.regular_cookie,url:info.url}]);
        assert.equal((await anonymous.request.get(info.url+'/rule-lab')).status(),403);
        assert.equal((await anonymous.request.get(info.url+'/api/platform/rule-lab')).status(),403);
        await anonymous.clearCookies();
        await anonymous.addCookies([{name:info.cookie_name,value:info.other_cookie,url:info.url}]);
        assert.equal((await anonymous.request.get(info.url+'/api/platform/rule-lab/batches/'+id)).status(),404);

        // Readable visual evidence is a separate administrator's real local batch.
        const demoResponse=await anonymous.request.post(info.url+'/api/platform/rule-lab/batches',{data:{name:'单双触发条件观察',mode:'live',predictor_id:1,metric:'odd_even',stake_amount:1,template_pack:'basic_v1'}});
        assert.equal(demoResponse.status(),201);
        const demo=(await demoResponse.json()).batch;
        assert.equal((await anonymous.request.post(info.url+'/api/platform/rule-lab/batches/'+demo.id+'/state',{data:{state:'running'}})).status(),200);
        assert.equal((await anonymous.request.post(info.url+'/__fixture/settle',{data:{batch_id:demo.id}})).status(),200);
        const display=await anonymous.newPage();
        await display.setViewportSize({width:1440,height:1000});
        await display.goto(info.url+'/rule-lab');
        await display.locator('#labResultTitle').filter({hasText:'单双触发条件观察'}).waitFor();
        await display.locator('#labGenerator').evaluate(el=>el.open=false);
        await display.evaluate(()=>{document.activeElement.blur();window.scrollTo({top:0,behavior:'instant'});});
        await display.screenshot({path:'/tmp/pc28-rule-lab-ui-desktop.png',fullPage:true});
        await display.screenshot({path:'/tmp/pc28-rule-lab-ui-desktop-viewport.png'});
        await display.setViewportSize({width:390,height:844});
        await display.evaluate(()=>{document.activeElement.blur();window.scrollTo({top:0,behavior:'instant'});});
        assert.ok(await display.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'demo mobile overflow');
        await display.screenshot({path:'/tmp/pc28-rule-lab-ui-mobile.png',fullPage:true});
        await display.screenshot({path:'/tmp/pc28-rule-lab-ui-mobile-viewport.png'});
        await anonymous.close();
        assert.deepEqual(errors,[]);
        console.log('Rule lab browser passed: real WSGI create/pause/run, paired settlement results, detail, CSV, lost-response safety, permissions, XSS text and mobile/keyboard.');
    } finally {
        if(browser)await browser.close();
        server.stdin.end();
    }
})().catch(error=>{console.error(error);process.exitCode=1;});
