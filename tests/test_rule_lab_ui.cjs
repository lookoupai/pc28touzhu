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
        await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
        assert.match(await page.locator('#labPerformanceStatus').innerText(),/省资源.*0\.2 核/);
        assert.equal(await page.locator('#labEconomyButton').isDisabled(),true);
        await page.locator('#labFastButton').click();
        await page.locator('#labPerformanceStatus').filter({hasText:'当前生效：加速'}).waitFor();
        assert.match(await page.locator('#labPerformanceStatus').innerText(),/1 核/);
        assert.equal(await page.locator('#labFastButton').getAttribute('aria-pressed'),'true');
        await page.locator('#labEconomyButton').click();
        await page.locator('#labPerformanceStatus').filter({hasText:'当前生效：省资源'}).waitFor();
        // A lost mutation response is reconciled with GET, never retried as a second POST.
        let modePosts=0;
        await page.route('**/api/platform/rule-lab/performance',async route=>{
            if(route.request().method()==='POST'){modePosts++;await route.fetch();await route.abort();}
            else await route.continue();
        });
        await page.locator('#labFastButton').click();
        await page.locator('#labPerformanceFeedback').filter({hasText:'已重新核对'}).waitFor();
        assert.equal(modePosts,1);
        assert.equal(await page.locator('#labFastButton').getAttribute('aria-pressed'),'true');
        await page.unroute('**/api/platform/rule-lab/performance');
        await page.locator('#labEconomyButton').click();
        await page.locator('#labPerformanceStatus').filter({hasText:'当前生效：省资源'}).waitFor();
        assert.equal(await page.locator('#labGenerator').getAttribute('open'),'');
        assert.match(await page.locator('#labCandidatePreview').innerText(),/50/);
        await page.getByLabel('投注玩法',{exact:true}).selectOption('combo');
        assert.match(await page.locator('#labCandidatePreview').innerText(),/92/);
        assert.match(await page.locator('#labTemplateHelp').innerText(),/7、8、9、10、11、12、13、14、15、20、25、30/);
        assert.match(await page.locator('#labTemplateHelp').innerText(),/无需另跑/);
        assert.equal(await page.locator('#labTemplatePack option[value="basic_v1"]').count(),0);
        await page.getByLabel('条件模板',{exact:true}).selectOption('combo_long_miss_v1');
        assert.match(await page.locator('#labCandidatePreview').innerText(),/13 个条件.*26 个实验版本/);
        assert.match(await page.locator('#labTemplateHelp').innerText(),/7、8、9、10、11、12、13、14、15、20、25、30/);
        await page.getByLabel('投注玩法',{exact:true}).selectOption('odd_even');
        assert.equal(await page.locator('#labTemplatePack').inputValue(),'basic_v2');
        assert.equal(await page.locator('#labTemplatePack option[value="combo_long_miss_v1"]').count(),0);
        await page.locator('#labSettlementDetails').evaluate(el=>el.open=true);
        assert.match(await page.locator('#labSettlementSummary').innerText(),/2\.846/);
        assert.match(await page.locator('#labSettlementSummary').innerText(),/1\.998/);
        assert.match(await page.locator('#labSettlementSummary').innerText(),/3\.71/);
        assert.match(await page.locator('#labSettlementSummary').innerText(),/4\.32/);
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
        await page.waitForFunction(()=>document.querySelector('#labResultsBody tr').textContent.includes('+1.00'));
        const first=page.locator('#labResultsBody tr').first();
        assert.match(await first.innerText(),/1\.00/);
        assert.match(await page.locator('#labResultsBody tr').nth(1).innerText(),/0\.85/);
        await first.getByRole('button',{name:'详情',exact:true}).click();
        await page.locator('#labDailyBody tr').first().waitFor();
        assert.equal(await page.locator('#labRecentBody').count(),0);
        assert.match(await page.locator('#labResultRetention').innerText(),/当前待结算 0 笔/);
        assert.match(await page.locator('#labDetailMeta').innerText(),/本实验冻结赔率：1\.998/);
        assert.equal(await page.locator('#labRoundsBody').count(),0);
        assert.match(await page.locator('#labDetailStats').innerText(),/有效完成轮/);
        assert.equal(await first.getByRole('button',{name:'详情',exact:true}).getAttribute('aria-expanded'),'true');
        await page.locator('#labMonthlyBody tr').first().waitFor();
        assert.match(await page.locator('#labMonthlyBody').innerText(),/1\.00/);
        assert.match(await page.locator('#labMonthlyNotice').innerText(),/仅 0 个完整/);
        assert.match(await page.locator('#labPerformanceSummary').innerText(),/最大回撤金额（逐笔）\s+1\.00/);
        assert.match(await page.locator('#labPerformanceSummary').innerText(),/胜率（不含退款）\s+66\.67%/);
        assert.doesNotMatch(await page.locator('#labMonthly').innerText(),/压力倍数|折算本金|本金回撤容忍度/);
        assert.equal(await page.locator('#labInitialCapital').inputValue(),'');
        assert.equal(await page.locator('#labCapitalSummary').innerText(),'');
        await page.locator('#labInitialCapital').fill('100');
        assert.match(await page.locator('#labCapitalSummary').innerText(),/期末资金\s+101\.00/);
        assert.match(await page.locator('#labCapitalSummary').innerText(),/累计收益率\s+1\.00%/);
        // Intraday drawdown was 1; the daily close was positive, so daily percentage is zero.
        assert.match(await page.locator('#labCapitalSummary').innerText(),/最大回撤率（日结）\s+0\.00%/);
        assert.equal(await page.locator('#labEquityChart svg').count(),1);
        await page.locator('#labInitialCapital').fill('0');
        assert.equal(await page.locator('#labCapitalSummary').innerText(),'');
        assert.match(await page.locator('#labCapitalFeedback').innerText(),/0\.01/);
        await page.locator('#labInitialCapital').fill('100.001');
        assert.match(await page.locator('#labCapitalFeedback').innerText(),/两位小数/);
        await page.locator('#labInitialCapital').fill('1000');
        const monthlyDownload=page.waitForEvent('download');await page.locator('#labMonthlyExport').click();
        const monthlyCsv=fs.readFileSync(await(await monthlyDownload).path(),'utf8');
        assert.match(monthlyCsv,/daily_close_drawdown/);assert.match(monthlyCsv,/"3","3","1","33\.33"/);
        await page.setViewportSize({width:390,height:844});
        await page.locator('#labMonthlyTitle').scrollIntoViewIfNeeded();
        assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),'monthly mobile overflow');
        await page.screenshot({path:'/tmp/pc28-monthly-fixture-mobile.png'});
        await page.setViewportSize({width:1440,height:1000});
        await page.locator('#labCloseDetailButton').click();
        assert.equal(await first.getByRole('button',{name:'详情',exact:true}).evaluate(el=>el===document.activeElement),true);
        // Known equity paths verify peak-relative percentages and an exhausted initial balance.
        const firstId=await first.getByRole('button',{name:'详情',exact:true}).getAttribute('data-experiment-id');
        const detailPath='**/api/platform/rule-lab/batches/'+id+'/experiments/'+firstId;
        await page.route(detailPath,async route=>{
            const response=await route.fetch(),data=await response.json();
            data.monthly.capital.net_profit=50;
            data.monthly.equity={complete_history:true,window_start:'2026-10-01',points:[{date:'2026-10-02',cumulative_net_cents:10000},{date:'2026-10-03',cumulative_net_cents:5000}]};
            await route.fulfill({response,json:data});
        });
        await first.getByRole('button',{name:'月度成绩',exact:true}).click();
        await page.locator('#labMonthlyBody tr').first().waitFor();
        await page.locator('#labInitialCapital').fill('100');
        assert.match(await page.locator('#labCapitalSummary').innerText(),/最大回撤率（日结）\s+25\.00%/);
        await page.locator('#labCloseDetailButton').click();await page.unroute(detailPath);
        await page.route(detailPath,async route=>{
            const response=await route.fetch(),data=await response.json();
            data.monthly.capital.net_profit=50;
            data.monthly.equity={complete_history:true,window_start:'2026-10-01',points:[{date:'2026-10-02',cumulative_net_cents:-10000},{date:'2026-10-03',cumulative_net_cents:5000}]};
            await route.fulfill({response,json:data});
        });
        await first.getByRole('button',{name:'月度成绩',exact:true}).click();
        await page.locator('#labMonthlyBody tr').first().waitFor();
        assert.match(await page.locator('#labCapitalFeedback').innerText(),/小于等于0/);
        assert.match(await page.locator('#labCapitalSummary').innerText(),/累计收益率\s+—/);
        await page.locator('#labCloseDetailButton').click();await page.unroute(detailPath);
        await first.getByRole('button',{name:'月度成绩',exact:true}).click();
        await page.locator('#labMonthlyBody tr').first().waitFor();
        await page.waitForFunction(()=>document.activeElement===document.getElementById('labMonthlyTitle'));
        await page.locator('#labCloseDetailButton').click();
        const downloaded=page.waitForEvent('download');
        await page.locator('#labExportButton').click();
        const download=await downloaded;
        const csv=fs.readFileSync(await download.path(),'utf8');
        assert.match(csv,/experiment_id/);
        assert.match(csv,/risk_policy/);
        assert.match(csv,/,1\.0,/);
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

        // Backtest form validates locally and saves the full 92-version combo batch paused.
        await page.getByLabel('批次名称',{exact:true}).fill('组合历史条件观察');
        await page.getByLabel('投注玩法',{exact:true}).selectOption('combo');
        await page.getByRole('radio',{name:'历史回测',exact:true}).check();
        assert.match(await page.locator('#labModeHelp').innerText(),/增量续算.*手动追加.*每日自动续算/);
        const beforeInvalid=creates;
        await page.getByRole('button',{name:'生成并保存',exact:true}).click();
        assert.equal(await page.getByLabel('开始日期',{exact:true}).getAttribute('aria-invalid'),'true');
        assert.equal(creates,beforeInvalid);
        const date=await page.evaluate(()=>new Date(Date.now()+8*3600000).toISOString().slice(0,10));
        await page.getByLabel('开始日期',{exact:true}).fill(date);
        await page.getByLabel('结束日期',{exact:true}).fill(date);
        await page.locator('#labRiskMode').selectOption('round');
        await page.locator('#labRiskPreset').selectOption('6:9:7:combo');
        const historical=await saved('/api/platform/rule-lab/batches',()=>page.getByRole('button',{name:'生成并保存',exact:true}).click());
        assert.equal(historical.generated_count,92);
        assert.equal(historical.batch.mode,'backtest');
        assert.equal(historical.batch.status,'paused');
        await page.locator('#labResultTitle').filter({hasText:'组合历史条件观察'}).waitFor();
        assert.match(await page.locator('#labResultPageInfo').innerText(),/共 92/);
        assert.match(await page.locator('#labBatchMeta').innerText(),/历史回测/);
        assert.equal(await page.locator('#labBatchNotice').isVisible(),true);
        assert.match(await page.locator('#labResultsBody tr').first().innerText(),/—/);

        // Expanded combo grid fits within the 1000-version saved capacity.
        await page.locator('#labCompareButton').click();
        await page.locator('#labComparisonCapacity').filter({hasText:'644'}).waitFor();
        assert.match(await page.locator('#labComparisonCapacity').innerText(),/836 \/ 1000/);
        assert.equal(await page.locator('#labCreateStudyButton').isDisabled(),false);
        // Create a two-way metric source with eight variants.
        const preset=await page.evaluate(async()=>{const data=await(await fetch('/api/platform/rule-lab/options')).json();return data.risk_presets.find(p=>p.metric==='big_small').policy;});
        const studySource=await page.evaluate(async({preset,date})=>(await(await fetch('/api/platform/rule-lab/batches',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:'风控比较源',mode:'backtest',predictor_id:1,metric:'big_small',start_date:date,end_date:date,risk_policy:preset})})).json()).batch,{preset,date});
        await page.locator('#labRefreshButton').click();await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
        await page.locator('#labBatchList button[data-batch-id="'+studySource.id+'"]').click();
        await page.locator('#labResultTitle').filter({hasText:'风控比较源'}).waitFor();
        await page.locator('#labCompareButton').click();await page.locator('#labComparisonCapacity').filter({hasText:'400'}).waitFor();
        await page.locator('#labStudyName').fill('五风控统一比较 · 组合连挂观察的五组风控对比');
        const group=await saved('/api/platform/rule-lab/studies',()=>page.locator('#labCreateStudyButton').click());
        assert.equal(group.study.status,'paused');assert.equal(group.study.children.length,8);
        await page.locator('#labStudyTitle').filter({hasText:'五风控统一比较'}).waitFor();
        assert.equal(await page.locator('#labStudyRows tr').count(),20);
        await page.locator('#labStudyRows tr').first().getByRole('button',{name:'月度成绩',exact:true}).click();
        await page.locator('#labMonthly').waitFor();
        assert.match(await page.locator('#labCapitalFeedback').innerText(),/尚无已结算交易/);
        assert.match(await page.locator('#labMonthlyBody').innerText(),/尚未回放/);
        await page.locator('#labCloseDetailButton').click();
        await saved('/api/platform/rule-lab/studies/'+group.study.id+'/state',()=>page.locator('#labStudyStateButton').click());
        await page.locator('#labStudyMeta').filter({hasText:'等待运行额度'}).waitFor();

        // A reopened page selects the queue and follows real worker progress without manual refresh.
        const monitor=await context.newPage();
        await monitor.clock.install();await monitor.clock.pauseAt(new Date());
        const monitorErrors=[];let progressReads=0,progressWrites=0;
        monitor.on('pageerror',error=>monitorErrors.push(String(error)));
        monitor.on('request',request=>{
            const pathname=new URL(request.url()).pathname;
            if(pathname==='/api/platform/rule-lab'&&request.method()==='GET')progressReads++;
            if(pathname.startsWith('/api/platform/rule-lab')&&request.method()!=='GET')progressWrites++;
        });
        await monitor.goto(info.url+'/rule-lab');
        await monitor.locator('#labStudyMeta').filter({hasText:'等待运行额度'}).waitFor();
        await monitor.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
        assert.match(await monitor.locator('#labStudyRows').innerText(),/等待队列运行/);
        assert.doesNotMatch(await monitor.locator('#labStudyRows').innerText(),/样本较少/);
        assert.match(await monitor.locator('#labStudyProgress').innerText(),/等待前序方案完成/);
        await monitor.locator('#labStudyVariant').selectOption('baseline');
        await monitor.locator('#labStudySettlement').selectOption('pc28_netdisk_regular');
        await monitor.locator('#labStudyNext').click();
        assert.match(await monitor.locator('#labStudyPageInfo').innerText(),/21–25/);

        // Editing and hidden tabs do not issue polling requests or overwrite the user's draft.
        await monitor.locator('#labName').fill('尚未保存的草稿');
        let beforePoll=progressReads;
        await monitor.clock.runFor(16000);
        assert.equal(progressReads,beforePoll);
        assert.equal(await monitor.locator('#labName').inputValue(),'尚未保存的草稿');
        await monitor.evaluate(()=>{document.activeElement.blur();Object.defineProperty(document,'hidden',{value:true,configurable:true});});
        await monitor.clock.runFor(16000);assert.equal(progressReads,beforePoll);
        await monitor.evaluate(()=>delete document.hidden);

        await page.evaluate(()=>fetch('/__fixture/study-tick',{method:'POST'}));
        await monitor.clock.runFor(16000);
        await monitor.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
        await monitor.locator('#labStudyMeta').filter({hasText:'已完成 1/5'}).waitFor();
        assert.ok(progressReads>beforePoll);
        assert.equal(await monitor.locator('#labStudyVariant').inputValue(),'baseline');
        assert.equal(await monitor.locator('#labStudySettlement').inputValue(),'pc28_netdisk_regular');
        assert.match(await monitor.locator('#labStudyPageInfo').innerText(),/21–25/);
        assert.equal(await monitor.locator('#labName').inputValue(),'尚未保存的草稿');
        await monitor.locator('#labStudyPrevious').click();
        assert.match(await monitor.locator('#labStudyRows tr').first().innerText(),/6\.00/);

        // A transient polling failure keeps previous results and recovers on the next interval.
        await monitor.route('**/api/platform/rule-lab',route=>route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({error:'暂时不可用',code:'store_busy'})}));
        await monitor.clock.runFor(16000);
        await monitor.locator('#statusMessage').filter({hasText:'上次读取结果'}).waitFor();
        assert.match(await monitor.locator('#labStudyRows tr').first().innerText(),/6\.00/);
        await monitor.unroute('**/api/platform/rule-lab');
        await monitor.clock.runFor(16000);
        await monitor.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled&&!document.getElementById('statusMessage').textContent);
        assert.equal(progressWrites,0,'automatic updates only read the ledger');
        assert.deepEqual(monitorErrors,[]);
        await monitor.close();

        await page.locator('#labRefreshButton').click();await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
        assert.match(await page.locator('#labStudyMeta').innerText(),/已完成|比较中/);
        if(await page.locator('#labStudyStateButton').isVisible())await saved('/api/platform/rule-lab/studies/'+group.study.id+'/state',()=>page.locator('#labStudyStateButton').click());
        await page.locator('#labStudyMeta').filter({hasText:'已暂停'}).waitFor();
        await saved('/api/platform/rule-lab/studies/'+group.study.id+'/state',()=>page.locator('#labStudyStateButton').click());
        for(let tick=0;tick<15;tick++){
            await page.evaluate(()=>fetch('/__fixture/study-tick',{method:'POST'}));
            const current=await page.evaluate(async id=>(await(await fetch('/api/platform/rule-lab/studies/'+id)).json()).study,group.study.id);
            if(current.status==='completed')break;
        }
        await page.locator('#labRefreshButton').click();await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
        await page.locator('#labStudyMeta').filter({hasText:'全部完成'}).waitFor();
        assert.match(await page.locator('#labStudyNotice').innerText(),/输入指纹与实际覆盖一致/);
        await page.locator('#labStudyVariant').selectOption('baseline');
        assert.match(await page.locator('#labStudyRows tr').first().innerText(),/0\.00/);
        await page.locator('#labStudyVariant').selectOption('');
        await page.locator('#labStudySettlement').selectOption('pc28_netdisk_regular');
        assert.doesNotMatch(await page.locator('#labStudyRows').innerText(),/高赔/);
        const comparisonDownload=page.waitForEvent('download');await page.locator('#labStudyExportButton').click();
        const comparisonCsv=fs.readFileSync(await(await comparisonDownload).path(),'utf8');assert.match(comparisonCsv,/comparison_reason/);assert.match(comparisonCsv,/cutoff_issue/);
        assert.match(comparisonCsv,/True/);assert.match(comparisonCsv,/,6\.0,/);

        await page.evaluate(()=>{document.activeElement.blur();window.scrollTo({top:0,behavior:'instant'});});
        await page.screenshot({path:'/tmp/pc28-rule-lab-ui-desktop.png',fullPage:true});
        await page.screenshot({path:'/tmp/pc28-rule-lab-ui-desktop-viewport.png'});
        await page.setViewportSize({width:390,height:844});
        await page.reload();
        await page.locator('#labBatchList button[data-batch-id]').first().waitFor();
        await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
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

        // Reuse the owned populated fixture for visual evidence; capacity remains enforced.
        const display=await context.newPage();
        await display.setViewportSize({width:1440,height:1000});
        await display.goto(info.url+'/rule-lab');
        await display.locator('#labBatchList button[data-batch-id="'+id+'"]').waitFor();
        await display.locator('#labBatchList button[data-batch-id="'+id+'"]').click();
        await display.locator('#labResultTitle').filter({hasText:malicious}).waitFor();
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
