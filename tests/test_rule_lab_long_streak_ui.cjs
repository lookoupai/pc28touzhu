/* Real WSGI fixture; verifies the new pack independently of old capacity scenarios. */
const assert=require('node:assert/strict');
const path=require('node:path');
const {spawn}=require('node:child_process');
const {chromium}=require(path.join(process.env.PC28_UI_NODE_MODULES||'/tmp/pc28-cycle-ui-tools/node_modules','playwright-core'));
const root=path.resolve(__dirname,'..');
(async()=>{
 const server=spawn(process.env.PYTHON||'python3',['-u','-m','tests.rule_lab_ui_server'],{cwd:root,stdio:['pipe','pipe','pipe']});
 let browser,stderr='';server.stderr.on('data',data=>stderr+=data);
 try{
  const info=await new Promise((resolve,reject)=>{
   let output='';const timer=setTimeout(()=>reject(new Error('Fixture timeout '+stderr)),20000);
   server.on('exit',code=>{clearTimeout(timer);reject(new Error('Fixture exit '+code+': '+stderr));});
   server.stdout.on('data',data=>{output+=data;if(output.includes('\n')){clearTimeout(timer);resolve(JSON.parse(output.split('\n')[0]));}});
  });
  browser=await chromium.launch({headless:true,executablePath:process.env.PC28_UI_CHROMIUM||'/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',args:['--no-sandbox']});
  const context=await browser.newContext({viewport:{width:390,height:844}});
  await context.addCookies([{name:info.cookie_name,value:info.cookie,url:info.url}]);
  await context.route('**/*',route=>new URL(route.request().url()).origin===info.url?route.continue():route.abort());
  const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(String(error)));
  await page.goto(info.url+'/rule-lab');
  await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
  await page.locator('#labNewExperiment').click();
  await page.locator('[data-source-id="1"]').check();
  await page.locator('[data-metric-check="odd_even"]').uncheck();
  await page.locator('[data-metric-check="combo"]').check();
  await page.locator('#labBulkMode').selectOption('live');
  await page.locator('[data-condition-template="combo"]').selectOption('builtin:combo_long_miss_v1:combo');
  await page.locator('#labBulkPreviewButton').click();
  await page.waitForFunction(()=>document.getElementById('labBulkPreviewSummary').textContent.includes('26 个版本'));
  assert.match(await page.locator('#labBulkPreviewRows').innerText(),/13 条件.*26 版本/);
  const name=page.locator('#labBulkPreviewRows input');await name.fill('组合长连错测试');await name.blur();
  await page.locator('#labBulkPreviewButton').click();
  await page.waitForFunction(()=>!document.getElementById('labBulkSaveDraft').disabled);
  const saved=page.waitForResponse(r=>r.request().method()==='POST'&&new URL(r.url()).pathname==='/api/platform/rule-lab/batches/bulk');
  await page.locator('#labBulkSaveDraft').click();
  const response=await saved;assert.equal(response.status(),201);
  const data=await response.json();assert.equal(data.generated_count,26);assert.equal(data.batches.length,1);assert.equal(data.batches[0].status,'paused');
  await page.waitForFunction(()=>document.getElementById('labSelfFeedback').textContent.includes('已创建')&&!document.getElementById('labRefreshButton').disabled);
  await page.locator('#labSelfClose').click();
  await page.locator('#labBatchTools summary').click();
  await page.locator('#labResultTitle').filter({hasText:'组合长连错测试'}).waitFor();
  assert.match(await page.locator('#labResultPageInfo').innerText(),/共 26/);
  const displayed=await page.locator('#labResultsBody').innerText();
  for(const threshold of [9,11,13,14])assert.ok(new RegExp('预测连错 ≥ '+threshold+'\\s*期').test(displayed));
  assert.doesNotMatch(displayed,/预测连错 ≥ [3456]\s*期/);
  await page.locator('#labResultNext').click();
  const row=page.locator('#labResultsBody tr').filter({hasText:/预测连错 ≥ 30\s*期/}).filter({hasText:'OK游戏网盘'});
  assert.equal(await row.count(),1);
  await row.locator('button[data-experiment-id]').click();
  await page.locator('#labMonthlyBody tr').first().waitFor();
  assert.match(await page.locator('#labDetailMeta').innerText(),/组合长连错/);
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  await page.screenshot({path:'/tmp/pc28-long-streak-mobile.png'});
  assert.deepEqual(errors,[]);
  console.log('Long streak UI passed: template choice and renamed preview, create paused26 via bulk, threshold30 paired rules, detail label, mobile.');
 }finally{if(browser)await browser.close();server.stdin.end();}
})().catch(error=>{console.error(error);process.exitCode=1;});
