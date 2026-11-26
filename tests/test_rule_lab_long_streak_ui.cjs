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
  await page.getByLabel('投注玩法',{exact:true}).selectOption('combo');
  await page.getByLabel('条件模板',{exact:true}).selectOption('combo_long_miss_v1');
  await page.getByLabel('批次名称',{exact:true}).fill('组合长连错测试');
  await page.locator('#labRiskMode').selectOption('legacy');
  await page.locator('#labRefreshButton').click();
  await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
  assert.equal(await page.locator('#labTemplatePack').inputValue(),'combo_long_miss_v1');
  assert.match(await page.locator('#labCandidatePreview').innerText(),/26/);
  const saved=page.waitForResponse(r=>r.request().method()==='POST'&&new URL(r.url()).pathname==='/api/platform/rule-lab/batches');
  await page.getByRole('button',{name:'生成并保存',exact:true}).click();
  const response=await saved;assert.equal(response.status(),201);
  const data=await response.json();assert.equal(data.generated_count,26);assert.equal(data.batch.status,'paused');
  await page.locator('#labResultTitle').filter({hasText:'组合长连错测试'}).waitFor();
  await page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
  assert.match(await page.locator('#labResultPageInfo').innerText(),/共 26/);
  await page.locator('#labResultNext').click();
  const row=page.locator('#labResultsBody tr').filter({hasText:'预测连错 ≥ 30 期'}).filter({hasText:'OK游戏网盘'});
  assert.equal(await row.count(),1);
  await row.getByRole('button',{name:'详情',exact:true}).click();
  await page.locator('#labMonthlyBody tr').first().waitFor();
  assert.match(await page.locator('#labDetailMeta').innerText(),/组合长连错/);
  assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1));
  await page.screenshot({path:'/tmp/pc28-long-streak-mobile.png'});
  assert.deepEqual(errors,[]);
  console.log('Long streak UI passed: pack switching/preservation, create paused26, threshold30 paired rules, detail label, mobile.');
 }finally{if(browser)await browser.close();server.stdin.end();}
})().catch(error=>{console.error(error);process.exitCode=1;});
