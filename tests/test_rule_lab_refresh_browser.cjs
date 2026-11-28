/* Real WSGI/SQLite + Chromium. Timers are triggered explicitly to test focus races. */
const assert=require('node:assert/strict'),path=require('node:path'),fs=require('node:fs'),{spawn}=require('node:child_process');
const {chromium}=require('/tmp/pc28-cycle-ui-tools/node_modules/playwright-core');
const root=path.resolve(__dirname,'..'),base='/api/platform/rule-lab';
(async()=>{
 const fixture=spawn('python3',['-u','-m','tests.rule_lab_management_ui_server'],{cwd:root,stdio:['pipe','pipe','pipe']});let browser,stderr='';fixture.stderr.on('data',x=>stderr+=x);
 try{
  const info=await new Promise((resolve,reject)=>{let output='';const timer=setTimeout(()=>reject(Error(stderr||'fixture timeout')),20000);fixture.once('exit',code=>{clearTimeout(timer);reject(Error('fixture '+code+' '+stderr));});fixture.stdout.on('data',x=>{output+=x;if(output.includes('\n')){clearTimeout(timer);resolve(JSON.parse(output.split('\n')[0]));}});});
  browser=await chromium.launch({headless:true,executablePath:'/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',args:['--no-sandbox']});
  const context=await browser.newContext({viewport:{width:1440,height:1000}});await context.addCookies([{name:info.cookie_name,value:info.cookie,url:info.url}]);
  await context.route('**/*',r=>new URL(r.request().url()).origin===info.url?r.continue():r.abort());
  await context.addInitScript(()=>{
   const interval=window.setInterval.bind(window),timeout=window.setTimeout.bind(window);
   window.setInterval=(callback,delay,...args)=>{if(callback.name==='autoRefresh'){window.__autoRefresh=callback;window.__refreshDelay=delay;return 99991;}return interval(callback,delay,...args);};
   window.setTimeout=(callback,delay,...args)=>{if(callback.name==='pollManagementJobs'){window.__managementPoll=callback;window.__managementDelay=delay;return 99992;}return timeout(callback,delay,...args);};
  });
  const api=async(route,payload)=>{const response=await context.request[payload===undefined?'get':'post'](info.url+route,payload===undefined?{}:{data:payload});assert.ok(response.ok(),await response.text());return response.json();};
  await api(base+'/batches/'+info.pristine_id+'/state',{state:'running'});
  const deleted=(await api(base+'/batches',{name:'已申请删除的空壳',mode:'backtest',predictor_id:1,metric:'big_small',start_date:'2026-11-26',end_date:'2026-11-26'})).batch;
  const preview=await api(base+'/batches/'+deleted.id+'/delete-preview');
  await api(base+'/batches/'+deleted.id+'/delete',{request_id:'refresh-delete-fixture',expected_revision:preview.expected_revision,confirm_name:preview.name});
  const page=await context.newPage(),errors=[],reads=[];page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>{if(r.method()==='GET')reads.push(new URL(r.url()).pathname);});
  const idle=()=>page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
  const tick=()=>page.evaluate(()=>window.__autoRefresh());const overviewCount=()=>reads.filter(p=>p===base).length;
  await page.goto(info.url+'/rule-lab');await idle();
  assert.equal(await page.evaluate(()=>window.__refreshDelay),30000,'Background results refresh should be every 30 seconds');
  assert.equal(await page.evaluate(()=>window.__managementDelay),5000,'Management progress should poll every 5 seconds');
  assert.equal(await page.locator('#labBatchSelect option[value="'+deleted.id+'"]').count(),0,'Accepted deletions must disappear from normal selection immediately');
  await page.locator('#labBatchSelect').focus();const before=overviewCount();await tick();
  assert.equal(overviewCount(),before,'Focused batch selector pauses automatic reads');
  assert.equal(await page.locator('#labBatchSelect').isEnabled(),true);
  assert.match(await page.locator('#labAutoRefreshStatus').textContent(),/选择|操作/);
  await page.locator('#labSort').focus();await tick();assert.equal(overviewCount(),before,'Other native select controls also pause automatic reads');
  await page.locator('#labRefreshButton').focus();
  let release;await page.route(info.url+base,async route=>{await new Promise(resolve=>release=resolve);await route.continue();},{times:1});
  const pending=page.evaluate(()=>window.__autoRefresh());
  while(!release)await page.waitForTimeout(10);
  assert.equal(await page.locator('#labBatchSelect').isEnabled(),true,'A background request cannot disable the selector');
  await page.locator('#labBatchSelect').focus();const selected=await page.locator('#labBatchSelect').inputValue();
  await page.evaluate(()=>window.__originalOption=document.getElementById('labBatchSelect').options[0]);
  release();await pending;
  assert.equal(await page.locator('#labBatchSelect').inputValue(),selected);
  assert.equal(await page.evaluate(()=>document.getElementById('labBatchSelect').options[0]===window.__originalOption),true,'Response arriving during selection cannot rebuild options');
  // Even an idle background update reuses unchanged native options.
  await page.locator('#labRefreshButton').focus();await tick();
  assert.equal(await page.evaluate(()=>document.getElementById('labBatchSelect').options[0]===window.__originalOption),true);
  // Job completion must not force a page refresh while a selector is open.
  await api('/__fixture/maintenance',{});await page.locator('#labBatchSelect').focus();const beforePoll=overviewCount();
  await page.evaluate(()=>window.__managementPoll());assert.equal(overviewCount(),beforePoll);
  await page.locator('#labRefreshButton').focus();await tick();
  assert.ok(overviewCount()>beforePoll,'Deferred completion update resumes when idle');
  // A newer user choice wins over an older in-flight automatic response.
  release=null;await page.route(info.url+base,async route=>{await new Promise(resolve=>release=resolve);await route.continue();},{times:1});
  const old=page.evaluate(()=>window.__autoRefresh());while(!release)await page.waitForTimeout(10);
  await page.locator('#labBatchSelect').selectOption(String(info.existing_id));await idle();release();await old;
  assert.equal(await page.locator('#labBatchSelect').inputValue(),String(info.existing_id));
  await page.setViewportSize({width:390,height:844});assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  fs.mkdirSync('/tmp/pc28-delete-refresh',{recursive:true});await page.screenshot({path:'/tmp/pc28-delete-refresh/refresh-mobile.png'});
  assert.deepEqual(errors,[]);console.log('Refresh browser passed: 30s updates, 5s job polling, immediate deletion removal, focused selects and in-flight responses protected, unchanged options reused, newer selection wins, mobile and zero script errors.');
 }finally{if(browser)await browser.close();fixture.stdin.end();fixture.kill('SIGTERM');}
})().catch(e=>{console.error(e);process.exitCode=1;});
