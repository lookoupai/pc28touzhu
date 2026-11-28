/* Real temporary WSGI/SQLite. All network stays within the disposable fixture. */
const assert=require('node:assert/strict'),path=require('node:path'),fs=require('node:fs');
const {spawn}=require('node:child_process');
const {chromium}=require(path.join(process.env.PC28_UI_NODE_MODULES||'/tmp/pc28-cycle-ui-tools/node_modules','playwright-core'));
const root=path.resolve(__dirname,'..'),prefix='/api/platform/rule-lab';
(async()=>{
 const server=spawn(process.env.PYTHON||'python3',['-u','-m','tests.rule_lab_management_ui_server'],{cwd:root,stdio:['pipe','pipe','pipe']});
 let browser,stderr='';server.stderr.on('data',d=>stderr+=d);
 try{
  const info=await new Promise((resolve,reject)=>{let out='';const timer=setTimeout(()=>reject(new Error('Fixture timeout '+stderr)),20000);server.on('exit',code=>{clearTimeout(timer);reject(new Error('Fixture exit '+code+' '+stderr));});server.stdout.on('data',d=>{out+=d;if(out.includes('\n')){clearTimeout(timer);resolve(JSON.parse(out.split('\n')[0]));}});});
  browser=await chromium.launch({headless:true,executablePath:process.env.PC28_UI_CHROMIUM||'/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',args:['--no-sandbox']});
  const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});await context.addCookies([{name:info.cookie_name,value:info.cookie,url:info.url}]);
  await context.route('**/*',route=>new URL(route.request().url()).origin===info.url?route.continue():route.abort());
  const page=await context.newPage(),errors=[],writes=[];page.on('pageerror',e=>errors.push(String(e)));page.on('request',r=>{if(r.method()==='POST'&&new URL(r.url()).pathname.startsWith(prefix+'/'))writes.push({url:r.url(),data:r.postDataJSON()});});
  const api=async(route,body)=>{const response=await context.request[body===undefined?'get':'post'](info.url+route,body===undefined?{}:{data:body});assert.equal(response.ok(),true,await response.text());return response.json();};
  const idle=()=>page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled);
  const visible=id=>page.locator('#'+id).waitFor({state:'visible'});
  async function submit(id,suffix){const response=page.waitForResponse(r=>r.url().endsWith(suffix)&&r.request().method()==='POST');await page.locator('#'+id).click();const result=await response;await idle();return {status:result.status(),body:await result.json()};}
  async function openBatch(id){if(await page.locator('#labManagementPanel').isVisible())await page.locator('#labManagementClose').click();if(await page.locator('#labSystemPanel').isVisible())await page.locator('#labSystemClose').click();await page.locator('#labBatchSelect').selectOption(String(id));await idle();if(!await page.locator('#labBatchTools').evaluate(n=>n.open))await page.locator('#labBatchTools summary').click();await page.locator('#labManageBatchButton').click();await idle();await visible('labManagementPanel');}
  async function note(text){await page.waitForFunction(value=>document.getElementById('labManagementFeedback').textContent.includes(value),text);}
  await page.goto(info.url+'/rule-lab');await idle();
  assert.equal(writes.length,0,'Initial reads must not mutate experiments');
  await openBatch(info.pristine_id);assert.equal(writes.length,0,'Management preview must be read-only');
  assert.equal(await page.locator('#labManagementEdit').isVisible(),true);
  await page.locator('#labManagementEdit').click();
  await page.locator('#labDefinitionName').fill('已编辑的暂停实验');await page.locator('#labDefinitionMetric').selectOption('combo');await page.locator('#labDefinitionStake').fill('2.25');
  const edited=await submit('labDefinitionSave','/edit');assert.equal(edited.status,200);assert.equal(edited.body.batch.status,'paused');assert.equal(edited.body.batch.id,info.pristine_id);assert.equal(edited.body.batch.experiment_count,92);assert.equal(edited.body.batch.stake_amount,2.25);assert.equal(edited.body.queue_removed,true);await note('手动启动');
  assert.equal(writes.some(row=>row.url.endsWith('/state')),false,'Editing cannot start an experiment');
  const xssName='管理 <img src=x onerror=window.managementXss=1>';
  await page.locator('#labManagementName').fill(xssName);assert.equal((await submit('labRenameSave','/rename')).status,200);await note('名称已保存');
  assert.equal(await page.locator('#labManagementTitle img').count(),0);assert.equal(await page.evaluate(()=>window.managementXss),undefined);
  // A stale preview must be rejected and refreshed, with no automatic mutation replay.
  const remote=await api(prefix+'/batches/'+info.pristine_id+'/management');
  await api(prefix+'/batches/'+info.pristine_id+'/rename',{request_id:'external-rename-check',expected_revision:remote.expected_revision,name:'并发更名后的实验'});
  await page.locator('#labManagementName').fill('过期页面的名字');assert.equal((await submit('labRenameSave','/rename')).status,409);
  assert.equal(await page.locator('#labManagementName').inputValue(),'并发更名后的实验');
  // Response loss after commit: GET receipt recovers without a second write.
  const renameUrl=info.url+prefix+'/batches/'+info.pristine_id+'/rename',renamesBefore=writes.filter(row=>row.url===renameUrl).length;
  await page.route(renameUrl,async route=>{await route.fetch();await route.abort('failed');},{times:1});
  await page.locator('#labManagementName').fill('已核对的名称');await page.locator('#labRenameSave').click();await note('名称已保存');await idle();
  assert.equal(writes.filter(row=>row.url===renameUrl).length,renamesBefore+1,'Receipt recovery must not issue a second POST');
  assert.equal(await page.locator('#labManagementRetry').isVisible(),false);
  await page.locator('#labManagementClose').click();assert.equal(await page.locator('#labManageBatchButton').evaluate(n=>n===document.activeElement),true);
  await openBatch(info.existing_id);assert.equal(await page.locator('#labManagementEdit').isVisible(),false);
  await page.locator('#labManagementFork').click();assert.match(await page.locator('#labDefinitionNote').innerText(),/冻结条件及赔率/);
  await page.locator('#labDefinitionName').fill('容量拒绝后仍可重试');
  await api('/__fixture/capacity/full',{});const capacity=await submit('labDefinitionSave','/fork');assert.equal(capacity.status,409);await api('/__fixture/capacity/normal',{});
  assert.equal((await api('/__fixture/state')).batches.length,2,'Capacity error must not create a partial batch');
  assert.equal(await page.locator('#labDefinitionName').inputValue(),'容量拒绝后仍可重试');
  // Lose committed fork response and first reconciliation read. Explicit retry uses the original payload/key.
  const forkUrl=info.url+prefix+'/batches/'+info.existing_id+'/fork',previewUrl=info.url+prefix+'/batches/'+info.existing_id+'/management';
  await page.route(forkUrl,async route=>{await route.fetch();await route.abort('failed');},{times:1});
  await page.route(previewUrl,route=>route.abort('failed'),{times:1});
  await page.locator('#labDefinitionName').fill('独立的新版本');await page.locator('#labDefinitionStake').fill('3');await page.locator('#labDefinitionSave').click();await visible('labManagementRetry');await idle();
  assert.equal(await page.locator('#labDefinitionSave').isDisabled(),true);assert.equal(await page.locator('#labDefinitionRiskRoundProfit').isDisabled(),true);
  assert.equal(await page.locator('#labManagementClose').isDisabled(),true);
  const forkCount=writes.filter(row=>row.url===forkUrl).length;
  // The next GET is also unavailable; a deliberate retry must send the same idempotent request.
  await page.route(previewUrl,route=>route.abort('failed'),{times:1});
  const retried=await submit('labManagementRetry','/fork');assert.equal(retried.status,200);assert.equal(retried.body.created,false);
  const forkWrites=writes.filter(row=>row.url===forkUrl);assert.equal(forkWrites.length,forkCount+1);assert.deepEqual(forkWrites.at(-1).data,forkWrites.at(-2).data);
  const forkId=retried.body.batch.id,afterFork=await api('/__fixture/state');assert.equal(afterFork.batches.length,3);assert.deepEqual(afterFork.original,afterFork.before);assert.equal(retried.body.batch.status,'paused');
  // Delete needs exact full name; cancellation performs no write.
  await openBatch(forkId);await page.locator('#labManagementDelete').click();await visible('labDeleteForm');await idle();
  assert.equal(await page.locator('#labDeleteConfirm').isDisabled(),true);await page.locator('#labDeleteConfirmName').fill('wrong');assert.equal(await page.locator('#labDeleteConfirm').isDisabled(),true);
  const beforeCancel=writes.length;await page.locator('#labDeleteCancel').click();assert.equal(writes.length,beforeCancel);
  await page.locator('#labManagementDelete').click();await visible('labDeleteForm');await idle();await page.locator('#labDeleteConfirmName').fill('独立的新版本');
  const deleted=await submit('labDeleteConfirm','/delete');assert.equal(deleted.status,202);await page.locator('#labSystemToggle').click();assert.equal(await page.locator('#labManagementJobs').isVisible(),true);await page.locator('#labSystemClose').click();
  const deleting=await api(prefix+'/batches/'+forkId);assert.equal(deleting.batch.status,'deleting');
  await page.locator('#labBatchSelect').selectOption(String(forkId));await idle();assert.equal(await page.locator('#labBatchStateButton').isDisabled(),true,'Deleting batch cannot be restored');
  await api('/__fixture/maintenance',{});await page.waitForFunction(id=>![...document.getElementById('labBatchSelect').options].some(option=>option.value===String(id)),forkId,{timeout:15000});
  assert.equal((await api(prefix+'/management-jobs/'+deleted.body.job.id)).job.status,'completed');
  assert.deepEqual((await api('/__fixture/state')).original,afterFork.before);
  // Storage values come from the actual temporary database and a durable reclaim job.
  await page.locator('#labSystemToggle').click();await page.locator('#labStoragePanel summary').click();await page.waitForFunction(()=>document.getElementById('labStorageSummary').children.length>0);
  assert.match(await page.locator('#labStorageExplanation').innerText(),/不等于已释放磁盘/);
  await page.waitForFunction(()=>!document.getElementById('labStorageReclaim').disabled,{timeout:10000});
  const reclaimed=await submit('labStorageReclaim','/storage/reclaim');assert.equal(reclaimed.status,202);await api('/__fixture/maintenance',{});
  assert.equal((await api(prefix+'/management-jobs/'+reclaimed.body.job.id)).job.status,'completed');
  // Dependencies are named, and the management link operates on the entire group.
  await page.locator('#labSystemClose').click();const study=await api('/__fixture/create-study',{});await page.locator('#labRefreshButton').click();await idle();await openBatch(info.existing_id);await page.locator('#labManagementDelete').click();await visible('labDeleteForm');await idle();
  assert.equal(await page.locator('#labDeleteConfirmName').isDisabled(),true);assert.match(await page.locator('#labDeleteBlockers').innerText(),/比较组管理样本/);
  await page.locator('#labDeleteBlockers button').click();await idle();assert.match(await page.locator('#labManagementTitle').innerText(),/管理比较组/);
  assert.equal(await page.locator('#labManagementFork').isVisible(),false);await page.locator('#labManagementName').fill('整体删除测试组');assert.equal((await submit('labRenameSave','/rename')).status,200);
  await page.locator('#labManagementDelete').click();await visible('labDeleteForm');await idle();assert.match(await page.locator('#labManagementNotice').innerText(),/原始参考批次保留/);
  await page.locator('#labDeleteConfirmName').fill('整体删除测试组');const groupDeleted=await submit('labDeleteConfirm','/delete');assert.equal(groupDeleted.status,202);assert.equal(groupDeleted.body.job.owner_id,study.study.id);await api('/__fixture/maintenance',{});await page.waitForFunction(()=>document.getElementById('labManagementJobList').textContent.includes('已完成'));
  assert.deepEqual((await api('/__fixture/state')).original,afterFork.before);
  const evidence=process.env.PC28_MANAGEMENT_EVIDENCE||'/tmp/pc28-management-ui-evidence';fs.mkdirSync(evidence,{recursive:true});
  await openBatch(info.pristine_id);await page.locator('#labManagementEdit').click();await page.locator('#labManagementPanel').screenshot({path:path.join(evidence,'management-desktop.png')});
  await page.setViewportSize({width:390,height:844});await page.locator('#labManagementPanel').scrollIntoViewIfNeeded();
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'Management form must not overflow mobile viewport');
  for(const id of ['labDefinitionSave','labDefinitionCancel','labManagementClose'])assert.ok((await page.locator('#'+id).boundingBox()).height>=44,'Touch target: '+id);
  await page.locator('#labManagementPanel').screenshot({path:path.join(evidence,'management-mobile.png')});
  await page.locator('#labDefinitionCancel').click();await page.locator('#labManagementDelete').click();await idle();await page.locator('#labDeleteForm').screenshot({path:path.join(evidence,'delete-mobile.png')});
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);assert.deepEqual(errors,[]);
  console.log('Management UI passed: real WSGI edit, rename, stale preview, capacity, response loss/explicit idempotent retry, fork parity, batch/group deletion, storage reclaim, mobile and XSS. Screenshots: '+evidence);
 }finally{if(browser)await browser.close();server.stdin.end();setTimeout(()=>server.kill(),1000).unref();}
})().catch(e=>{console.error(e);process.exitCode=1;});
