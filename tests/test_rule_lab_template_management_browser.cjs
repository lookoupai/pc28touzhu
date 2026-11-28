/* Template basics on real local WSGI/SQLite, with no production writes. */
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),{spawn}=require('node:child_process');
const {chromium}=require('/tmp/pc28-cycle-ui-tools/node_modules/playwright-core');
const root=path.resolve(__dirname,'..'),base='/api/platform/rule-lab';
(async()=>{
 const fixture=spawn('python3',['-u','-m','tests.rule_lab_self_service_ui_server'],{cwd:root,stdio:['pipe','pipe','pipe']});
 let browser,stderr='';fixture.stderr.on('data',part=>stderr+=part);
 try{
  const info=await new Promise((resolve,reject)=>{let output='';const timer=setTimeout(()=>reject(Error(stderr||'Fixture timeout')),20000);fixture.once('exit',code=>{clearTimeout(timer);reject(Error('Fixture '+code+' '+stderr));});fixture.stdout.on('data',part=>{output+=part;if(output.includes('\n')){clearTimeout(timer);resolve(JSON.parse(output.split('\n')[0]));}});});
  browser=await chromium.launch({headless:true,executablePath:'/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',args:['--no-sandbox']});
  const context=await browser.newContext({viewport:{width:1440,height:1000},reducedMotion:'reduce'});
  await context.addCookies([{name:info.cookie_name,value:info.cookie,url:info.url}]);
  await context.route('**/*',route=>new URL(route.request().url()).origin===info.url?route.continue():route.abort());
  const page=await context.newPage(),errors=[],deletions=[];let nativeDialogs=0;
  page.on('pageerror',e=>errors.push(e.message));page.on('dialog',dialog=>{nativeDialogs++;dialog.dismiss();});
  page.on('request',request=>{if(request.method()==='POST'&&/\/templates\/\d+\/delete$/.test(request.url()))deletions.push({path:new URL(request.url()).pathname,body:request.postDataJSON()});});
  const api=async(route,payload)=>{const response=await context.request[payload===undefined?'get':'post'](info.url+route,payload===undefined?{}:{data:payload});assert.ok(response.ok(),await response.text());return response.json();};
  const idle=()=>page.waitForFunction(()=>!document.getElementById('labRefreshButton').disabled&&document.getElementById('labSelfServicePanel').getAttribute('aria-busy')!=='true');
  const click=async id=>{await page.locator('#'+id).click();await idle();};
  const feedback=text=>page.waitForFunction(text=>document.getElementById('labSelfFeedback').textContent.includes(text),text);
  const evidence='/tmp/pc28-template-basics';fs.mkdirSync(evidence,{recursive:true});
  await page.goto(info.url+'/rule-lab');await idle();await click('labTemplatesButton');await page.locator('[data-template-ref]').first().waitFor();
  assert.match(await page.locator('#labSelfBody').innerText(),/我创建的模板/,'Library must explicitly distinguish editable templates');
  assert.match(await page.locator('#labSelfBody').innerText(),/系统预置.*不能删除/,'Why system templates have no delete action must be clear');
  const builtin=page.locator('[data-template-ref="builtin:basic_v2:big_small"]');
  assert.equal(await builtin.locator('[data-template-action="delete"]').count(),0);
  await builtin.locator('[data-template-action="view"]').click();await idle();
  assert.match(await page.locator('#labSelfBody').innerText(),/系统预置.*不能.*删除/);
  await click('labTemplateCopy');await page.locator('#labTemplateName').fill('可删除的条件模板');await click('labTemplateSave');await feedback('已保存');
  const template=(await api(base+'/templates')).templates.find(t=>t.name==='可删除的条件模板');assert.ok(template&&!template.builtin);
  assert.equal(await page.locator('[data-template-ref]').first().getAttribute('data-template-ref'),'custom:'+template.id,'My templates must appear before presets');
  const batch=(await api(base+'/batches',{name:'模板删除后仍然保留',mode:'backtest',predictor_id:1,metric:'big_small',start_date:'2026-11-26',end_date:'2026-11-26',condition_selection:{template_ref:template.ref,expected_revision:template.revision}})).batch;
  const frozen=await api(base+'/batches/'+batch.id);
  const row=page.locator('[data-template-ref="custom:'+template.id+'"]');await row.locator('[data-template-action="view"]').click();await idle();
  await page.locator('#labTemplateForm [data-template-action="delete"]').click();await page.locator('[data-template-delete-confirm]').waitFor();
  assert.equal(deletions.length,0,'Opening delete confirmation is read-only');
  await page.locator('[data-template-delete-cancel]').click();assert.equal(await page.locator('[data-template-delete-confirm]').count(),0);assert.equal(deletions.length,0);
  assert.equal(await page.locator('#labTemplateForm [data-template-action="delete"]').evaluate(n=>n===document.activeElement),true);
  await page.locator('#labTemplateForm [data-template-action="delete"]').click();
  // Server accepts delete; a lost response must keep the original request ID.
  await page.route(info.url+base+'/templates/'+template.id+'/delete',async route=>{await route.fetch();await route.abort('failed');},{times:1});
  await page.locator('[data-template-delete-confirm]').click();await idle();await feedback('原请求');assert.equal(deletions.length,1);
  await page.locator('[data-template-delete-confirm]').click();await idle();await feedback('模板已删除');assert.equal(deletions.length,2);assert.deepEqual(deletions[0],deletions[1]);
  assert.equal((await api(base+'/templates')).templates.some(t=>t.id===template.id),false);
  assert.deepEqual(await api(base+'/batches/'+batch.id),frozen,'Template deletion must preserve existing definitions and results');
  // Risk templates have the same visible delete affordance, including the editor.
  await page.getByRole('button',{name:'风控模板',exact:true}).click();await click('labTemplateNew');
  await page.locator('#labTemplateName').fill('可删除的风控模板');await click('labTemplateSave');await feedback('已保存');
  const risk=(await api(base+'/templates')).templates.find(t=>t.name==='可删除的风控模板');assert.ok(risk);
  await page.locator('[data-template-ref="custom:'+risk.id+'"] [data-template-action="edit"]').click();await idle();
  await page.locator('#labTemplateDescription').fill('未保存的笔记');await page.locator('#labTemplateForm [data-template-action="delete"]').click();
  await page.locator('[data-template-delete-cancel]').click();assert.equal(await page.locator('#labTemplateDescription').inputValue(),'未保存的笔记','Cancel must preserve unsaved edits');
  await page.setViewportSize({width:390,height:844});await page.locator('#labTemplateForm [data-template-action="delete"]').click();
  await page.locator('[data-template-delete-confirm]').scrollIntoViewIfNeeded();
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
  assert.equal(await page.locator('#labSelfServicePanel').evaluate(n=>n.scrollWidth<=n.clientWidth),true);
  assert.ok((await page.locator('[data-template-delete-confirm]').boundingBox()).height>=44);
  await page.locator('#labSelfServicePanel').screenshot({path:path.join(evidence,'delete-mobile.png')});
  await page.locator('[data-template-delete-confirm]').click();await idle();await feedback('模板已删除');
  assert.equal((await api(base+'/templates')).templates.some(t=>t.id===risk.id),false);
  await page.getByRole('button',{name:'条件模板',exact:true}).click();await idle();await page.locator('#labSelfServicePanel').screenshot({path:path.join(evidence,'library-mobile.png')});
  assert.equal(nativeDialogs,0,'Template deletion must use the in-page confirmation');assert.deepEqual(errors,[]);
  console.log('Template management Chromium passed: explicit preset restrictions, copy, custom-first listing, view/edit delete, cancel preserves draft, lost-response retry, both template kinds, frozen experiments unchanged, mobile and zero native dialogs/errors.');
 }finally{if(browser)await browser.close();fixture.stdin.end();fixture.kill('SIGTERM');}
})().catch(error=>{console.error(error);process.exitCode=1;});
