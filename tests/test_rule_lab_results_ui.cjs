/* Real WSGI plus jsdom; no browser process or user profile is opened. */
const assert=require('node:assert/strict');
const path=require('node:path');
const {spawn}=require('node:child_process');
const {JSDOM,VirtualConsole}=require(process.env.PC28_UI_JSDOM||'/tmp/pc28-draw-trigger-ui-check/node_modules/jsdom');
const root=path.resolve(__dirname,'..');
async function waitFor(check,label,timeout=10000){const end=Date.now()+timeout;while(Date.now()<end){if(await check())return;await new Promise(resolve=>setTimeout(resolve,25));}throw new Error('Timed out: '+label);}
(async()=>{
 const server=spawn(process.env.PYTHON||'python3',['-u','-m','tests.rule_lab_results_ui_server'],{cwd:root,stdio:['pipe','pipe','pipe']});
 let dom,stderr='';server.stderr.on('data',value=>stderr+=value);
 try{
  const info=await new Promise((resolve,reject)=>{let data='';const timer=setTimeout(()=>reject(new Error('fixture timeout '+stderr)),20000);server.once('exit',code=>{clearTimeout(timer);reject(new Error('fixture exit '+code+' '+stderr));});server.stdout.on('data',value=>{data+=value;if(data.includes('\n')){clearTimeout(timer);resolve(JSON.parse(data.split('\n')[0]));}});});
  const errors=[],requests=[],cookie=info.cookie_name+'='+info.cookie;
  const api=async(url,options={})=>{const target=new URL(url,info.url);assert.equal(target.origin,info.url);requests.push({path:target.pathname,method:options.method||'GET'});return fetch(target,{...options,headers:{...(options.headers||{}),Cookie:cookie}});};
  const html=await(await api('/rule-lab')).text();
  const virtualConsole=new VirtualConsole();virtualConsole.on('jsdomError',error=>errors.push(error.message));
  dom=new JSDOM(html,{url:info.url+'/rule-lab',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole});
  const w=dom.window,d=w.document;w.fetch=api;w.AbortController=AbortController;w.structuredClone=structuredClone;w.URL.createObjectURL=()=> 'blob:test';w.URL.revokeObjectURL=()=>{};w.HTMLElement.prototype.scrollIntoView=function(){};w.crypto.randomUUID=require('node:crypto').randomUUID;
  w.eval(await(await api('/assets/rule_lab.js')).text());
  await waitFor(()=>!d.getElementById('labRefreshButton').disabled,'initial page');
  assert.equal(d.getElementById('labRecentBody'),null,'Results page must not offer retained per-bet history');
  assert.equal(d.getElementById('labRoundsBody'),null,'Results page must not offer closed-round history');
  assert.equal(d.getElementById('labMonthlySnapshots'),null,'Results page must not offer historical report revisions');
  assert.match(d.getElementById('labBudgetDescription').textContent,/只保留.*结果|保留.*成绩/);
  assert.doesNotMatch(d.getElementById('labBudgetDescription').textContent,/明细保留.*天/);
  assert.equal(requests.filter(item=>item.method!=='GET').length,0);
  const get=async route=>{const response=await api(route);assert.equal(response.status,200,await response.clone().text());return response.json();};
  const post=async(route,value={})=>{const response=await api(route,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(value)});assert.ok(response.ok,await response.clone().text());return response.json();};
  const fire=(id,type)=>d.getElementById(id).dispatchEvent(new w.Event(type,{bubbles:true,cancelable:true}));
  const fill=(id,value)=>{d.getElementById(id).value=String(value);fire(id,'input');};
  const submit=id=>fire(id,'submit');
  const refresh=async()=>{d.getElementById('labRefreshButton').click();await waitFor(()=>!d.getElementById('labRefreshButton').disabled,'refresh');};
  const openFirst=async()=>{const button=[...d.querySelectorAll('#labResultsBody tr:first-child button')].find(item=>item.textContent==='详情');assert.ok(button);button.click();await waitFor(()=>d.querySelector('#labMonthlyBody tr')&&d.getElementById('labDetailFeedback').textContent==='','detail results');};
  const original=await get('/api/platform/rule-lab/batches/'+info.batch_id);
  assert.equal(original.experiments[0].settled_count,6);
  await openFirst();assert.match(d.getElementById('labResultRetention').textContent,/当前待结算 0 笔/);
  assert.match(d.getElementById('labPerformanceSummary').textContent,/最长连续亏损笔数/);
  assert.match(d.getElementById('labDetailStats').textContent,/有效完成轮/);
  fill('labInitialCapital',1000);assert.match(d.getElementById('labCapitalSummary').textContent,new RegExp((1000+original.experiments[0].net_profit).toFixed(2).replace('.', '\\.')));
  const beforeEstimate=requests.length;fill('labDisplayStake',10);assert.match(d.getElementById('labAmountEstimateNote').textContent,/估算/);assert.match(d.getElementById('labPerformanceSummary').textContent,/\+20.00/);assert.equal(requests.length,beforeEstimate);
  d.getElementById('labAmountReset').click();assert.equal(d.getElementById('labDisplayStake').value,'1');
  assert.deepEqual((await get('/api/platform/rule-lab/batches/'+info.batch_id)).experiments,original.experiments);
  d.getElementById('labManageBatchButton').click();await waitFor(()=>!d.getElementById('labRenameSave').disabled,'management loaded');fill('labManagementName','修改名称 <img onerror=alert(1)>');submit('labRenameForm');await waitFor(()=>d.getElementById('labResultTitle').textContent.includes('修改名称'),'renamed');assert.equal(d.querySelector('#labResultTitle img'),null);d.getElementById('labManagementClose').click();
  const childContinuation=await get('/api/platform/rule-lab/batches/'+info.batch_id+'/continuation-preview');assert.equal(childContinuation.eligible,true,JSON.stringify(childContinuation));d.getElementById('labContinuationButton').click();await waitFor(()=>!d.getElementById('labContinuationPanel').hidden&&d.getElementById('labContinuationSummary').textContent.length>0,'continuation preview');assert.equal(d.getElementById('labContinuationCapture').checked,false);assert.equal(d.getElementById('labContinuationCompute').checked,false);assert.match(d.getElementById('labContinuationCoverage').textContent,/仅含计算依赖范围/);d.getElementById('labContinuationClose').click();
  await openFirst();await post('/__fixture/remove-source-history');const afterRemoval=requests.length;fill('labDisplayStake',5);assert.match(d.getElementById('labPerformanceSummary').textContent,/\+10.00/);assert.equal(requests.length,afterRemoval);
  assert.equal(requests.filter(item=>item.path.includes('/replay')).length,0);
  d.getElementById('labCloseDetailButton').click();d.getElementById('labStoragePanel').open=true;fire('labStoragePanel','toggle');await waitFor(()=>d.getElementById('labStorageSummary').children.length>0,'storage');assert.match(d.getElementById('labStorageExplanation').textContent,/已结算逐笔及已结束轮次不保留/);assert.match(d.getElementById('labStorageSummary').textContent,/历史冗余整理/);
  const exported=await api('/api/platform/rule-lab/batches/'+info.batch_id+'/export.csv');assert.equal(exported.status,200);assert.match(await exported.text(),/net_profit/);
  // Use the actual generator and management deletion flow for a new independent batch.
  fill('labName','DOM新条件验证');fill('labRiskMode','legacy');fire('labRiskMode','change');const live=d.querySelector('input[name="mode"][value="live"]');live.checked=true;live.dispatchEvent(new w.Event('change',{bubbles:true}));submit('labBatchForm');
  await waitFor(()=>{if(errors.length)throw new Error(errors.join(' | '));const message=d.getElementById('labFormFeedback').textContent;if(d.getElementById('labFormFeedback').classList.contains('is-error'))throw new Error(message);return d.getElementById('labResultTitle').textContent==='DOM新条件验证';},'new generated batch');
  const generated=(await get('/api/platform/rule-lab')).batches.find(item=>item.name==='DOM新条件验证');assert.equal(generated.experiment_count,50);assert.equal(generated.status,'paused');
  d.getElementById('labManageBatchButton').click();await waitFor(()=>!d.getElementById('labManagementDelete').disabled,'new batch management');d.getElementById('labManagementDelete').click();await waitFor(()=>!d.getElementById('labDeleteForm').hidden,'delete preview');
  assert.equal(d.getElementById('labDeleteConfirm').disabled,true);fill('labDeleteConfirmName',generated.name);assert.equal(d.getElementById('labDeleteConfirm').disabled,false);submit('labDeleteForm');await waitFor(()=>d.getElementById('statusMessage').textContent.includes('删除')||d.getElementById('labManagementFeedback').textContent.includes('删除'),'delete accepted');
  await post('/__fixture/tick');await refresh();assert.equal((await get('/api/platform/rule-lab')).batches.some(item=>item.id===generated.id),false);
  assert.deepEqual(errors,[]);
  console.log('Results DOM flow passed: real WSGI/SQLite results, capital/daily/monthly reports, local amount estimation, original parity, rename/XSS, continuation defaults, source-independent amounts, storage and CSV. No browser/profile opened; layout not measured.');
 }catch(error){console.error(stderr);throw error;}finally{if(dom)dom.window.close();server.stdin.end();}
})().catch(error=>{console.error(error);process.exitCode=1;});
