/* Actual WSGI/SQLite + real page JS in jsdom. No browser or user profile. */
const assert=require('node:assert/strict');
const path=require('node:path');
const {spawn}=require('node:child_process');
const {JSDOM,VirtualConsole}=require(process.env.PC28_UI_JSDOM||'/tmp/pc28-draw-trigger-ui-check/node_modules/jsdom');
const root=path.resolve(__dirname,'..');
const preferenceKey='ruleLabObservation:v1';
async function until(check,label,timeout=15000){const end=Date.now()+timeout;while(Date.now()<end){if(await check())return;await new Promise(r=>setTimeout(r,20));}throw new Error('Timed out: '+label);}
(async()=>{
 const server=spawn(process.env.PYTHON||'python3',['-u','-m','tests.rule_lab_observation_ui_server'],{cwd:root,stdio:['pipe','pipe','pipe']});
 let stderr='',dom;server.stderr.on('data',x=>stderr+=x);
 try{
  const info=await new Promise((resolve,reject)=>{let buffer='';const timer=setTimeout(()=>reject(new Error('fixture timeout '+stderr)),20000);server.once('exit',code=>{clearTimeout(timer);reject(new Error('fixture '+code+' '+stderr));});server.stdout.on('data',x=>{buffer+=x;if(buffer.includes('\n')){clearTimeout(timer);resolve(JSON.parse(buffer.split('\n')[0]));}});});
  const requests=[],errors=[],cookie=info.cookie_name+'='+info.cookie;
  let delayMonths=null,releaseDelay=null,failMonths=null,denyMonths=null,autoWork=false,autoRefreshCallback=null;
  const api=async(url,options={})=>{
   const target=new URL(url,info.url);assert.equal(target.origin,info.url);assert.equal(options.method||'GET','GET','Observation controls must not write to any API');
   requests.push(target.pathname+target.search);
   const response=await fetch(target,{...options,headers:{...(options.headers||{}),Cookie:cookie}});
   if(denyMonths!==null&&target.searchParams.get('observation_months')===denyMonths)return new Response(JSON.stringify({error:'登录状态已变化'}),{status:403,headers:{'Content-Type':'application/json'}});
   if(failMonths!==null&&target.searchParams.get('observation_months')===failMonths)return new Response(JSON.stringify({error:'测试读取中断'}),{status:503,headers:{'Content-Type':'application/json'}});
   if(delayMonths!==null&&target.searchParams.get('observation_months')===delayMonths){await new Promise(resolve=>{const previous=releaseDelay;releaseDelay=()=>{if(previous)previous();resolve();};});}
   if(autoWork&&target.pathname==='/api/platform/rule-lab'){const data=await response.json();data.batches.forEach(b=>{b.continuation={...(b.continuation||{}),capture_enabled:true};});return new Response(JSON.stringify(data),{status:200,headers:{'Content-Type':'application/json'}});}
   return response;
  };
  const get=async url=>{const r=await api(url);assert.equal(r.status,200);return r.json();};
  const original=await get('/api/platform/rule-lab/batches/'+info.batch_id);
  const html=await(await api('/rule-lab')).text(),script=await(await api('/assets/rule_lab.js')).text();
  async function openPage(saved,denyStorage=false){
   if(dom)dom.window.close();const vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error.message));
   dom=new JSDOM(html,{url:info.url+'/rule-lab',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
   const w=dom.window;w.fetch=api;w.AbortController=AbortController;w.structuredClone=structuredClone;w.HTMLElement.prototype.scrollIntoView=function(){};w.crypto.randomUUID=require('node:crypto').randomUUID;w.setInterval=fn=>{autoRefreshCallback=fn;return 1;};
   if(saved!==undefined)w.localStorage.setItem(preferenceKey,saved);
   if(denyStorage)Object.defineProperty(w,'localStorage',{get(){throw new Error('storage disabled');}});
   w.eval(script);await until(()=>!w.document.getElementById('labRefreshButton').disabled,'initial page');
   const selector=w.document.getElementById('labBatchSelect');selector.value=String(info.batch_id);selector.dispatchEvent(new w.Event('change',{bubbles:true}));
   await until(()=>!w.document.getElementById('labRefreshButton').disabled&&w.document.getElementById('labResultTitle').textContent===original.batch.name,'base selected');
  }
  await openPage();
  const el=id=>{const value=dom.window.document.getElementById(id);assert.ok(value,'Missing '+id);return value;};
  const fire=(id,type='input')=>el(id).dispatchEvent(new dom.window.Event(type,{bubbles:true,cancelable:true}));
  const fill=(id,value)=>{el(id).value=String(value);fire(id);};
  const check=(id,value)=>{el(id).checked=value;fire(id,'change');};
  const apply=async()=>{fire('labObservationForm','submit');await until(()=>!el('labObservationApply').disabled&&!el('labRefreshButton').disabled,'observation applied');};
  const rows=()=>[...el('labResultsBody').querySelectorAll('tr[data-experiment-id]')];
  const observationRequests=()=>requests.filter(x=>x.includes('observation_months='));
  assert.ok(el('labObservationPanel'));
  assert.equal(el('labObservationMonths').value,'2');assert.equal(el('labObservationMinimumRoi').value,'3');
  assert.equal(el('labObservationMinimumCount').value,'200');assert.equal(el('labObservationMinimumDays').value,'10');
  assert.equal(el('labObservationPartial').value,'true');
  assert.equal(rows().length,20);assert.match(rows()[0].textContent,/阶段达标/);assert.match(el('labResultMeta').textContent,/2026-11-27/);
  assert.match(rows()[1].textContent,/3\.00%/);assert.match(rows()[1].textContent,/未达标/,'2.995% rounded display must not satisfy 3%');
  assert.match(rows()[2].textContent,/样本不足/);assert.match(rows()[3].textContent,/样本不足/);
  assert.equal(dom.window.document.querySelector('#labResultTitle img'),null);
  assert.ok(observationRequests().some(x=>x.includes('/studies/')&&x.includes('observation_months=2')));
  assert.match(el('labStudyRows').textContent,/阶段达标/);
  const localRequests=requests.length;
  check('labObservationMatchesOnly',true);await apply();assert.equal(requests.length,localRequests);
  assert.equal(rows().length,20);assert.match(el('labResultPageInfo').textContent,/共 25/);assert.match(el('labStudyPageInfo').textContent,new RegExp('共 '+25*info.child_ids.length));
  el('labResultNext').click();assert.equal(rows().length,5);assert.equal(el('labResultNext').disabled,true);
  fill('labObservationMaximumSpread',2);await apply();assert.equal(el('labResultPrevious').disabled,true,'Filtering resets pagination');
  assert.match(el('labResultPageInfo').textContent,/共 20/,'2.004pp must fail 2pp despite rounded 2.00');assert.equal(el('labResultNext').disabled,true);
  assert.match(el('labObservationActiveSummary').textContent,/差距.*2\.00|2\.00.*百分点/,'Collapsed summary must expose active optional limits');
  assert.equal(requests.length,localRequests);
  check('labObservationMatchesOnly',false);fill('labObservationMaximumSpread','');fill('labObservationMonths',1);fill('labObservationPartial','false');await apply();
  assert.match(rows()[0].textContent,/完整月达标/);assert.ok(rows()[0].querySelector('.lab-observation-match'));
  assert.equal(rows()[0].querySelector('.lab-observation-match').dataset.highlight,'red');
  const colorRequests=requests.length;fill('labObservationHighlight','blue');await apply();assert.equal(requests.length,colorRequests);
  assert.equal(rows()[0].querySelector('.lab-observation-match').dataset.highlight,'blue');
  const saved=dom.window.localStorage.getItem(preferenceKey);assert.ok(saved);await openPage(saved);assert.equal(el('labObservationMonths').value,'1');assert.equal(el('labObservationHighlight').value,'blue');assert.match(rows()[0].textContent,/完整月达标/);
  fill('labObservationMonths',2);await apply();assert.match(rows()[0].textContent,/历史不足/);assert.match(rows()[0].querySelector('.lab-observation-tag').title,/1\s*\/\s*2/);
  check('labObservationMatchesOnly',true);await apply();assert.equal(rows().length,0);assert.match(el('labResultsBody').textContent,/没有符合|无符合/);assert.equal(el('labResultNext').disabled,true);assert.equal(el('labResultPrevious').disabled,true);
  const settingsBefore=dom.window.localStorage.getItem(preferenceKey),invalidRequests=requests.length;
  fill('labObservationMonths',13);fire('labObservationForm','submit');assert.match(el('labObservationFeedback').textContent,/月份|月数/);assert.equal(el('labObservationMonths').getAttribute('aria-invalid'),'true');assert.equal(requests.length,invalidRequests);assert.equal(dom.window.localStorage.getItem(preferenceKey),settingsBefore);
  fill('labObservationMonths',2);fill('labObservationMinimumRoi','1e300');fire('labObservationForm','submit');assert.match(el('labObservationFeedback').textContent,/收益率/);assert.equal(requests.length,invalidRequests);
  el('labObservationReset').click();await until(()=>!el('labObservationApply').disabled&&!el('labRefreshButton').disabled,'reset');assert.equal(el('labObservationMonths').value,'2');assert.equal(el('labObservationMatchesOnly').checked,false);assert.equal(el('labObservationHighlight').value,'red');assert.match(rows()[0].textContent,/阶段达标/);
  // Responses for an earlier window must never win a race with a newer apply.
  delayMonths='3';fill('labObservationMonths',3);fire('labObservationForm','submit');await until(()=>releaseDelay,'held first window response');
  delayMonths=null;fill('labObservationMonths',1);fill('labObservationPartial','false');fire('labObservationForm','submit');
  await until(()=>!el('labObservationApply').disabled&&rows()[0]?.textContent.includes('完整月达标'),'newer window applied');
  releaseDelay();releaseDelay=null;await new Promise(r=>setTimeout(r,60));assert.equal(el('labObservationMonths').value,'1');assert.match(rows()[0].textContent,/完整月达标/);assert.doesNotMatch(el('labObservationActiveSummary').textContent,/近3个月/);
  // Failed window reads retain and label the prior actual display settings.
  failMonths='6';fill('labObservationMonths',6);fire('labObservationForm','submit');await until(()=>!el('labObservationApply').disabled,'failed read');assert.match(el('labObservationFeedback').textContent,/读取|失败|中断/);assert.match(el('labObservationActiveSummary').textContent,/1个月/);assert.match(rows()[0].textContent,/完整月达标/);failMonths=null;
  el('labObservationReset').click();await until(()=>!el('labObservationApply').disabled&&!el('labRefreshButton').disabled,'reset after failure');
  const optionalRequests=requests.length;
  fill('labObservationMaximumMonthlyDrawdownUnits',0);await apply();assert.match(rows()[0].textContent,/收益达标.*需关注/);assert.match(rows()[0].querySelector('.lab-observation-tag').title,/月回撤超限/);assert.match(el('labObservationActiveSummary').textContent,/回撤.*0\.00.*注/);
  fill('labObservationMaximumMonthlyDrawdownUnits','');fill('labObservationMaximumUnderwaterDays',0);await apply();assert.match(rows()[0].querySelector('.lab-observation-tag').title,/回撤持续超限/);assert.match(el('labObservationActiveSummary').textContent,/持续.*0.*天/);
  fill('labObservationMaximumUnderwaterDays','');check('labObservationShowRuntime',true);await apply();assert.equal(JSON.parse(dom.window.localStorage.getItem(preferenceKey)).showRuntime,true);check('labObservationShowRuntime',false);await apply();assert.equal(JSON.parse(dom.window.localStorage.getItem(preferenceKey)).showRuntime,false);assert.equal(requests.length,optionalRequests);
  autoWork=true;el('labRefreshButton').click();await until(()=>!el('labRefreshButton').disabled,'running overview fixture');el('labResultTitle').focus();
  el('labAdjustObservation').click();const beforePausedRead=requests.length;autoRefreshCallback();await new Promise(r=>setTimeout(r,30));assert.equal(requests.length,beforePausedRead,'Reading a settings drawer pauses automatic refresh');
  el('labObservationClose').click();el('labResultTitle').focus();autoRefreshCallback();await until(()=>!el('labRefreshButton').disabled&&requests.length>beforePausedRead,'automatic refresh resumes');autoWork=false;
  delayMonths='3';fill('labObservationMonths',3);fire('labObservationForm','submit');await until(()=>releaseDelay,'pending request before auth change');
  delayMonths=null;dom.window.document.dispatchEvent(new dom.window.Event('platform-auth:changed'));await until(()=>!el('labRefreshButton').disabled,'auth refresh');assert.equal(el('labObservationApply').textContent,'应用规则','Auth reset clears old observation loading state');releaseDelay();releaseDelay=null;await new Promise(r=>setTimeout(r,40));
  fill('labBatchSelect',info.batch_id);fire('labBatchSelect','change');await until(()=>!el('labRefreshButton').disabled&&el('labResultTitle').textContent===original.batch.name,'base after auth');
  denyMonths='3';fill('labObservationMonths',3);fire('labObservationForm','submit');await until(()=>el('labResults').hidden,'window permission denied');assert.equal(el('labRefreshButton').disabled,false,'Denied window requests must release the page loading state');
  denyMonths=null;dom.window.document.dispatchEvent(new dom.window.Event('platform-auth:changed'));await until(()=>!el('labRefreshButton').disabled,'auth restored');fill('labBatchSelect',info.batch_id);fire('labBatchSelect','change');await until(()=>!el('labRefreshButton').disabled&&el('labResultTitle').textContent===original.batch.name,'base restored');
  // Detail and amount conversion remain the actual independent report path.
  const detail=rows()[0].querySelector('button[data-experiment-id]');assert.ok(detail);detail.click();await until(()=>el('labDetailFeedback').textContent===''&&!el('labAmountEstimateResults').hidden,'detail opened');
  const beforeAmount=requests.length;fill('labDisplayStake',10);assert.match(el('labAmountEstimateNote').textContent,/金额估算/);assert.equal(requests.length,beforeAmount);el('labCloseDetailButton').click();
  assert.deepEqual(await get('/api/platform/rule-lab/batches/'+info.batch_id),original);
  await openPage('{bad json');assert.equal(el('labObservationMonths').value,'2');assert.match(el('labObservationFeedback').textContent,/默认|设置/);
  await openPage(JSON.stringify({version:1,months:2,highlight:'url(javascript:alert(1))'}));assert.equal(el('labObservationHighlight').value,'red');
  await openPage(undefined,true);fill('labObservationHighlight','purple');await apply();assert.equal(el('labObservationHighlight').value,'purple');assert.match(el('labObservationFeedback').textContent,/本页|保存|浏览器/);
  assert.deepEqual(errors,[]);
  console.log('Observation DOM passed: real WSGI/SQLite, exact monthly ROI/spread boundary, phase/full/history/sample states, both lists, filter pagination, persisted validated settings, unavailable storage, request races/failure, immutable results and amount detail regression. No browser/profile; layout unmeasured.');
 }catch(error){console.error(stderr);throw error;}finally{if(dom)dom.window.close();server.stdin.end();}
})().catch(error=>{console.error(error);process.exitCode=1;});
