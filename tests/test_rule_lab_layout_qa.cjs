/* Independent layout/interaction QA: actual WSGI + SQLite + page JS in jsdom.
 * No browser process/profile is opened; CSS structure checks are not pixel tests.
 */
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const {spawn}=require('node:child_process');
const {JSDOM,VirtualConsole}=require(process.env.PC28_UI_JSDOM||'/tmp/pc28-draw-trigger-ui-check/node_modules/jsdom');
const root=path.resolve(__dirname,'..');
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(check,label,timeout=15000){const deadline=Date.now()+timeout;while(Date.now()<deadline){if(await check())return;await sleep(20);}throw new Error('Timed out: '+label);}
function textOf(node){return node.textContent.replace(/\s+/g,' ').trim();}
function visible(node){for(let current=node;current;current=current.parentElement){if(current.hidden||current.getAttribute('aria-hidden')==='true'||current.ownerDocument.defaultView.getComputedStyle(current).display==='none')return false;}return true;}
(async()=>{
 const server=spawn(process.env.PYTHON||'python3',['-u','-m','tests.rule_lab_observation_ui_server'],{cwd:root,stdio:['pipe','pipe','pipe']});
 let stderr='',dom;server.stderr.on('data',part=>stderr+=part);
 try{
  const info=await new Promise((resolve,reject)=>{let output='';const timeout=setTimeout(()=>reject(new Error('Fixture timeout '+stderr)),20000);server.once('exit',code=>{clearTimeout(timeout);reject(new Error('Fixture exit '+code+' '+stderr));});server.stdout.on('data',part=>{output+=part;if(output.includes('\n')){clearTimeout(timeout);resolve(JSON.parse(output.split('\n')[0]));}});});
  const cookie=info.cookie_name+'='+info.cookie,requests=[],errors=[],blobs=[];
  const held=new Map();let holdPath=null;
  const api=async(url,options={})=>{
   const target=new URL(url,info.url);assert.equal(target.origin,info.url);assert.equal(options.method||'GET','GET','Layout/display controls must not write to APIs');
   requests.push({path:target.pathname,query:target.search});
   const response=await fetch(target,{...options,headers:{...(options.headers||{}),Cookie:cookie}});
   if(target.pathname===holdPath){await new Promise(resolve=>held.set(target.pathname,resolve));}
   return response;
  };
  const get=async route=>{const response=await api(route);assert.equal(response.status,200);return response.json();};
  const original=await get('/api/platform/rule-lab/batches/'+info.batch_id);
  const rawStudy=await get('/api/platform/rule-lab/studies/'+info.study_id);
  const html=await(await api('/rule-lab')).text(),script=await(await api('/assets/rule_lab.js')).text();
  const css=fs.readFileSync(path.join(root,'src/pc28touzhu/ui/rule_lab.css'),'utf8');
  const vc=new VirtualConsole();vc.on('jsdomError',error=>errors.push(error.message));
  dom=new JSDOM(html,{url:info.url+'/rule-lab',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});
  const w=dom.window,d=w.document;
  const layoutStyle=d.createElement('style');layoutStyle.textContent=css;d.head.append(layoutStyle);
  w.fetch=api;w.AbortController=AbortController;w.structuredClone=structuredClone;w.Blob=Blob;w.crypto.randomUUID=require('node:crypto').randomUUID;
  w.URL.createObjectURL=blob=>{blobs.push(blob);return 'blob:qa';};w.URL.revokeObjectURL=()=>{};w.HTMLAnchorElement.prototype.click=function(){};w.HTMLElement.prototype.scrollIntoView=function(){};
  let scrollY=185;Object.defineProperty(w,'scrollY',{get:()=>scrollY});w.scrollTo=(x,y)=>{scrollY=typeof x==='object'?Number(x.top||0):Number(y||0);};
  w.eval(script);
  const el=id=>{const node=d.getElementById(id);assert.ok(node,'Missing '+id);return node;};
  const fire=(node,type='change')=>node.dispatchEvent(new w.Event(type,{bubbles:true,cancelable:true}));
  const key=(node,value,shift=false)=>node.dispatchEvent(new w.KeyboardEvent('keydown',{key:value,shiftKey:shift,bubbles:true,cancelable:true}));
  const fill=(id,value)=>{el(id).value=String(value);fire(el(id),'input');};
  const choose=(id,value)=>{el(id).value=String(value);fire(el(id));};
  const rows=id=>[...el(id).querySelectorAll('tr[data-experiment-id]')];
  const panelVisible=(active,inactive)=>{assert.equal(visible(el(active)),true,active+' should be visible');assert.equal(visible(el(inactive)),false,inactive+' should be hidden');if(el(inactive).hidden)assert.equal(w.getComputedStyle(el(inactive)).display,'none','CSS must honor hidden '+inactive);const hiddenSelection=active==='labResults'?'labStudySelection':'labBatchSelection',shownSelection=active==='labResults'?'labBatchSelection':'labStudySelection';assert.equal(w.getComputedStyle(el(hiddenSelection)).display,'none','CSS must hide inactive owner selector '+hiddenSelection);assert.notEqual(w.getComputedStyle(el(shownSelection)).display,'none');};
  const waitReady=()=>until(()=>!el('labRefreshButton').disabled,'page ready');
  const release=route=>{const done=held.get(route);assert.ok(done,'No held request '+route);held.delete(route);done();};
  const assertFiveColumns=(tableId,bodyId)=>{
   const headers=[...el(tableId).querySelectorAll('thead th')];assert.equal(headers.length,5,'Overview must have exactly five columns');
   for(const row of rows(bodyId)){
    assert.equal(row.children.length,5);assert.equal(row.querySelectorAll('button,a').length,1,'One condition/detail entry per row');
    assert.equal(row.querySelectorAll('details,summary').length,0,'Overview must not contain nested report disclosures');
    const entry=row.querySelector('button[data-experiment-id]');assert.ok(entry);assert.equal(entry.type,'button');assert.notEqual(row.getAttribute('role'),'button','Avoid nested button semantics');
    for(const cell of row.children){assert.ok(cell.childElementCount<=2,'Overview cells contain at most two short blocks');assert.ok(cell.querySelectorAll('br').length<=1);}
   }
  };
  await waitReady();
  // Effective RED on the previous design: a results-first tab bar does not exist.
  assert.ok(el('labResultTabs'));
  assert.equal(el('labTabExperiments').getAttribute('aria-selected'),'true');
  choose('labBatchSelect',info.batch_id);await waitReady();
  assert.equal(el('labBatchSelect').value,String(info.batch_id));panelVisible('labResults','labStudyResults');
  assert.equal(visible(el('labGenerator')),false);assert.equal(visible(el('labSystemPanel')),false);assert.equal(visible(el('labObservationPanel')),false);
  assert.equal(el('mainContent').contains(el('labGenerator')),false,'Dialogs must sit outside the inert background');
  assert.equal(el('mainContent').contains(el('labExperimentDetail')),false);
  for(const id of ['labExperimentDetail','labObservationPanel','labGenerator','labSystemPanel','labManagementPanel','labContinuationPanel']){
   assert.ok(el(id).closest('.lab-page'),id+' must retain the lab typography, CSS variables and form rules outside main');
   if(el(id).hidden)assert.equal(w.getComputedStyle(el(id)).display,'none','CSS must not override hidden '+id);
  }
  assertFiveColumns('labResultsTable','labResultsBody');
  assert.equal(d.querySelectorAll('.lab-batch-select').length,0,'Do not leave a second hidden batch selector interface');
  assert.equal(el('labNewExperiment').type,'button');assert.equal(el('labSystemToggle').type,'button');assert.ok(visible(el('labAdjustObservation')));
  assert.equal(d.querySelector('#labResultTitle img'),null,'Untrusted names remain text');

  // Observe October only, making window PnL differ from the all-history total.
  el('labAdjustObservation').click();assert.equal(el('labObservationPanel').getAttribute('role'),'dialog');assert.equal(el('labObservationPanel').getAttribute('aria-modal'),'true');
  fill('labObservationMonths',1);choose('labObservationPartial','false');fire(el('labObservationForm'),'submit');
  await until(()=>!el('labObservationApply').disabled&&!el('labRefreshButton').disabled,'October observation applied');
  if(visible(el('labObservationPanel')))el('labObservationClose').click();
  const october=await get('/api/platform/rule-lab/batches/'+info.batch_id+'?observation_months=1&observation_partial=false');
  const first=october.experiments[0],observed=first.observation;
  assert.notEqual(observed.summary.net_cents/100,first.net_profit,'Fixture must distinguish selected window from cumulative money');
  let row=rows('labResultsBody')[0];
  const netText=(observed.summary.net_cents>0?'+':'')+(observed.summary.net_cents/100).toFixed(2);
  assert.match(textOf(row.children[4]),new RegExp(netText.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));
  assert.doesNotMatch(textOf(row.children[4]),new RegExp(first.net_profit.toFixed(2).replace('.','\\.')),'Window PnL must not use cumulative result');
  const beforeProfitSort=requests.length;choose('labSort','profit');
  const expectedProfitOrder=october.experiments.slice().sort((a,b)=>b.observation.summary.net_cents-a.observation.summary.net_cents||a.id-b.id).slice(0,20).map(item=>String(item.id));
  assert.deepEqual(rows('labResultsBody').map(item=>item.dataset.experimentId),expectedProfitOrder,'Profit sorting must use the displayed observation window');
  assert.equal(requests.length,beforeProfitSort);choose('labSort','paired');
  const baseRequests=requests.length;
  el('labViewMonthly').click();assert.equal(el('labViewMonthly').getAttribute('aria-pressed'),'true');
  const month=observed.months[0],headerTexts=[...el('labResultsTable').querySelectorAll('thead th')].map(textOf);
  const octoberColumn=headerTexts.findIndex(value=>value.includes('2026-10'));assert.ok(octoberColumn>=0);
  assert.match(textOf(rows('labResultsBody')[0].children[octoberColumn]),new RegExp(month.roi_percent.toFixed(2).replace('.','\\.')+'%'));
  choose('labMonthlyMetricSelect','drawdown');
  assert.match(textOf(rows('labResultsBody')[0].children[octoberColumn]),new RegExp((month.daily_close_drawdown_cents/100).toFixed(2).replace('.','\\.')));
  el('labViewRisk').click();assert.ok(el('labResultsTable').querySelectorAll('thead th').length<=6);
  assert.match(textOf(rows('labResultsBody')[0]),new RegExp((observed.summary.net_without_best_three_days_cents/100).toFixed(2).replace('.','\\.')));
  el('labViewOverview').click();assertFiveColumns('labResultsTable','labResultsBody');assert.equal(requests.length,baseRequests,'Three views and month metric switch are purely local');

  // Native condition button is the one keyboard focus target; row empty space also opens.
  el('labResultNext').click();const pageText=el('labResultPageInfo').textContent;
  const listScroll=el('labResultsTable').closest('.lab-table-scroll');listScroll.scrollLeft=73;
  row=rows('labResultsBody')[0];const entry=row.querySelector('button[data-experiment-id]');entry.focus();row.children[4].click();
  await until(()=>visible(el('labExperimentDetail'))&&el('labDetailFeedback').textContent===''&&!el('labAmountEstimateResults').hidden,'row detail');
  assert.equal(el('labExperimentDetail').getAttribute('role'),'dialog');assert.equal(el('labExperimentDetail').getAttribute('aria-modal'),'true');
  assert.ok(el('labExperimentDetail').contains(d.activeElement));
  assert.equal(el('labExperimentDetail').getAttribute('aria-labelledby'),'labDetailTitle');
  assert.ok(el('mainContent').inert||el('mainContent').hasAttribute('inert')||el('mainContent').getAttribute('aria-hidden')==='true','Background is unavailable during modal interaction');
  const focusables=[...el('labExperimentDetail').querySelectorAll('button:not(:disabled),input:not(:disabled),select:not(:disabled),summary,[tabindex="0"]')].filter(visible);
  assert.ok(focusables.length>=3);focusables.at(-1).focus();key(focusables.at(-1),'Tab');assert.equal(d.activeElement,focusables[0],'Tab wraps at final drawer control');
  focusables[0].focus();key(focusables[0],'Tab',true);assert.equal(d.activeElement,focusables.at(-1),'Shift+Tab wraps at first drawer control');
  const restoredFocusCalls=[],originalEntryFocus=entry.focus.bind(entry);entry.focus=options=>{restoredFocusCalls.push(options);return originalEntryFocus(options);};
  key(d.activeElement,'Escape');assert.equal(visible(el('labExperimentDetail')),false);
  assert.ok(restoredFocusCalls.length>0);assert.ok(restoredFocusCalls.every(options=>options?.preventScroll===true),'Restoring focus must never scroll the underlying list');
  assert.equal(d.activeElement,entry,'Escape restores the exact row condition entry');assert.equal(el('labResultPageInfo').textContent,pageText);assert.equal(listScroll.scrollLeft,73);assert.equal(scrollY,185);
  assert.equal(el('labTabExperiments').getAttribute('aria-selected'),'true');assert.equal(el('labViewOverview').getAttribute('aria-pressed'),'true');

  // Closing a loading drawer cannot let a delayed response reopen it.
  const heldRow=rows('labResultsBody')[0],heldId=heldRow.dataset.experimentId;
  holdPath='/api/platform/rule-lab/batches/'+info.batch_id+'/experiments/'+heldId;
  heldRow.querySelector('button').click();await until(()=>held.has(holdPath),'held detail request');
  el('labCloseDetailButton').click();assert.equal(visible(el('labExperimentDetail')),false);const stalePath=holdPath;holdPath=null;release(stalePath);await sleep(50);assert.equal(visible(el('labExperimentDetail')),false);
  // Open a different row while the older request is still held; only latest row wins.
  holdPath='/api/platform/rule-lab/batches/'+info.batch_id+'/experiments/'+heldId;
  heldRow.querySelector('button').click();await until(()=>held.has(holdPath),'old row held');
  el('labCloseDetailButton').click();
  const nextRow=rows('labResultsBody')[1],nextId=nextRow.dataset.experimentId;nextRow.querySelector('button').click();
  await until(()=>el('labDetailFeedback').textContent===''&&!el('labAmountEstimateResults').hidden,'new row loaded');
  const currentTitle=el('labDetailTitle').textContent;const oldPath=holdPath;holdPath=null;release(oldPath);await sleep(50);
  assert.equal(el('labDetailTitle').textContent,currentTitle);assert.ok(requests.some(x=>x.path.endsWith('/experiments/'+nextId)));
  el('labDrawerBackdrop').click();assert.equal(visible(el('labExperimentDetail')),false);assert.equal(d.activeElement,nextRow.querySelector('button'));

  // A study child's detail has its own policy, while the underlying batch stays selected.
  el('labTabStudies').click();choose('labStudySelect',info.study_id);await waitReady();panelVisible('labStudyResults','labResults');
  assert.equal(el('labBatchSelect').value,String(info.batch_id));
  choose('labStudyVariant','round_tp_half');choose('labStudySettlement','pc28_netdisk_regular');
  el('labViewRisk').click();const studyFilter=el('labStudyVariant').value,selectedBatch=el('labBatchSelect').value;
  const selectedStudyRow=rows('labStudyRows')[0],childId=selectedStudyRow.dataset.experimentId;
  const expectedChild=rawStudy.rows.find(item=>String(item.experiment_id)===childId);assert.ok(expectedChild);assert.equal(expectedChild.risk_policy.round.profit_target,5);
  const childEntry=selectedStudyRow.querySelector('button[data-experiment-id]');childEntry.focus();childEntry.click();
  await until(()=>visible(el('labExperimentDetail'))&&el('labDetailFeedback').textContent===''&&!el('labAmountEstimateResults').hidden,'study detail');
  assert.ok(requests.some(x=>x.path==='/api/platform/rule-lab/batches/'+expectedChild.batch_id+'/experiments/'+childId));
  assert.equal(el('labBatchSelect').value,selectedBatch);assert.equal(el('labTabStudies').getAttribute('aria-selected'),'true');assert.equal(el('labViewRisk').getAttribute('aria-pressed'),'true');
  assert.match(el('labDetailMeta').textContent,/单轮止盈 5/);
  assert.ok(visible(el('labDetailComparison')),'Study baseline differences remain available inside detail');
  const comparison=Object.fromEntries([...el('labDetailComparison').querySelectorAll('dt')].map(node=>[node.textContent,node.nextElementSibling.textContent]));
  for(const [label,value] of [['Δ净利',expectedChild.delta_net_profit],['ΔROI',expectedChild.delta_roi_pp],['Δ回撤',expectedChild.delta_drawdown]]){
   const metric=Object.keys(comparison).find(key=>key.includes(label));assert.ok(metric,'Missing study comparison '+label);
   assert.match(comparison[metric],new RegExp(Number(value).toFixed(2).replace('.','\\.')));
  }
  const beforeAmount=requests.length;fill('labDisplayStake',10);assert.match(el('labAmountRiskPreview').textContent,/单轮止盈 50/);assert.doesNotMatch(el('labAmountRiskPreview').textContent,/单轮止盈 100/);assert.equal(requests.length,beforeAmount);
  el('labMonthlyExport').click();assert.ok(blobs.length);assert.match(await blobs.at(-1).text(),/estimate/);
  key(d.activeElement,'Escape');assert.equal(visible(el('labExperimentDetail')),false);assert.equal(d.activeElement,childEntry);assert.equal(el('labStudyVariant').value,studyFilter);assert.equal(el('labBatchSelect').value,selectedBatch);assert.equal(el('labViewRisk').getAttribute('aria-pressed'),'true');
  const beforeStudyViews=requests.length;el('labViewOverview').click();assertFiveColumns('labStudyTable','labStudyRows');el('labViewMonthly').click();choose('labMonthlyMetricSelect','roi');el('labViewOverview').click();assert.equal(requests.length,beforeStudyViews);

  // Creation/system/management/continuation remain reachable, without invoking mutations.
  el('labNewExperiment').click();assert.ok(visible(el('labGenerator')));assert.ok(visible(el('labBatchForm')));assert.equal(el('labGenerator').getAttribute('role'),'dialog');el('labGeneratorClose').click();
  el('labSystemToggle').click();assert.ok(visible(el('labSystemPanel')));assert.ok(visible(el('labFastButton')));assert.ok(visible(el('labStoragePanel')));el('labSystemClose').click();
  for(const [triggerId,panelId,readyId,closeId] of [['labManageStudyButton','labManagementPanel','labRenameSave','labManagementClose'],['labStudyContinuationButton','labContinuationPanel','labContinuationRefresh','labContinuationClose']]){
   const trigger=el(triggerId),focusCalls=[],originalFocus=trigger.focus.bind(trigger);trigger.focus=options=>{focusCalls.push(options);return originalFocus(options);};
   trigger.click();await until(()=>visible(el(panelId))&&!el(readyId).disabled,panelId+' opened');el(closeId).click();
   assert.ok(focusCalls.length>0);assert.ok(focusCalls.every(options=>options?.preventScroll===true),panelId+' must restore focus without scrolling');
   assert.equal(d.activeElement,trigger);assert.equal(visible(el(panelId)),false);
  }
  assert.ok(el('labStudyExportButton'));assert.ok(el('labExportButton'));
  assert.deepEqual(await get('/api/platform/rule-lab/batches/'+info.batch_id),original,'Layout interactions preserve every original report field');
  assert.deepEqual(await get('/api/platform/rule-lab/studies/'+info.study_id),rawStudy);

  // Parse real CSS declarations: this verifies adaptive intent only, not rendering.
  const media=[];for(const rule of layoutStyle.sheet.cssRules){if(rule.type===4&&/max-width/.test(rule.conditionText))media.push(rule);}
  assert.ok(media.length>0,'Mobile breakpoints must exist');
  const mobileCss=media.map(rule=>rule.cssText).join('\n');
  assert.match(mobileCss,/lab-(?:result|overview|drawer|detail)/,'Mobile styles must address results or drawer');
  assert.match(mobileCss,/(?:grid-template-columns|display:\s*grid|width:\s*(?:100%|100vw)|inset:\s*0)/,'Mobile needs deliberate layout rules');
  assert.match(css,/overflow(?:-x)?:\s*(?:auto|scroll)/,'Monthly overflow remains bounded in a scroll region');
  assert.deepEqual(errors,[]);
  console.log('Layout QA passed: real WSGI/SQLite, single results tab, five compact overview columns, selected-window PnL and month ROI/DD, zero-request views, row drawer focus/ESC/backdrop/races, independent study risk context, preserved selection/paging/scroll, reachable forms, unchanged results, mobile CSS structure. No browser/profile or pixel layout claim.');
 }catch(error){console.error(stderr);throw error;}finally{if(dom)dom.window.close();server.stdin.end();}
})().catch(error=>{console.error(error);process.exitCode=1;});
