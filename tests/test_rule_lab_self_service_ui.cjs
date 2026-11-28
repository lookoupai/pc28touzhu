/* 真实临时 API/SQLite 与页面脚本，不用生产数据库。 */
const assert=require('node:assert/strict'),path=require('node:path'),{spawn}=require('node:child_process');
const {JSDOM,VirtualConsole}=require(process.env.PC28_UI_JSDOM||'/tmp/pc28-draw-trigger-ui-check/node_modules/jsdom');
const root=path.resolve(__dirname,'..'),sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(fn,label,timeout=15000){const end=Date.now()+timeout;while(Date.now()<end){if(await fn())return;await sleep(20)}throw Error('Timeout '+label)}
(async()=>{const server=spawn(process.env.PYTHON||'python3',['-u','-m','tests.rule_lab_self_service_ui_server'],{cwd:root,stdio:['pipe','pipe','pipe']});let dom,stderr='';server.stderr.on('data',x=>stderr+=x);
try{const info=await new Promise((resolve,reject)=>{let out='';const t=setTimeout(()=>reject(Error('Fixture timeout '+stderr)),20000);server.once('exit',c=>reject(Error('Fixture exit '+c+' '+stderr)));server.stdout.on('data',x=>{out+=x;if(out.includes('\n')){clearTimeout(t);resolve(JSON.parse(out.split('\n')[0]))}})});
const requests=[],errors=[];const api=async(url,options={})=>{const target=new URL(url,info.url);assert.equal(target.origin,info.url);requests.push({path:target.pathname,method:options.method||'GET',body:options.body});return fetch(target,{...options,headers:{...(options.headers||{}),Cookie:info.cookie_name+'='+info.cookie}})};
const html=await(await api('/rule-lab')).text(),script=await(await api('/assets/rule_lab.js')).text();const vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e.message));dom=new JSDOM(html,{url:info.url+'/rule-lab',runScripts:'outside-only',pretendToBeVisual:true,virtualConsole:vc});const w=dom.window,d=w.document;
w.fetch=api;w.AbortController=AbortController;w.structuredClone=structuredClone;w.Blob=Blob;w.crypto.randomUUID=require('node:crypto').randomUUID;w.HTMLElement.prototype.scrollIntoView=()=>{};w.scrollTo=()=>{};w.confirm=()=>true;w.eval(script);
const el=id=>{const n=d.getElementById(id);assert.ok(n,'Missing '+id);return n};await until(()=>!el('labRefreshButton').disabled,'initial page');
assert.ok(d.getElementById('labTemplatesButton'),'模板库必须有用户可操作的入口');
assert.ok(d.getElementById('labQueueButton'),'普通实验必须有运行队列入口');
const extra=await api('/assets/rule_lab_self_service.js');assert.equal(extra.status,200);w.eval(await extra.text());
// 下面是新增用户路径，服务端 API 尚未就绪时在此暴露真实失败。
const fire=(n,type='change')=>n.dispatchEvent(new w.Event(type,{bubbles:true,cancelable:true}));
const fill=(id,v)=>{el(id).value=String(v);fire(el(id),'input');fire(el(id))};
const ready=()=>until(()=>!el('labSelfServicePanel').getAttribute('aria-busy')||el('labSelfServicePanel').getAttribute('aria-busy')==='false','self-service ready');
el('labTemplatesButton').click();await until(()=>d.querySelectorAll('[data-template-ref]').length>0,'template library');
el('labTemplateNew').click();fill('labTemplateName','自助测试模板');fill('labTemplateMetric','big_small');
el('labConditionAdd').click();const row=d.querySelector('[data-condition-row]');assert.ok(row);row.querySelector('[data-condition-field="threshold"]').value='7';fire(row.querySelector('[data-condition-field="threshold"]'));
fire(el('labTemplateForm'),'submit');await until(()=>el('labSelfFeedback').textContent.includes('已保存'),'template saved');
const templates=await(await api('/api/platform/rule-lab/templates?kind=condition')).json();const created=templates.templates.find(t=>t.name==='自助测试模板');assert.ok(created);assert.equal(created.condition_count,2);
const libraryRow=[...d.querySelectorAll('[data-template-ref]')].find(n=>n.textContent.includes('自助测试模板'));assert.ok(libraryRow);libraryRow.querySelector('[data-template-action="edit"]').click();fill('labTemplateName','自助测试模板已改');fire(el('labTemplateForm'),'submit');await until(()=>el('labSelfFeedback').textContent.includes('已保存'),'template edited');
el('labSelfClose').click();el('labNewExperiment').click();await until(()=>d.querySelector('[data-source-id="1"]'),'bulk source list');
for(const n of d.querySelectorAll('[data-source-id]')){n.checked=true;fire(n)}const m=d.querySelector('[data-metric-check="big_small"]');m.checked=true;fire(m);
for(const n of d.querySelectorAll('[data-metric-check]'))if(n!==m){n.checked=false;fire(n)}
fill('labBulkStart','2026-11-26');fill('labBulkEnd','2026-11-26');const select=d.querySelector('[data-condition-template="big_small"]');select.value='custom:'+created.id;fire(select);
el('labBulkPreviewButton').click();await until(()=>el('labBulkPreviewSummary').textContent.includes('8'),'bulk preview').catch(e=>{e.message+=': '+el('labSelfFeedback').textContent;throw e;});assert(!el('labBulkPreviewRows').textContent.includes('11.26'));
el('labBulkSaveDraft').click();await until(()=>el('labSelfFeedback').textContent.includes('已创建'),'bulk saved');
const overview=await(await api('/api/platform/rule-lab')).json();const born=overview.batches.filter(b=>b.template_snapshot?.id===created.id||b.name.includes('自助测试模板已改'));assert.equal(born.length,2);
assert.equal((await(await api('/__fixture/state')).json()).lab_series,0);
el('labSelfClose').click();el('labQueueButton').click();await until(()=>d.querySelectorAll('[data-draft-id]').length>0,'queue drafts');await ready();const draft=d.querySelector('[data-draft-id="'+born[0].id+'"]');assert.ok(draft);draft.querySelector('button').click();await until(()=>d.querySelector('[data-queue-owner="'+born[0].id+'"]'),'queued');await ready();const qrow=d.querySelector('[data-queue-owner="'+born[0].id+'"]');qrow.querySelector('[data-queue-action="pause"]').click();await until(()=>d.querySelector('[data-queue-owner="'+born[0].id+'"]').textContent.includes('手动暂停'),'paused');await ready();
el('labSelfClose').click();el('labCompareButton').click();await until(()=>d.querySelector('[data-compare-condition]'),'comparison choices');const conditions=[...d.querySelectorAll('[data-compare-condition]')];conditions.forEach((n,i)=>{n.checked=i===0;fire(n)});const odds=[...d.querySelectorAll('[data-compare-odds]')];odds.forEach((n,i)=>{n.checked=i===0;fire(n)});d.querySelector('[data-candidate-key="round_sl_half"]').checked=true;fire(d.querySelector('[data-candidate-key="round_sl_half"]'));el('labSelectivePreview').click();await until(()=>el('labSelectiveSummary').textContent.includes('2 个版本'),'selective preview');
assert.equal(errors.length,0,errors.join('\n'));console.log('Self-service UI passed: actual template create/edit, atomic bulk drafts, queue pause, selective single-odds comparison, no schedules, zero JS errors.');
}finally{if(dom)dom.window.close();server.stdin.end();server.kill('SIGTERM')}})().catch(e=>{console.error(e);process.exitCode=1});
