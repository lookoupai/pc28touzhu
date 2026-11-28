(function () {
    'use strict';
    let initialized=false;
    function initialize() {
        if(initialized||!window.RuleLabUI)return;initialized=true;
        const ui=window.RuleLabUI,$=id=>document.getElementById(id),base='/api/platform/rule-lab';
        const metricNames={big_small:'大小',odd_even:'单双',combo:'组合'},statusNames={queued:'等待运行',running:'运行中',paused:'手动暂停',completed:'已完成',removed:'已移出队列',error:'需要处理',resource_paused:'资源限制暂停'};
        const body=$('labSelfBody'),panel=$('labSelfServicePanel'),drafts=new Map(),receipts=new Map();let view=null,busy=false,leaveAction=null,templates=[];
        const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
        const button=(text,fn,id,primary=false)=>{const b=node('button',text,'lab-button'+(primary?' lab-button-primary':''));b.type='button';if(id)b.id=id;b.addEventListener('click',fn);return b;};
        const note=text=>node('p',text,'lab-muted');
        function input(id,type='text',value=''){const n=node('input');n.id=id;n.type=type;if(type==='checkbox')n.checked=Boolean(value);else n.value=value;return n;}
        function select(id,options,value){const n=node('select');if(id)n.id=id;options.forEach(([key,label])=>{const o=node('option',label);o.value=key;n.append(o);});if(value!==undefined)n.value=value;return n;}
        function field(label,control){const wrap=node('div',undefined,'lab-field'),caption=node('label',label);if(control.id)caption.htmlFor=control.id;else{control.id='labSelfField'+(++field.sequence);caption.htmlFor=control.id;}wrap.append(caption,control);return wrap;}field.sequence=0;
        function checkbox(text,attrs={},checked=false){const l=node('label',undefined,'lab-self-check'),c=input('', 'checkbox',checked);Object.assign(c.dataset,attrs);l.append(c,node('span',text));return l;}
        function message(text,error=false){ui.feedback('labSelfFeedback',text,error);}
        const key=t=>t.builtin?'builtin:'+t.ref.key+':'+t.metrics[0]:'custom:'+t.id;
        const clone=v=>structuredClone(v);
        function copyName(template){const names=new Set(templates.filter(t=>t.kind===template.kind).map(t=>t.name));for(let i=1;i<=101;i++){const suffix=i===1?'（副本）':'（副本'+i+'）',name=Array.from(template.name).slice(0,60-Array.from(suffix).length).join('')+suffix;if(!names.has(name))return name;}throw Error('副本名称已满，请新建并设置名称。');}
        async function readTemplates(){const response=await ui.request(base+'/templates');templates=response.templates;return templates;}
        async function run(task){
            if(busy||ui.getContext().mutating||ui.getContext().accessDenied)return;
            const locked=[...panel.querySelectorAll('input,select,textarea,button')].map(n=>[n,n.disabled]);
            busy=true;panel.setAttribute('aria-busy','true');ui.setBusy(true);locked.forEach(([n])=>n.disabled=true);
            try{return await task();}catch(error){if(error.status===401||error.status===403){ui.denyAccess();ui.feedback('statusMessage',error.message,true);}else message(error.message+(error.unknown?'；可再次提交原请求核对结果。':''),true);}
            finally{busy=false;locked.forEach(([n,was])=>{if(n.isConnected)n.disabled=was;});ui.setBusy(false);panel.setAttribute('aria-busy','false');if(view?.controls)view.controls();}
        }
        async function write(path,payload){const signature=path+JSON.stringify(payload);if(!receipts.has(signature))receipts.set(signature,crypto.randomUUID());const result=await ui.request(base+path,'POST',{...payload,request_id:receipts.get(signature)});receipts.delete(signature);return result;}
        function mark(){if(view)view.dirty=true;}
        function saveView(){if(view?.dirty){const fragment=document.createDocumentFragment();fragment.append(...body.childNodes);view.fragment=fragment;drafts.set(view.kind,view);}}
        function dismiss(restore=true){$('labSelfLeave').hidden=true;ui.dismissDrawer('labSelfServicePanel',restore);}
        function guard(restore=true,then=null){if(!view?.dirty)return true;leaveAction=()=>{dismiss(restore);if(then)then();};$('labSelfLeave').hidden=false;$('labSelfStay').focus();return false;}
        function enter(kind,title,trigger,build){
            if(busy||ui.getContext().accessDenied)return;
            const proceed=()=>{
                if(view?.kind===kind&&body.childNodes.length&&view.dirty){ui.openDrawer('labSelfServicePanel',trigger);return;}
                saveView();const prior=drafts.get(kind);body.replaceChildren();view=prior||{kind,dirty:false};$('labSelfTitle').textContent=title;message('');
                if(prior){body.append(prior.fragment);drafts.delete(kind);}else build(view);
                ui.openDrawer('labSelfServicePanel',trigger);
            };
            if(!panel.hidden&&view?.dirty&&view.kind!==kind){guard(false,proceed);return;}proceed();
        }
        $('labSelfClose').addEventListener('click',()=>ui.closeActiveDrawer(true));
        $('labSelfKeep').addEventListener('click',()=>{const next=leaveAction;leaveAction=null;$('labSelfLeave').hidden=true;if(next)next();});
        $('labSelfDiscard').addEventListener('click',()=>{if(view){view.dirty=false;drafts.delete(view.kind);}body.replaceChildren();const next=leaveAction;leaveAction=null;if(next)next();});
        $('labSelfStay').addEventListener('click',()=>{$('labSelfLeave').hidden=true;leaveAction=null;});
        window.addEventListener('beforeunload',event=>{if(view?.dirty||drafts.size){event.preventDefault();event.returnValue='';}});
        function reset(){drafts.clear();receipts.clear();templates=[];leaveAction=null;view=null;body.replaceChildren();dismiss(false);}
        ui.register({close:guard,reset,bulk:()=>enter('bulk','新建实验',$('labNewExperiment'),buildBulk),compare:()=>enter('compare','选择风控对照',$('labCompareButton'),buildComparison)});
        $('labTemplatesButton').addEventListener('click',()=>enter('templates','模板库',$('labTemplatesButton'),v=>run(async()=>{await readTemplates();library(v);})));
        $('labQueueButton').addEventListener('click',()=>enter('queue','运行队列',$('labQueueButton'),buildQueue));

        function library(v,kind=v.templateKind||'condition'){
            body.replaceChildren();v.dirty=false;v.templateKind=kind;v.controls=null;
            const tabs=node('div',undefined,'lab-actions'),search=input('labTemplateSearch','search'),metric=select('labTemplateFilter',[['','全部玩法'],...Object.entries(metricNames)]),list=node('div');
            search.placeholder='搜索模板名称';tabs.append(button('条件模板',()=>library(v,'condition')),button('风控模板',()=>library(v,'risk')),button('新建模板',()=>editor(v,null,kind),'labTemplateNew',true));
            body.append(tabs,field('搜索模板',search),field('适用玩法',metric),note('模板保存后独立使用；编辑或删除模板不会改动已有实验。内置模板可复制。'),list);
            function render(){list.replaceChildren();const rows=templates.filter(t=>t.kind===kind&&(!metric.value||t.metrics.includes(metric.value))&&t.name.toLowerCase().includes(search.value.toLowerCase()));
                if(!rows.length)list.append(note('暂无符合条件的模板。可新建模板，或清除筛选后复制内置模板。'));
                rows.forEach(t=>{const row=node('article',undefined,'lab-self-card');row.dataset.templateRef=key(t);row.append(node('h3',t.name),note(t.metrics.map(m=>metricNames[m]).join(' / ')+' · '+(t.builtin?'内置只读':'自定义')+' · v'+t.revision+(t.condition_count?' · '+t.condition_count+' 条条件':'')));if(t.description)row.append(note(t.description));
                    const actions=node('div',undefined,'lab-actions');const action=(label,kind,fn)=>{const b=button(label,fn);b.dataset.templateAction=kind;actions.append(b);};
                    action('查看','view',()=>editor(v,t,kind,true));action('复制','copy',()=>editor(v,{...clone(t),id:null,builtin:false,name:copyName(t),copy:true},kind));
                    if(!t.builtin){action('编辑','edit',()=>editor(v,t,kind));action('删除','delete',()=>{if(!window.confirm('删除模板“'+t.name+'”？已有实验及成绩会保留。'))return;run(async()=>{await write('/templates/'+t.id+'/delete',{expected_revision:t.revision});await readTemplates();library(v,kind);message('模板已删除；原实验与成绩保留。');});});}row.append(actions);list.append(row);
                });}
            search.addEventListener('input',render);metric.addEventListener('change',render);render();
        }
        const conditionTypes=[['miss_streak','预测连错'],['hit_streak','预测连中'],['draw_streak','连续开出'],['draw_absence','连续不出'],['hit_rate','近 N 期预测方向命中率']];
        function editor(v,template,kind,readonly=false){
            body.replaceChildren();message('');v.dirty=false;v.controls=null;
            const form=node('form');form.id='labTemplateForm';form.noValidate=true;
            const name=input('labTemplateName','text',template?.name||''),description=node('textarea');name.maxLength=60;description.id='labTemplateDescription';description.maxLength=300;description.value=template?.description||'';
            const metric=select('labTemplateMetric',Object.entries(metricNames),template?.metrics?.[0]||ui.getContext().batch?.metric||'odd_even'),top=node('div',undefined,'lab-form-grid');
            top.append(field('名称（1～60字符）',name),field('玩法',metric));form.append(top,field('说明（最多300字符）',description));
            const list=node('div'),result=node('div');result.id='labTemplatePreview';let riskPrefix='labTemplateRisk';
            const currentMetric=()=>metric.value;
            function addCondition(c={type:'miss_streak',operator:'gte',threshold:3}){
                const row=node('div',undefined,'lab-condition-editor');row.dataset.conditionRow='';
                const enabled=input('', 'checkbox',true),type=select('',conditionTypes,c.type),operator=select('',[['gte','≥'],['lte','≤']],c.operator||'gte'),threshold=input('','text',c.threshold??3),windowSize=select('',[['20','20'],['50','50'],['100','100']],String(c.window_size||20));
                const direction=select('',[],c.bet_value),remove=button('移除',()=>{row.remove();mark();});
                function directions(){const values=currentMetric()==='big_small'?['大','小']:currentMetric()==='odd_even'?['单','双']:['大单','大双','小单','小双'],previous=direction.value;direction.replaceChildren();values.forEach(value=>{const option=node('option',value);option.value=value;direction.append(option);});direction.value=values.includes(previous)?previous:(c.bet_value||values[0]);}
                directions();for(const [k,n] of Object.entries({enabled,type,operator,threshold,window_size:windowSize,bet_value:direction}))n.dataset.conditionField=k;
                const enabledLabel=checkbox('启用',{},true);enabledLabel.replaceChildren(enabled,node('span','启用'));
                const directionField=field('方向',direction),windowField=field('窗口',windowSize);row.append(enabledLabel,field('条件类型',type),directionField,field('比较',operator),field('阈值（逗号分隔多值）',threshold),windowField,remove);
                function update(){directionField.hidden=!['draw_streak','draw_absence'].includes(type.value);windowField.hidden=type.value!=='hit_rate';threshold.placeholder=type.value==='hit_rate'?'例如 35,40,45':'例如 3,5,7';}
                type.addEventListener('change',update);metric.addEventListener('change',()=>{directions();mark();});update();list.append(row);
            }
            if(kind==='condition'){
                form.append(note('每条条件是一个独立实验；无条件基准固定保留。最多50条去重后的条件，阈值范围1～100；命中率0～100%。'),node('p','✓ 无条件基准（固定保留）','lab-note'),list,button('添加条件',()=>{addCondition();mark();},'labConditionAdd'));
                (template?.definition?.conditions||[]).filter(c=>c.type!=='always').forEach(addCondition);
            }else{
                const metrics=node('div',undefined,'lab-actions');metrics.id='labRiskTemplateMetrics';Object.entries(metricNames).forEach(([m,label])=>metrics.append(checkbox(label,{riskMetric:m},template?.metrics?.includes(m)||(!template&&m===metric.value))));form.append(node('h3','适用玩法'),metrics);
                const target=node('div');form.append(target);ui.createRiskEditor(riskPrefix,target);ui.renderRiskPresets(riskPrefix,metric.value);
                ui.fillRisk(riskPrefix,template?.definition?.policy||ui.getContext().batch?.risk_policy||{version:1,mode:'legacy'});
                metric.addEventListener('change',()=>ui.renderRiskPresets(riskPrefix,metric.value));
                form.append(button('复制当前实验风控',()=>{if(ui.getContext().batch){ui.fillRisk(riskPrefix,ui.getContext().batch.risk_policy);mark();}}));
            }
            function payload(){
                const value={kind,name:name.value.trim(),description:description.value.trim()};
                if(!value.name||value.name.length>60)throw Error('请填写1～60字符的模板名称。');
                if(kind==='risk'){const policy=ui.riskValue(riskPrefix),issue=ui.riskError(policy);if(issue)throw Error(issue);value.definition={metrics:[...form.querySelectorAll('[data-risk-metric]:checked')].map(n=>n.dataset.riskMetric),policy};if(!value.definition.metrics.length)throw Error('请至少选择一种适用玩法。');}
                else {const conditions=[{type:'always'}];list.querySelectorAll('[data-condition-row]').forEach(row=>{const f=k=>row.querySelector('[data-condition-field="'+k+'"]');if(!f('enabled').checked)return;const type=f('type').value,values=f('threshold').value.split(/[,，\s]+/).filter(Boolean);if(!values.length)throw Error('请填写条件阈值。');values.forEach(value=>{const c={type,operator:f('operator').value,threshold:Number(value)};if(!Number.isFinite(c.threshold))throw Error('阈值必须是有效数字。');if(type==='hit_rate')c.window_size=Number(f('window_size').value);if(['draw_streak','draw_absence'].includes(type))c.bet_value=f('bet_value').value;conditions.push(c);});});value.definition={metric:metric.value,conditions};}
                return value;
            }
            const actions=node('div',undefined,'lab-actions');actions.append(button('预览条件与校验',()=>run(async()=>{const preview=await ui.request(base+'/templates/preview','POST',payload());result.replaceChildren(note((kind==='condition'?preview.condition_count+' 条条件；去重 '+preview.duplicates_removed+' 条。':'风控参数校验通过。')+(preview.warnings||[]).join('；')));(preview.conditions||[]).forEach(c=>result.append(note(c.name)));}),'labTemplatePreviewButton'));
            const save=button('保存模板',()=>{} ,'labTemplateSave',true);save.type='submit';actions.append(save,button('返回模板库',()=>{if(v.dirty){message('修改尚未保存。请保存，或使用下方“放弃修改”。');return;}library(v,kind);}),button('放弃修改',()=>library(v,kind)));
            form.append(result,actions);body.append(form);form.addEventListener('input',mark);form.addEventListener('change',mark);
            form.addEventListener('submit',event=>{event.preventDefault();if(readonly)return;run(async()=>{const data=payload(),editing=template?.id&&!template.copy;if(editing)delete data.kind;await write(editing?'/templates/'+template.id+'/edit':'/templates',{...data,...(editing?{expected_revision:template.revision}:{})});v.dirty=false;await readTemplates();library(v,kind);message('模板已保存；已有实验保持原冻结配置。');});});
            if(readonly){form.dataset.readonly='true';form.querySelectorAll('input,select,textarea,button').forEach(n=>n.disabled=true);body.append(button('返回模板库',()=>library(v,kind)));}
        }
        function capacityText(capacity={}){return '已保存 '+(capacity.saved??'—')+' / '+(capacity.max_saved??'—')+' 版本；剩余 '+(capacity.remaining_saved??'—')+'。当前运行 '+(capacity.running??'—')+' / '+(capacity.max_running??'—')+'；可再建 '+(capacity.remaining_batches??'—')+' 批次。';}
        function buildBulk(v){
            body.replaceChildren(note('正在读取可选来源与模板…'));
            run(async()=>{
                await readTemplates();body.replaceChildren();const context=ui.getContext(),options=context.options;if(!options)throw Error('实验配置未加载，请先刷新主页面。');
                const form=node('form');form.noValidate=true;form.id='labBulkForm';
                const sourceSearch=input('labBulkSourceSearch','search'),sourceList=node('div',undefined,'lab-choice-list');sourceSearch.placeholder='搜索来源';
                (options.predictors||[]).forEach(p=>sourceList.append(checkbox(p.name,{sourceId:String(p.id)},p.id===context.batch?.predictor_id)));
                sourceSearch.addEventListener('input',()=>{[...sourceList.children].forEach(l=>l.hidden=!l.textContent.toLowerCase().includes(sourceSearch.value.toLowerCase()));});
                const mode=select('labBulkMode',[['backtest','历史回测'],['live','前向模拟']],'backtest'),start=input('labBulkStart','date',context.batch?.start_date||''),end=input('labBulkEnd','date',context.batch?.end_date||''),stake=input('labBulkStake','number',options.default_stake_amount||1),grid=node('div',undefined,'lab-form-grid');stake.min='0.01';stake.max='10000';stake.step='0.01';grid.append(field('观察方式',mode),field('每注模拟金额',stake),field('开始日期（北京时间）',start),field('结束日期（北京时间）',end));
                const metrics=node('div',undefined,'lab-actions'),configs=node('div'),configViews=new Map(),names=new Map();
                Object.entries(metricNames).forEach(([metric,label])=>metrics.append(checkbox(label,{metricCheck:metric},metric===(context.batch?.metric||'odd_even'))));
                form.append(node('h3','1. 来源与回测区间'),field('搜索来源（最多20个）',sourceSearch),sourceList,grid,node('h3','2. 各玩法条件与风控'),metrics,configs,note('普通实验固定使用两套当前 OK 赔率，保存时冻结。条件之间互相独立；跨玩法使用风控时请逐项核对参数。前向模拟只保存草稿，需在结果页单独启动。'));
                let preview=null,previewPayload=null;
                const summary=node('p',undefined,'lab-note');summary.id='labBulkPreviewSummary';const rows=node('div');rows.id='labBulkPreviewRows';const actions=node('div',undefined,'lab-actions');
                const previewButton=button('预览创建清单',()=>run(async()=>{const payload=value(),response=await ui.request(base+'/batches/preview','POST',payload);preview=response;previewPayload=payload;renderPreview(response);}), 'labBulkPreviewButton');
                const save=button('保存草稿',()=>submit(false),'labBulkSaveDraft',true),enqueue=button('保存并加入队列',()=>submit(true),'labBulkSaveQueue',true);actions.append(previewButton,save,enqueue);form.append(node('h3','3. 预览与保存'),summary,rows,actions);body.append(form);
                v.controls=()=>{save.disabled=busy||!preview?.available;enqueue.disabled=busy||!preview?.available||mode.value==='live';};
                function invalidate(){preview=null;previewPayload=null;summary.textContent='配置已修改，请重新预览后保存。';mark();v.controls();}
                function metricConfig(metric){
                    if(configViews.has(metric))return configViews.get(metric);
                    const section=node('section',undefined,'lab-self-card'),conditionSelect=select('',templates.filter(t=>t.kind==='condition'&&t.metrics.includes(metric)).map(t=>[key(t),t.name+' · v'+t.revision]),'builtin:basic_v2:'+metric);conditionSelect.dataset.conditionTemplate=metric;
                    const conditions=node('div',undefined,'lab-choice-list'),riskSelect=select('',[['manual','手动配置 / 当前实验风控'],...templates.filter(t=>t.kind==='risk'&&t.metrics.includes(metric)).map(t=>[key(t),t.name+' · v'+t.revision])],'manual');riskSelect.dataset.riskTemplate=metric;
                    const details=node('details',undefined,'lab-risk-box'),target=node('div'),prefix='labBulkRisk'+metric;details.append(node('summary','查看与编辑完整风控参数'),target);section.append(node('h3',metricNames[metric]),field('条件模板',conditionSelect),conditions,field('风控模板',riskSelect),details);configs.append(section);
                    ui.createRiskEditor(prefix,target);ui.renderRiskPresets(prefix,metric);ui.fillRisk(prefix,context.batch?.metric===metric?context.batch.risk_policy:{version:1,mode:'legacy'});
                    function renderConditions(){conditions.replaceChildren();const t=templates.find(t=>key(t)===conditionSelect.value);(t?.definition?.conditions||[]).forEach(c=>{const choice=checkbox(c.name,{conditionKey:c.condition_key},true);choice.querySelector('input').disabled=c.type==='always';conditions.append(choice);});}
                    conditionSelect.addEventListener('change',renderConditions);riskSelect.addEventListener('change',()=>{const t=templates.find(t=>key(t)===riskSelect.value);if(t)ui.fillRisk(prefix,t.definition.policy);});target.addEventListener('change',()=>riskSelect.value='manual');renderConditions();
                    const result={section,conditionSelect,conditions,riskSelect,prefix};configViews.set(metric,result);return result;
                }
                function updateMetrics(){metrics.querySelectorAll('input').forEach(c=>{if(c.checked)metricConfig(c.dataset.metricCheck).section.hidden=false;else if(configViews.has(c.dataset.metricCheck))configViews.get(c.dataset.metricCheck).section.hidden=true;});}
                metrics.addEventListener('change',updateMetrics);updateMetrics();
                mode.addEventListener('change',()=>{start.disabled=mode.value==='live';end.disabled=mode.value==='live';});
                form.addEventListener('input',event=>{if(event.target!==sourceSearch)invalidate();});form.addEventListener('change',event=>{if(event.target!==sourceSearch)invalidate();});form.addEventListener('submit',event=>{event.preventDefault();previewButton.click();});
                function value(){const sources=[...sourceList.querySelectorAll('input:checked')].map(n=>Number(n.dataset.sourceId));if(!sources.length||sources.length>20)throw Error('请选择1～20个来源。');const selected=[...metrics.querySelectorAll('input:checked')].map(c=>c.dataset.metricCheck);if(!selected.length)throw Error('请至少选择一种玩法。');
                    const metric_configs=selected.map(metric=>{const c=configViews.get(metric),t=templates.find(t=>key(t)===c.conditionSelect.value);if(!t)throw Error('请选择'+metricNames[metric]+'条件模板。');const condition_keys=[...c.conditions.querySelectorAll('input:checked')].map(n=>n.dataset.conditionKey),risk=templates.find(t=>key(t)===c.riskSelect.value),policy=ui.riskValue(c.prefix),issue=ui.riskError(policy);if(issue)throw Error(metricNames[metric]+'：'+issue);return {metric,condition_selection:{template_ref:t.ref,expected_revision:t.revision,condition_keys},risk_selection:risk?{template_ref:risk.ref,expected_revision:risk.revision}:{policy}};});
                    return {mode:mode.value,sources,start_date:mode.value==='backtest'?start.value:null,end_date:mode.value==='backtest'?end.value:null,stake_amount:Number(stake.value),metric_configs,name_pattern:'{source} · {metric} · {template}',names:[...names.values()]};}
                function renderPreview(response){summary.textContent='将创建 '+response.batch_count+' 批次 / '+response.new_experiments+' 个版本。'+capacityText(response.capacity);rows.replaceChildren();
                    (response.blockers||[]).forEach(text=>rows.append(node('p',typeof text==='string'?text:text.message,'lab-feedback is-error')));(response.warnings||[]).forEach(text=>rows.append(note(typeof text==='string'?text:text.message)));
                    (response.batches||[]).forEach(b=>{const row=node('article',undefined,'lab-self-card'),name=input('', 'text',b.name);name.maxLength=80;name.addEventListener('change',()=>{names.set(b.predictor_id+':'+b.metric,{predictor_id:b.predictor_id,metric:b.metric,name:name.value.trim()});});row.append(field('批次名称',name),note(b.predictor_name+' · '+metricNames[b.metric]+' · '+b.condition_count+' 条件 / '+b.experiment_count+' 版本'),note((b.start_date||'前向')+' → '+(b.end_date||'持续观察')+' · 模板 v'+(b.template_snapshot?.revision||'—')),note(ui.riskSummary(b.risk_policy)));rows.append(row);});v.controls();}
                function submit(enqueueValue){run(async()=>{if(!preview?.available||!previewPayload)throw Error('请先完成有效预览。');const result=await write('/batches/bulk',{...previewPayload,preview_revision:preview.preview_revision,enqueue:enqueueValue});v.dirty=false;preview=null;message('已创建 '+result.batches.length+' 批次 / '+result.generated_count+' 版本'+(result.queued_count?'，'+result.queued_count+' 批次已加入队列。':'，已保存为草稿。'));rows.replaceChildren();summary.textContent='保存成功；可在结果页选择批次，或打开运行队列。';await ui.refresh(false);});}
                v.controls();
            });
        }

        function buildComparison(v){
            const batch=ui.getContext().batch;if(!batch){message('请先选择一个历史批次。',true);return;}
            body.replaceChildren(note('读取原实验冻结条件与赔率…'));
            run(async()=>{
                const responses=await Promise.allSettled([ui.request(base+'/batches/'+batch.id+'/comparison-preview'),readTemplates()]);for(const response of responses)if(response.status==='rejected')throw response.reason;const original=responses[0].value;
                body.replaceChildren();const form=node('form');form.noValidate=true;const name=input('labSelectiveName','text',batch.name+' · 风控对照');name.maxLength=80;
                const conditionList=node('div',undefined,'lab-choice-list'),oddsList=node('div',undefined,'lab-choice-list'),candidates=node('div',undefined,'lab-choice-list');
                const selected=ui.getContext().selectedCondition?.condition_key;
                (original.conditions||[]).forEach(c=>conditionList.append(checkbox(c.name,{compareCondition:c.condition_key},c.condition_key===selected)));
                (original.settlement_rules||[]).forEach(r=>oddsList.append(checkbox(r.name,{compareOdds:r.id},true)));
                const baseline=checkbox('原风控基准（固定保留）',{},true);baseline.querySelector('input').disabled=true;candidates.append(baseline);
                const presets=[['round_tp_half','提前止盈'],['round_sl_half','提前止损'],['day_sl_half','降低日止损'],['gate_off','关闭全部启用日层的命中率熔断']];
                presets.forEach(([key,label])=>{const variant=(original.variants||[]).find(x=>x.key===key),choice=checkbox(label,{candidateKey:key});if(variant?.available===false){choice.querySelector('input').disabled=true;choice.append(node('small','：'+variant.reason));}candidates.append(choice);});
                templates.filter(t=>t.kind==='risk'&&t.metrics.includes(batch.metric)).forEach(t=>candidates.append(checkbox(t.name+' · v'+t.revision+'（完整政策，可能包含多项变化）',{candidateTemplate:String(t.id)})));
                const thresholds=input('labCompareThresholds','text',''),samples=input('labCompareSamples','text','');thresholds.placeholder='例如 25,40,50';samples.placeholder='例如 20,30';
                const custom=checkbox('添加本次自定义风控（不保存为模板）',{candidateCustom:'true'}),customEditor=node('details'),customTarget=node('div');customEditor.append(node('summary','编辑本次自定义风控'),customTarget);ui.createRiskEditor('labCompareRisk',customTarget);ui.renderRiskPresets('labCompareRisk',batch.metric);ui.fillRisk('labCompareRisk',batch.risk_policy);
                const summary=node('p',undefined,'lab-note');summary.id='labSelectiveSummary';const changes=node('div');changes.id='labSelectiveChanges';let preview=null,payload=null;
                const previewButton=button('预览方案差异',()=>run(async()=>{payload=value();preview=await ui.request(base+'/studies/preview','POST',payload);summary.textContent=preview.candidate_count+' 个互异方案 · '+preview.new_experiments+' 个版本。'+capacityText(preview.capacity);changes.replaceChildren();(preview.variants||[]).forEach(item=>{const card=node('article',undefined,'lab-self-card');card.append(node('h3',item.label+(item.available===false?' · 不生成':'')),note(item.reason||''));(item.changes||[]).forEach(change=>card.append(note(change)));changes.append(card);});for(const text of [...(preview.warnings||[]),...(preview.blockers||[])])changes.append(note(typeof text==='string'?text:text.message));v.controls();}),'labSelectivePreview');
                const save=button('保存对照草稿',()=>submit(false),'labSelectiveSave',true),enqueue=button('保存并加入队列',()=>submit(true),'labSelectiveEnqueue',true),actions=node('div',undefined,'lab-actions');actions.append(previewButton,save,enqueue);
                form.append(note('基于“'+batch.name+'”的冻结条件与赔率，所有方案从零重算。请选择要比较的条件，原风控基准固定保留。'),field('比较组名称',name),node('h3','1. 选择条件'),conditionList,node('h3','2. 选择原实验赔率'),oddsList,node('h3','3. 选择风控方案'),candidates,field('实赔命中率熔断阈值（逗号分隔，%）',thresholds),field('最少有效单数（逗号分隔）',samples),note('阈值与样本数组展开组合；同时修改全部启用的日风控层。与原基准相同的方案会去重，最多12个互异方案。'),custom,customEditor,summary,changes,actions);body.append(form);
                v.controls=()=>{save.disabled=busy||!preview?.available;enqueue.disabled=busy||!preview?.available;};
                function invalidate(){preview=null;payload=null;mark();summary.textContent='选择已修改，请重新预览。';v.controls();}form.addEventListener('input',invalidate);form.addEventListener('change',invalidate);form.addEventListener('submit',e=>{e.preventDefault();previewButton.click();});
                function value(){const condition_keys=[...conditionList.querySelectorAll('input:checked')].map(n=>n.dataset.compareCondition),settlement_rule_ids=[...oddsList.querySelectorAll('input:checked')].map(n=>n.dataset.compareOdds);if(!condition_keys.length||!settlement_rule_ids.length)throw Error('请至少选择一个条件和一套赔率。');const chosen=[...candidates.querySelectorAll('[data-candidate-key]:checked')].map(n=>({kind:'preset',key:n.dataset.candidateKey}));
                    candidates.querySelectorAll('[data-candidate-template]:checked').forEach(n=>{const t=templates.find(t=>t.id===Number(n.dataset.candidateTemplate));chosen.push({kind:'risk_template',template_id:t.id,expected_revision:t.revision});});
                    const numbers=text=>text.trim()?text.trim().split(/[,，\s]+/).map(Number):[];const rates=numbers(thresholds.value),counts=numbers(samples.value);if(rates.length||counts.length){if(!rates.length||!counts.length)throw Error('阈值和最少有效单数需同时填写。');if(rates.length*counts.length>48)throw Error('阈值组合过多，请减少后预览。');rates.forEach(threshold=>counts.forEach(min_settled_count=>chosen.push({kind:'gate',threshold,min_settled_count})));}
                    if(custom.querySelector('input').checked){const policy=ui.riskValue('labCompareRisk'),issue=ui.riskError(policy);if(issue)throw Error(issue);chosen.push({kind:'policy',label:'本次自定义风控',policy});}if(!chosen.length)throw Error('请至少选择一个不同于基准的风控方案。');return {source_batch_id:batch.id,condition_keys,settlement_rule_ids,candidates:chosen};}
                function submit(enqueueValue){run(async()=>{if(!preview?.available||!payload)throw Error('请先预览有效对照。');if(!name.value.trim())throw Error('请填写比较组名称。');const result=await write('/studies',{...payload,name:name.value.trim(),preview_revision:preview.preview_revision,enqueue:enqueueValue});v.dirty=false;preview=null;message('已创建风控比较组'+(enqueueValue?'并加入队列。':'草稿。'));await ui.refresh(false);});}v.controls();
            });
        }

        function buildQueue(v){
            body.replaceChildren();v.dirty=false;v.controls=null;
            const stateSelect=select('labQueueState',[['active','进行中与草稿'],['completed','已完成'],['all','全部']],'active'),source=input('labQueueSearch','search'),metric=select('labQueueMetric',[['','全部玩法'],...Object.entries(metricNames)]),summary=node('p',undefined,'lab-note'),list=node('div');list.id='labQueueList';source.placeholder='搜索批次或来源';
            const actions=node('div',undefined,'lab-actions');actions.append(field('队列范围',stateSelect),button('刷新队列',()=>run(load),'labQueueRefresh'));body.append(actions,field('搜索批次 / 来源',source),field('玩法',metric),summary,list);let result=null;
            async function load(){result=await ui.request(base+'/queue?state='+stateSelect.value);summary.textContent=capacityText(result.capacity);render();}
            function matches(row){return (!metric.value||row.metric===metric.value)&&(!source.value||(row.name+' '+(row.predictor_name||'')).toLowerCase().includes(source.value.toLowerCase()));}
            function render(){list.replaceChildren();const items=(result?.items||[]).filter(matches),draftRows=(result?.drafts||[]).filter(matches);if(!items.length&&!draftRows.length)list.append(note('当前筛选下没有队列任务。保存历史实验草稿后可在这里加入队列。'));
                items.forEach(item=>{const row=node('article',undefined,'lab-self-card');row.dataset.queueOwner=String(item.owner_id);row.dataset.queueKind=item.owner_kind;row.append(node('h3',item.name),note((statusNames[item.state]||item.state)+' · '+item.experiment_count+' 版本 · 已处理 '+item.processed_issues+(item.total_issues?' / '+item.total_issues:'')+' 期'+(item.queue_order?' · 顺序 '+item.queue_order:'')));if(item.waiting_reason)row.append(node('p',item.waiting_reason,'lab-note'));if(item.error)row.append(node('p',item.error,'lab-feedback is-error'));const actions=node('div',undefined,'lab-actions');(item.actions||[]).forEach(action=>{const labels={pause:'暂停',resume:'继续排队',remove:'移出队列',retry:'修复后重试',view:'查看结果'};if(action==='view')return;const b=button(labels[action]||action,()=>run(async()=>{await write('/queue/'+item.id+'/state',{expected_revision:item.expected_revision,action});await load();await ui.refresh(false);message('队列已更新；实验成绩与进度保留。');}));b.dataset.queueAction=action;actions.append(b);});actions.append(button('查看结果',async()=>{dismiss(true);if(item.owner_kind==='study')await ui.openStudy(item.owner_id);else await ui.selectBatch(item.owner_id);}));row.append(actions);list.append(row);});
                if(draftRows.length)list.append(node('h3','尚未入队的历史草稿'));draftRows.forEach(draft=>{const row=node('article',undefined,'lab-self-card');row.dataset.draftId=String(draft.owner_id);row.append(node('h3',draft.name),note('草稿 · '+draft.experiment_count+' 版本'),button('加入队列',()=>run(async()=>{await write('/queue/enqueue',{owner_kind:draft.owner_kind,owner_id:draft.owner_id,expected_revision:draft.expected_revision});await load();await ui.refresh(false);message('已加入运行队列。');})));list.append(row);});}
            stateSelect.addEventListener('change',()=>run(load));source.addEventListener('input',render);metric.addEventListener('change',render);run(load);
        }
    }
    if(window.RuleLabUI)initialize();else document.addEventListener('rulelab:ready',initialize,{once:true});
}());
