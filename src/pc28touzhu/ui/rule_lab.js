(function () {
    "use strict";
    const $ = id => document.getElementById(id);
    const base = "/api/platform/rule-lab";
    const statusNames = {draft:"草稿",queued:"等待运行",removed:"已移出队列",paused:"已暂停", running:"运行中", completed:"回测完成", error:"处理异常", resource_paused:"资源限制暂停",deleting:"删除中"};
    const phaseNames = {idle:"等待任务", warming:"准备历史窗口", observing:"等待条件或开奖", importing:"分批读取历史", replaying:"正在回放历史"};
    const workerNames = {starting:"等待后台启动", idle:"等待任务", running:"正在处理", stale:"后台暂未更新", resource_paused:"资源限制暂停", error:"处理异常"};
    const metricNames = {big_small:"大小", odd_even:"单双", combo:"组合"};
    const reasonNames = {insufficient_history:"历史样本不足", late_decision:"错过决策窗口", late_prediction:"预测到达过晚", missing_prediction:"缺少预测", invalid_draw:"开奖数据不完整", input_conflict:"输入内容冲突", missing_draw:"缺少开奖", source_unavailable:"来源暂不可用", input_expired:"所需输入已过保留期"};
    Object.assign(reasonNames,{round_profit_target:"单轮止盈",round_loss_limit:"单轮止损",round_max_bets:"单轮下注上限",day_profit_target:"日止盈",day_loss_limit:"日止损",day_max_bets:"日下注上限",day_max_rounds:"当日开轮次数已达上限",daily_hit_rate_low:"日命中率熔断",cooldown:"等待开轮间隔",no_valid_bets:"无有效投注"});
    const riskStates={legacy:"逐期对照",active:"轮内跟随",unfinished:"区间结束·轮未完结",waiting:"等待条件 / 冷却",day_stopped:"统计日已停止"};
    const riskEditors={};
    const extensions={};const drawerExtensions=new Map();let templateCatalog=[];
    const pageSize = 20;
    const observationDefaults=Object.freeze({version:1,months:2,includePartial:true,minimumRoi:3,minimumCount:200,minimumDays:10,
        maximumSpread:null,maximumMonthlyDrawdownUnits:null,maximumUnderwaterDays:null,highlight:'red',matchesOnly:false,showRuntime:false});
    const observationPreferenceKey='ruleLabObservation:v1';
    const observationFields={months:'labObservationMonths',includePartial:'labObservationPartial',minimumRoi:'labObservationMinimumRoi',
        minimumCount:'labObservationMinimumCount',minimumDays:'labObservationMinimumDays',maximumSpread:'labObservationMaximumSpread',
        maximumMonthlyDrawdownUnits:'labObservationMaximumMonthlyDrawdownUnits',maximumUnderwaterDays:'labObservationMaximumUnderwaterDays',
        highlight:'labObservationHighlight',matchesOnly:'labObservationMatchesOnly',showRuntime:'labObservationShowRuntime'};
    const observationBlockerNames={legacy_engine:'旧回放算法需复核',input_conflict:'输入有冲突',archive_gap:'输入缺档',
        window_pending:'窗口内尚有待结算',history_mismatch:'日汇总与累计成绩不一致',study_not_comparable:'比较组输入尚不可比'};
    const state = {options:null, overview:null, batch:null, experiments:[], selectedId:null, detailId:null, detailTrigger:null, resultPage:0,
        layout:{tab:'experiments',view:'overview',monthlyMetric:'roi',drawer:null},detailContext:null,loading:false, mutating:false, exporting:false, resultFilteredCount:0,studyFilteredCount:0,observation:{settings:{...observationDefaults},dirty:false,revision:0,loading:false}, readRevision:0, detailRevision:0, readAt:null, accessDenied:false,riskEditingId:null,
        studies:[],study:null,studyRows:[],studyId:null,studyPage:0,preview:null,studyRequest:null,monthly:null,reportExperiment:null,detailResult:null,amountView:null,performance:null,continuation:null,continuationRevision:0,management:null,managementJobs:new Map(),managementPoll:null,storage:null,storageRequest:null};
    const studyNames={paused:'已暂停',queued:'等待运行额度',running:'比较中',completed:'全部完成',error:'比较异常',resource_paused:'资源限制暂停',deleting:'删除中'};
    const monthStatus={complete:'完整月份',no_samples:'完整月份 · 无交易',partial:'部分月份',not_replayed:'尚未回放',not_observed:'尚未观察',pending:'等待结算'};
    const continuationNames={disabled:'未启用',idle:'等待追加',catching_up:'正在保存后续数据',waiting_inputs:'等待输入齐备',running:'正在追加计算',paused:'本段计算已暂停',archive_gap:'输入档案有缺口',storage_paused:'存储预算暂停',source_unavailable:'来源暂不可用',error:'续算异常'};
    const coverageNames={present:'已有预测',no_prediction:'已确认无可用预测',invalid_prediction:'预测格式无效',waiting:'等待核对',archive_gap:'输入已缺失',complete:'覆盖完整',known_skips:'包含已知跳过',conflict:'输入存在冲突'};
    const segmentNames={queued:'等待计算',running:'计算中',paused:'计算已暂停',completed:'本段计算完成',error:'处理异常',resource_paused:'存储预算暂停'};

    function element(tag, text, className) {
        const node = document.createElement(tag);
        if (text !== undefined && text !== null) node.textContent = String(text);
        if (className) node.className = className;
        return node;
    }
    function blankRisk() {return {version:1,mode:"round",round:{enabled:true,profit_target:0,loss_limit:0,max_bets:0},day:{enabled:false,profit_target:0,loss_limit:0,max_bets:0,max_rounds:0,hit_rate_gate:{enabled:false,threshold:0,min_settled_count:20}},additional_day_guards:[],cooldown_issues:0,source:{}};}
    function createRiskEditor(prefix, target) {
        const editor={prefix,root:target,source:{},guards:[],selectedPreset:"",fields:{}};riskEditors[prefix]=editor;
        const grid=element("div",null,"lab-form-grid");
        function field(key,label,type,choices) {
            const wrap=element("div",null,"lab-field"),id=prefix+key,input=element(type==="select"?"select":"input");input.id=id;
            const caption=element("label",label);caption.htmlFor=id;
            if(type==="select") choices.forEach(([value,text])=>{const option=element("option",text);option.value=value;input.append(option);});
            else {input.type=type;if(type==="number"){input.min="0";input.step="1";}}
            wrap.append(caption,input);grid.append(wrap);editor.fields[key]=input;return input;
        }
        field("Mode","运行口径","select",[["round","按轮模拟"],["legacy","逐期条件对照（无轮风控）"]]);
        field("Preset","复制现有规则风控","select",[["","手动设置（不自动选择预设）"]]);
        field("RoundEnabled","启用单轮退出","checkbox");field("RoundProfit","单轮止盈（0不限）","number");field("RoundLoss","单轮止损（0不限）","number");field("RoundMax","单轮最多下注（0不限）","number");
        field("DayEnabled","启用日风控","checkbox");field("DayProfit","日止盈（0不限）","number");field("DayLoss","日止损（0不限）","number");field("DayMax","每日最多下注（0不限）","number");field("DayRounds","每日最多开轮（0不限）","number");
        field("GateEnabled","启用日命中率熔断","checkbox");field("Threshold","熔断命中率阈值（%）","number");field("Minimum","最少实赔＋全亏单数","number");field("Cooldown","距上次开轮至少间隔（期）","number");
        field("ExtraPresent","附加一套独立日保护","checkbox");field("ExtraEnabled","启用附加日保护","checkbox");field("ExtraProfit","附加日止盈（0不限）","number");field("ExtraLoss","附加日止损（0不限）","number");field("ExtraMax","附加日最多下注（0不限）","number");field("ExtraRounds","附加日最多开轮（0不限）","number");field("ExtraGate","附加实赔命中率熔断","checkbox");field("ExtraThreshold","附加熔断阈值（%）","number");field("ExtraMinimum","附加最少实赔＋全亏单数","number");
        ['RoundProfit','RoundLoss','DayProfit','DayLoss','Threshold','ExtraProfit','ExtraLoss','ExtraThreshold'].forEach(key=>editor.fields[key].step='0.01');
        editor.notice=element("p",null,"lab-note");editor.provenance=element("p",null,"lab-muted");editor.extra=element("p",null,"lab-note");
        target.append(grid,element("p","命中率 = 实赔 ÷（实赔＋全亏），退本不计分母；达到最少单数且低于阈值才停止。冷却从上次开轮期号算起，不是收工后等待。日次数按目标开奖日预占、最终盈亏按实际开奖日结算。","lab-muted"),editor.provenance,editor.extra,editor.notice);
        grid.addEventListener("change",event=>{
            if(event.target===editor.fields.Preset){
                const found=(state.options?.risk_presets||[]).find(p=>p.id===event.target.value);
                if(found){editor.selectedPreset=found.id;fillRisk(prefix,found.policy);editor.fields.Preset.value=found.id;}
                else {editor.source={};editor.guards=[];editor.fields.ExtraPresent.checked=false;editor.selectedPreset="";}
            } else if(event.target!==editor.fields.Mode && Object.keys(editor.source).length)editor.source.modified=true;
            updateRiskEditor(prefix);
        });
        fillRisk(prefix,blankRisk());
    }
    function fillRisk(prefix,policy) {
        const editor=riskEditors[prefix],f=editor.fields,p=policy?.mode==="round"?policy:blankRisk();
        f.Mode.value=policy?.mode||"round";
        f.RoundEnabled.checked=p.round.enabled;f.RoundProfit.value=p.round.profit_target;f.RoundLoss.value=p.round.loss_limit;f.RoundMax.value=p.round.max_bets;
        f.DayEnabled.checked=p.day.enabled;f.DayProfit.value=p.day.profit_target;f.DayLoss.value=p.day.loss_limit;f.DayMax.value=p.day.max_bets;f.DayRounds.value=p.day.max_rounds;
        f.GateEnabled.checked=p.day.hit_rate_gate.enabled;f.Threshold.value=p.day.hit_rate_gate.threshold;f.Minimum.value=p.day.hit_rate_gate.min_settled_count;f.Cooldown.value=p.cooldown_issues;
        editor.source=structuredClone(p.source||{});editor.guards=structuredClone(p.additional_day_guards||[]);
        const extra=editor.guards[0]||blankRisk().day;f.ExtraPresent.checked=editor.guards.length>0;f.ExtraEnabled.checked=extra.enabled;
        f.ExtraProfit.value=extra.profit_target;f.ExtraLoss.value=extra.loss_limit;f.ExtraMax.value=extra.max_bets;f.ExtraRounds.value=extra.max_rounds;
        f.ExtraGate.checked=extra.hit_rate_gate.enabled;f.ExtraThreshold.value=extra.hit_rate_gate.threshold;f.ExtraMinimum.value=extra.hit_rate_gate.min_settled_count;
        updateRiskEditor(prefix);
    }
    function riskValue(prefix) {
        const editor=riskEditors[prefix],f=editor.fields;
        if(f.Mode.value==="legacy")return {version:1,mode:"legacy"};
        return {version:1,mode:"round",round:{enabled:f.RoundEnabled.checked,profit_target:Number(f.RoundProfit.value),loss_limit:Number(f.RoundLoss.value),max_bets:Number(f.RoundMax.value)},
            day:{enabled:f.DayEnabled.checked,profit_target:Number(f.DayProfit.value),loss_limit:Number(f.DayLoss.value),max_bets:Number(f.DayMax.value),max_rounds:Number(f.DayRounds.value),
                hit_rate_gate:{enabled:f.GateEnabled.checked,threshold:Number(f.Threshold.value),min_settled_count:Number(f.Minimum.value)}},
            cooldown_issues:Number(f.Cooldown.value),additional_day_guards:f.ExtraPresent.checked?[{enabled:f.ExtraEnabled.checked,profit_target:Number(f.ExtraProfit.value),loss_limit:Number(f.ExtraLoss.value),max_bets:Number(f.ExtraMax.value),max_rounds:Number(f.ExtraRounds.value),hit_rate_gate:{enabled:f.ExtraGate.checked,threshold:Number(f.ExtraThreshold.value),min_settled_count:Number(f.ExtraMinimum.value)}}]:[],source:structuredClone(editor.source)};
    }
    function riskError(p) {
        if(p.mode==="legacy")return "";
        const controls=[p.round,p.day,...p.additional_day_guards];
        if(!controls.some(c=>c.enabled && (c.profit_target>0||c.loss_limit>0||c.max_bets>0)))return "尚未设置退出条件：请启用至少一项单轮或日止盈、止损、最多下注，或明确选择逐期对照。";
        if(controls.some(c=>[c.profit_target,c.loss_limit,c.max_bets,c.max_rounds??0].some(v=>!Number.isFinite(v)||v<0)))return "风控金额和次数必须是有效非负数字。";
        if(controls.some(c=>!Number.isInteger(c.max_bets)||!Number.isInteger(c.max_rounds??0))||!Number.isInteger(p.cooldown_issues)||p.cooldown_issues<0)return "次数与开轮间隔必须是非负整数。";
        if([p.day,...p.additional_day_guards].some(c=>c.hit_rate_gate.enabled && (!(c.hit_rate_gate.threshold>0&&c.hit_rate_gate.threshold<100)||!Number.isInteger(c.hit_rate_gate.min_settled_count)||c.hit_rate_gate.min_settled_count<1)))return "启用命中率熔断时，请填写0～100之间的阈值及正整数最少单数。";
        return "";
    }
    function riskSummary(p) {
        if(!p||p.mode!=="round")return "逐期条件对照：每期重新判断触发条件，无按轮止盈止损。";
        const n=v=>v?String(v):"不限",r=p.round,d=p.day,g=d.hit_rate_gate;
        return "按轮模拟 · 单轮"+(r.enabled?"止盈 "+n(r.profit_target)+" / 止损 "+n(r.loss_limit)+" / 最多下注 "+n(r.max_bets):"退出关闭")+" · 日"+(d.enabled?"止盈 "+n(d.profit_target)+" / 止损 "+n(d.loss_limit)+" / 最多下注 "+n(d.max_bets)+" / 最多开轮 "+n(d.max_rounds):"风控关闭")+(d.enabled&&g.enabled?" · 实赔命中率 < "+g.threshold+"%，至少 "+g.min_settled_count+" 单":"")+" · 距上次开轮 ≥ "+p.cooldown_issues+" 期"+(p.additional_day_guards?.length?" · 另有一套独立规则日保护":"");
    }
    function updateRiskEditor(prefix) {
        const editor=riskEditors[prefix];if(!editor)return;
        const p=riskValue(prefix),f=editor.fields,legacy=p.mode==="legacy",busy=state.mutating||state.accessDenied||Boolean(editor.root.closest('[data-readonly=true]'))||(prefix==='labDefinitionRisk'&&Boolean(state.management?.pending));
        Object.entries(f).forEach(([key,input])=>{input.disabled=busy||(legacy&&!['Mode'].includes(key));});
        if(!legacy){['RoundProfit','RoundLoss','RoundMax'].forEach(k=>f[k].disabled=busy||!f.RoundEnabled.checked);['DayProfit','DayLoss','DayMax','DayRounds','GateEnabled'].forEach(k=>f[k].disabled=busy||!f.DayEnabled.checked);['Threshold','Minimum'].forEach(k=>f[k].disabled=busy||!f.DayEnabled.checked||!f.GateEnabled.checked);}
        if(!legacy){['ExtraEnabled','ExtraProfit','ExtraLoss','ExtraMax','ExtraRounds','ExtraGate','ExtraThreshold','ExtraMinimum'].forEach(k=>{f[k].closest('.lab-field').hidden=!f.ExtraPresent.checked;f[k].disabled=busy||!f.ExtraPresent.checked||(k!=='ExtraEnabled'&&!f.ExtraEnabled.checked)||(['ExtraThreshold','ExtraMinimum'].includes(k)&&!f.ExtraGate.checked);});}
        editor.guards=p.additional_day_guards||[];
        const source=editor.source;editor.provenance.textContent=source.rule_id?"复制自："+[source.rule_name,source.route_name,source.subscription_name].join(" · ")+(source.modified?"（已修改预设）":"")+"。仅复制风控，来源与每注金额不变。":"未选择现有规则预设，当前为手动配置。";
        editor.extra.hidden=!editor.guards.length;editor.extra.textContent=editor.guards.length?"附加独立日保护："+riskSummary({mode:'round',round:{enabled:false},day:editor.guards[0],cooldown_issues:0,additional_day_guards:[]})+"。可在上方编辑或关闭此附加层。":"";
        editor.notice.textContent=riskError(p)||riskSummary(p);editor.notice.classList.toggle('is-error',Boolean(riskError(p)));
    }
    function renderRiskPresets(prefix,metric) {
        const editor=riskEditors[prefix],select=editor.fields.Preset,selected=select.value;
        select.replaceChildren();const manual=element('option','手动设置（不自动选择预设）');manual.value='';select.append(manual);
        (state.options?.risk_presets||[]).filter(p=>p.metric===metric).forEach(p=>{const option=element('option',p.label);option.value=p.id;select.append(option);});
        if([...select.options].some(o=>o.value===selected))select.value=selected;
        else if(editor.source.rule_id){editor.source={};editor.guards=[];}
        updateRiskEditor(prefix);
    }
    function feedback(id, message, error) {
        const node = $(id);
        node.textContent = message || "";
        node.classList.toggle("is-error", Boolean(error));
    }
    function number(value, fallback) { return typeof value === "number" && Number.isFinite(value) ? value : fallback; }
    function money(value, signed) {
        const n = number(value, null);
        return n === null ? "—" : (signed && n > 0 ? "+" : "") + n.toFixed(2);
    }
    function percent(value) { return value === null || value === undefined ? "—" : money(value, false) + "%"; }
    function normalizeObservationSettings(value) {
        const invalid=(key,message)=>{const error=new Error(message);error.field=observationFields[key];throw error;};
        if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).length!==Object.keys(observationDefaults).length||value.version!==1)throw new Error('观察设置格式无效，已恢复默认。');
        for(const key of Object.keys(value))if(!(key in observationDefaults))throw new Error('观察设置包含未知字段。');
        const numeric=(key,label,min,max,integer=false,optional=false)=>{
            const n=value[key];if(optional&&n===null)return;
            if(typeof n!=='number'||!Number.isFinite(n)||n<min||n>max||(integer?!Number.isInteger(n):Math.abs(n*100-Math.round(n*100))>1e-7))invalid(key,label+'无效，请填写'+min+'～'+max+(integer?'之间的整数。':'之间、最多两位小数的数值。'));
        };
        numeric('months','观察月数',1,12,true);numeric('minimumRoi','每月最低收益率',-100,1000);
        numeric('minimumCount','每月结算笔数',1,1000000000,true);numeric('minimumDays','每月投注日数',1,31,true);
        numeric('maximumSpread','月收益率差距',0,1000000000,false,true);numeric('maximumMonthlyDrawdownUnits','月回撤注数',0,1000000000,false,true);numeric('maximumUnderwaterDays','回撤持续天数',0,1000000000,true,true);
        for(const key of ['includePartial','matchesOnly','showRuntime'])if(typeof value[key]!=='boolean')invalid(key,'观察开关无效。');
        if(!['red','blue','purple'].includes(value.highlight))invalid('highlight','请选择有效的高亮颜色。');
        return {...value};
    }
    function observationFormValue() {
        const value={version:1};
        for(const [key,id] of Object.entries(observationFields)){
            const input=$(id);input.setAttribute('aria-invalid','false');
            if(key==='includePartial')value[key]=input.value==='true'?true:input.value==='false'?false:null;
            else if(['matchesOnly','showRuntime'].includes(key))value[key]=input.checked;
            else if(key==='highlight')value[key]=input.value;
            else value[key]=input.value===''?(key.startsWith('maximum')?null:NaN):Number(input.value);
        }
        return normalizeObservationSettings(value);
    }
    function fillObservationForm(settings) {
        for(const [key,id] of Object.entries(observationFields)){
            const input=$(id);input.setAttribute('aria-invalid','false');
            if(['matchesOnly','showRuntime'].includes(key))input.checked=settings[key];
            else input.value=settings[key]===null?'':String(settings[key]);
        }
    }
    function observationRuleText(settings) {
        const parts=['近'+settings.months+'个月',settings.includePartial?'包含未完整月':'仅完整月','每月ROI ≥ '+money(settings.minimumRoi)+'%','每月 ≥ '+settings.minimumCount+'笔 / '+settings.minimumDays+'日'];
        if(settings.maximumSpread!==null)parts.push('月间差距 ≤ '+money(settings.maximumSpread)+' 个百分点');
        if(settings.maximumMonthlyDrawdownUnits!==null)parts.push('月回撤 ≤ '+money(settings.maximumMonthlyDrawdownUnits)+' 注');
        if(settings.maximumUnderwaterDays!==null)parts.push('回撤持续 ≤ '+settings.maximumUnderwaterDays+' 天');
        if(settings.matchesOnly)parts.push('仅看符合规则');
        return parts.join(' · ');
    }
    function renderObservationSettings() {
        $('labObservationActiveSummary').textContent='当前显示：'+observationRuleText(state.observation.settings);
        $('labQuickObservationSummary').textContent='近'+state.observation.settings.months+'个月 · 每月 ≥ '+money(state.observation.settings.minimumRoi)+'%';
        $('labQuickMatchesOnly').checked=state.observation.settings.matchesOnly;
    }
    function initializeObservation() {
        let notice='';
        try{const saved=localStorage.getItem(observationPreferenceKey);if(saved)state.observation.settings=normalizeObservationSettings(JSON.parse(saved));}
        catch(error){state.observation.settings={...observationDefaults};notice='已使用默认观察规则；之前的设置无法读取或格式无效，本页仍可调整。';}
        fillObservationForm(state.observation.settings);renderObservationSettings();if(notice)feedback('labObservationFeedback',notice,false);
    }
    function persistObservationSettings(settings) {
        try{localStorage.setItem(observationPreferenceKey,JSON.stringify(settings));return '观察规则已应用并保存到本浏览器。';}
        catch(_){return '观察规则已在本页生效；当前浏览器不能保存设置，重新打开后将使用默认值。';}
    }
    function observationUrl(kind,id,settings=state.observation.settings) {
        return base+'/'+(kind==='study'?'studies':'batches')+'/'+id+'?observation_months='+settings.months+'&observation_partial='+String(settings.includePartial);
    }
    function validateObservationRows(rows,settings) {
        const fail=()=>{throw responseError('观察数据不完整或口径不一致，请重新读取',0,'invalid_observation');};
        if(!Array.isArray(rows))fail();
        const integer=n=>Number.isSafeInteger(n),nullable=n=>n===null||integer(n),date=s=>typeof s==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(s);
        const monthIndex=s=>Number(s.slice(0,4))*12+Number(s.slice(5,7))-1;
        const status=['complete','partial','pending','no_samples','not_replayed','not_observed','missing'];
        for(const row of rows){
            const o=row.observation,w=o?.window;
            if(o?.version!==1||!integer(o.original_stake_cents)||o.original_stake_cents<=0||!w||w.requested_months!==settings.months||w.include_partial!==settings.includePartial||!date(w.start_date)||!date(w.end_date)||w.actual_through_date!==null&&!date(w.actual_through_date)||typeof w.period_reached!=='boolean'||typeof w.has_partial!=='boolean'||!integer(w.available_months)||!integer(w.complete_months)||!Array.isArray(o.months)||o.months.length!==settings.months)fail();
            let previous=null;
            for(const m of o.months){
                if(!m||typeof m.month!=='string'||!/^\d{4}-(?:0[1-9]|1[0-2])$/.test(m.month)||!status.includes(m.status)||typeof m.complete_month!=='boolean'||!integer(m.stake_cents)||m.stake_cents<0||!integer(m.net_cents)||!integer(m.settled_count)||m.settled_count<0||!integer(m.betting_days)||m.betting_days<0||!integer(m.pending_count)||m.pending_count<0||!nullable(m.daily_close_drawdown_cents)||!nullable(m.worst_day_cents))fail();
                const index=monthIndex(m.month);if(previous!==null&&index!==previous+1)fail();previous=index;
            }
            if(o.months[0].month!==w.start_month||o.months.at(-1).month!==w.end_month)fail();
            const summary=o.summary;
            if(!summary||!integer(summary.net_cents)||!integer(summary.stake_cents)||!integer(summary.settled_count)||!integer(summary.min_month_count)||!integer(summary.min_month_days)||!nullable(summary.min_month_drawdown_cents)||!nullable(summary.max_month_drawdown_cents)||!nullable(summary.net_without_best_three_days_cents)||!nullable(summary.best_three_days_cents)||!nullable(summary.longest_underwater_days)||!nullable(summary.current_underwater_days)||typeof summary.recovery_left_censored!=='boolean')fail();
            if(!o.quality||!Array.isArray(o.quality.blockers)||!Array.isArray(o.quality.warnings)||o.quality.blockers.some(x=>typeof x!=='string')||o.quality.warnings.some(x=>typeof x!=='string'))fail();
            for(const period of [o.recent,o.previous])if(!period||typeof period.available!=='boolean'||!date(period.start_date)||!date(period.end_date)||!nullable(period.stake_cents)||!nullable(period.net_cents)||!nullable(period.settled_count))fail();
        }
    }
    async function applyObservation() {
        if(state.mutating||state.accessDenied||(state.loading&&!state.observation.loading))return;
        let next;
        try{next=observationFormValue();}
        catch(error){if(error.field){$(error.field).setAttribute('aria-invalid','true');$(error.field).focus();}feedback('labObservationFeedback',error.message,true);return;}
        const current=state.observation.settings,windowChanged=next.months!==current.months||next.includePartial!==current.includePartial;
        const observationRevision=++state.observation.revision;
        if(!windowChanged){
            if(state.observation.loading){state.readRevision++;state.observation.loading=false;state.loading=false;}
            state.observation.settings=next;state.observation.dirty=false;state.resultPage=0;state.studyPage=0;
            renderObservationSettings();renderResults();renderStudyResults();feedback('labObservationFeedback',persistObservationSettings(next),false);updateControls();return;
        }
        const revision=++state.readRevision,batchId=state.selectedId,studyId=state.studyId;
        state.observation.loading=true;state.loading=true;updateControls();
        feedback('labObservationFeedback','正在读取新窗口；下方仍显示 '+observationRuleText(current)+'。',false);
        try{
            const [batch,study]=await Promise.all([batchId===null?null:request(observationUrl('batch',batchId,next)),studyId===null?null:request(observationUrl('study',studyId,next))]);
            if(revision!==state.readRevision||observationRevision!==state.observation.revision||batchId!==state.selectedId||studyId!==state.studyId)return;
            if(batch)validateObservationRows(batch.experiments,next);if(study)validateObservationRows(study.rows,next);
            closeDetail(false);state.observation.settings=next;state.observation.dirty=false;state.resultPage=0;state.studyPage=0;
            if(batch){state.batch=batch.batch;state.experiments=batch.experiments;}
            if(study){state.study=study.study;state.studyRows=study.rows;}
            renderObservationSettings();renderResults();renderStudyResults();feedback('labObservationFeedback',persistObservationSettings(next),false);
        }catch(error){
            if(revision!==state.readRevision||observationRevision!==state.observation.revision)return;
            if(error.status===401||error.status===403)denyAccess();
            state.observation.dirty=true;feedback('labObservationFeedback',error.message+'。新窗口未应用，当前仍显示 '+observationRuleText(state.observation.settings)+'。',true);
        }finally{
            if(revision===state.readRevision&&observationRevision===state.observation.revision){state.observation.loading=false;state.loading=false;updateControls();}
        }
    }
    function observationRatioOrder(a,b) {
        const difference=BigInt(a.net_cents)*BigInt(b.stake_cents)-BigInt(b.net_cents)*BigInt(a.stake_cents);
        return difference<0n?-1:difference>0n?1:0;
    }
    function observationRoiBounds(o) {
        const months=o.months.filter(m=>m.stake_cents>0);if(!months.length)return null;
        let low=months[0],high=months[0];for(const m of months){if(observationRatioOrder(m,low)<0)low=m;if(observationRatioOrder(m,high)>0)high=m;}
        return {low,high,count:months.length};
    }
    function assessObservation(item) {
        const settings=state.observation.settings,o=item.observation;
        const result=(kind,label,reason,matches=false)=>({kind,label,reason,matches});
        if(!o)return result('unready','待读取观察数据','');
        if(o.quality.blockers.length)return result('blocked','数据待核对',o.quality.blockers.map(key=>observationBlockerNames[key]||'数据状态需核对').join('；'));
        const missing=o.months.some(m=>['missing','not_replayed','not_observed'].includes(m.status));
        if(missing||!settings.includePartial&&o.months.some(m=>!m.complete_month))return result('history','历史不足','已有 '+(settings.includePartial?o.window.available_months:o.window.complete_months)+' / '+settings.months+' 个月');
        if(!o.window.period_reached)return result('unready','数据未就绪','尚未计算至窗口末日');
        if(o.months.some(m=>m.settled_count<settings.minimumCount||m.betting_days<settings.minimumDays||m.stake_cents<=0))return result('sample','样本不足','每月最少 '+o.summary.min_month_count+' 笔 / '+o.summary.min_month_days+' 日');
        const threshold=BigInt(Math.round(settings.minimumRoi*100));
        if(o.months.some(m=>BigInt(m.net_cents)*10000n<threshold*BigInt(m.stake_cents)))return result('unmet','未达标','有月份收益率低于 '+money(settings.minimumRoi)+'%');
        const bounds=observationRoiBounds(o),failures=[];
        if(settings.maximumSpread!==null){
            if(!bounds||bounds.count<2)failures.push('月间差距样本不足');
            else{
                const difference=BigInt(bounds.high.net_cents)*BigInt(bounds.low.stake_cents)-BigInt(bounds.low.net_cents)*BigInt(bounds.high.stake_cents);
                if(difference*10000n>BigInt(Math.round(settings.maximumSpread*100))*BigInt(bounds.low.stake_cents)*BigInt(bounds.high.stake_cents))failures.push('月间波动超限');
            }
        }
        if(settings.maximumMonthlyDrawdownUnits!==null){
            if(o.summary.max_month_drawdown_cents===null)failures.push('月回撤尚不可计算');
            else if(BigInt(o.summary.max_month_drawdown_cents)*100n>BigInt(Math.round(settings.maximumMonthlyDrawdownUnits*100))*BigInt(o.original_stake_cents))failures.push('月回撤超限');
        }
        if(settings.maximumUnderwaterDays!==null){
            if(o.summary.longest_underwater_days===null)failures.push('回撤持续尚不可计算');
            else if(o.summary.longest_underwater_days>settings.maximumUnderwaterDays)failures.push('回撤持续超限');
        }
        if(failures.length)return result('risk','收益达标 · 需关注',failures.join('；'));
        return o.window.has_partial?result('stage','阶段达标','含未完整月',true):result('match','完整月达标','符合当前观察规则',true);
    }
    function observationMetric(item,kind) {
        const o=item.observation;if(!o)return null;
        const bounds=observationRoiBounds(o);
        if(kind==='minimum_roi')return bounds?bounds.low.net_cents/bounds.low.stake_cents*100:null;
        if(kind==='monthly_spread')return bounds?.count>=2?(bounds.high.net_cents/bounds.high.stake_cents-bounds.low.net_cents/bounds.low.stake_cents)*100:null;
        if(kind==='monthly_drawdown')return o.summary.max_month_drawdown_cents===null?null:o.summary.max_month_drawdown_cents/o.original_stake_cents;
        return null;
    }
    function sortObservationRows(rows,kind) {
        if(kind==='profit')rows.sort((a,b)=>(b.observation?.summary.net_cents??-Infinity)-(a.observation?.summary.net_cents??-Infinity)||a.id-b.id);
        else if(kind==='sample')rows.sort((a,b)=>b.settled_count-a.settled_count||a.id-b.id);
        else if(kind==='drawdown')rows.sort((a,b)=>a.max_drawdown-b.max_drawdown||a.id-b.id);
        else if(kind==='observation'){
            const ranks={match:0,stage:1,risk:2,unmet:3,sample:4,history:5,unready:6,blocked:7};rows.sort((a,b)=>ranks[assessObservation(a).kind]-ranks[assessObservation(b).kind]||a.id-b.id);
        }else if(['minimum_roi','monthly_spread','monthly_drawdown'].includes(kind))rows.sort((a,b)=>{
            const av=observationMetric(a,kind),bv=observationMetric(b,kind);if(av===null||bv===null)return av===bv?a.id-b.id:av===null?1:-1;return (kind==='minimum_roi'?bv-av:av-bv)||a.id-b.id;
        });
    }
    function observationEmptyRow(target,columns,message) {const row=element('tr'),cell=element('td',message,'lab-empty');cell.colSpan=columns;row.append(cell);target.append(row);}
    function renderObservationCount(id,rows) {
        const full=rows.filter(r=>r.assessment.kind==='match').length,stage=rows.filter(r=>r.assessment.kind==='stage').length;
        $(id).textContent='当前候选 '+rows.length+' 个 · 完整月达标 '+full+' · 阶段达标 '+stage+(state.observation.settings.matchesOnly?' · 仅显示符合规则':'')+'。规则用于观察筛选，不代表未来收益。';
    }
    function observationRuntime(item,batch) {
        const details=element('details',null,'lab-observation-runtime');details.open=state.observation.settings.showRuntime;details.append(element('summary','运行信息'));
        const risk=item.risk_state||{mode:'legacy',state:'legacy'};
        const lines=[sampleLabel(item,batch),'实赔 / 退本 / 全亏：'+resultCounts(item),'轮状态：'+(riskStates[risk.state]||risk.state),'统计日：'+(risk.day_date||'—'),
            '该日净利 / 开轮：'+(risk.mode==='round'?money(risk.day_net_profit,true)+' / '+risk.day_round_count:'—'),'本轮净利：'+money(risk.round_net_profit,true),
            '停止原因：'+(reasonNames[risk.stop_reason]||risk.stop_reason||'—')];
        if(item.completed_rounds!==undefined)lines.push('完成轮：'+item.completed_rounds,'盈利日 / 投注日：'+item.profitable_days+' / '+item.betting_days);
        lines.forEach(line=>details.append(element('p',line,'lab-muted')));return details;
    }
    function fillOwnerSelect(id,items,selected,empty) {
        const select=$(id);select.replaceChildren();
        if(!items.length){const option=element('option',empty);option.value='';select.append(option);return;}
        for(const item of items){const option=element('option',item.name);option.value=String(item.id);select.append(option);}
        if(items.some(item=>item.id===selected))select.value=String(selected);
    }
    function renderLayout() {
        const studies=state.layout.tab==='studies';
        $('labTabExperiments').setAttribute('aria-selected',String(!studies));$('labTabExperiments').tabIndex=studies?-1:0;
        $('labTabStudies').setAttribute('aria-selected',String(studies));$('labTabStudies').tabIndex=studies?0:-1;
        $('labBatchSelection').hidden=studies;$('labStudySelection').hidden=!studies;
        $('labResults').hidden=studies||!state.batch;$('labStudyResults').hidden=!studies||!state.study;
        $('labNoResults').hidden=studies?Boolean(state.study):Boolean(state.batch);
        $('labNoResults').textContent=studies?'暂无风控比较组。可在实验操作中创建比较。':'暂无实验结果。可选择批次或新建实验。';
        for(const button of document.querySelectorAll('[data-result-view]'))button.setAttribute('aria-pressed',String(button.dataset.resultView===state.layout.view));
        $('labMonthlyMetricField').hidden=state.layout.view!=='monthly';
        $('labMonthlyMetricSelect').value=state.layout.monthlyMetric;
        const rows=studies?state.studyRows:state.experiments,owner=studies?state.overview?.batches.find(b=>b.id===rows[0]?.batch_id):state.batch;
        const observation=rows[0]?.observation;
        const sources=new Set(rows.map(row=>state.overview?.batches.find(b=>b.id===row.batch_id)?.predictor_name).filter(Boolean));
        const source=sources.size===1?[...sources][0]:owner?.predictor_name||'';
        const stakes=new Set(rows.map(row=>row.observation?.original_stake_cents).filter(Number.isFinite));
        const scope=observation?observation.window.start_date+'—'+observation.window.end_date:'';
        const known=rows.some(row=>row.observation?.quality.warnings.includes('known_skips'));
        $('labResultMeta').textContent=[source,owner?metricNames[owner.metric]:'',scope,stakes.size===1?'每注 '+money([...stakes][0]/100):'',known?'含已知跳过':''].filter(Boolean).join(' · ');
        if(state.selectedId!==null&&[...$('labBatchSelect').options].some(o=>o.value===String(state.selectedId)))$('labBatchSelect').value=String(state.selectedId);
        if(state.studyId!==null&&[...$('labStudySelect').options].some(o=>o.value===String(state.studyId)))$('labStudySelect').value=String(state.studyId);
    }
    function changeResultView(view) {
        if(!['overview','monthly','risk'].includes(view))return;
        state.layout.view=view;renderResults();renderStudyResults();renderLayout();updateControls();
    }
    function compactColumnCount(rows) {return state.layout.view==='overview'?5:state.layout.view==='risk'?6:2+(rows[0]?.observation?.months.length||state.observation.settings.months);}
    function compactStatus(item) {
        const assessment=assessObservation(item),cell=element('td',null,'lab-compact-status');
        cell.dataset.label='观察状态';const tag=element('span',assessment.label,'lab-observation-tag lab-observation-'+assessment.kind);
        if(assessment.kind==='match')tag.dataset.highlight=state.observation.settings.highlight;
        tag.title=assessment.reason;tag.setAttribute('aria-label',assessment.label+(assessment.reason?'：'+assessment.reason:''));cell.append(tag);return cell;
    }
    function compactValue(value,label,className='') {const cell=element('td',value,'lab-compact-value '+className);cell.dataset.label=label;return cell;}
    function renderCompactTable(tableId,body,rows,study) {
        const table=$(tableId),view=state.layout.view;table.dataset.resultView=view;
        const metric=state.layout.monthlyMetric,months=(rows[0]||(study?state.studyRows[0]:state.experiments[0]))?.observation?.months||[];
        const headings=view==='overview'?['条件 / 赔率','观察状态','最低月 ROI','最高月回撤（日结）','窗口净利']:
            view==='risk'?['条件 / 赔率','观察状态','最大月回撤（日结）','回撤持续 / 未恢复','去掉最佳3天净利','末14天 / 前14天 ROI']:
            ['条件 / 赔率','观察状态',...months.map(month=>month.month+(metric==='roi'?' ROI':' 日结回撤'))];
        const head=element('tr');headings.forEach(text=>{const th=element('th',text);th.scope='col';head.append(th);});table.querySelector('thead').replaceChildren(head);
        table.querySelector('caption').textContent=view==='overview'?'观察窗口内统计；点选条件查看详情。':view==='risk'?'窗口内日结统计；回撤持续含尚未恢复时段。':'* 未完整月；— 无可用数据。';
        body.replaceChildren();
        for(const item of rows){
            const row=element('tr'),id=item.id||item.experiment_id,o=item.observation,assessment=assessObservation(item);
            row.dataset.experimentId=String(id);row.dataset.observationState=assessment.kind;row.dataset.matches=String(assessment.matches);
            const identity=element('td',null,'lab-compact-identity');
            const button=actionButton(conditionName(item.condition),()=>openDetail(item,button));button.className='lab-condition-button';button.dataset.experimentId=String(id);button.setAttribute('aria-haspopup','dialog');button.setAttribute('aria-controls','labExperimentDetail');
            identity.append(button,element('span',settlementName(item)+(study?' · '+item.variant_label:''),'lab-identity-secondary'));row.append(identity,compactStatus(item));
            if(view==='overview'){
                row.append(compactValue(percent(observationMetric(item,'minimum_roi')),'最低月ROI'),
                    compactValue(o?.summary.max_month_drawdown_cents===null||!o?'—':money(o.summary.max_month_drawdown_cents/100),'月回撤'),
                    compactValue(o?money(o.summary.net_cents/100,true):'—','窗口净利',o?profitClass(o.summary.net_cents):''));
            }else if(view==='monthly'){
                for(const month of o?.months||[]){const value=metric==='roi'?(month.stake_cents>0?percent(month.net_cents/month.stake_cents*100):'—'):(month.daily_close_drawdown_cents===null?'—':money(month.daily_close_drawdown_cents/100));row.append(compactValue(value+(month.complete_month?'':' *'),month.month));}
            }else{
                const max=o?.summary.max_month_drawdown_cents,days=o?.summary.longest_underwater_days,current=o?.summary.current_underwater_days;
                row.append(compactValue(max===null||max===undefined?'—':money(max/100),'月回撤'),
                    compactValue(days===null||days===undefined?'—':days+' / '+(current??'—')+'天','持续 / 未恢复'),
                    compactValue(o?.summary.net_without_best_three_days_cents===null||!o?'—':money(o.summary.net_without_best_three_days_cents/100,true),'去掉最佳3天'),
                    compactValue([o?.recent,o?.previous].map(period=>period?.available&&period.stake_cents>0?percent(period.net_cents/period.stake_cents*100):'—').join(' / '),'末14 / 前14天'));
            }
            row.addEventListener('click',event=>{if(!event.target.closest('button,a,input,select,summary'))button.click();});
            row.addEventListener('keydown',event=>{if(event.target===row&&(event.key==='Enter'||event.key===' ')){event.preventDefault();button.click();}});
            body.append(row);
        }
    }
    function drawerElements() {return ['labExperimentDetail','labObservationPanel','labGenerator','labSystemPanel','labManagementPanel','labContinuationPanel','labSelfServicePanel','labResearchPanel'].map($);}
    function drawerFocusable(node) {
        return [...node.querySelectorAll('button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),summary,[href],[tabindex="0"]')].filter(item=>{
            if(item.closest('[hidden]'))return false;
            for(let parent=item;parent&&parent!==node;parent=parent.parentElement){
                if(getComputedStyle(parent).display==='none'||getComputedStyle(parent).visibility==='hidden')return false;
                if(parent.tagName==='DETAILS'&&!parent.open&&parent!==node&&!parent.querySelector('summary')?.contains(item))return false;
            }
            return true;
        });
    }
    function openDrawer(id,trigger) {
        if(state.layout.drawer?.id===id)return true;
        if(closeActiveDrawer(false,true)===false)return false;
        const node=$(id),background=[...document.body.children].filter(child=>!drawerElements().includes(child)&&child.id!=='labDrawerBackdrop'&&!['SCRIPT','STYLE'].includes(child.tagName));
        state.layout.drawer={id,trigger:trigger||document.activeElement,background:background.map(child=>({node:child,inert:child.inert,aria:child.getAttribute('aria-hidden')}))};
        node.hidden=false;if(node.tagName==='DETAILS')node.open=true;
        $('labDrawerBackdrop').hidden=false;document.body.classList.add('lab-drawer-open');
        for(const child of background){child.inert=true;child.setAttribute('aria-hidden','true');}
        const focus=drawerFocusable(node)[0]||node;focus.focus({preventScroll:true});return true;
    }
    function dismissDrawer(id,restoreFocus=true) {
        const drawer=state.layout.drawer;if(!drawer||drawer.id!==id)return;
        state.layout.drawer=null;const node=$(id);node.hidden=true;if(node.tagName==='DETAILS')node.open=false;
        $('labDrawerBackdrop').hidden=true;document.body.classList.remove('lab-drawer-open');
        for(const item of drawer.background){item.node.inert=item.inert;if(item.aria===null)item.node.removeAttribute('aria-hidden');else item.node.setAttribute('aria-hidden',item.aria);}
        if(restoreFocus&&drawer.trigger?.isConnected)drawer.trigger.focus({preventScroll:true});
    }
    function closeActiveDrawer(restoreFocus=true,force=false) {
        const drawer=state.layout.drawer;if(!drawer)return true;if(state.mutating&&!force)return false;
        const hooks=drawerExtensions.get(drawer.id);if(hooks?.close&&!hooks.close(restoreFocus))return false;
        if(drawer.id==='labExperimentDetail')closeDetail(restoreFocus);
        else if(drawer.id==='labManagementPanel')closeManagement(restoreFocus);
        else if(drawer.id==='labContinuationPanel')closeContinuation(restoreFocus);
        else{if(drawer.id==='labObservationPanel'&&!state.observation.loading){fillObservationForm(state.observation.settings);state.observation.dirty=false;}dismissDrawer(drawer.id,restoreFocus);}
    }
    function initializeLayout() {
        for(const node of drawerElements()){node.classList.add('lab-page');document.body.append(node);node.hidden=true;}
        $('labBatchSelect').addEventListener('change',()=>{if($('labBatchSelect').value)selectBatch(Number($('labBatchSelect').value));});
        $('labStudySelect').addEventListener('change',()=>{if($('labStudySelect').value)openStudy(Number($('labStudySelect').value));});
        const selectTab=tab=>{if(state.mutating)return;state.layout.tab=tab;renderLayout();};
        $('labTabExperiments').addEventListener('click',()=>selectTab('experiments'));$('labTabStudies').addEventListener('click',()=>selectTab('studies'));
        $('labResultTabs').addEventListener('keydown',event=>{if(['ArrowLeft','ArrowRight','Home','End'].includes(event.key)){event.preventDefault();const tab=event.key==='Home'?'experiments':event.key==='End'?'studies':state.layout.tab==='studies'?'experiments':'studies';selectTab(tab);$(tab==='studies'?'labTabStudies':'labTabExperiments').focus();}});
        for(const button of document.querySelectorAll('[data-result-view]'))button.addEventListener('click',()=>changeResultView(button.dataset.resultView));
        $('labMonthlyMetricSelect').addEventListener('change',()=>{state.layout.monthlyMetric=$('labMonthlyMetricSelect').value;renderResults();renderStudyResults();});
        $('labQuickMatchesOnly').addEventListener('change',()=>{const checked=$('labQuickMatchesOnly').checked;fillObservationForm(state.observation.settings);$('labObservationMatchesOnly').checked=checked;applyObservation();});
        $('labAdjustObservation').addEventListener('click',()=>{fillObservationForm(state.observation.settings);openDrawer('labObservationPanel',$('labAdjustObservation'));});
        $('labNewExperiment').addEventListener('click',()=>extensions.bulk?extensions.bulk():openDrawer('labGenerator',$('labNewExperiment')));$('labSystemToggle').addEventListener('click',()=>openDrawer('labSystemPanel',$('labSystemToggle')));
        for(const id of ['labObservationClose','labGeneratorClose','labSystemClose'])$(id).addEventListener('click',()=>closeActiveDrawer(true));
        $('labDrawerBackdrop').addEventListener('click',()=>closeActiveDrawer(true));
        document.addEventListener('keydown',event=>{
            const drawer=state.layout.drawer;if(!drawer)return;
            if(event.key==='Escape'){event.preventDefault();closeActiveDrawer(true);}
            else if(event.key==='Tab'){
                const node=$(drawer.id),focusable=drawerFocusable(node),first=focusable[0]||node,last=focusable.at(-1)||node;
                if(event.shiftKey&&(document.activeElement===first||!node.contains(document.activeElement))){event.preventDefault();last.focus();}
                else if(!event.shiftKey&&(document.activeElement===last||!node.contains(document.activeElement))){event.preventDefault();first.focus();}
            }
        });
        renderLayout();
    }
    function bytes(value) {
        const n = number(value, null);
        if (n === null) return "—";
        return n >= 1073741824 ? (n / 1073741824).toFixed(2) + " GiB" : (n / 1048576).toFixed(1) + " MiB";
    }
    function time(value) {
        const stamp = Date.parse(value || "");
        return Number.isFinite(stamp) ? new Intl.DateTimeFormat("zh-CN", {timeZone:"Asia/Shanghai", year:"numeric", month:"2-digit", day:"2-digit", hour:"2-digit", minute:"2-digit", second:"2-digit", hour12:false}).format(stamp) : "—";
    }
    function today() { return new Date(Date.now() + 8 * 3600000).toISOString().slice(0, 10); }
    function modeName(mode) { return mode === "backtest" ? "历史回测" : "前向模拟"; }
    function settlementName(item) {return item.settlement_rule_name+(item.legacy_odds?'（旧赔率）':'');}
    function frozenOddsText(item) {
        const odds=item.settlement_odds||{},metric=item.condition?.metric;
        const n=value=>typeof value==='number'?String(value):'未知';
        return metric==='combo'?'小单/大双 '+n(odds.combo?.['小单'])+'；大单/小双 '+n(odds.combo?.['大单']):n(odds[metric]);
    }
    function conditionName(c) {
        if (!c || typeof c !== "object") return "条件信息不可用";
        const compare = c.operator === "lte" ? "≤" : "≥";
        if (c.type === "always") return "无条件跟随（基准）";
        if (c.type === "hit_rate") return "近 " + c.window_size + " 期预测命中率 " + compare + " " + c.threshold + "%";
        if (c.type === "miss_streak") return "预测连错 " + compare + " " + c.threshold + " 期";
        if (c.type === "hit_streak") return "预测连中 " + compare + " " + c.threshold + " 期";
        if (c.type === "draw_streak") return c.bet_value + "连续开出 " + compare + " " + c.threshold + " 期";
        if (c.type === "draw_absence") return c.bet_value + "连续不出 " + compare + " " + c.threshold + " 期";
        return "未知条件";
    }
    function profitClass(value) { return value > 0 ? "lab-profit" : value < 0 ? "lab-loss" : ""; }
    function resultCounts(item) { return item.hit_count + " / " + item.refund_count + " / " + item.miss_count; }
    function putSummary(target, entries) {
        target.replaceChildren();
        entries.forEach(([label, value]) => {const wrap = element("div"); wrap.append(element("dt", label), element("dd", value)); target.append(wrap);});
    }
    function actionButton(label, action) {
        const button = element("button", label, "lab-button"); button.type = "button"; button.addEventListener("click", action); return button;
    }
    function responseError(message, status, code, unknown) {
        const error = new Error(message); error.status = status; error.code = code; error.unknown = Boolean(unknown); return error;
    }
    async function request(path, method, payload, csv) {
        const verb = method || "GET";
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15000);
        try {
            let response;
            try {
                response = await fetch(path, {method:verb, credentials:"same-origin", signal:controller.signal,
                    headers:payload === undefined ? {} : {"Content-Type":"application/json"},
                    body:payload === undefined ? undefined : JSON.stringify(payload)});
            } catch (error) { throw responseError("连接暂时中断，请稍后刷新", 0, "network_error", verb === "POST"); }
            if (csv && response.ok) {
                if (!(response.headers.get("content-type") || "").includes("text/csv")) throw responseError("导出内容无效，请刷新后重试", response.status, "invalid_response");
                return response.blob();
            }
            const result = await response.json().catch(() => null);
            if (!response.ok) throw responseError(result && typeof result.error === "string" ? result.error : "请求失败，请稍后重试", response.status, result && result.code, verb === "POST" && response.status >= 500);
            if (!result || typeof result !== "object" || Array.isArray(result)) throw responseError("服务返回的数据不完整，请刷新后重试", response.status, "invalid_response", verb === "POST");
            return result;
        } finally {clearTimeout(timeout);}
    }
    function validateOptions(value) {
        const ids = (value.settlement_rules || []).map(rule => rule.id).sort().join(",");
        if (!Array.isArray(value.predictors) || !Array.isArray(value.metrics) || !value.limits || ids !== "pc28_high_regular,pc28_netdisk_regular") throw responseError("实验配置尚未就绪，请刷新后重试", 0, "invalid_response");
        return value;
    }
    function hasSources() { return Boolean(state.options && state.options.source_available && state.options.predictors.length); }
    function locked() { return state.loading || state.mutating || state.accessDenied; }
    function hasRunningWork() {
        const updating = item => item.continuation?.capture_enabled || item.continuation?.active_segment_id;
        return state.overview?.batches.some(batch => ['running','queued'].includes(batch.display_status||batch.status) || updating(batch)) ||
            state.studies.some(study => ['running', 'queued'].includes(study.status) || updating(study));
    }
    function isReadingOrEditing() {
        return state.detailId !== null || state.management !== null || state.continuation?.dirty || state.observation.dirty || state.layout.drawer !== null || !$("labEditRiskForm").hidden || !$("labComparisonPreview").hidden ||
            Boolean(document.activeElement?.matches('input,textarea,[contenteditable="true"]'));
    }
    function updateRefreshStatus() {
        $("labAutoRefreshStatus").textContent = !state.overview ? '' :
            (hasRunningWork() ? (isReadingOrEditing() ? '查看结果或编辑期间暂停页面自动更新，后台继续计算。' : '运行中每15秒自动更新；离开此页面后后台仍会继续计算。') : '当前没有运行中的模拟，可手动刷新。') +
            ' 最近读取：' + time(state.readAt);
    }
    function autoRefresh() {
        updateRefreshStatus();
        if (!document.hidden && !locked() && !state.exporting && hasRunningWork() && !isReadingOrEditing()) refresh(false);
    }
    function sampleLabel(item, batch) {
        if (item.settled_count > 0) return item.sample_label;
        if(batch?.display_status==='queued')return '等待队列运行';
        if(batch?.display_status==='draft')return '草稿尚未入队';
        if (batch?.status === 'completed') return '无已结算样本';
        if (batch?.status === 'running') return '尚无结算，处理中';
        if (batch?.status === 'paused' && !batch.processed_issues) return batch.study_id ? '等待队列运行' : '尚未启动';
        return '暂无已结算样本';
    }
    function updateControls() {
        $("labRefreshButton").disabled = state.loading || state.mutating;
        $("labRefreshButton").textContent = state.loading ? "读取中…" : "刷新";
        $("labRefreshButton").setAttribute("aria-busy", String(state.loading));
        $("labCreateButton").disabled = locked() || !hasSources();
        $("labCreateButton").textContent = state.mutating ? "处理中…" : "生成并保存";
        $("labBatchForm").setAttribute("aria-busy", String(state.mutating));
        $("labBatchForm").querySelectorAll("input,select").forEach(input => {input.disabled = state.mutating || state.accessDenied;});
        $("labPredictor").disabled = state.mutating || state.accessDenied || !hasSources();
        $("labBatchStateButton").disabled = locked() || !state.batch || ['completed','deleting'].includes(state.batch.status);
        $("labExportButton").disabled = locked() || state.exporting || !state.batch;
        $("labExportButton").textContent = state.exporting ? "导出中…" : "导出 CSV";
        $("labSort").disabled = locked() || !state.experiments.length;
        $("labResultPrevious").disabled = locked() || state.resultPage <= 0;
        $("labResultNext").disabled = locked() || (state.resultPage + 1) * pageSize >= state.resultFilteredCount;
        $("labBatchSelect").disabled=locked();$("labStudySelect").disabled=locked();
        $("labEditRiskButton").disabled=locked();$("labSaveRiskButton").disabled=locked();$("labCancelRiskButton").disabled=state.mutating;
        ['labCompareButton','labCreateStudyButton','labStudyExportButton','labTemplatesButton','labQueueButton','labNewExperiment','labResearchButton','labFavoriteDetailButton'].forEach(id=>$(id).disabled=locked());
        $("labStudyPrevious").disabled=locked()||state.studyPage<=0;
        $("labStudyNext").disabled=locked()||(state.studyPage+1)*pageSize>=state.studyFilteredCount;
        $("labStudySort").disabled=locked()||!state.studyRows.length;
        $("labObservationApply").disabled=state.mutating||state.accessDenied||(state.loading&&!state.observation.loading);
        $("labObservationReset").disabled=state.mutating||state.accessDenied;
        $("labObservationApply").textContent=state.observation.loading?'读取中，可应用其他规则':'应用规则';
        $("labObservationForm").setAttribute('aria-busy',String(state.observation.loading));
        $("labStudyStateButton").disabled=locked()||!state.study||['completed','deleting'].includes(state.study.status);
        ['labDisplayStake','labAmountReset'].forEach(id=>$(id).disabled=locked()||!state.detailResult);

        ['labContinuationButton','labStudyContinuationButton','labContinuationRefresh','labContinuationMore','labContinuationPause'].forEach(id=>$(id).disabled=locked());
        $("labContinuationClose").disabled=state.mutating;
        const continuation=state.continuation,preview=continuation?.preview,summary=preview?.continuation;
        const continuationLocked=locked()||!preview||!preview.eligible||Boolean(continuation?.unconfirmed);
        $("labContinuationRetry").hidden=!continuation?.unconfirmed?.body;
        $("labContinuationRetry").disabled=locked();
        ['labContinuationCapture','labContinuationSave'].forEach(id=>$(id).disabled=continuationLocked);
        $("labContinuationCompute").disabled=continuationLocked||!$("labContinuationCapture").checked;
        $("labContinuationDaily").disabled=continuationLocked||!$("labContinuationCompute").checked;
        const waiting=preview?.latest_request?.kind==='append'&&preview.latest_request.status==='waiting_inputs';
        $("labContinuationAppend").disabled=continuationLocked||!summary?.capture_enabled||Boolean(summary?.active_segment_id)||waiting;
        $("labContinuationEndDate").disabled=continuationLocked||Boolean(summary?.active_segment_id)||waiting;
        $("labContinuationPanel").setAttribute('aria-busy',String(state.loading||state.mutating));
        [['labEconomyButton','economy'],['labFastButton','fast']].forEach(([id,mode])=>{
            $(id).disabled=locked()||!state.performance?.available||state.performance.mode===mode;
            $(id).setAttribute('aria-pressed',String(state.performance?.mode===mode));
        });
        if(state.preview)$("labCreateStudyButton").disabled=locked()||state.preview.new_experiments>state.preview.capacity.remaining_saved||state.preview.candidate_count<2;
        const management=state.management,managementBlocked=locked()||!management?.preview||Boolean(management?.pending);
        ['labManageBatchButton','labManageStudyButton','labStorageRefresh'].forEach(id=>$(id).disabled=locked());
        ['labRenameSave','labManagementEdit','labManagementFork','labManagementDelete','labDefinitionSave'].forEach(id=>$(id).disabled=managementBlocked);
        $("labManagementClose").disabled=state.mutating||Boolean(management?.pending);$("labManagementRetry").hidden=!management?.pending;$("labManagementRetry").disabled=locked();
        $("labDeleteConfirm").disabled=managementBlocked||!management?.deletePreview?.eligible||$("labDeleteConfirmName").value!==management?.deletePreview?.name;
        ['labDeleteCancel','labDefinitionCancel'].forEach(id=>$(id).disabled=state.mutating);
        $("labManagementPanel").querySelectorAll('input,select').forEach(input=>input.disabled=state.mutating||Boolean(management?.pending));
        $("labDeleteConfirmName").disabled=state.mutating||Boolean(management?.pending)||!management?.deletePreview?.eligible;
        $("labManagementPanel").setAttribute('aria-busy',String(state.loading||state.mutating));
        if(management?.pending){['labManageBatchButton','labManageStudyButton','labContinuationButton','labStudyContinuationButton'].forEach(id=>$(id).disabled=true);$("labBatchSelect").disabled=true;$("labStudySelect").disabled=true;}
        $("labStorageReclaim").disabled=locked()||!state.storage?.incremental_ready||[...state.managementJobs.values()].some(job=>job.kind==='reclaim'&&!['completed','error'].includes(job.status));
        Object.keys(riskEditors).forEach(updateRiskEditor);
        updateRefreshStatus();
    }
    function renderOptions() {
        const options = state.options;
        if (!options) return;
        const selected = $("labPredictor").value;
        $("labPredictor").replaceChildren();
        options.predictors.forEach(item => {const option = element("option", item.name); option.value = String(item.id); $("labPredictor").append(option);});
        if (options.predictors.some(item => String(item.id) === selected)) $("labPredictor").value = selected;
        if (!options.predictors.length) {const option = element("option", "暂无可用预测来源"); option.value = ""; $("labPredictor").append(option);}
        feedback("labPredictorError", hasSources() ? "" : options.source_message || "暂无可用预测来源", true);
        $("labSettlementSummary").replaceChildren();
        options.settlement_rules.forEach(rule => {
            const row = element("div", null, "lab-settlement-row");
            const odds = n => number(n, 0).toLocaleString("en-US", {useGrouping:false, maximumFractionDigits:4});
            row.append(element("strong", rule.name), element("p", "大小 / 单双 " + odds(rule.big_small_odds) + "；小单 / 大双 " + odds(rule.small_odd_big_even_odds) + "；大单 / 小双 " + odds(rule.big_odd_small_even_odds)), element("p", rule.refund_note, "lab-muted"));
            $("labSettlementSummary").append(row);
        });
        const limits = options.limits;
        $("labBudgetDescription").textContent = "默认省资源CPU上限0.2核，加速上限1核；内存上限 " + bytes(limits.memory_max_bytes) + "；全服务最多同时运行 " + limits.max_running_experiments + " 个实验、保存 " + limits.max_total_experiments + " 个实验。长期保留模拟成绩和日/月结果，已结算逐笔及已结束轮次不保留。当前生效档位见上方运行速度。";
        $("labDateHelp").textContent = "日期按北京时间计算，最多回测 " + limits.max_backtest_days + " 天；仅使用实际留存的数据。";
        $("labStartDate").max = today(); $("labEndDate").max = today();
        updatePreview();
        renderRiskPresets("labRisk",$("labMetric").value);
    }
    function renderOverview() {
        if (!state.overview) return;
        const overview = state.overview, worker = overview.worker || {}, storage = overview.storage || {};
        putSummary($("labWorkerSummary"), [["后台状态", workerNames[worker.state] || "—"], ["后台更新于（北京时间）", time(worker.heartbeat_at)], ["我的实验版本", overview.batches.reduce((sum, batch) => sum + batch.experiment_count, 0)], ["模拟存储 / 预算", bytes(storage.used_bytes) + " / " + bytes(storage.budget_bytes)]]);
        $("labWorkerMessage").textContent = worker.message || "";
        fillOwnerSelect('labBatchSelect',overview.batches,state.selectedId,'暂无实验批次');
    }
    function updatePreview() {
        const metric=$("labMetric").value,selected=$("labTemplatePack").value;
        const packs=(state.options?.template_packs||[{id:'basic_v2',name:'综合条件 v2（多种触发）',metrics:['combo','big_small','odd_even'],condition_counts:{combo:46,big_small:25,odd_even:25},description:'含无条件基准、预测连错/连中、连续开出/不出和命中率；组合含7～30期长连错。'}]).filter(pack=>pack.selectable!==false&&pack.metrics.includes(metric));
        $("labTemplatePack").replaceChildren();packs.forEach(pack=>{const option=element('option',pack.name);option.value=pack.id;$("labTemplatePack").append(option);});
        if(packs.some(pack=>pack.id===selected))$("labTemplatePack").value=selected;
        const pack=packs.find(pack=>pack.id===$("labTemplatePack").value),count=pack?.condition_counts[metric]||0;
        $("labTemplateHelp").textContent=pack?.descriptions_by_metric?.[metric]||pack?.description||'暂无可用模板';
        $("labCandidatePreview").textContent = count + " 个条件 × 2 套结算 = " + (count*2) + " 个实验版本";
        const historical = $("labBatchForm").elements.mode.value === "backtest";
        $("labDates").hidden = !historical;
        $("labStartDate").required = historical; $("labEndDate").required = historical;
        $("labModeHelp").textContent = historical ? "先回放所选日期区间。完成后可在“增量续算”中保留后续数据、手动追加或开启每日自动续算。" : "从启动后的可用信号开始持续观察；与历史回测分别记账，不接续旧批次成绩。";
        if(riskEditors.labRisk)renderRiskPresets('labRisk',$("labMetric").value);
    }
    function clearFieldErrors() {
        $("labBatchForm").querySelectorAll("[aria-invalid]").forEach(input => input.removeAttribute("aria-invalid"));
        ["labName", "labPredictor", "labStake", "labStartDate", "labEndDate"].forEach(id => {$(id + "Error").textContent = "";});
    }
    function payloadFromForm() {
        clearFieldErrors();
        const fields = $("labBatchForm").elements;
        const payload = {name:fields.name.value.trim(), predictor_id:Number(fields.predictor_id.value), metric:fields.metric.value,
            mode:fields.mode.value, stake_amount:Number(fields.stake_amount.value), template_pack:fields.template_pack.value};
        const failures = [];
        function invalid(id, message) {$(id).setAttribute("aria-invalid", "true"); $(id + "Error").textContent = message; failures.push(id);}
        if (!payload.name || payload.name.length > 80) invalid("labName", "请填写 1～80 字符的批次名称");
        if (!hasSources() || !state.options.predictors.some(item => item.id === payload.predictor_id)) invalid("labPredictor", "请选择可用预测来源");
        if (!fields.stake_amount.value || !Number.isFinite(payload.stake_amount) || payload.stake_amount < .01 || payload.stake_amount > 10000 || Math.abs(payload.stake_amount * 100 - Math.round(payload.stake_amount * 100)) > 1e-7) invalid("labStake", "请输入 0.01～10000，最多两位小数");
        if (payload.mode === "backtest") {
            payload.start_date = fields.start_date.value; payload.end_date = fields.end_date.value;
            if (!payload.start_date) invalid("labStartDate", "请选择开始日期");
            if (!payload.end_date) invalid("labEndDate", "请选择结束日期");
            if (payload.end_date && payload.end_date > today()) invalid("labEndDate", "结束日期不能晚于今天");
            if (payload.start_date && payload.end_date) {
                const days = (Date.parse(payload.end_date) - Date.parse(payload.start_date)) / 86400000;
                if (days < 0) invalid("labEndDate", "结束日期不能早于开始日期");
                else if (days + 1 > state.options.limits.max_backtest_days) invalid("labEndDate", "回测区间不能超过 " + state.options.limits.max_backtest_days + " 天");
            }
        }
        if (failures.length) {$(failures[0]).focus(); return null;}
        payload.risk_policy=riskValue('labRisk');
        const riskProblem=riskError(payload.risk_policy);
        if(riskProblem){feedback('labFormFeedback',riskProblem,true);riskEditors.labRisk.notice.focus?.();return null;}
        return payload;
    }
    function closeDetail(restoreFocus) {
        dismissDrawer("labExperimentDetail",restoreFocus);state.detailRevision++; state.detailId = null;state.detailContext=null; $("labExperimentDetail").hidden = true;
        state.monthly=null;state.reportExperiment=null;state.detailResult=null;state.amountView=null;$("labAmountProjection").hidden=true;$("labAmountEstimateResults").hidden=true;$("labMonthly").hidden=true;$("labMonthlyExport").disabled=true;
        document.querySelectorAll("#labResultsBody button[aria-expanded],#labStudyRows button[aria-expanded]").forEach(button => button.setAttribute("aria-expanded", "false"));
        state.detailTrigger = null;
    }
    function renderResults() {
        const batch = state.batch;
        $("labResults").hidden = !batch||state.layout.tab!=="experiments";
        if (!batch) return;
        $("labResultTitle").textContent = batch.name;
        $("labBatchMeta").textContent = [modeName(batch.mode), statusNames[batch.display_status||batch.status] || batch.status, phaseNames[batch.phase] || batch.phase,
            batch.predictor_name, metricNames[batch.metric], "每期 " + money(batch.stake_amount), "已处理 " + batch.processed_issues + (batch.total_issues === null ? " 期" : " / " + batch.total_issues + " 期"), "批次更新于 " + time(batch.updated_at)].filter(Boolean).join(" · ");
        $("labBatchNotice").textContent = batch.history_notice || ""; $("labBatchNotice").hidden = !batch.history_notice;
        $("labBatchError").textContent = batch.error || ""; $("labBatchError").hidden = !batch.error;
        $("labBatchRiskSummary").textContent=riskSummary(batch.risk_policy)+(batch.risk_policy?.source?.rule_id?" · 复制自 "+batch.risk_policy.source.rule_name+" / "+batch.risk_policy.source.route_name+" / "+batch.risk_policy.source.subscription_name:"");
        $("labEditRiskButton").hidden=!batch.risk_editable;
        $("labContinuationButton").hidden=batch.mode!=='backtest';
        $("labContinuationButton").textContent=batch.study_id?'比较组增量续算':'增量续算';
        $("labCompareButton").hidden=Boolean(batch.study_id)||batch.mode!=='backtest'||batch.risk_policy?.mode!=='round'||batch.experiment_count<2;
        $("labResultsBody").closest('table').querySelector('caption').textContent=(batch.experiment_count===1?'当前条件与赔率的独立实验版本；':'同一条件的两套结算版本；')+'样本表现仍需验证。* 表示未完整月；— 为无可用收益率。统计日为最近处理日，回测中不代表真实今天。';
        if(!batch.risk_editable)$("labEditRiskForm").hidden=true;
        const skipCounts = Object.entries(batch.skip_counts || {}).filter(([, count]) => count > 0).map(([reason, count]) => (reasonNames[reason] || reason) + " " + count);
        $("labSkipCounts").textContent = "未执行记录：" + skipCounts.join("；"); $("labSkipCounts").hidden = !skipCounts.length;
        $("labBatchStateButton").hidden = batch.status === "completed"||Boolean(batch.study_id);
        if(batch.study_id)$("labBatchRiskSummary").append(document.createTextNode(' · 此子批次由比较组 #'+batch.study_id+' 统一管理'));
        $("labBatchStateButton").textContent = batch.continuation?.active_segment_id ? (batch.status==='running'?'暂停本段计算':'恢复本段计算') : batch.display_status === "queued" ? "暂停排队" : batch.status === "running" ? "暂停模拟" : batch.mode === "backtest" ? "加入运行队列" : batch.status === "paused" ? "启动模拟" : "恢复模拟";
        if(batch.continuation?.enabled)$("labBatchMeta").append(document.createTextNode(' · '+continuationBrief(batch.continuation)));
        const settled = state.experiments.reduce((sum, item) => sum + item.settled_count, 0);
        $("labResultEmpty").hidden = settled > 0;
        $("labResultEmpty").textContent = "尚无已结算模拟结果。" + (batch.study_id && batch.status === "paused" ? "此方案由比较队列统一调度，请在比较组查看当前进度。" : batch.status === "paused" ? "启动批次后开始观察。" : (phaseNames[batch.phase] || "等待可用数据") + "，条件不足时不会按零命中率处理。");
        const all=state.experiments.map(item=>({item,assessment:assessObservation(item)}));
        const list=all.filter(row=>!state.observation.settings.matchesOnly||row.assessment.matches).map(row=>row.item);
        const sort=$("labSort").value;
        sortObservationRows(list,sort);
        renderObservationCount('labObservationBatchSummary',all);
        $("labResultsBody").replaceChildren();state.resultFilteredCount=list.length;
        state.resultPage=Math.min(state.resultPage,Math.max(0,Math.ceil(list.length/pageSize)-1));
        const start=state.resultPage*pageSize;
        $("labResultPageInfo").textContent=list.length?'显示 '+(start+1)+'–'+Math.min(start+pageSize,list.length)+'，共 '+list.length+' 个实验版本':'没有符合当前观察规则的实验版本';
        renderCompactTable('labResultsTable',$("labResultsBody"),list.slice(start,start+pageSize),false);
        if(!list.length)observationEmptyRow($("labResultsBody"),compactColumnCount(list),'没有符合当前观察规则的实验版本。');
        renderLayout();
    }
    function denyAccess() {
        for(const handlers of drawerExtensions.values())if(handlers.reset)handlers.reset();
        closeActiveDrawer(false,true);
        state.observation.revision++;state.observation.loading=false;state.observation.dirty=false;state.loading=false;fillObservationForm(state.observation.settings);
        closeContinuation(false);closeManagement(false);state.managementJobs.clear();renderManagementJobs();
        if(state.managementPoll){clearTimeout(state.managementPoll);state.managementPoll=null;}
        state.storage=null;state.storageRequest=null;$("labStorageSummary").replaceChildren();$("labStorageExplanation").textContent='登录管理员账号后可查看存储状态。';feedback('labStorageFeedback','',false);
        state.accessDenied = true; state.selectedId = null; state.batch = null; state.experiments = []; state.overview = null;
        fillOwnerSelect("labBatchSelect",[],null,"请先登录管理员账号");
        $("labWorkerSummary").replaceChildren(); $("labWorkerMessage").textContent = "";
        closeDetail(false); renderResults();
        state.studies=[];state.study=null;state.studyRows=[];state.studyId=null;renderStudies();renderStudyResults();
        state.performance=null;renderPerformance();feedback('labPerformanceFeedback','',false);
    }
    async function selectBatch(id) {
        state.riskEditingId=null;$("labEditRiskForm").hidden=true;
        $("labComparisonPreview").hidden=true;state.preview=null;state.studyRequest=null;
        if (state.mutating || state.loading || state.accessDenied) return;
        if(state.continuation)closeContinuation(false);if(state.management)closeManagement(false);
        state.layout.tab='experiments';state.selectedId = id;closeDetail(false);
        await refresh(false);
    }
    async function refresh(includeOptions) {
        const revision = ++state.readRevision,observationSettings={...state.observation.settings};
        state.loading = true; updateControls();
        try {
            const calls = [request(base),request(base+'/studies'),readPerformance()];
            if (includeOptions !== false || !state.options) calls.push(request(base + "/options"));
            const [overview, studies, performance, options] = await Promise.all(calls);
            if (revision !== state.readRevision) return;
            if (!Array.isArray(overview.batches)) throw responseError("批次数据不完整，请刷新后重试", 0, "invalid_response");
            if (options) {state.options = validateOptions(options); renderOptions();}
            (overview.management_jobs||[]).forEach(trackManagementJob);
            if(state.storage&&overview.storage){state.storage=overview.storage;renderStorage();}
            state.accessDenied = false; state.overview = overview; state.readAt = new Date().toISOString();
            state.performance=performance;renderPerformance();
            state.studies=studies.studies||[];renderStudies();
            if (!state.studies.some(study => study.id === state.studyId)) {
                state.studyId = (state.studies.find(study => ['running','queued'].includes(study.status)) ||
                    state.studies[0])?.id ?? null;
                state.study=null;state.studyRows=[];renderStudyResults();renderStudies();
            }
            if (!overview.batches.some(batch => batch.id === state.selectedId)) state.selectedId = (overview.batches.find(batch => batch.status === 'running') || overview.batches[0])?.id ?? null;
            renderOverview();
            if (state.selectedId !== null) {
                const detail = await request(observationUrl("batch",state.selectedId,observationSettings));
                if (revision !== state.readRevision) return;
                if (!detail.batch || !Array.isArray(detail.experiments)) throw responseError("实验结果不完整，请刷新后重试", 0, "invalid_response");
                validateObservationRows(detail.experiments,observationSettings);
                if(state.batch?.id!==detail.batch.id){state.resultPage=0;$("labSort").value='paired';}
                closeDetail(false); state.batch = detail.batch; state.experiments = detail.experiments;
            } else {state.batch = null; state.experiments = [];}
            renderResults(); feedback("statusMessage", "", false);
            if(state.studyId!==null){const study=await request(observationUrl('study',state.studyId,observationSettings));if(revision!==state.readRevision)return;validateObservationRows(study.rows,observationSettings);state.study=study.study;state.studyRows=study.rows;renderStudyResults();}
            if(state.continuation)await readContinuation({preserveDraft:true});
        } catch (error) {
            if (revision !== state.readRevision) return;
            if (error.status === 401 || error.status === 403) denyAccess();
            else {state.selectedId=state.batch?.id??null;state.studyId=state.study?.id??null;renderOverview();renderStudies();renderLayout();}
            feedback("statusMessage", error.message + (state.overview ? "。所选结果读取失败，已保留之前显示的列表（" + time(state.readAt) + "）。" : "。可点击刷新重试。"), true);
        } finally {if (revision === state.readRevision) {state.loading = false; updateControls();}}
    }
    async function readPerformance() {
        try{return (await request(base+'/performance')).performance;}
        catch(error){if(error.status===401||error.status===403)throw error;return {available:false,mode:null,message:'暂时无法读取实际运行速度，请刷新重试'};}
    }
    function renderPerformance() {
        const p=state.performance;
        if(!p){$("labPerformanceStatus").textContent=state.accessDenied?'登录管理员账号后可查看运行速度':'正在读取实际运行档位…';return;}
        const name={economy:'省资源',fast:'加速'}[p.mode]||'待核对';
        $("labPerformanceStatus").textContent='当前生效：'+name+(typeof p.cpu_cores==='number'?' · CPU上限 '+p.cpu_cores+' 核':'')+(typeof p.memory_current_bytes==='number'?' · 当前内存 '+bytes(p.memory_current_bytes):'')+(p.message?'。'+p.message:'');
    }
    async function changePerformance(mode) {
        if(locked()||!state.performance?.available)return;
        state.mutating=true;updateControls();feedback('labPerformanceFeedback','正在切换运行速度…',false);
        try{
            state.performance=(await request(base+'/performance','POST',{mode})).performance;
            renderPerformance();feedback('labPerformanceFeedback','已切换为'+(mode==='fast'?'加速':'省资源')+'，当前实验将按新额度继续处理。',false);
        }catch(error){
            if(error.status===401||error.status===403){denyAccess();feedback('labPerformanceFeedback',error.message,true);}
            else{
                try{state.performance=await readPerformance();renderPerformance();}
                catch(readError){if(readError.status===401||readError.status===403)denyAccess();}
                const confirmed=state.performance?.available&&state.performance.mode===mode;
                feedback('labPerformanceFeedback',confirmed?'切换响应中断；已重新核对，当前生效为'+(mode==='fast'?'加速':'省资源')+'。':error.message+'。请以当前生效档位为准。',!confirmed);
            }
        }finally{state.mutating=false;updateControls();}
    }
    async function createBatch(event) {
        event.preventDefault();
        if (locked() || !hasSources()) return;
        const payload = payloadFromForm(); if (!payload) return;
        state.mutating = true; updateControls(); feedback("labFormFeedback", "正在生成实验版本…", false);
        try {
            const result = await request(base + "/batches", "POST", payload);
            if (!result.batch || !Number.isInteger(result.batch.id)) throw responseError("创建结果暂时无法确认", 0, "invalid_response", true);
            state.layout.tab='experiments';state.selectedId = result.batch.id; state.resultPage = 0; $("labSort").value = "paired";
            await refresh(false);
            feedback("labFormFeedback", "已保存 " + result.generated_count + " 个实验版本，当前已暂停。可在批次结果中启动模拟。", false);
            closeActiveDrawer(false,true);
            state.mutating=false;updateControls();$("labBatchSelect").focus({preventScroll:true});
            feedback("statusMessage", "实验批次已保存，尚未启动。", false);
        } catch (error) {
            if (error.status === 401 || error.status === 403) {denyAccess();feedback("statusMessage",error.message,true);}
            else {
                if (error.unknown) {
                    await refresh(false);
                    feedback("labFormFeedback", "创建结果暂时无法确认，已尝试刷新批次列表。请核对是否已生成，避免重复提交。", true);
                } else feedback("labFormFeedback", error.message, true);
                if(!state.accessDenied)openDrawer("labGenerator",$("labNewExperiment"));
            }
        } finally {state.mutating = false; updateControls();}
    }
    async function saveRisk(event) {
        event.preventDefault();if(locked()||!state.batch?.risk_editable||state.riskEditingId!==state.batch.id)return;
        const policy=riskValue('labEditRisk'),problem=riskError(policy);
        if(problem){feedback('labEditRiskFeedback',problem,true);return;}
        state.mutating=true;updateControls();
        try{
            await request(base+'/batches/'+state.batch.id+'/risk-policy','POST',{risk_policy:policy});
            $("labEditRiskForm").hidden=true;await refresh(false);feedback('statusMessage','风控已保存，批次仍暂停，来源与每注金额不变。',false);
        }catch(error){feedback('labEditRiskFeedback',error.message,true);}
        finally{state.mutating=false;updateControls();}
    }
    async function changeState(id, status) {
        if (locked()) return;
        state.mutating = true; updateControls();
        try {
            const pause=["running","queued"].includes(status);
            const result=await request(base + "/batches/" + id + "/state", "POST", {state:pause ? "paused" : "running"});
            await refresh(false); feedback("statusMessage", pause ? "已暂停调度，实验成绩与处理进度保留。" : result.batch?.display_status==="queued" ? "已加入队列，等待运行名额。" : "已启动模拟，运行进度与结果每15秒自动更新。", false);
        } catch (error) {
            if (error.status === 401 || error.status === 403) denyAccess();
            if (error.unknown) await refresh(false);
            feedback("statusMessage", error.message + (error.unknown ? "。请核对批次当前状态。" : ""), true);
        } finally {state.mutating = false; updateControls();}
    }
    function tableRows(target, rows, fields, emptyMessage) {
        target.replaceChildren();
        if (!rows.length) {const row = element("tr"), cell = element("td", emptyMessage, "lab-muted"); cell.colSpan = fields.length; row.append(cell); target.append(row); return;}
        rows.forEach(item => {const row = element("tr"); fields.forEach(field => row.append(element("td", field(item), "lab-number"))); target.append(row);});
    }
    async function openDetail(item,button,monthlyFocus=false) {
        if(state.mutating||state.accessDenied)return;
        closeDetail(false);state.detailId=item.id||item.experiment_id;state.detailTrigger=button;
        const batchId=item.batch_id||state.selectedId;
        const known=state.batch?.id===batchId?state.batch:state.overview?.batches.find(batch=>batch.id===batchId);
        state.detailContext={batchId,batch:known||null,sourceTab:state.layout.tab};
        const revision=++state.detailRevision;
        button?.setAttribute('aria-expanded','true');openDrawer('labExperimentDetail',button);
        $("labDetailTitle").textContent=conditionName(item.condition)+' · '+settlementName(item);
        const assessment=assessObservation(item),o=item.observation;
        $("labDetailObservation").textContent=assessment.label+(assessment.reason?' · '+assessment.reason:'')+(o?' · 观察窗口 '+o.window.start_date+'～'+o.window.end_date+'；实际至 '+(o.window.actual_through_date||'尚未处理')+(o.summary.recovery_left_censored?'。窗口内回撤持续按日结计算，窗口前高点未纳入。':''):'');
        $("labDetailRuntime").replaceChildren(observationRuntime(item,known));
        $("labDetailComparison").hidden=!item.variant_key;$("labDetailComparison").replaceChildren();
        if(item.variant_key)putSummary($("labDetailComparison"),[['累计 · Δ净利',money(item.delta_net_profit,true)],['累计 · ΔROI',item.delta_roi_pp===null?'—':money(item.delta_roi_pp,true)+'pp'],['累计 · Δ回撤',money(item.delta_drawdown,true)]]);
        $("labDetailMeta").textContent='';$("labDetailStats").replaceChildren();$("labDailyBody").replaceChildren();$("labResultRetention").textContent='';
        feedback('labDetailFeedback','读取模拟结果…',false);$("labDetailTitle").focus();
        try{
            const [result,batchResponse]=await Promise.all([request(base+'/batches/'+batchId+'/experiments/'+state.detailId),known?null:request(base+'/batches/'+batchId)]);
            if(revision!==state.detailRevision||state.layout.drawer?.id!=='labExperimentDetail')return;
            if(!result.experiment||!Array.isArray(result.daily))throw responseError('结果数据不完整，请关闭后重试',0,'invalid_response');
            const batch=known||batchResponse?.batch;
            if(!batch||batch.id!==batchId)throw responseError('实验配置不完整，请关闭后重试',0,'invalid_response');
            state.detailContext.batch=batch;
            const exp=result.experiment,packName=batch.template_snapshot?.name||(state.options?.template_packs||[]).find(pack=>pack.id===batch.template_pack)?.name||'已冻结条件';
            $("labDetailMeta").textContent=packName+' · 原模拟配置 · 固定金额 · 北京时间 · '+riskSummary(batch.risk_policy)+' · 本实验冻结赔率：'+frozenOddsText(exp)+(exp.legacy_odds?'。旧赔率成绩保留，未按当前赔率重算。':'');
            state.reportExperiment=exp;state.monthly=result.monthly||null;state.detailResult=result;
            $("labDisplayStake").value=String(state.monthly?.capital.stake_amount||batch.stake_amount||1);$("labAmountProjection").hidden=false;
            renderAmountEstimate();updateControls();$("labResultRetention").textContent='长期保留累计成绩与日/月结果，当前待结算 '+number(result.pending_count,0)+' 笔。';
            feedback('labDetailFeedback','',false);if(monthlyFocus&&state.monthly)$("labMonthlyTitle").focus();
        }catch(error){if(revision===state.detailRevision){if(error.status===401||error.status===403){denyAccess();feedback('statusMessage',error.message,true);}else feedback('labDetailFeedback',error.message+'。关闭后可重新查看。',true);}}
    }
    function renderMonthly() {
        const report=state.amountView?.monthly;$("labMonthly").hidden=!report;$("labMonthlyExport").disabled=!report;
        if(!report)return;
        const summary=report.summary;
        const exp=state.amountView.experiment,original=state.reportExperiment,valid=exp.hit_count+exp.miss_count;
        $("labCapitalStake").textContent=(state.amountView.estimated?'估算每注 ':'本次回测每注 ')+money(report.capital.stake_amount)+' 元。本金与每注金额共同决定资金使用比例。';
        $("labReportScope").textContent='每注 '+money(report.capital.stake_amount)+' · '+(report.capital.batch_status==='completed'?'回测已完成':'回测尚未完成，以下为阶段结果')+' · 每个条件、赔率和风控版本独立统计。';
        putSummary($("labPerformanceSummary"),[['累计净利润',money(exp.net_profit,true)],['累计投注额',money(exp.stake_total)],['投注收益率',percent(exp.roi_percent)],['最大回撤金额（逐笔）',money(exp.max_drawdown)],['已结算笔数',exp.settled_count],['胜率（不含退款）',percent(valid?exp.hit_count/valid*100:null)],['最长连续亏损笔数',exp.max_loss_streak],['收益回撤比',original.max_drawdown>0?money(original.net_profit/original.max_drawdown):'—']]);
        const notes=['月度胜率 = 盈利月份 ÷ 完整且有交易的月份。部分月份及无交易月份不参与计算。'];
        if(summary.eligible_months<6)notes.push('目前仅 '+summary.eligible_months+' 个完整且有交易的月份，样本不足以判断长期稳定性。');
        if(report.window.truncated)notes.push('只展示最近120个自然月，月度摘要也仅计算此范围；累计逐笔回撤仍覆盖全部已处理区间。');
        if(Object.keys(report.input_warnings||{}).length)notes.push('回放存在跳过或输入告警：'+Object.entries(report.input_warnings).map(([key,value])=>(reasonNames[key]||key)+' '+value).join('；')+'。');
        $("labMonthlyNotice").textContent=notes.join(' ');
        putSummary($("labMonthlySummary"),[['盈利月份 / 有效月份',summary.profitable_months+' / '+summary.eligible_months],['月度胜率',percent(summary.profitable_month_ratio)],['亏损月份 / 持平月份',summary.losing_months+' / '+summary.flat_months],['最长连续亏损月数',summary.longest_losing_months],['最佳月份净利润',money(summary.best_month_profit,true)],['最差月份净利润',money(summary.worst_month_profit,true)]]);
        tableRows($("labMonthlyBody"),report.rows,[r=>r.month,r=>monthStatus[r.status]||r.status,r=>r.settled_count,r=>money(r.stake_total),r=>r.settled_count?money(r.net_profit,true):'—',r=>percent(r.roi_percent),r=>r.profitable_days+' / '+r.losing_days+' / '+r.betting_days,r=>money(r.worst_day_profit,true),r=>money(r.daily_close_drawdown)],'暂无月度记录。');
        if(state.detailContext?.batch?.continuation?.enabled){
            $("labReportScope").textContent='每注 '+money(report.capital.stake_amount)+' · 累计统计至 '+(report.window?.end||state.detailContext.batch.continuation.effective_end_date||state.detailContext.batch.continuation.calculated_through_date||'—')+' · 待结算 '+number(report.pending_count,state.detailContext.batch.continuation.pending_count||0)+' 笔 · '+(coverageNames[report.coverage_quality]||'输入质量待核对')+'。';
            if(report.time_complete===false)$("labReportScope").append(document.createTextNode('当前区间仍为部分时段。'));
        }
        if(state.amountView.estimated)$("labReportScope").prepend(document.createTextNode('金额估算 · '));
        renderMonthlyChart(report.rows);
        renderCapital();
    }
    function renderMonthlyChart(rows) {
        const target=$("labMonthlyChart");target.replaceChildren();
        const shown=rows.slice(-24),width=Math.min(760,Math.max(280,target.clientWidth||620)),height=shown.length*34+26;
        const center=90+(width-165)/2,half=(width-165)/2-8;
        const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
        svg.setAttribute('viewBox','0 0 '+width+' '+height);svg.setAttribute('role','img');
        svg.setAttribute('aria-label','月度净收益柱状图；正值在右、负值在左，星号表示部分月份或无结算，最近最多24个月。详细数据见下方表格。');
        function shape(tag,attrs,text){const node=document.createElementNS(svg.namespaceURI,tag);Object.entries(attrs).forEach(([k,v])=>node.setAttribute(k,v));if(text!==undefined)node.textContent=text;svg.append(node);return node;}
        shape('line',{x1:center,x2:center,y1:6,y2:height-10,stroke:'currentColor','stroke-opacity':'.25'});
        const maximum=Math.max(1,...shown.map(row=>Math.abs(row.net_profit)));
        shown.forEach((row,index)=>{
            const y=index*34+20,length=Math.abs(row.net_profit)/maximum*half;
            shape('text',{x:4,y:y+4},row.month+(row.eligible?'':' *'));
            if(row.settled_count)shape('rect',{x:row.net_profit<0?center-length:center,y:y-9,width:Math.max(length,1),height:18,rx:2,class:row.net_profit<0?'lab-chart-loss':'lab-chart-profit'});
            shape('text',{x:width-5,y:y+4,'text-anchor':'end'},row.settled_count?money(row.net_profit,true):'无结算');
        });
        target.append(svg,element('p','* 为部分月份或无结算；图中展示最近最多24个月。','lab-muted'));
    }
    function renderCapital() {
        $("labCapitalSummary").replaceChildren();$("labEquityChart").replaceChildren();feedback('labCapitalFeedback','',false);
        const basis=state.amountView?.monthly?.capital;if(!basis)return;
        const raw=$("labInitialCapital").value,initial=Number(raw),cents=Math.round(initial*100);
        if(raw===''){feedback('labCapitalFeedback','填写初始资金后显示期末资金、累计收益率和资金曲线。',false);return;}
        if(!Number.isFinite(initial)||initial<.01||initial>1e9||Math.abs(initial*100-cents)>.00001){
            feedback('labCapitalFeedback','初始资金须为0.01～10亿之间的金额，最多两位小数。',true);return;
        }
        if(!basis.settled_count){feedback('labCapitalFeedback','尚无已结算交易，暂不能展示资金表现。',false);return;}
        const curve=state.amountView.monthly.equity;
        if(!curve?.complete_history){feedback('labCapitalFeedback','历史日记录未完整覆盖整个回测区间，暂不计算完整资金表现。累计盈亏和月度记录仍可查看。',false);return;}
        const points=[{date:curve.window_start,value:cents},...curve.points.map(p=>({date:p.date,value:cents+p.cumulative_net_cents}))];
        let peak=cents,maximum=0,minimum=cents;
        points.forEach(p=>{peak=Math.max(peak,p.value);maximum=Math.max(maximum,(peak-p.value)/peak*100);minimum=Math.min(minimum,p.value);});
        const bankrupt=minimum<=0;
        const entries=[['初始资金',money(initial)],['每注金额 / 占初始资金',money(basis.stake_amount)+' / '+percent(basis.stake_amount/initial*100)],['期末资金',money((cents+Math.round(basis.net_profit*100))/100)],['累计收益率',bankrupt?'—':percent(basis.net_profit/initial*100)],['最大回撤率（日结）',bankrupt?'—':percent(maximum)],['最低日终资金',money(minimum/100)]];
        putSummary($("labCapitalSummary"),entries);
        if(bankrupt)feedback('labCapitalFeedback','按此初始资金，日终余额曾小于等于0。本次回测未模拟资金不足停投，后续盈亏不能当作可实现收益，收益率与回撤率暂停展示。',true);
        renderEquityChart(points);
    }
    // Always derive from the fetched result. Repeated edits never scale an earlier estimate.
    function amountView(amount) {
        const original=state.monthly?.capital.stake_amount||state.detailContext?.batch?.stake_amount||1;
        const numerator=BigInt(Math.round(amount*100)),denominator=BigInt(Math.round(original*100));
        const scaleCents=value=>{
            if(!Number.isSafeInteger(value))throw new Error('原始金额超出可精确换算范围。');
            const absolute=BigInt(Math.abs(value));
            const rounded=(absolute*numerator*2n+denominator)/(denominator*2n);
            if(rounded>BigInt(Number.MAX_SAFE_INTEGER))throw new Error('换算金额过大，请降低展示每注金额。');
            return Number(rounded)*(value<0?-1:1);
        };
        const scale=value=>value==null?value:scaleCents(Math.round(Math.abs(value)*100)*(value<0?-1:1))/100;
        const fields=(target,keys)=>{if(target)keys.forEach(key=>{if(target[key]!=null)target[key]=scale(target[key]);});};
        const view=structuredClone(state.detailResult);
        view.stake=amount;view.originalStake=original;view.estimated=numerator!==denominator;
        fields(view.experiment,['net_profit','stake_total','max_drawdown']);
        fields(view.experiment.risk_state,['day_net_profit','round_net_profit']);
        view.daily.forEach(row=>fields(row,['net_profit','stake_total']));
        const report=view.monthly;
        if(report){
            fields(report.summary,['best_month_profit','worst_month_profit']);
            report.rows.forEach(row=>fields(row,['stake_total','net_profit','worst_day_profit','daily_close_drawdown']));
            fields(report.capital,['net_profit','stake_total','max_drawdown']);report.capital.stake_amount=amount;
            if(report.equity)report.equity.points.forEach(point=>{point.cumulative_net_cents=scaleCents(point.cumulative_net_cents);});
        }
        // Preview text may retain sub-cent thresholds; these are never submitted as a policy.
        view.risk=structuredClone(state.detailContext?.batch?.risk_policy);
        if(view.risk?.mode==='round'){
            [view.risk.round,view.risk.day,...(view.risk.additional_day_guards||[])].forEach(control=>{
                ['profit_target','loss_limit'].forEach(key=>{const value=control[key];control[key]=value>0?(scale(value)||'<0.01'):value;});
            });
        }
        return view;
    }
    function renderAmountEstimate() {
        if(!state.detailResult)return;
        const raw=$("labDisplayStake").value,amount=Number(raw);
        state.amountView=null;
        $("labAmountEstimateResults").hidden=true;$("labMonthlyExport").disabled=true;
        $("labAmountEstimateNote").textContent='';$("labAmountRiskPreview").textContent='';
        feedback('labAmountEstimateFeedback','',false);
        try{
            if(!raw||!Number.isFinite(amount)||amount<.01||amount>10000||Math.abs(amount*100-Math.round(amount*100))>1e-7)throw new Error('请输入0.01～10000的每注金额，最多两位小数。');
            const view=amountView(amount);state.amountView=view;
            $("labDisplayStake").setAttribute('aria-invalid','false');$("labAmountEstimateResults").hidden=false;
            $("labAmountEstimateNote").textContent=view.estimated?
                '金额估算：原每注 '+money(view.originalStake)+' → 展示每注 '+money(amount)+' 元；按原成绩同比换算收益、回撤和资金曲线，止盈止损金额同步同比调整。逐笔舍入及风控临界点可能使实际重测结果不同。':
                '当前为原每注 '+money(view.originalStake)+' 元的实际模拟成绩。修改金额即可在本页同比估算，无需重新排队；修改条件或风控比例请在管理中复制新版本回测。';
            $("labAmountRiskPreview").textContent=(view.estimated?'同比估算风控 · ':'原风控 · ')+riskSummary(view.risk);
            (view.risk?.additional_day_guards||[]).forEach((guard,index)=>$("labAmountRiskPreview").append(document.createTextNode(' · 附加日保护 '+(index+1)+'：止盈 '+(guard.profit_target||'不限')+' / 止损 '+(guard.loss_limit||'不限'))));
            const exp=view.experiment,rs=exp.risk_state;
            putSummary($("labDetailStats"),[["累计投注",money(exp.stake_total)],["累计净收益",money(exp.net_profit,true)],["已结算期数",exp.settled_count],["触发 / 跳过",exp.trigger_count+' / '+exp.skipped_count]]);
            if(rs?.mode==='round')putSummary($("labDetailStats"),[["累计净收益",money(exp.net_profit,true)],["有效完成轮 / 无效轮",rs.completed_rounds+' / '+rs.invalid_rounds],['盈利 / 亏损 / 平轮',rs.profit_rounds+' / '+rs.loss_rounds+' / '+rs.flat_rounds],['统计日',rs.day_date||'—'],['该日净利 / 开轮',money(rs.day_net_profit,true)+' / '+rs.day_round_count],['当前轮',riskStates[rs.state]||rs.state]]);
            tableRows($("labDailyBody"),view.daily,[d=>d.date,d=>d.settled_count,resultCounts,d=>money(d.stake_total),d=>money(d.net_profit,true)],'暂无每日结算记录。');
            renderMonthly();
        }catch(error){
            state.amountView=null;$("labAmountEstimateResults").hidden=true;$("labMonthlyExport").disabled=true;
            $("labDisplayStake").setAttribute('aria-invalid','true');feedback('labAmountEstimateFeedback',error.message,true);
        }
    }
    function renderEquityChart(points) {
        const target=$("labEquityChart"),width=Math.min(760,Math.max(280,target.clientWidth||620)),height=210;
        const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 '+width+' '+height);svg.setAttribute('role','img');svg.setAttribute('aria-label','日结资金曲线；初始资金加每日累计净利润。');
        function shape(tag,attrs,text){const node=document.createElementNS(svg.namespaceURI,tag);Object.entries(attrs).forEach(([k,v])=>node.setAttribute(k,v));if(text!==undefined)node.textContent=text;svg.append(node);return node;}
        const low=Math.min(...points.map(p=>p.value)),high=Math.max(...points.map(p=>p.value)),range=Math.max(1,high-low);
        const first=Date.parse(points[0].date+'T00:00:00Z'),last=Date.parse(points.at(-1).date+'T00:00:00Z');
        const x=p=>12+(Date.parse(p.date+'T00:00:00Z')-first)/Math.max(86400000,last-first)*(width-24),y=value=>40+(high-value)/range*125;
        shape('line',{x1:12,x2:width-12,y1:y(points[0].value),y2:y(points[0].value),stroke:'currentColor','stroke-opacity':'.2','stroke-dasharray':'4 4'});
        shape('polyline',{points:points.map(p=>x(p)+','+y(p.value)).join(' '),fill:'none',stroke:'var(--primary)','stroke-width':2,'vector-effect':'non-scaling-stroke'});
        shape('text',{x:12,y:18},'最高 '+money(high/100));shape('text',{x:width-12,y:18,'text-anchor':'end'},'最低 '+money(low/100));
        shape('text',{x:12,y:198},points[0].date);shape('text',{x:width-12,y:198,'text-anchor':'end'},points.at(-1).date);target.append(svg);
    }
    function exportMonthly() {
        const view=state.amountView,report=view?.monthly;if(!report||state.accessDenied)return;
        const columns=['month','status','range_start','range_end','complete_month','eligible','settled_count','stake_total','net_profit','roi_percent','betting_days','profitable_days','losing_days','worst_day_profit','daily_close_drawdown','amount_basis','original_stake_amount','display_stake_amount'];
        const cell=value=>'"'+String(value??'').replaceAll('"','""')+'"';
        const text=columns.join(',')+'\r\n'+report.rows.map(row=>columns.map(key=>cell(({amount_basis:view.estimated?'estimate':'original',original_stake_amount:view.originalStake,display_stake_amount:view.stake,...row})[key])).join(',')).join('\r\n');
        const url=URL.createObjectURL(new Blob(['\uFEFF',text],{type:'text/csv;charset=utf-8'})),link=element('a');
        link.href=url;link.download='rule-lab-'+state.detailContext.batchId+'-experiment-'+state.detailId+'-monthly'+(view.estimated?'-estimate':'')+'.csv';document.body.append(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
    }
    async function exportCsv() {
        if (!state.batch || state.exporting || locked()) return;
        state.exporting = true; updateControls();
        const id = state.batch.id;
        try {
            const blob = await request(base + "/batches/" + id + "/export.csv", "GET", undefined, true);
            const url = URL.createObjectURL(blob), link = element("a"); link.href = url; link.download = "rule-lab-" + id + ".csv";
            document.body.append(link); link.click(); link.remove(); setTimeout(() => URL.revokeObjectURL(url), 1000);
        } catch (error) {if (error.status === 401 || error.status === 403) denyAccess(); feedback("statusMessage", "导出失败：" + error.message, true);}
        finally {state.exporting = false; updateControls();}
    }
    function renderStudies(){fillOwnerSelect('labStudySelect',state.studies,state.studyId,'暂无风控比较组');}
    async function openStudy(id){
        if(locked())return;if(state.continuation)closeContinuation(false);if(state.management)closeManagement(false);state.layout.tab='studies';state.studyId=id;state.loading=true;updateControls();
        const revision=++state.readRevision;
        try{const settings={...state.observation.settings},data=await request(observationUrl('study',id,settings));if(revision!==state.readRevision)return;validateObservationRows(data.rows,settings);if(state.study?.id!==data.study.id)state.studyPage=0;state.study=data.study;state.studyRows=data.rows;renderStudyResults();}
        catch(error){if(revision!==state.readRevision)return;if(error.status===401||error.status===403)denyAccess();else{state.studyId=state.study?.id??null;renderStudies();renderLayout();}feedback('statusMessage',error.message+'。已保留之前显示的比较结果。',true);}
        finally{if(revision===state.readRevision){state.loading=false;updateControls();}}
    }
    function renderStudyResults(){
        const study=state.study;$("labStudyResults").hidden=!study||state.layout.tab!=="studies";if(!study){renderLayout();return;}
        $("labStudyTitle").textContent=study.name;
        const active=study.children.find(child=>child.status==='running');
        $("labStudyMeta").textContent=(studyNames[study.status]||study.status)+' · 已完成 '+study.completed_children+'/'+study.total_children+' 方案'+(active?' · 当前：'+active.label+'，已处理 '+active.processed_issues+' 期':'')+' · 共同截止期 '+(study.cutoff_issue||'首次调度时冻结')+' · 截止时间 '+time(study.cutoff_at);
        $("labStudyNotice").textContent=study.error||study.waiting_reason||(study.comparable?'输入指纹与实际覆盖一致，可查看相对基准差；历史表现仍需新数据验证。':study.comparison_reason);
        $("labStudyStateButton").textContent=study.continuation?.active_segment_id?(['running','queued'].includes(study.status)?'暂停本段计算':'恢复本段计算'):['running','queued'].includes(study.status)?'暂停比较队列':'启动比较队列';$("labStudyStateButton").hidden=study.status==='completed';
        if(study.continuation?.enabled)$("labStudyMeta").append(document.createTextNode(' · '+continuationBrief(study.continuation)));
        $("labStudyProgress").replaceChildren();study.children.forEach(child=>{const row=element('p');const waiting=child.status==='paused'&&['running','queued'].includes(study.status);row.append(document.createTextNode(child.label+'：'+(waiting?'等待前序方案完成':statusNames[child.status]||child.status)+'，已处理 '+child.processed_issues+' 期。'+child.changes.join('；')+' '));const button=actionButton('查看子批次',()=>selectBatch(child.batch_id));button.dataset.childBatch=child.batch_id;row.append(button);$("labStudyProgress").append(row);});
        function fillSelect(id,options){const select=$(id),value=select.value;select.replaceChildren();options.forEach(([key,label])=>{const option=element('option',label);option.value=key;select.append(option);});if(options.some(([key])=>key===value))select.value=value;}
        fillSelect('labStudyVariant',[['','全部风控方案'],...study.children.map(c=>[c.variant_key,c.label])]);
        const conditionMap=new Map();state.studyRows.forEach(r=>conditionMap.set(r.condition_key,conditionName(r.condition)));
        fillSelect('labStudyCondition',[['','全部条件'],...conditionMap.entries()]);
        const variantOrder=new Map(study.children.map((c,i)=>[c.variant_key,i]));
        const candidates=state.studyRows.filter(r=>(!$("labStudyVariant").value||r.variant_key===$("labStudyVariant").value)&&(!$("labStudySettlement").value||r.settlement_rule_id===$("labStudySettlement").value)&&(!$("labStudyCondition").value||r.condition_key===$("labStudyCondition").value));
        const all=candidates.map(item=>({item,assessment:assessObservation(item)}));
        const rows=all.filter(row=>!state.observation.settings.matchesOnly||row.assessment.matches).map(row=>row.item);
        const conditionOrder=new Map([...conditionMap.keys()].map((k,i)=>[k,i]));
        rows.sort((a,b)=>conditionOrder.get(a.condition_key)-conditionOrder.get(b.condition_key)||a.settlement_rule_id.localeCompare(b.settlement_rule_id)||variantOrder.get(a.variant_key)-variantOrder.get(b.variant_key));
        sortObservationRows(rows,$("labStudySort").value);renderObservationCount('labObservationStudySummary',all);
        state.studyFilteredCount=rows.length;state.studyPage=Math.min(state.studyPage,Math.max(0,Math.ceil(rows.length/pageSize)-1));const start=state.studyPage*pageSize;
        $("labStudyRows").replaceChildren();renderCompactTable('labStudyTable',$("labStudyRows"),rows.slice(start,start+pageSize),true);
        if(!rows.length)observationEmptyRow($("labStudyRows"),compactColumnCount(rows),'没有符合当前条件与观察规则的比较行。');
        $("labStudyPageInfo").textContent=rows.length?'显示 '+(start+1)+'–'+Math.min(start+pageSize,rows.length)+'，共 '+rows.length+' 行':'当前筛选无结果';
        renderLayout();
    }
    async function comparisonPreview(){
        if(locked()||!state.batch)return;state.loading=true;updateControls();
        try{state.preview=await request(base+'/batches/'+state.batch.id+'/comparison-preview');state.studyRequest=null;
            $("labStudyName").value=(state.batch.name+' · 风控比较').slice(0,80);$("labComparisonPreview").hidden=false;
            const p=state.preview,c=p.capacity;$("labComparisonCapacity").textContent='将新建 '+p.new_experiments+' 个版本；当前已保存 '+c.saved+'，创建后 '+(c.saved+p.new_experiments)+' / '+c.max_saved+'；剩余容量 '+c.remaining_saved+'。最多同时运行 '+c.max_running+' 个，其他方案排队。';
            $("labComparisonVariants").replaceChildren();p.variants.forEach(v=>$("labComparisonVariants").append(element('p',v.label+'：'+(v.available?v.changes.join('；'):'不可用 — '+v.reason))));feedback('labComparisonFeedback','',false);
        }catch(error){feedback('statusMessage',error.message,true);}finally{state.loading=false;updateControls();}
    }
    async function createStudy(){
        if(locked()||!state.preview)return;const name=$("labStudyName").value.trim();if(!name){feedback('labComparisonFeedback','请填写比较组名称',true);return;}
        const signature=JSON.stringify([state.preview.source_batch_id,name]);
        if(!state.studyRequest||state.studyRequest.signature!==signature)state.studyRequest={signature,id:crypto.randomUUID()};
        state.mutating=true;updateControls();
        try{const result=await request(base+'/studies','POST',{source_batch_id:state.preview.source_batch_id,name,request_id:state.studyRequest.id});state.layout.tab='studies';state.studyId=result.study.id;state.studyPage=0;await refresh(false);$("labComparisonPreview").hidden=true;state.mutating=false;updateControls();$('labStudySelect').focus({preventScroll:true});feedback('statusMessage','比较组已保存，默认暂停。点击“启动比较队列”才会开始分批计算。',false);}
        catch(error){feedback('labComparisonFeedback',error.message+(error.unknown?'。保留本次请求标识，可点击同名提交安全重试。':''),true);}
        finally{state.mutating=false;updateControls();}
    }
    async function changeStudyState(){
        if(locked()||!state.study)return;state.mutating=true;updateControls();
        try{await request(base+'/studies/'+state.study.id+'/state','POST',{state:['queued','running'].includes(state.study.status)?'paused':'running'});await refresh(false);}
        catch(error){feedback('statusMessage',error.message,true);}finally{state.mutating=false;updateControls();}
    }
    async function exportStudy(){
        if(locked()||!state.study)return;state.mutating=true;updateControls();
        try{const blob=await request(base+'/studies/'+state.study.id+'/export.csv','GET',undefined,true);const url=URL.createObjectURL(blob),a=element('a');a.href=url;a.download='rule-study-'+state.study.id+'.csv';document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);}
        catch(error){feedback('statusMessage','比较导出失败：'+error.message,true);}finally{state.mutating=false;updateControls();}
    }
    function continuationBrief(summary) {
        return (summary.compute_enabled?'自动续算已开启':'自动续算未开启')+' · 已计算至 '+(summary.calculated_through_date||'—')+
            (summary.calculated_through_issue?'（'+summary.calculated_through_issue+'期）':'')+
            (summary.capture_enabled?' · 持续保留后续数据':' · 后续数据保留已关闭');
    }
    function continuationPath(view) {return base+'/'+(view.kind==='study'?'studies':'batches')+'/'+view.id;}
    function continuationStorageKey(view) {return 'ruleLabContinuationPending:'+view.kind+':'+view.id;}
    function rememberContinuationRequest(view, value) {
        view.unconfirmed=value;
        try {if(value)sessionStorage.setItem(continuationStorageKey(view),JSON.stringify(value));else sessionStorage.removeItem(continuationStorageKey(view));} catch (_) {}
    }
    function closeContinuation(restoreFocus) {
        const trigger=state.continuation?.trigger;dismissDrawer("labContinuationPanel",restoreFocus);
        state.continuationRevision++;state.continuation=null;$("labContinuationPanel").hidden=true;
        ['labContinuationButton','labStudyContinuationButton'].forEach(id=>$(id).setAttribute('aria-expanded','false'));
    }
    async function openContinuation(kind,id,trigger) {
        if(state.management)closeManagement(false);
        if(locked())return;
        closeContinuation(false);
        const view={kind,id,trigger,preview:null,coverage:null,segments:[],nextCursor:null,dirty:false,unconfirmed:null};
        try {const saved=JSON.parse(sessionStorage.getItem(continuationStorageKey(view))||'null');if(saved&&typeof saved.request_id==='string')view.unconfirmed=saved;} catch (_) {}
        state.continuation=view;state.loading=true;
        openDrawer("labContinuationPanel",trigger);$("labContinuationPanel").hidden=false;trigger?.setAttribute('aria-expanded','true');
        $("labContinuationTitle").textContent=kind==='study'?'比较组增量续算':'增量续算';
        $("labContinuationSummary").replaceChildren();$("labContinuationSegments").replaceChildren();
        $("labContinuationBasis").textContent='正在核对实际计算边界与原实验版本…';
        $("labContinuationBlockers").hidden=true;feedback('labContinuationFeedback','读取续算状态…',false);updateControls();
        try {await readContinuation();feedback('labContinuationFeedback',view.unconfirmed?'上次提交结果尚未确认，请刷新续算状态核对后再操作。':'',Boolean(view.unconfirmed));}
        catch(error){feedback('labContinuationFeedback',error.message,true);if(error.status===401)denyAccess();}
        finally {state.loading=false;updateControls();if(state.continuation===view){$("labContinuationTitle").focus();}}
    }
    async function readContinuation({preserveDraft=false}={}) {
        const view=state.continuation;if(!view)return;
        const revision=++state.continuationRevision,path=continuationPath(view);
        const [preview,coverage,segments]=await Promise.all([request(path+'/continuation-preview'),request(path+'/input-coverage'),request(path+'/segments?limit=20')]);
        if(state.continuation!==view||revision!==state.continuationRevision)return;
        if(!preview.continuation||typeof preview.expected_revision!=='string'||typeof preview.eligible!=='boolean'||!Array.isArray(preview.blockers)||!Array.isArray(segments.segments))throw responseError('续算状态不完整，请刷新后重试',0,'invalid_response');
        view.preview=preview;view.coverage=coverage;view.segments=segments.segments;view.nextCursor=segments.next_cursor;
        if(view.unconfirmed&&preview.latest_request?.request_id===view.unconfirmed.request_id)rememberContinuationRequest(view,null);
        renderContinuation(!preserveDraft||!view.dirty);
    }
    function renderContinuation(fillFields) {
        const view=state.continuation,preview=view?.preview;if(!preview)return;
        const summary=preview.continuation,owner=view.kind==='study'?state.studies.find(s=>s.id===view.id):state.overview?.batches.find(b=>b.id===view.id);
        $("labContinuationOwner").textContent=(owner?.name||'所选实验')+(view.kind==='study'?' · 全部风控方案统一追加':'');
        $("labContinuationBasis").textContent='保留原 '+preview.saved_experiments+' 个实验版本、每注金额、条件、赔率和风控。'+(preview.next_issue?'下一期待计算：'+preview.next_issue+'。':'')+'累计收益与未结束轮次接着计算。';
        const watermark=(day,issue)=>day?(day+(issue?' · '+issue+'期':'')):(issue?issue+'期':'尚无');
        putSummary($("labContinuationSummary"),[
            ['续算状态',continuationNames[summary.state]||'等待核对'],
            ['已计算至',watermark(summary.calculated_through_date,summary.calculated_through_issue)],
            ['已保存数据至',watermark(summary.captured_through_date,summary.captured_through_issue)],
            ['结算进度',(summary.settled_through_issue?summary.settled_through_issue+'期':'各实验独立结算')+' · 待结算 '+number(summary.pending_count,0)+' 笔'],
            ['后续数据保留',summary.capture_enabled?'已开启':'未开启'],['每日自动续算',summary.compute_enabled?'已开启':'未开启'],
            ['下次自动追加',summary.compute_enabled?time(summary.next_run_at):'未安排'],
            ['输入质量',coverageNames[summary.coverage_quality]||'等待核对'],
        ]);
        const problems=preview.blockers.map(item=>item.message).filter(Boolean);if(summary.error)problems.push(summary.error);
        $("labContinuationBlockers").textContent=problems.join('；');$("labContinuationBlockers").hidden=!problems.length;
        const receipt=preview.latest_request;
        $("labContinuationRequest").textContent=receipt?.kind==='append'?'最近追加'+(receipt.end_date?'至 '+receipt.end_date:'')+'：'+({waiting_inputs:'请求已保存，等待输入齐备后计算',up_to_date:'已经追平，没有生成空段',completed:'已完成',running:'正在计算',queued:'已进入计算队列',error:'等待处理异常'}[receipt.status]||'请求已保存')+'。':'';
        if(fillFields){
            $("labContinuationCapture").checked=summary.capture_enabled===true;$("labContinuationCompute").checked=summary.compute_enabled===true;
            $("labContinuationDaily").value=summary.daily_at||'00:10';view.dirty=false;
        }
        const end=$("labContinuationEndDate");end.max=today();end.min=summary.calculated_through_date||'';
        if(!end.value||end.value<end.min||end.value>end.max)end.value=today();
        $("labContinuationAppendHelp").textContent=summary.capture_enabled?'每次最多追加90天；选择今天时，只计算已经保存的期数，不代表今天已完整结束。':'先开启“保留后续数据”并保存设置，再选择日期手动追加；每日自动续算可以保持关闭。';
        const active=Boolean(summary.active_segment_id),paused=summary.state==='paused';
        $("labContinuationPause").hidden=!active;$("labContinuationPause").textContent=paused?'恢复本段计算':'暂停本段计算';
        $("labContinuationPauseHelp").textContent=active?(summary.capture_enabled?'暂停本段计算后，仍保留后续数据。':'当前未开启后续数据保留。'):'';
        const coverage=view.coverage?.coverage||{},storage=view.coverage?.storage||{};
        const counts=Object.entries(coverage.counts||{}).map(([key,value])=>(coverageNames[key]||key)+' '+value+' 期');
        $("labContinuationCoverage").textContent=(coverage.history_notice?coverage.history_notice+'。':'')+(counts.length?counts.join('；'):'当前没有需要展示的输入覆盖记录。')+' 最近成功保存：'+time(coverage.last_success_at)+'。模拟存储 '+bytes(storage.used_bytes)+' / '+bytes(storage.budget_bytes)+'。';
        const gaps=(coverage.gaps||[]).map(item=>(item.issue_no?item.issue_no+'期：':'')+(coverageNames[item.reason]||item.reason));
        if(coverage.error)gaps.unshift(coverage.error);
        $("labContinuationGaps").textContent=gaps.join('；');$("labContinuationGaps").hidden=!gaps.length;
        renderSegments();updateControls();
    }
    function renderSegments() {
        const view=state.continuation,target=$("labContinuationSegments");if(!view)return;
        const openIds=new Set([...target.querySelectorAll('details[open]')].map(item=>item.dataset.segmentId));
        target.replaceChildren();$("labContinuationMore").hidden=!view.nextCursor;
        if(!view.segments.length){target.append(element('p','尚无计算段。保存续算设置后会保留原成绩作为基线。','lab-empty'));return;}
        view.segments.forEach(segment=>{
            const detail=element('details',null,'lab-segment');detail.dataset.segmentId=String(segment.id);detail.open=openIds.has(String(segment.id));
            detail.append(element('summary',(segment.kind==='baseline'?'原回测基线':'第 '+segment.sequence+' 段追加')+' · 截至 '+segment.requested_end_date+' · '+(segmentNames[segment.status]||'等待核对')));
            const batches=Array.isArray(segment.summary?.batches)?segment.summary.batches:[],pending=batches.reduce((sum,b)=>sum+number(b.pending_count,0),0);
            const notes=['截止期 '+(segment.cutoff_issue||'—'),'保存于 '+time(segment.completed_at||segment.created_at)];
            if(pending)notes.push('待结算 '+pending+' 笔，下一段按可见时间继续结算');
            if(segment.summary?.baseline_audit)notes.push('迁移时保存的原回测结果');
            if(segment.time_complete===false||segment.summary?.time_complete===false)notes.push('此日期仍为部分时段');
            if(segment.summary?.coverage_quality)notes.push(coverageNames[segment.summary.coverage_quality]||'输入质量待核对');
            detail.append(element('p',notes.join(' · '),'lab-muted lab-segment-meta'));
            if(!batches.length){detail.append(element('p','本段尚在处理中，完成后显示保存的结果。','lab-empty'));target.append(detail);return;}
            const scroll=element('div',null,'lab-table-scroll');scroll.setAttribute('role','region');scroll.setAttribute('aria-label','本段批次计算汇总，可横向滚动');scroll.tabIndex=0;
            const table=element('table',null,'lab-table');table.append(element('caption','金额单位：元。本表为各版本的计算量汇总，方案表现请查看各版本结果，合计净额不代表组合投资收益。'));
            const head=element('thead'),headRow=element('tr');['批次','本段处理期数','本段结算笔数合计','本段各版本净额合计','待结算笔数'].forEach(label=>{const th=element('th',label);th.scope='col';headRow.append(th);});head.append(headRow);table.append(head);
            const body=element('tbody');
            batches.forEach(batch=>{
                const add=batch.incremental||{},row=element('tr'),name=state.overview?.batches.find(item=>item.id===batch.batch_id)?.name||'批次 '+batch.batch_id;
                [name,number(batch.processed_issues,0),segment.kind==='baseline'?'—':number(add.settled_count,0),segment.kind==='baseline'?'—':money(number(add.net_cents,0)/100,true),number(batch.pending_count,0)].forEach(value=>row.append(element('td',value)));body.append(row);
            });table.append(body);scroll.append(table);detail.append(scroll);target.append(detail);
        });
    }
    async function refreshContinuation() {
        if(locked()||!state.continuation)return;state.loading=true;updateControls();
        try{await readContinuation({preserveDraft:true});feedback('labContinuationFeedback',state.continuation?.unconfirmed?'提交结果尚未确认，已保留请求标识。请稍后刷新核对，暂不重复提交。':'续算状态已更新。',Boolean(state.continuation?.unconfirmed));}
        catch(error){feedback('labContinuationFeedback',error.message,true);}finally{state.loading=false;updateControls();}
    }
    async function moreSegments() {
        const view=state.continuation;if(locked()||!view?.nextCursor)return;state.loading=true;updateControls();
        try{const result=await request(continuationPath(view)+'/segments?limit=20&before_id='+encodeURIComponent(view.nextCursor));if(state.continuation!==view)return;
            if(!Array.isArray(result.segments))throw responseError('计算段列表不完整',0,'invalid_response');
            const ids=new Set(view.segments.map(s=>s.id));view.segments.push(...result.segments.filter(s=>!ids.has(s.id)));view.nextCursor=result.next_cursor;renderSegments();
        }catch(error){feedback('labContinuationFeedback',error.message,true);}finally{state.loading=false;updateControls();}
    }
    async function mutateContinuation(kind, payload, retry=null) {
        const view=state.continuation;if(locked()||!view?.preview||(view.unconfirmed&&!retry))return;
        const requestId=retry?.request_id||crypto.randomUUID(),body=retry?.body||{...payload,request_id:requestId,expected_revision:view.preview.expected_revision};
        rememberContinuationRequest(view,{request_id:requestId,kind,body});state.mutating=true;updateControls();
        try {
            const result=await request(continuationPath(view)+(kind==='append'?'/append':'/continuation'),'POST',body);
            rememberContinuationRequest(view,null);view.dirty=false;await refresh(false);
            const message=kind==='append'?(result.up_to_date?'已追平，没有生成空计算段。':result.queued?'追加请求已保存，输入齐备后将接着计算；每日自动续算设置不变。':'已保存本次追加任务。'):'续算设置已保存。'+(body.compute_enabled?'将按北京时间每天追加。':'每日自动续算未开启，已有追加任务继续按原状态运行。');
            feedback('labContinuationFeedback',message,false);
        } catch(error) {
            if(!error.unknown)rememberContinuationRequest(view,null);
            if(error.unknown||error.code==='stale_revision'){
                try {await readContinuation({preserveDraft:!error.unknown});} catch (_) {}
                const confirmed=error.unknown&&!view.unconfirmed;
                if(confirmed)view.dirty=false;
                feedback('labContinuationFeedback',confirmed?'连接中断后已重新核对，提交已保存；没有重复发送请求。':error.unknown?'连接中断，提交结果尚未确认。请刷新续算状态核对，暂不重复提交。':error.message+'。已尝试更新状态，请核对后再次操作。',!confirmed);
            } else feedback('labContinuationFeedback',error.message,true);
        } finally {state.mutating=false;updateControls();}
    }
    async function retryContinuation() {
        const view=state.continuation,pending=view?.unconfirmed;if(locked()||!pending?.body)return;
        state.loading=true;updateControls();
        try{await readContinuation();}
        catch(error){feedback('labContinuationFeedback','暂时无法核对原提交，稍后刷新再试。',true);return;}
        finally{state.loading=false;updateControls();}
        if(state.continuation!==view)return;
        if(!view.unconfirmed){feedback('labContinuationFeedback','已核对到原提交，无需再次发送。',false);return;}
        await mutateContinuation(pending.kind,pending.body,pending);
    }
    function saveContinuation(event) {
        event.preventDefault();if(locked())return;
        const capture=$("labContinuationCapture").checked,compute=$("labContinuationCompute").checked,daily=$("labContinuationDaily").value;
        if(!/^(?:[01][0-9]|2[0-3]):[0-5][0-9]$/.test(daily)){feedback('labContinuationFeedback','请选择有效的每日续算时间。',true);return;}
        if(compute&&!capture){feedback('labContinuationFeedback','自动续算需要同时保留后续数据。',true);return;}
        mutateContinuation('configure',{capture_enabled:capture,compute_enabled:compute,daily_at:daily,timezone:'Asia/Shanghai'});
    }
    function appendContinuation(event) {
        event.preventDefault();const view=state.continuation;if(locked()||!view?.preview)return;
        const end=$("labContinuationEndDate").value,start=view.preview.continuation.calculated_through_date;
        const difference=(Date.parse(end)-Date.parse(start))/86400000;
        if(!/^\d{4}-\d{2}-\d{2}$/.test(end)||!Number.isFinite(Date.parse(end))||end>today()||difference<0||difference>90){feedback('labContinuationFeedback','追加日期不能早于已计算日期、晚于今天，或一次超过90天。',true);return;}
        mutateContinuation('append',{end_date:end});
    }
    async function pauseContinuation() {
        const view=state.continuation;if(locked()||!view?.preview?.continuation.active_segment_id)return;
        const paused=view.preview.continuation.state==='paused';state.mutating=true;updateControls();
        try{await request(continuationPath(view)+'/state','POST',{state:paused?'running':'paused'});await refresh(false);feedback('labContinuationFeedback',paused?'已恢复本段计算。':'本段计算已暂停；已开启的后续数据保留继续运行。',false);}
        catch(error){if(error.unknown){try{await refresh(false);}catch(_){}}feedback('labContinuationFeedback',error.message+(error.unknown?'。已尝试重新核对当前状态。':''),true);}
        finally{state.mutating=false;updateControls();}
    }
    function managementPath(view) {return base+'/'+(view.kind==='study'?'studies':'batches')+'/'+view.id;}
    function closeManagement(restoreFocus) {
        const trigger=state.management?.trigger;dismissDrawer("labManagementPanel",restoreFocus);state.management=null;$("labManagementPanel").hidden=true;
        $("labDefinitionForm").hidden=true;$("labDeleteForm").hidden=true;
        updateControls();
    }
    async function openManagement(kind,id,trigger) {
        if(locked())return;if(state.continuation)closeContinuation(false);
        state.management={kind,id,trigger,preview:null,mode:null,dirty:false,pending:null,deletePreview:null};
        openDrawer("labManagementPanel",trigger);$("labManagementPanel").hidden=false;$("labDefinitionForm").hidden=true;$("labDeleteForm").hidden=true;
        feedback('labManagementFeedback','读取管理选项…',false);state.loading=true;updateControls();
        try{await readManagement();feedback('labManagementFeedback','',false);$("labManagementTitle").focus();}
        catch(error){feedback('labManagementFeedback',error.message,true);if(error.status===401||error.status===403)denyAccess();}
        finally{state.loading=false;updateControls();}
    }
    async function readManagement() {
        const view=state.management;if(!view)return;
        const preview=await request(managementPath(view)+'/management');if(state.management!==view)return;
        view.preview=preview;$("labManagementTitle").textContent=(view.kind==='study'?'管理比较组 · ':'管理实验 · ')+preview.name;
        $("labManagementName").value=preview.name;
        $("labManagementNotice").textContent=view.kind==='study'?'改名保留所有子方案成绩；删除比较组将删除组内全部子方案，原始参考批次保留。':preview.editable?'尚未开始计算，可直接编辑配置。保存修改后退出自动排队并保持暂停，需要手动启动。':'已有运行记录，策略参数保持冻结；修改参数请新建独立版本，原成绩与续算保持。';
        $("labManagementEdit").hidden=view.kind!=='batch'||!preview.editable;
        $("labManagementFork").hidden=view.kind!=='batch'||!preview.can_fork;
        $("labManagementDelete").textContent=view.kind==='study'?'删除整个比较组…':'删除这个批次…';
        updateControls();
    }
    function definitionOptions() {
        const metric=$("labDefinitionMetric").value,select=$("labDefinitionTemplate"),selected=select.value;
        select.replaceChildren();
        if(metric===state.management?.preview?.config?.metric){const original=element('option','保留原实验条件与赔率（冻结快照）');original.value='__snapshot__';select.append(original);}
        const candidates=templateCatalog.filter(t=>t.kind==='condition'&&t.metrics.includes(metric));
        if(candidates.length)candidates.forEach(t=>{const option=element('option',t.name+' · v'+t.revision);option.value=t.builtin?t.ref.key:'custom:'+t.id;select.append(option);});
        else (state.options?.template_packs||[]).filter(pack=>pack.selectable!==false&&pack.metrics.includes(metric)).forEach(pack=>{const option=element('option',pack.name);option.value=pack.id;select.append(option);});
        if([...select.options].some(option=>option.value===selected))select.value=selected;
        const current=candidates.find(t=>(t.builtin?t.ref.key:'custom:'+t.id)===select.value),pack=(state.options?.template_packs||[]).find(pack=>pack.id===select.value),count=current?.condition_count||pack?.condition_counts?.[metric];
        $("labDefinitionCount").textContent=select.value==='__snapshot__'?'保留原实验实际选择的条件及冻结赔率；源模板改名、删除不影响复制。':count?count+' 个条件 × 2 套当前OK赔率 = '+(count*2)+' 个版本。':'';
        const riskSelect=$("labDefinitionRiskTemplate"),riskSelected=riskSelect.value;
        riskSelect.replaceChildren();const manual=element('option','原实验风控 / 手动配置');manual.value='manual';riskSelect.append(manual);
        templateCatalog.filter(t=>t.kind==='risk'&&t.metrics.includes(metric)).forEach(t=>{const option=element('option',t.name+' · v'+t.revision);option.value='custom:'+t.id;riskSelect.append(option);});
        if([...riskSelect.options].some(option=>option.value===riskSelected))riskSelect.value=riskSelected;
        const history=$("labDefinitionMode").value==='backtest';$("labDefinitionStartField").hidden=!history;$("labDefinitionEndField").hidden=!history;
        renderRiskPresets('labDefinitionRisk',metric);updateControls();
    }
    function editDefinition(mode) {
        const view=state.management;if(locked()||!view?.preview?.config)return;
        view.mode=mode;view.dirty=false;const config=view.preview.config;
        $("labDeleteForm").hidden=true;$("labDefinitionForm").hidden=false;
        $("labDefinitionTitle").textContent=mode==='fork'?'修改并新建版本':'编辑未启动实验';
        $("labDefinitionNote").textContent=mode==='fork'?'新版本默认保留原实验冻结条件及赔率；显式选择新模板时才重新生成。保存后为草稿，成绩从零开始。原实验及历史成绩保持。仅按比例调整金额时，可直接在详情中修改“展示每注金额”查看估算。':'保存后按新配置重新生成实验版本，仍保持暂停，并退出当前自动排队；请检查后手动启动。';
        $("labDefinitionSave").textContent=mode==='fork'?'保存为新版本（暂停）':'保存修改（保持暂停）';
        $("labDefinitionName").value=(mode==='fork'?config.name+' · 修改版':config.name).slice(0,80);
        const select=$("labDefinitionPredictor");select.replaceChildren();(state.options?.predictors||[]).forEach(p=>{const option=element('option',p.name);option.value=p.id;select.append(option);});
        if(![...select.options].some(option=>option.value===String(config.predictor_id))){const option=element('option','原来源目前不可用');option.value=config.predictor_id;option.disabled=true;select.append(option);}
        select.value=String(config.predictor_id);$("labDefinitionMetric").value=config.metric;$("labDefinitionStake").value=config.stake_amount;
        $("labDefinitionMode").value=config.mode;$("labDefinitionStart").value=config.start_date||'';$("labDefinitionEnd").value=config.end_date||'';
        $("labDefinitionRiskTemplate").value='manual';definitionOptions();$("labDefinitionTemplate").value="__snapshot__";
        request(base+"/templates").then(data=>{if(state.management!==view)return;templateCatalog=data.templates;definitionOptions();}).catch(error=>feedback("labManagementFeedback",error.message,true));
        fillRisk('labDefinitionRisk',config.risk_policy);definitionOptions();$("labDefinitionName").focus();
    }
    function definitionValue() {
        const mode=$("labDefinitionMode").value,value=$("labDefinitionTemplate").value,metric=$("labDefinitionMetric").value;
        const config={name:$("labDefinitionName").value.trim(),mode,predictor_id:Number($("labDefinitionPredictor").value),metric,
            stake_amount:Number($("labDefinitionStake").value),start_date:mode==='backtest'?$("labDefinitionStart").value:null,
            end_date:mode==='backtest'?$("labDefinitionEnd").value:null,risk_policy:riskValue('labDefinitionRisk')};
        if(value!=='__snapshot__'){
            const template=templateCatalog.find(t=>t.kind==='condition'&&t.metrics.includes(metric)&&(t.builtin?t.ref.key:'custom:'+t.id)===value);
            config.condition_selection={template_ref:template?.ref||{kind:'builtin',key:value,metric},expected_revision:template?.revision||(value==='basic_v2'?2:1)};
        }
        const riskTemplate=templateCatalog.find(t=>t.kind==='risk'&&t.metrics.includes(metric)&&'custom:'+t.id===$("labDefinitionRiskTemplate").value);
        if(riskTemplate){delete config.risk_policy;config.risk_selection={template_ref:riskTemplate.ref,expected_revision:riskTemplate.revision};}
        return config;
    }
    async function showDeletePreview() {
        const view=state.management;if(locked()||!view?.preview)return;state.loading=true;updateControls();
        try{
            const value=await request(managementPath(view)+'/delete-preview');if(state.management!==view)return;
            view.deletePreview=value;$("labDefinitionForm").hidden=true;$("labDeleteForm").hidden=false;$("labDeleteConfirmName").value='';
            $("labDeleteExplanation").textContent='将永久删除“'+value.name+'”及所属成绩、计算段和快照，同时停止该对象的计算、采集和自动续算。删除不能撤销。';
            const labels={batches:'批次',experiments:'实验版本',decisions:'逐笔记录',pending:'待结算模拟记录',daily_rows:'日账记录',rounds:'轮次记录',segments:'计算段',checkpoints:'检查点'};
            $("labDeleteCounts").replaceChildren();Object.entries(labels).forEach(([key,label])=>{if(value.counts?.[key]!==undefined){const cell=element('div');cell.append(element('dt',label),element('dd',value.counts[key]));$("labDeleteCounts").append(cell);}});
            $("labDeleteShared").textContent=value.shared_inputs_retained?'其他实验仍需要共享预测与开奖，这些共享数据会保留。删除完成会释放版本额度；磁盘实际回收量以后台任务显示为准。':'不再被其他实验使用的输入会随后台清理；磁盘实际回收量以后台任务显示为准。';
            $("labDeleteBlockers").replaceChildren();(value.blockers||[]).forEach(block=>{
                $("labDeleteBlockers").append(element('p',block.message));
                (block.studies||[]).forEach(study=>{const button=actionButton('管理比较组：'+study.name,()=>openManagement('study',study.id,button));$("labDeleteBlockers").append(button);});
            });
            $("labDeleteConfirmName").disabled=!value.eligible;$("labDeleteConfirmName").focus();
        }catch(error){feedback('labManagementFeedback',error.message,true);}
        finally{state.loading=false;updateControls();}
    }
    function trackManagementJob(job) {
        if(!job||!Number.isInteger(job.id))return;
        state.managementJobs.set(job.id,job);renderManagementJobs();scheduleManagementPoll();
    }
    function renderManagementJobs() {
        const target=$("labManagementJobList");target.replaceChildren();$("labManagementJobs").hidden=!state.managementJobs.size;
        const names={queued:'等待处理',running:'正在处理',waiting:'等待安全回收条件',completed:'已完成',error:'处理异常'};
        [...state.managementJobs.values()].sort((a,b)=>b.id-a.id).slice(0,20).forEach(job=>{
            const row=element('div',null,'lab-management-job');row.dataset.jobId=job.id;
            row.append(element('strong',(job.kind==='delete'?'删除实验':'回收空间')+' · '+(names[job.status]||job.status)));
            row.append(element('p','已处理 '+number(job.processed_rows,0)+' 条'+(typeof job.total_rows==='number'?' / 约 '+job.total_rows+' 条':'')+' · 实际释放 '+bytes(job.freed_bytes||0),'lab-muted'));
            if(job.released_versions)row.append(element('p','已释放 '+job.released_versions+' 个实验版本额度。','lab-muted'));
            if(job.error)row.append(element('p',job.error,'lab-feedback is-error'));target.append(row);
        });
    }
    function scheduleManagementPoll() {
        if(state.managementPoll||state.accessDenied||![...state.managementJobs.values()].some(job=>!['completed','error'].includes(job.status)))return;
        state.managementPoll=setTimeout(pollManagementJobs,2000);
    }
    async function pollManagementJobs() {
        state.managementPoll=null;if(state.accessDenied)return;
        let completed=false;
        for(const job of [...state.managementJobs.values()].filter(job=>!['completed','error'].includes(job.status)).slice(0,20)){
            try{const value=await request(base+'/management-jobs/'+job.id);state.managementJobs.set(job.id,value.job);completed|=value.job.status==='completed';}
            catch(error){if(error.status===401||error.status===403){denyAccess();return;}if(error.status===404)state.managementJobs.delete(job.id);}
        }
        renderManagementJobs();if(completed&&!locked())await refresh(false);
        if(completed&&$("labStoragePanel").open)await readStorage();scheduleManagementPoll();
    }
    async function managementSucceeded(view,action,result) {
        view.pending=null;view.dirty=false;
        if(result.job){trackManagementJob(result.job);$("labDeleteForm").hidden=true;}
        if(action==='fork'){state.selectedId=result.batch.id;closeManagement(false);await refresh(false);feedback('statusMessage','已创建新的暂停版本，原实验成绩保持。请检查后启动。',false);return;}
        if(action==='delete'){
            closeManagement(false);if(view.kind==='batch'&&state.selectedId===view.id){state.selectedId=null;state.batch=null;state.experiments=[];}
            if(view.kind==='study'&&state.studyId===view.id){state.studyId=null;state.study=null;state.studyRows=[];}
            await refresh(false);feedback('statusMessage','已停止该对象的后续计算，后台正在删除。可在“系统”中查看处理与空间回收进度。',false);return;
        }
        $("labDefinitionForm").hidden=true;await refresh(false);
        if(state.management===view)await readManagement();feedback('labManagementFeedback',action==='edit'?'配置已保存，批次保持暂停并退出自动排队；请检查后手动启动。':'名称已保存，策略和成绩保持。',false);
    }
    async function mutateManagement(action,payload,retry) {
        const view=state.management;if(locked()||!view?.preview)return;
        const token=action==='delete'?view.deletePreview?.expected_revision:view.preview.expected_revision;
        const body=retry?.body||{...payload,request_id:crypto.randomUUID(),expected_revision:token};
        const pending={action,body,path:managementPath(view)+'/'+action};view.pending=pending;state.mutating=true;updateControls();
        try{const result=await request(pending.path,'POST',body);await managementSucceeded(view,action,result);}
        catch(error){
            if(error.unknown){
                try{await readManagement();const receipt=view.preview?.latest_request;if(receipt?.request_id===body.request_id&&receipt.result){await managementSucceeded(view,action,receipt.result);return;}}catch(_){}
                feedback('labManagementFeedback','提交结果暂未确认。已尝试只读核对；请使用“核对后重试原请求”，不会创建重复任务。',true);
            }else{view.pending=null;feedback('labManagementFeedback',error.message,true);if(error.status===409){try{await readManagement();}catch(_){}}}
        }finally{state.mutating=false;updateControls();}
    }
    async function retryManagement() {
        const view=state.management,pending=view?.pending;if(locked()||!pending)return;
        // The original idempotency key and payload are retained; retry is an explicit user action.
        try{await readManagement();const receipt=view.preview?.latest_request;if(receipt?.request_id===pending.body.request_id&&receipt.result){state.mutating=true;try{await managementSucceeded(view,pending.action,receipt.result);}finally{state.mutating=false;updateControls();}return;}}catch(_){}
        await mutateManagement(pending.action,null,pending);
    }
    async function readStorage() {
        if(state.accessDenied)return;
        try{state.storage=await request(base+'/storage');renderStorage();if(state.storage.reclaim_job)trackManagementJob(state.storage.reclaim_job);}
        catch(error){if(error.status===401||error.status===403)denyAccess();feedback('labStorageFeedback',error.message,true);}finally{updateControls();}
    }
    function renderStorage() {
        const storage=state.storage;if(!storage)return;const target=$("labStorageSummary");target.replaceChildren();
        [['目录实际占用','used_bytes'],['数据库文件','database_bytes'],['WAL 日志','wal_bytes'],['内部可复用空间','reusable_bytes'],['预计可回收整页','reclaimable_bytes']].forEach(([label,key])=>{const cell=element('div');cell.append(element('dt',label),element('dd',bytes(storage[key])));target.append(cell);});
        const retention=storage.retention;
        if(retention){
            const states={idle:'当前无需整理',completed:'历史冗余整理完成',complete:'历史冗余整理完成',waiting:'等待空闲后继续',error:'整理暂时遇到问题'};
            const progress=element('div');progress.append(element('dt','历史冗余整理'),element('dd',states[retention.phase]||'后台分批整理中'));target.append(progress);
            if(Number.isFinite(retention.removed_rows)){const removed=element('div');removed.append(element('dt','已移除冗余记录'),element('dd',String(retention.removed_rows)+' 条'));target.append(removed);}
            if(retention.error){const issue=element('div');issue.append(element('dt','整理说明'),element('dd',retention.error));target.append(issue);}
        }
        $("labStorageExplanation").textContent='长期保存模拟结果，只保留运行必需数据；已结算逐笔及已结束轮次不保留。'+(storage.incremental_ready?'已启用分批空间回收。':'当前库尚未完成空间回收模式转换，需要维护操作后启用。')+' 内部可复用空间和已移除记录数不等于已释放磁盘；后台按实际文件变化记录释放量，遇到读写占用会等待。';
    }
    async function reclaimStorage() {
        if(locked()||!state.storage?.incremental_ready)return;
        state.mutating=true;updateControls();const body=state.storageRequest||{request_id:crypto.randomUUID()};state.storageRequest=body;
        try{const value=await request(base+'/storage/reclaim','POST',body);state.storageRequest=null;trackManagementJob(value.job);feedback('labStorageFeedback','空间回收已排队，后台分批处理。',false);await readStorage();}
        catch(error){feedback('labStorageFeedback',error.message+(error.unknown?'。再次点击将使用原请求标识核对，避免重复任务。':''),true);if(!error.unknown)state.storageRequest=null;}
        finally{state.mutating=false;updateControls();}
    }
    $("labBatchForm").addEventListener("submit", createBatch);
    createRiskEditor('labRisk',$("labRiskEditor"));createRiskEditor('labEditRisk',$("labEditRiskEditor"));createRiskEditor('labDefinitionRisk',$("labDefinitionRiskEditor"));
    $("labEditRiskButton").addEventListener('click',()=>{
        if(!state.batch?.risk_editable)return;renderRiskPresets('labEditRisk',state.batch.metric);
        state.riskEditingId=state.batch.id;
        fillRisk('labEditRisk',state.batch.risk_policy?.mode==='round'?state.batch.risk_policy:blankRisk());
        riskEditors.labEditRisk.fields.Preset.value='';$("labEditRiskForm").hidden=false;feedback('labEditRiskFeedback','',false);
    });
    $("labEditRiskForm").addEventListener('submit',saveRisk);
    $("labCancelRiskButton").addEventListener('click',()=>{$("labEditRiskForm").hidden=true;});
    $("labBatchForm").addEventListener("change", updatePreview);
    $("labRefreshButton").addEventListener("click", () => refresh(true));
    $("labEconomyButton").addEventListener('click',()=>changePerformance('economy'));
    $("labFastButton").addEventListener('click',()=>changePerformance('fast'));
    $("labBatchStateButton").addEventListener("click", () => state.batch && changeState(state.batch.id, state.batch.display_status||state.batch.status));
    $("labSort").addEventListener("change", () => {state.resultPage = 0; closeDetail(false); renderResults(); updateControls();});
    $("labResultPrevious").addEventListener("click", () => {if (state.resultPage > 0) {state.resultPage--; closeDetail(false); renderResults(); updateControls();}});
    $("labResultNext").addEventListener("click", () => {if ((state.resultPage + 1) * pageSize < state.resultFilteredCount) {state.resultPage++; closeDetail(false); renderResults(); updateControls();}});
    $("labCloseDetailButton").addEventListener("click", () => closeDetail(true));
    $("labContinuationButton").addEventListener('click',()=>{
        const batch=state.batch;if(!batch)return;
        openContinuation(batch.study_id?'study':'batch',batch.study_id||batch.id,$("labContinuationButton"));
    });
    $("labStudyContinuationButton").addEventListener('click',()=>state.study&&openContinuation('study',state.study.id,$("labStudyContinuationButton")));
    $("labContinuationClose").addEventListener('click',()=>{if(!state.mutating)closeContinuation(true);});
    $("labContinuationRefresh").addEventListener('click',refreshContinuation);
    $("labContinuationMore").addEventListener('click',moreSegments);
    $("labContinuationSettings").addEventListener('submit',saveContinuation);
    $("labContinuationAppendForm").addEventListener('submit',appendContinuation);
    $("labContinuationPause").addEventListener('click',pauseContinuation);
    $("labContinuationRetry").addEventListener('click',retryContinuation);
    $("labContinuationSettings").addEventListener('change',()=>{
        if(state.continuation)state.continuation.dirty=true;
        if(!$("labContinuationCapture").checked)$("labContinuationCompute").checked=false;
        feedback('labContinuationFeedback','设置尚未保存。',false);updateControls();
    });
    $("labMonthlyExport").addEventListener('click',exportMonthly);
    $("labInitialCapital").addEventListener('input',renderCapital);
    $("labDisplayStake").addEventListener('input',renderAmountEstimate);
    $("labAmountReset").addEventListener('click',()=>{if(!state.detailResult)return;$("labDisplayStake").value=String(state.monthly?.capital.stake_amount||state.detailContext?.batch?.stake_amount||1);renderAmountEstimate();});
    $("labExportButton").addEventListener("click", exportCsv);
    $("labCompareButton").addEventListener('click',()=>extensions.compare?extensions.compare():comparisonPreview());$("labCreateStudyButton").addEventListener('click',createStudy);
    $("labStudyStateButton").addEventListener('click',changeStudyState);$("labStudyExportButton").addEventListener('click',exportStudy);
    ['labStudyVariant','labStudySettlement','labStudyCondition','labStudySort'].forEach(id=>$(id).addEventListener('change',()=>{state.studyPage=0;renderStudyResults();updateControls();}));
    $("labStudyPrevious").addEventListener('click',()=>{state.studyPage=Math.max(0,state.studyPage-1);renderStudyResults();updateControls();});$("labStudyNext").addEventListener('click',()=>{if((state.studyPage+1)*pageSize<state.studyFilteredCount)state.studyPage++;renderStudyResults();updateControls();});
    $("labManageBatchButton").addEventListener('click',()=>{const batch=state.batch;if(batch)openManagement(batch.study_id?'study':'batch',batch.study_id||batch.id,$("labManageBatchButton"));});
    $("labManageStudyButton").addEventListener('click',()=>state.study&&openManagement('study',state.study.id,$("labManageStudyButton")));
    $("labManagementClose").addEventListener('click',()=>{if(!state.mutating&&!state.management?.pending)closeManagement(true);});
    $("labRenameForm").addEventListener('submit',event=>{event.preventDefault();const name=$("labManagementName").value.trim();if(!name||name.length>80){feedback('labManagementFeedback','请填写1～80个字符的名称。',true);return;}mutateManagement('rename',{name});});
    $("labManagementEdit").addEventListener('click',()=>editDefinition('edit'));$("labManagementFork").addEventListener('click',()=>editDefinition('fork'));
    $("labDefinitionRiskTemplate").addEventListener('change',()=>{const template=templateCatalog.find(t=>t.kind==='risk'&&'custom:'+t.id===$("labDefinitionRiskTemplate").value);if(template)fillRisk('labDefinitionRisk',template.definition.policy);});
    $("labDefinitionRiskEditor").addEventListener('change',()=>{$("labDefinitionRiskTemplate").value='manual';});
    $("labDefinitionForm").addEventListener('change',()=>{if(state.management)state.management.dirty=true;definitionOptions();});
    $("labDefinitionForm").addEventListener('submit',event=>{event.preventDefault();const config=definitionValue();const issue=riskError(riskValue('labDefinitionRisk'));if(!config.name||!Number.isFinite(config.stake_amount)||config.stake_amount<.01||config.stake_amount>10000||issue){feedback('labManagementFeedback',issue||'请填写名称及有效的每注金额（0.01～10000）。',true);return;}mutateManagement(state.management?.mode,{config});});
    $("labDefinitionCancel").addEventListener('click',()=>{$("labDefinitionForm").hidden=true;if(state.management)state.management.dirty=false;});
    $("labManagementDelete").addEventListener('click',showDeletePreview);$("labDeleteCancel").addEventListener('click',()=>{$("labDeleteForm").hidden=true;});
    $("labDeleteConfirmName").addEventListener('input',updateControls);$("labDeleteForm").addEventListener('submit',event=>{event.preventDefault();if($("labDeleteConfirmName").value!==state.management?.deletePreview?.name)return;mutateManagement('delete',{confirm_name:$("labDeleteConfirmName").value});});
    $("labManagementRetry").addEventListener('click',retryManagement);
    $("labStoragePanel").addEventListener('toggle',()=>{if($("labStoragePanel").open)readStorage();});
    $("labStorageRefresh").addEventListener('click',readStorage);$("labStorageReclaim").addEventListener('click',reclaimStorage);
    document.addEventListener("platform-auth:changed", () => {state.readRevision++; state.detailRevision++; denyAccess(); state.options = null; refresh(true);});
    document.addEventListener('visibilitychange',autoRefresh);
    window.addEventListener('resize',()=>{if(state.amountView?.monthly){renderMonthlyChart(state.amountView.monthly.rows);renderCapital();}});
    setInterval(autoRefresh,15000);
    $("labObservationForm").addEventListener('input',()=>{state.observation.dirty=true;});
    $("labObservationForm").addEventListener('change',()=>{state.observation.dirty=true;});
    $("labObservationForm").addEventListener('submit',event=>{event.preventDefault();applyObservation();});
    $("labObservationReset").addEventListener('click',()=>{fillObservationForm(observationDefaults);state.observation.dirty=true;applyObservation();});
    document.querySelectorAll('[data-observation-months]').forEach(button=>button.addEventListener('click',()=>{$("labObservationMonths").value=button.dataset.observationMonths;state.observation.dirty=true;}));
    window.RuleLabUI={request,refresh,openDrawer,dismissDrawer,closeActiveDrawer,denyAccess,feedback,createRiskEditor,fillRisk,riskValue,riskError,riskSummary,renderRiskPresets,
        getContext:()=>({options:state.options,batch:state.batch,selectedId:state.selectedId,selectedCondition:state.detailContext?.item?.condition,detailBatch:state.detailContext?.batch,detailExperiment:state.detailResult?.experiment,accessDenied:state.accessDenied,loading:state.loading,mutating:state.mutating}),
        setBusy:value=>{state.mutating=Boolean(value);updateControls();},register:handlers=>{const {drawerId,close,reset,...actions}=handlers;if(drawerId)drawerExtensions.set(drawerId,{close,reset});Object.assign(extensions,actions);},selectBatch,openStudy};
    initializeLayout();initializeObservation();updatePreview(); updateControls(); refresh(true);
    document.dispatchEvent(new CustomEvent('rulelab:ready'));
}());
