(function () {
    "use strict";
    const $ = id => document.getElementById(id);
    const base = "/api/platform/rule-lab";
    const statusNames = {paused:"已暂停", running:"运行中", completed:"回测完成", error:"处理异常", resource_paused:"资源限制暂停"};
    const phaseNames = {idle:"等待任务", warming:"准备历史窗口", observing:"等待条件或开奖", importing:"分批读取历史", replaying:"正在回放历史"};
    const workerNames = {starting:"等待后台启动", idle:"等待任务", running:"正在处理", stale:"后台暂未更新", resource_paused:"资源限制暂停", error:"处理异常"};
    const metricNames = {big_small:"大小", odd_even:"单双", combo:"组合"};
    const reasonNames = {insufficient_history:"历史样本不足", late_decision:"错过决策窗口", late_prediction:"预测到达过晚", missing_prediction:"缺少预测", invalid_draw:"开奖数据不完整", input_conflict:"输入内容冲突", missing_draw:"缺少开奖", source_unavailable:"来源暂不可用", input_expired:"所需输入已过保留期"};
    Object.assign(reasonNames,{round_profit_target:"单轮止盈",round_loss_limit:"单轮止损",round_max_bets:"单轮下注上限",day_profit_target:"日止盈",day_loss_limit:"日止损",day_max_bets:"日下注上限",day_max_rounds:"当日开轮次数已达上限",daily_hit_rate_low:"日命中率熔断",cooldown:"等待开轮间隔",no_valid_bets:"无有效投注"});
    const riskStates={legacy:"逐期对照",active:"轮内跟随",unfinished:"区间结束·轮未完结",waiting:"等待条件 / 冷却",day_stopped:"统计日已停止"};
    const riskEditors={};
    const pageSize = 20;
    const state = {options:null, overview:null, batch:null, experiments:[], selectedId:null, detailId:null, detailTrigger:null, resultPage:0,
        loading:false, mutating:false, exporting:false, readRevision:0, detailRevision:0, readAt:null, accessDenied:false,riskEditingId:null,
        studies:[],study:null,studyRows:[],studyId:null,studyPage:0,preview:null,studyRequest:null,monthly:null,reportExperiment:null,performance:null};
    const studyNames={paused:'已暂停',queued:'等待运行额度',running:'比较中',completed:'全部完成',error:'比较异常',resource_paused:'资源限制暂停'};
    const monthStatus={complete:'完整月份',no_samples:'完整月份 · 无交易',partial:'部分月份',not_replayed:'尚未回放',not_observed:'尚未观察',pending:'等待结算'};

    function element(tag, text, className) {
        const node = document.createElement(tag);
        if (text !== undefined && text !== null) node.textContent = String(text);
        if (className) node.className = className;
        return node;
    }
    function blankRisk() {return {version:1,mode:"round",round:{enabled:true,profit_target:0,loss_limit:0,max_bets:0},day:{enabled:false,profit_target:0,loss_limit:0,max_bets:0,max_rounds:0,hit_rate_gate:{enabled:false,threshold:0,min_settled_count:20}},additional_day_guards:[],cooldown_issues:0,source:{}};}
    function createRiskEditor(prefix, target) {
        const editor={prefix,source:{},guards:[],selectedPreset:"",fields:{}};riskEditors[prefix]=editor;
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
        editor.notice=element("p",null,"lab-note");editor.provenance=element("p",null,"lab-muted");editor.extra=element("p",null,"lab-note");
        target.append(grid,element("p","命中率 = 实赔 ÷（实赔＋全亏），退本不计分母；达到最少单数且低于阈值才停止。冷却从上次开轮期号算起，不是收工后等待。日次数按目标开奖日预占、最终盈亏按实际开奖日结算。","lab-muted"),editor.provenance,editor.extra,editor.notice);
        grid.addEventListener("change",event=>{
            if(event.target===editor.fields.Preset){
                const found=(state.options?.risk_presets||[]).find(p=>p.id===event.target.value);
                if(found){editor.selectedPreset=found.id;fillRisk(prefix,found.policy);editor.fields.Preset.value=found.id;}
                else {editor.source={};editor.guards=[];editor.selectedPreset="";}
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
        editor.source=structuredClone(p.source||{});editor.guards=structuredClone(p.additional_day_guards||[]);updateRiskEditor(prefix);
    }
    function riskValue(prefix) {
        const editor=riskEditors[prefix],f=editor.fields;
        if(f.Mode.value==="legacy")return {version:1,mode:"legacy"};
        return {version:1,mode:"round",round:{enabled:f.RoundEnabled.checked,profit_target:Number(f.RoundProfit.value),loss_limit:Number(f.RoundLoss.value),max_bets:Number(f.RoundMax.value)},
            day:{enabled:f.DayEnabled.checked,profit_target:Number(f.DayProfit.value),loss_limit:Number(f.DayLoss.value),max_bets:Number(f.DayMax.value),max_rounds:Number(f.DayRounds.value),
                hit_rate_gate:{enabled:f.GateEnabled.checked,threshold:Number(f.Threshold.value),min_settled_count:Number(f.Minimum.value)}},
            cooldown_issues:Number(f.Cooldown.value),additional_day_guards:structuredClone(editor.guards),source:structuredClone(editor.source)};
    }
    function riskError(p) {
        if(p.mode==="legacy")return "";
        const controls=[p.round,p.day,...p.additional_day_guards];
        if(!controls.some(c=>c.enabled && (c.profit_target>0||c.loss_limit>0||c.max_bets>0)))return "尚未设置退出条件：请启用至少一项单轮或日止盈、止损、最多下注，或明确选择逐期对照。";
        if(controls.some(c=>[c.profit_target,c.loss_limit,c.max_bets,c.max_rounds??0].some(v=>!Number.isFinite(v)||v<0)))return "风控金额和次数必须是有效非负数字。";
        if(controls.some(c=>!Number.isInteger(c.max_bets)||!Number.isInteger(c.max_rounds??0))||!Number.isInteger(p.cooldown_issues)||p.cooldown_issues<0)return "次数与开轮间隔必须是非负整数。";
        if(p.day.hit_rate_gate.enabled && (!(p.day.hit_rate_gate.threshold>0&&p.day.hit_rate_gate.threshold<100)||!Number.isInteger(p.day.hit_rate_gate.min_settled_count)||p.day.hit_rate_gate.min_settled_count<1))return "启用命中率熔断时，请填写0～100之间的阈值及正整数最少单数。";
        return "";
    }
    function riskSummary(p) {
        if(!p||p.mode!=="round")return "逐期条件对照：每期重新判断触发条件，无按轮止盈止损。";
        const n=v=>v?String(v):"不限",r=p.round,d=p.day,g=d.hit_rate_gate;
        return "按轮模拟 · 单轮"+(r.enabled?"止盈 "+n(r.profit_target)+" / 止损 "+n(r.loss_limit)+" / 最多下注 "+n(r.max_bets):"退出关闭")+" · 日"+(d.enabled?"止盈 "+n(d.profit_target)+" / 止损 "+n(d.loss_limit)+" / 最多下注 "+n(d.max_bets)+" / 最多开轮 "+n(d.max_rounds):"风控关闭")+(d.enabled&&g.enabled?" · 实赔命中率 < "+g.threshold+"%，至少 "+g.min_settled_count+" 单":"")+" · 距上次开轮 ≥ "+p.cooldown_issues+" 期"+(p.additional_day_guards?.length?" · 另有一套独立规则日保护":"");
    }
    function updateRiskEditor(prefix) {
        const editor=riskEditors[prefix];if(!editor)return;
        const p=riskValue(prefix),f=editor.fields,legacy=p.mode==="legacy",busy=state.mutating||state.accessDenied;
        Object.entries(f).forEach(([key,input])=>{input.disabled=busy||(legacy&&!['Mode'].includes(key));});
        if(!legacy){['RoundProfit','RoundLoss','RoundMax'].forEach(k=>f[k].disabled=busy||!f.RoundEnabled.checked);['DayProfit','DayLoss','DayMax','DayRounds','GateEnabled'].forEach(k=>f[k].disabled=busy||!f.DayEnabled.checked);['Threshold','Minimum'].forEach(k=>f[k].disabled=busy||!f.DayEnabled.checked||!f.GateEnabled.checked);}
        const source=editor.source;editor.provenance.textContent=source.rule_id?"复制自："+[source.rule_name,source.route_name,source.subscription_name].join(" · ")+(source.modified?"（已修改预设）":"")+"。仅复制风控，来源与每注金额不变。":"未选择现有规则预设，当前为手动配置。";
        editor.extra.hidden=!editor.guards.length;editor.extra.textContent=editor.guards.length?"附加独立日保护："+riskSummary({mode:'round',round:{enabled:false},day:editor.guards[0],cooldown_issues:0,additional_day_guards:[]})+"。选择手动配置可移除此附加层。":"";
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
        return state.overview?.batches.some(batch => batch.status === 'running') ||
            state.studies.some(study => ['running', 'queued'].includes(study.status));
    }
    function isReadingOrEditing() {
        return state.detailId !== null || !$("labEditRiskForm").hidden || !$("labComparisonPreview").hidden ||
            Boolean(document.activeElement?.matches('input,textarea,[contenteditable="true"]'));
    }
    function updateRefreshStatus() {
        $("labAutoRefreshStatus").textContent = !state.overview ? '' :
            (hasRunningWork() ? (isReadingOrEditing() ? '查看明细或编辑期间暂停页面自动更新，后台继续计算。' : '运行中每15秒自动更新；离开此页面后后台仍会继续计算。') : '当前没有运行中的模拟，可手动刷新。') +
            ' 最近读取：' + time(state.readAt);
    }
    function autoRefresh() {
        updateRefreshStatus();
        if (!document.hidden && !locked() && !state.exporting && hasRunningWork() && !isReadingOrEditing()) refresh(false);
    }
    function sampleLabel(item, batch) {
        if (item.settled_count > 0) return item.sample_label;
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
        $("labBatchStateButton").disabled = locked() || !state.batch || state.batch.status === "completed";
        $("labExportButton").disabled = locked() || state.exporting || !state.batch;
        $("labExportButton").textContent = state.exporting ? "导出中…" : "导出 CSV";
        $("labSort").disabled = locked() || !state.experiments.length;
        $("labResultPrevious").disabled = locked() || state.resultPage <= 0;
        $("labResultNext").disabled = locked() || (state.resultPage + 1) * pageSize >= state.experiments.length;
        $("labBatchList").querySelectorAll("button").forEach(button => {button.disabled = locked();});
        $("labEditRiskButton").disabled=locked();$("labSaveRiskButton").disabled=locked();$("labCancelRiskButton").disabled=state.mutating;
        ['labCompareButton','labCreateStudyButton','labStudyExportButton','labStudyPrevious','labStudyNext'].forEach(id=>$(id).disabled=locked());
        $("labStudyStateButton").disabled=locked()||!state.study||state.study.status==='completed';
        ['labReplayStake','labReplayRiskMode','labReplayCreateButton'].forEach(id=>$(id).disabled=locked()||state.batch?.mode!=='backtest'||!state.monthly);
        $("labStudyList").querySelectorAll('button').forEach(button=>button.disabled=locked());
        [['labEconomyButton','economy'],['labFastButton','fast']].forEach(([id,mode])=>{
            $(id).disabled=locked()||!state.performance?.available||state.performance.mode===mode;
            $(id).setAttribute('aria-pressed',String(state.performance?.mode===mode));
        });
        if(state.preview)$("labCreateStudyButton").disabled=locked()||state.preview.new_experiments>state.preview.capacity.remaining_saved||state.preview.candidate_count<2;
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
        $("labBudgetDescription").textContent = "默认省资源CPU上限0.2核，加速上限1核；内存上限 " + bytes(limits.memory_max_bytes) + "；全服务最多同时运行 " + limits.max_running_experiments + " 个实验、保存 " + limits.max_total_experiments + " 个实验。近期明细保留 " + limits.detail_retention_days + " 天。当前生效档位见上方运行速度。";
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
        $("labBatchCount").textContent = overview.batches.length + " 个批次";
        $("labBatchList").replaceChildren();
        if (!overview.batches.length) {
            $("labBatchList").append(element("p", "还没有实验批次。先选择来源与玩法生成候选。", "lab-empty"));
            $("labGenerator").open = true;
        }
        overview.batches.forEach(batch => {
            const row = element("div", null, "lab-batch"); row.dataset.selected = String(batch.id === state.selectedId);
            const select = element("button", null, "lab-batch-select"); select.type = "button"; select.dataset.batchId = String(batch.id);
            select.setAttribute("aria-label", "查看批次：" + batch.name);
            if (batch.id === state.selectedId) select.setAttribute("aria-current", "true");
            select.append(element("strong", batch.name), element("span", modeName(batch.mode) + " · " + (metricNames[batch.metric] || batch.metric) + " · " + (statusNames[batch.status] || batch.status) + " · " + batch.experiment_count + " 个版本 · 已处理 " + batch.processed_issues + " 期"), element("span", batch.mode === "backtest" ? batch.start_date + " 至 " + batch.end_date : "启动于 " + time(batch.started_at)));
            select.addEventListener("click", () => selectBatch(batch.id));
            row.append(select);
            if (batch.status !== "completed" && !batch.study_id) {
                const actions = element("div", null, "lab-batch-actions");
                actions.append(actionButton(batch.status === "running" ? "暂停" : batch.status === "paused" ? "启动" : "恢复", () => changeState(batch.id, batch.status)));
                row.append(actions);
            }
            if (batch.study_id) row.append(actionButton('查看比较队列', async () => {
                await openStudy(batch.study_id);
                if (state.study?.id === batch.study_id) $("labStudyResults").scrollIntoView({block:'start'});
            }));
            row.querySelectorAll('button').forEach(button=>button.disabled=locked());
            $("labBatchList").append(row);
        });
    }
    function updatePreview() {
        const metric=$("labMetric").value,selected=$("labTemplatePack").value;
        const packs=(state.options?.template_packs||[{id:'basic_v1',name:'基础条件 v1',metrics:['combo','big_small','odd_even'],condition_counts:{combo:39,big_small:25,odd_even:25},description:'含无条件跟随基准、连续条件与窗口命中率。'}]).filter(pack=>pack.metrics.includes(metric));
        $("labTemplatePack").replaceChildren();packs.forEach(pack=>{const option=element('option',pack.name);option.value=pack.id;$("labTemplatePack").append(option);});
        if(packs.some(pack=>pack.id===selected))$("labTemplatePack").value=selected;
        const pack=packs.find(pack=>pack.id===$("labTemplatePack").value),count=pack?.condition_counts[metric]||0;
        $("labTemplateHelp").textContent=pack?.description||'暂无可用模板';
        $("labCandidatePreview").textContent = count + " 个条件 × 2 套结算 = " + (count*2) + " 个实验版本";
        const historical = $("labBatchForm").elements.mode.value === "backtest";
        $("labDates").hidden = !historical;
        $("labStartDate").required = historical; $("labEndDate").required = historical;
        $("labModeHelp").textContent = historical ? "按现存历史记录重放，与前向模拟分别统计。" : "从启动后的可用信号开始观察，等待条件及开奖。";
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
        state.detailRevision++; state.detailId = null; $("labExperimentDetail").hidden = true;
        state.monthly=null;state.reportExperiment=null;$("labMonthly").hidden=true;$("labMonthlyExport").disabled=true;
        $("labResultsBody").querySelectorAll("button[aria-expanded]").forEach(button => button.setAttribute("aria-expanded", "false"));
        if (restoreFocus && state.detailTrigger && state.detailTrigger.isConnected) state.detailTrigger.focus();
        state.detailTrigger = null;
    }
    function renderResults() {
        const batch = state.batch;
        $("labResults").hidden = !batch;
        if (!batch) return;
        $("labResultTitle").textContent = batch.name;
        $("labBatchMeta").textContent = [modeName(batch.mode), statusNames[batch.status] || batch.status, phaseNames[batch.phase] || batch.phase,
            batch.predictor_name, metricNames[batch.metric], "每期 " + money(batch.stake_amount), "已处理 " + batch.processed_issues + (batch.total_issues === null ? " 期" : " / " + batch.total_issues + " 期"), "批次更新于 " + time(batch.updated_at)].filter(Boolean).join(" · ");
        $("labBatchNotice").textContent = batch.history_notice || ""; $("labBatchNotice").hidden = !batch.history_notice;
        $("labBatchError").textContent = batch.error || ""; $("labBatchError").hidden = !batch.error;
        $("labBatchRiskSummary").textContent=riskSummary(batch.risk_policy)+(batch.risk_policy?.source?.rule_id?" · 复制自 "+batch.risk_policy.source.rule_name+" / "+batch.risk_policy.source.route_name+" / "+batch.risk_policy.source.subscription_name:"");
        $("labEditRiskButton").hidden=!batch.risk_editable;
        $("labCompareButton").hidden=Boolean(batch.study_id)||batch.mode!=='backtest'||batch.risk_policy?.mode!=='round'||batch.experiment_count<2;
        $("labResultsBody").closest('table').querySelector('caption').textContent=(batch.experiment_count===1?'当前条件与赔率的独立实验版本；':'同一条件的两套结算版本；')+'样本表现仍需验证。统计日为最近处理日，回测中不代表真实今天。';
        if(!batch.risk_editable)$("labEditRiskForm").hidden=true;
        const skipCounts = Object.entries(batch.skip_counts || {}).filter(([, count]) => count > 0).map(([reason, count]) => (reasonNames[reason] || reason) + " " + count);
        $("labSkipCounts").textContent = "未执行记录：" + skipCounts.join("；"); $("labSkipCounts").hidden = !skipCounts.length;
        $("labBatchStateButton").hidden = batch.status === "completed"||Boolean(batch.study_id);
        if(batch.study_id)$("labBatchRiskSummary").append(document.createTextNode(' · 此子批次由比较组 #'+batch.study_id+' 统一管理'));
        $("labBatchStateButton").textContent = batch.status === "running" ? "暂停模拟" : batch.status === "paused" ? "启动模拟" : "恢复模拟";
        const settled = state.experiments.reduce((sum, item) => sum + item.settled_count, 0);
        $("labResultEmpty").hidden = settled > 0;
        $("labResultEmpty").textContent = "尚无已结算模拟结果。" + (batch.study_id && batch.status === "paused" ? "此方案由比较队列统一调度，请在比较组查看当前进度。" : batch.status === "paused" ? "启动批次后开始观察。" : (phaseNames[batch.phase] || "等待可用数据") + "，条件不足时不会按零命中率处理。");
        const list = state.experiments.slice();
        const sort = $("labSort").value;
        if (sort === "profit") list.sort((a,b) => b.net_profit - a.net_profit || a.id - b.id);
        if (sort === "sample") list.sort((a,b) => b.settled_count - a.settled_count || a.id - b.id);
        if (sort === "drawdown") list.sort((a,b) => a.max_drawdown - b.max_drawdown || a.id - b.id);
        $("labResultsBody").replaceChildren();
        state.resultPage = Math.min(state.resultPage, Math.max(0, Math.ceil(list.length / pageSize) - 1));
        const start = state.resultPage * pageSize;
        $("labResultPageInfo").textContent = list.length ? "显示 " + (start + 1) + "–" + Math.min(start + pageSize, list.length) + "，共 " + list.length + " 个实验版本" : "没有实验版本";
        list.slice(start, start + pageSize).forEach((item, index) => {
            const row = element("tr"); if (sort === "paired" && index > 0 && index % 2 === 0) row.className = "lab-pair-start";
            row.append(element("td", conditionName(item.condition), "lab-condition"), element("td", settlementName(item)),
                element("td", item.settled_count, "lab-number"), element("td", money(item.net_profit, true), "lab-number " + profitClass(item.net_profit)),
                element("td", percent(item.roi_percent), "lab-number"), element("td", money(item.max_drawdown), "lab-number"),
                element("td", item.max_loss_streak, "lab-number"), element("td", resultCounts(item), "lab-number"));
            const risk=item.risk_state||{mode:'legacy',state:'legacy'};
            row.append(element('td',riskStates[risk.state]||risk.state),element('td',risk.day_date||'—'),
                element('td',risk.mode==='round'?money(risk.day_net_profit,true)+' / '+risk.day_round_count:'—','lab-number'),
                element('td',money(risk.round_net_profit,true),'lab-number'),element('td',reasonNames[risk.stop_reason]||risk.stop_reason||'—'),element("td", sampleLabel(item, batch), "lab-sample"));
            const cell = element("td"); const button = actionButton("详情", () => openDetail(item, button));
            button.setAttribute("aria-expanded", "false"); button.setAttribute("aria-controls", "labExperimentDetail"); button.dataset.experimentId = String(item.id); cell.append(button); row.append(cell); $("labResultsBody").append(row);
            const monthly=actionButton('月度成绩',()=>openDetail(item,monthly,true));cell.append(monthly);
        });
    }
    function denyAccess() {
        state.accessDenied = true; state.selectedId = null; state.batch = null; state.experiments = []; state.overview = null;
        $("labBatchList").replaceChildren(element("p", "请使用有权限的管理员账号登录后刷新。", "lab-empty"));
        $("labWorkerSummary").replaceChildren(); $("labWorkerMessage").textContent = ""; $("labBatchCount").textContent = "—";
        closeDetail(false); renderResults();
        state.studies=[];state.study=null;state.studyRows=[];state.studyId=null;renderStudies();renderStudyResults();
        state.performance=null;renderPerformance();feedback('labPerformanceFeedback','',false);
    }
    async function selectBatch(id) {
        state.riskEditingId=null;$("labEditRiskForm").hidden=true;
        $("labComparisonPreview").hidden=true;state.preview=null;state.studyRequest=null;
        if (state.mutating || state.loading || state.accessDenied) return;
        state.selectedId = id; state.resultPage = 0; closeDetail(false); $("labSort").value = "paired";
        await refresh(false);
    }
    async function refresh(includeOptions) {
        const revision = ++state.readRevision;
        state.loading = true; updateControls();
        try {
            const calls = [request(base),request(base+'/studies'),readPerformance()];
            if (includeOptions !== false || !state.options) calls.push(request(base + "/options"));
            const [overview, studies, performance, options] = await Promise.all(calls);
            if (revision !== state.readRevision) return;
            if (!Array.isArray(overview.batches)) throw responseError("批次数据不完整，请刷新后重试", 0, "invalid_response");
            if (options) {state.options = validateOptions(options); renderOptions();}
            state.accessDenied = false; state.overview = overview; state.readAt = new Date().toISOString();
            state.performance=performance;renderPerformance();
            state.studies=studies.studies||[];renderStudies();
            if (!state.studies.some(study => study.id === state.studyId)) {
                state.studyId = (state.studies.find(study => ['running','queued'].includes(study.status)) ||
                    (state.studies.length === 1 ? state.studies[0] : null))?.id ?? null;
                state.study=null;state.studyRows=[];renderStudyResults();
            }
            if (!overview.batches.some(batch => batch.id === state.selectedId)) state.selectedId = (overview.batches.find(batch => batch.status === 'running') || overview.batches[0])?.id ?? null;
            renderOverview();
            if (state.selectedId !== null) {
                const detail = await request(base + "/batches/" + state.selectedId);
                if (revision !== state.readRevision) return;
                if (!detail.batch || !Array.isArray(detail.experiments)) throw responseError("实验结果不完整，请刷新后重试", 0, "invalid_response");
                closeDetail(false); state.batch = detail.batch; state.experiments = detail.experiments;
            } else {state.batch = null; state.experiments = [];}
            renderResults(); feedback("statusMessage", "", false);
            if(state.studyId!==null){const study=await request(base+'/studies/'+state.studyId);if(revision!==state.readRevision)return;state.study=study.study;state.studyRows=study.rows;renderStudyResults();}
        } catch (error) {
            if (revision !== state.readRevision) return;
            if (error.status === 401 || error.status === 403) denyAccess();
            feedback("statusMessage", error.message + (state.overview ? "。更新失败，以下为上次读取结果（" + time(state.readAt) + "）。" : "。可点击刷新重试。"), true);
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
            state.selectedId = result.batch.id; state.resultPage = 0; $("labSort").value = "paired";
            await refresh(false);
            feedback("labFormFeedback", "已保存 " + result.generated_count + " 个实验版本，当前已暂停。可在批次结果中启动模拟。", false);
            $("labGenerator").open = false;
            $("labResultTitle").focus({preventScroll:true});
            feedback("statusMessage", "实验批次已保存，尚未启动。", false);
        } catch (error) {
            if (error.status === 401 || error.status === 403) denyAccess();
            if (error.unknown) {
                await refresh(false);
                feedback("labFormFeedback", "创建结果暂时无法确认，已尝试刷新批次列表。请核对是否已生成，避免重复提交。", true);
            } else feedback("labFormFeedback", error.message, true);
            $("labGenerator").open = true;
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
            await request(base + "/batches/" + id + "/state", "POST", {state:status === "running" ? "paused" : "running"});
            await refresh(false); feedback("statusMessage", status === "running" ? "已暂停新模拟，已有待结算记录仍会完成结算。" : "已启动模拟，运行进度与结果每15秒自动更新。", false);
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
    async function openDetail(item, button, monthlyFocus=false) {
        if (state.detailId === item.id && !$("labExperimentDetail").hidden) {
            if(monthlyFocus && state.monthly){$("labMonthlyTitle").focus();return;}
            closeDetail(true); return;
        }
        closeDetail(false); state.detailId = item.id; state.detailTrigger = button;
        const revision = ++state.detailRevision, batchId = state.selectedId;
        button.setAttribute("aria-expanded", "true"); $("labExperimentDetail").hidden = false;
        $("labDetailTitle").textContent = conditionName(item.condition) + " · " + settlementName(item);
        const packName=(state.options?.template_packs||[]).find(pack=>pack.id===state.batch?.template_pack)?.name||state.batch?.template_pack||'基础条件 v1';
        $("labDetailMeta").textContent = packName+" · 固定金额 · 北京时间 · "+riskSummary(state.batch?.risk_policy);
        $("labDetailStats").replaceChildren(); $("labDailyBody").replaceChildren(); $("labRecentBody").replaceChildren();$("labRoundsBody").replaceChildren();
        feedback("labDetailFeedback", "读取明细…", false); $("labDetailTitle").focus();
        try {
            const result = await request(base + "/batches/" + batchId + "/experiments/" + item.id);
            if (revision !== state.detailRevision || batchId !== state.selectedId) return;
            if (!result.experiment || !Array.isArray(result.daily) || !Array.isArray(result.recent)) throw responseError("明细数据不完整，请关闭后重试", 0, "invalid_response");
            const exp = result.experiment;
            $("labDetailMeta").append(document.createTextNode(' · 本实验冻结赔率：'+frozenOddsText(exp)+(exp.legacy_odds?'。旧赔率成绩保留，未按当前赔率重算。':'')));
            state.reportExperiment=exp;
            state.monthly=result.monthly||null;
            $("labReplayStake").value=String(state.monthly?.capital.stake_amount||state.batch?.stake_amount||1);
            $("labReplayRiskMode").value='scale';$("labStakeReplay").open=false;feedback('labReplayFeedback','',false);
            renderMonthly();
            renderReplay();updateControls();
            putSummary($("labDetailStats"), [["累计投注", money(exp.stake_total)], ["累计净收益", money(exp.net_profit, true)], ["已结算期数", exp.settled_count], ["触发 / 跳过", exp.trigger_count + " / " + exp.skipped_count]]);
            const rs=exp.risk_state;
            if(rs?.mode==='round')putSummary($("labDetailStats"),[["累计净收益",money(exp.net_profit,true)],["有效完成轮 / 无效轮",rs.completed_rounds+' / '+rs.invalid_rounds],['盈利 / 亏损 / 平轮',rs.profit_rounds+' / '+rs.loss_rounds+' / '+rs.flat_rounds],['统计日',rs.day_date||'—'],['该日净利 / 开轮',money(rs.day_net_profit,true)+' / '+rs.day_round_count],['当前轮',riskStates[rs.state]||rs.state]]);
            tableRows($("labRoundsBody"),result.recent_rounds||[],[r=>r.round_no,r=>time(r.started_at),r=>time(r.ended_at),r=>r.bet_count+' / '+r.settled_count,r=>money(r.net_profit,true),r=>r.status==='active'?(state.batch?.status==='completed'?'区间结束·未完结':'进行中'):r.stop_reason==='no_valid_bets'?'无有效投注':'已完成',r=>reasonNames[r.stop_reason]||r.stop_reason||'—'],'暂无轮次记录；逐期对照不会生成轮次。');
            tableRows($("labDailyBody"), result.daily, [d=>d.date, d=>d.settled_count, resultCounts, d=>money(d.stake_total), d=>money(d.net_profit,true)], "暂无每日结算记录。");
            tableRows($("labRecentBody"), result.recent, [d=>d.issue_no, d=>time(d.decided_at), d=>d.bet_value, d=>money(d.stake_amount),
                d=>({pending:"待结算",settled:"已结算",skipped:"已跳过"}[d.status] || d.status),
                d=>({hit:"实赔",refund:"退本",miss:"全亏"}[d.result_type] || "—"), d=>money(d.net_profit,true),
                d=>d.refund_reason || reasonNames[d.skip_reason] || d.skip_reason || "—"], "暂无逐期明细。");
            feedback("labDetailFeedback", "", false);
            if(monthlyFocus && state.monthly)$("labMonthlyTitle").focus();
        } catch (error) {if (revision === state.detailRevision) {if (error.status === 401 || error.status === 403) denyAccess(); feedback("labDetailFeedback", error.message + "。关闭后可重新查看。", true);}}
    }
    function renderMonthly() {
        const report=state.monthly;$("labMonthly").hidden=!report;$("labMonthlyExport").disabled=!report;
        if(!report)return;
        const summary=report.summary;
        const exp=state.reportExperiment,valid=exp.hit_count+exp.miss_count;
        $("labCapitalStake").textContent='本次回测每注 '+money(report.capital.stake_amount)+' 元。本金与每注金额共同决定资金使用比例。';
        $("labReportScope").textContent='每注 '+money(report.capital.stake_amount)+' · '+(report.capital.batch_status==='completed'?'回测已完成':'回测尚未完成，以下为阶段结果')+' · 每个条件、赔率和风控版本独立统计。';
        putSummary($("labPerformanceSummary"),[['累计净利润',money(exp.net_profit,true)],['累计投注额',money(exp.stake_total)],['投注收益率',percent(exp.roi_percent)],['最大回撤金额（逐笔）',money(exp.max_drawdown)],['已结算笔数',exp.settled_count],['胜率（不含退款）',percent(valid?exp.hit_count/valid*100:null)],['最长连续亏损笔数',exp.max_loss_streak],['收益回撤比',exp.max_drawdown>0?money(exp.net_profit/exp.max_drawdown):'—']]);
        const notes=['月度胜率 = 盈利月份 ÷ 完整且有交易的月份。部分月份及无交易月份不参与计算。'];
        if(summary.eligible_months<6)notes.push('目前仅 '+summary.eligible_months+' 个完整且有交易的月份，样本不足以判断长期稳定性。');
        if(report.window.truncated)notes.push('只展示最近120个自然月，月度摘要也仅计算此范围；累计逐笔回撤仍覆盖全部已处理区间。');
        if(Object.keys(report.input_warnings||{}).length)notes.push('回放存在跳过或输入告警：'+Object.entries(report.input_warnings).map(([key,value])=>(reasonNames[key]||key)+' '+value).join('；')+'。');
        $("labMonthlyNotice").textContent=notes.join(' ');
        putSummary($("labMonthlySummary"),[['盈利月份 / 有效月份',summary.profitable_months+' / '+summary.eligible_months],['月度胜率',percent(summary.profitable_month_ratio)],['亏损月份 / 持平月份',summary.losing_months+' / '+summary.flat_months],['最长连续亏损月数',summary.longest_losing_months],['最佳月份净利润',money(summary.best_month_profit,true)],['最差月份净利润',money(summary.worst_month_profit,true)]]);
        tableRows($("labMonthlyBody"),report.rows,[r=>r.month,r=>monthStatus[r.status]||r.status,r=>r.settled_count,r=>money(r.stake_total),r=>r.settled_count?money(r.net_profit,true):'—',r=>percent(r.roi_percent),r=>r.profitable_days+' / '+r.losing_days+' / '+r.betting_days,r=>money(r.worst_day_profit,true),r=>money(r.daily_close_drawdown)],'暂无月度记录。');
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
        const basis=state.monthly?.capital;if(!basis)return;
        const raw=$("labInitialCapital").value,initial=Number(raw),cents=Math.round(initial*100);
        if(raw===''){feedback('labCapitalFeedback','填写初始资金后显示期末资金、累计收益率和资金曲线。',false);return;}
        if(!Number.isFinite(initial)||initial<.01||initial>1e9||Math.abs(initial*100-cents)>.00001){
            feedback('labCapitalFeedback','初始资金须为0.01～10亿之间的金额，最多两位小数。',true);return;
        }
        if(!basis.settled_count){feedback('labCapitalFeedback','尚无已结算交易，暂不能展示资金表现。',false);return;}
        const curve=state.monthly.equity;
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
    function renderReplay() {
        feedback('labReplayFeedback','',false);
        const batch=state.batch;$("labStakeReplay").hidden=batch?.mode!=='backtest';
        if(!batch)return;
        const amount=Number($("labReplayStake").value),scale=$("labReplayRiskMode").value==='scale';
        const policy=structuredClone(batch.risk_policy);
        if(!Number.isFinite(amount)||amount<.01||amount>10000||Math.abs(amount*100-Math.round(amount*100))>1e-7){
            $("labReplayRiskPreview").textContent='填写0.01～10000的金额（最多两位小数）后预览止盈止损。';return;
        }
        if(scale&&policy.mode==='round'){
            const numerator=BigInt(Math.round(amount*100)),denominator=BigInt(Math.round(batch.stake_amount*100));
            [policy.round,policy.day,...policy.additional_day_guards].forEach(control=>{
                ['profit_target','loss_limit'].forEach(key=>{
                    const original=BigInt(Math.round(control[key]*100));
                    control[key]=Number((original*numerator*2n+denominator)/(denominator*2n))/100;
                });
            });
        }
        $("labReplayRiskPreview").textContent='新回测每注 '+money(amount)+' · '+riskSummary(policy);
        if(policy.additional_day_guards?.length){const guard=policy.additional_day_guards[0];$("labReplayRiskPreview").append(document.createTextNode(' · 附加日止盈 '+guard.profit_target+' / 止损 '+guard.loss_limit));}
    }
    async function createAmountReplay(event) {
        event.preventDefault();
        if(locked()||state.batch?.mode!=='backtest'||!state.monthly)return;
        const raw=$("labReplayStake").value,amount=Number(raw);
        if(!raw||!Number.isFinite(amount)||amount<.01||amount>10000||Math.abs(amount*100-Math.round(amount*100))>1e-7){
            feedback('labReplayFeedback','请输入0.01～10000的每注金额，最多两位小数。',true);$("labReplayStake").focus();return;
        }
        const batchId=state.batch.id,experimentId=state.detailId;
        state.mutating=true;updateControls();feedback('labReplayFeedback','正在生成新的金额回测…',false);
        try{
            const result=await request(base+'/batches/'+batchId+'/experiments/'+experimentId+'/replay','POST',{
                stake_amount:amount,risk_amount_mode:$("labReplayRiskMode").value});
            if(!result.batch||!Number.isInteger(result.batch.id))throw responseError('创建结果暂时无法确认',0,'invalid_response',true);
            state.selectedId=result.batch.id;state.resultPage=0;$("labSort").value='paired';
            await refresh(false);$("labGenerator").open=false;
            feedback('statusMessage','已生成当前条件的新金额回测，每注 '+money(amount)+' 元。点击“启动模拟”开始重算，完成后查看新实验的资金表现。',false);
            $("labResultTitle").focus();
        }catch(error){
            if(error.status===401||error.status===403)denyAccess();
            if(error.unknown){await refresh(false);feedback('statusMessage','生成结果暂时无法确认，已尝试刷新列表。请核对是否已有新金额回测，避免重复提交。',true);}
            else feedback('labReplayFeedback',error.message,true);
        }finally{state.mutating=false;updateControls();}
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
        const report=state.monthly;if(!report||state.accessDenied)return;
        const columns=['month','status','range_start','range_end','complete_month','eligible','settled_count','stake_total','net_profit','roi_percent','betting_days','profitable_days','losing_days','worst_day_profit','daily_close_drawdown'];
        const cell=value=>'"'+String(value??'').replaceAll('"','""')+'"';
        const text=columns.join(',')+'\r\n'+report.rows.map(row=>columns.map(key=>cell(row[key])).join(',')).join('\r\n');
        const url=URL.createObjectURL(new Blob(['\uFEFF',text],{type:'text/csv;charset=utf-8'})),link=element('a');
        link.href=url;link.download='rule-lab-'+state.selectedId+'-experiment-'+state.detailId+'-monthly.csv';document.body.append(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
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
    function renderStudies(){
        $("labStudyList").replaceChildren();
        if(!state.studies.length)$("labStudyList").append(element('p','暂无比较组。选择一个历史按轮批次，点击“比较风控方案”。','lab-empty'));
        state.studies.forEach(study=>{const row=element('div',null,'lab-batch');const button=actionButton(study.name+' · '+(studyNames[study.status]||study.status)+' · 已完成 '+study.completed_children+'/'+study.total_children,()=>openStudy(study.id));button.dataset.studyId=study.id;row.append(button);$("labStudyList").append(row);});
    }
    async function openStudy(id){
        if(locked())return;state.studyId=id;state.studyPage=0;state.loading=true;updateControls();
        const revision=++state.readRevision;
        try{const data=await request(base+'/studies/'+id);if(revision!==state.readRevision)return;state.study=data.study;state.studyRows=data.rows;renderStudyResults();}
        catch(error){if(revision!==state.readRevision)return;if(error.status===401||error.status===403)denyAccess();feedback('statusMessage',error.message,true);}
        finally{if(revision===state.readRevision){state.loading=false;updateControls();}}
    }
    function renderStudyResults(){
        const study=state.study;$("labStudyResults").hidden=!study;if(!study)return;
        $("labStudyTitle").textContent=study.name;
        const active=study.children.find(child=>child.status==='running');
        $("labStudyMeta").textContent=(studyNames[study.status]||study.status)+' · 已完成 '+study.completed_children+'/'+study.total_children+' 方案'+(active?' · 当前：'+active.label+'，已处理 '+active.processed_issues+' 期':'')+' · 共同截止期 '+(study.cutoff_issue||'首次调度时冻结')+' · 截止时间 '+time(study.cutoff_at);
        $("labStudyNotice").textContent=study.error||study.waiting_reason||(study.comparable?'输入指纹与实际覆盖一致，可查看相对基准差；历史表现仍需新数据验证。':study.comparison_reason);
        $("labStudyStateButton").textContent=['running','queued'].includes(study.status)?'暂停比较队列':'启动比较队列';$("labStudyStateButton").hidden=study.status==='completed';
        $("labStudyProgress").replaceChildren();study.children.forEach(child=>{const row=element('p');const waiting=child.status==='paused'&&['running','queued'].includes(study.status);row.append(document.createTextNode(child.label+'：'+(waiting?'等待前序方案完成':statusNames[child.status]||child.status)+'，已处理 '+child.processed_issues+' 期。'+child.changes.join('；')+' '));const button=actionButton('查看子批次',()=>selectBatch(child.batch_id));button.dataset.childBatch=child.batch_id;row.append(button);$("labStudyProgress").append(row);});
        function fillSelect(id,options){const select=$(id),value=select.value;select.replaceChildren();options.forEach(([key,label])=>{const option=element('option',label);option.value=key;select.append(option);});if(options.some(([key])=>key===value))select.value=value;}
        fillSelect('labStudyVariant',[['','全部风控方案'],...study.children.map(c=>[c.variant_key,c.label])]);
        const conditionMap=new Map();state.studyRows.forEach(r=>conditionMap.set(r.condition_key,conditionName(r.condition)));
        fillSelect('labStudyCondition',[['','全部条件'],...conditionMap.entries()]);
        const variantOrder=new Map(study.children.map((c,i)=>[c.variant_key,i]));
        const rows=state.studyRows.filter(r=>(!$("labStudyVariant").value||r.variant_key===$("labStudyVariant").value)&&(!$("labStudySettlement").value||r.settlement_rule_id===$("labStudySettlement").value)&&(!$("labStudyCondition").value||r.condition_key===$("labStudyCondition").value));
        const conditionOrder=new Map([...conditionMap.keys()].map((k,i)=>[k,i]));
        rows.sort((a,b)=>conditionOrder.get(a.condition_key)-conditionOrder.get(b.condition_key)||a.settlement_rule_id.localeCompare(b.settlement_rule_id)||variantOrder.get(a.variant_key)-variantOrder.get(b.variant_key));
        state.studyPage=Math.min(state.studyPage,Math.max(0,Math.ceil(rows.length/pageSize)-1));const start=state.studyPage*pageSize;
        $("labStudyRows").replaceChildren();rows.slice(start,start+pageSize).forEach(r=>{const tr=element('tr');
            const child=study.children.find(child=>child.batch_id===r.batch_id);
            [conditionName(r.condition),settlementName(r),r.variant_label,r.settled_count,money(r.stake_total),money(r.net_profit,true),percent(r.roi_percent),money(r.max_drawdown),money(r.worst_day_profit,true),r.profitable_days+' / '+r.betting_days,r.completed_rounds,money(r.delta_net_profit,true),r.delta_roi_pp===null?'—':money(r.delta_roi_pp,true),money(r.delta_drawdown,true),sampleLabel(r,{...child,study_id:study.id})].forEach(v=>tr.append(element('td',v,'lab-number')));
            const td=element('td');td.append(actionButton('子批次',()=>selectBatch(r.batch_id)));
            const monthly=actionButton('月度成绩',async()=>{if(locked())return;await selectBatch(r.batch_id);const item=state.experiments.find(item=>item.id===r.experiment_id);if(state.selectedId===r.batch_id&&item)await openDetail(item,monthly,true);});td.append(monthly);tr.append(td);$("labStudyRows").append(tr);});
        $("labStudyPageInfo").textContent=rows.length?'显示 '+(start+1)+'–'+Math.min(start+pageSize,rows.length)+'，共 '+rows.length+' 行':'当前筛选无结果';
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
        try{const result=await request(base+'/studies','POST',{source_batch_id:state.preview.source_batch_id,name,request_id:state.studyRequest.id});state.studyId=result.study.id;state.studyPage=0;await refresh(false);$("labComparisonPreview").hidden=true;feedback('statusMessage','比较组已保存，默认暂停。点击“启动比较队列”才会开始分批计算。',false);}
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
    $("labBatchForm").addEventListener("submit", createBatch);
    createRiskEditor('labRisk',$("labRiskEditor"));createRiskEditor('labEditRisk',$("labEditRiskEditor"));
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
    $("labBatchStateButton").addEventListener("click", () => state.batch && changeState(state.batch.id, state.batch.status));
    $("labSort").addEventListener("change", () => {state.resultPage = 0; closeDetail(false); renderResults(); updateControls();});
    $("labResultPrevious").addEventListener("click", () => {if (state.resultPage > 0) {state.resultPage--; closeDetail(false); renderResults(); updateControls();}});
    $("labResultNext").addEventListener("click", () => {if ((state.resultPage + 1) * pageSize < state.experiments.length) {state.resultPage++; closeDetail(false); renderResults(); updateControls();}});
    $("labCloseDetailButton").addEventListener("click", () => closeDetail(true));
    $("labMonthlyExport").addEventListener('click',exportMonthly);
    $("labInitialCapital").addEventListener('input',renderCapital);
    $("labReplayStake").addEventListener('input',renderReplay);
    $("labReplayRiskMode").addEventListener('change',renderReplay);
    $("labReplayForm").addEventListener('submit',createAmountReplay);
    $("labExportButton").addEventListener("click", exportCsv);
    $("labCompareButton").addEventListener('click',comparisonPreview);$("labCreateStudyButton").addEventListener('click',createStudy);
    $("labStudyStateButton").addEventListener('click',changeStudyState);$("labStudyExportButton").addEventListener('click',exportStudy);
    ['labStudyVariant','labStudySettlement','labStudyCondition'].forEach(id=>$(id).addEventListener('change',()=>{state.studyPage=0;renderStudyResults();}));
    $("labStudyPrevious").addEventListener('click',()=>{state.studyPage=Math.max(0,state.studyPage-1);renderStudyResults();});$("labStudyNext").addEventListener('click',()=>{state.studyPage++;renderStudyResults();});
    document.addEventListener("platform-auth:changed", () => {state.readRevision++; state.detailRevision++; denyAccess(); state.options = null; refresh(true);});
    document.addEventListener('visibilitychange',autoRefresh);
    window.addEventListener('resize',()=>{if(state.monthly){renderMonthlyChart(state.monthly.rows);renderCapital();}});
    setInterval(autoRefresh,15000);
    updatePreview(); updateControls(); refresh(true);
}());
