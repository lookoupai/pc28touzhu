(function () {
    "use strict";
    const $ = id => document.getElementById(id);
    const base = "/api/platform/rule-lab";
    const statusNames = {paused:"已暂停", running:"运行中", completed:"回测完成", error:"处理异常", resource_paused:"资源限制暂停"};
    const phaseNames = {idle:"等待任务", warming:"准备历史窗口", observing:"等待条件或开奖", importing:"分批读取历史", replaying:"正在回放历史"};
    const workerNames = {starting:"等待后台启动", idle:"等待任务", running:"正在处理", stale:"后台暂未更新", resource_paused:"资源限制暂停", error:"处理异常"};
    const metricNames = {big_small:"大小", odd_even:"单双", combo:"组合"};
    const reasonNames = {insufficient_history:"历史样本不足", late_decision:"错过决策窗口", late_prediction:"预测到达过晚", missing_prediction:"缺少预测", invalid_draw:"开奖数据不完整", input_conflict:"输入内容冲突", missing_draw:"缺少开奖", source_unavailable:"来源暂不可用", input_expired:"所需输入已过保留期"};
    const pageSize = 20;
    const state = {options:null, overview:null, batch:null, experiments:[], selectedId:null, detailId:null, detailTrigger:null, resultPage:0,
        loading:false, mutating:false, exporting:false, readRevision:0, detailRevision:0, readAt:null, accessDenied:false};

    function element(tag, text, className) {
        const node = document.createElement(tag);
        if (text !== undefined && text !== null) node.textContent = String(text);
        if (className) node.className = className;
        return node;
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
        $("labBudgetDescription").textContent = "后台 CPU 上限 " + (limits.cpu_quota_percent / 100) + " 个核心，内存上限 " + bytes(limits.memory_max_bytes) + "；全服务最多同时运行 " + limits.max_running_experiments + " 个实验、保存 " + limits.max_total_experiments + " 个实验。近期明细保留 " + limits.detail_retention_days + " 天。这里显示配置预算，不是实时资源用量。";
        $("labDateHelp").textContent = "日期按北京时间计算，最多回测 " + limits.max_backtest_days + " 天；仅使用实际留存的数据。";
        $("labStartDate").max = today(); $("labEndDate").max = today();
        updatePreview();
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
            select.append(element("strong", batch.name), element("span", modeName(batch.mode) + " · " + (metricNames[batch.metric] || batch.metric) + " · " + (statusNames[batch.status] || batch.status) + " · " + batch.experiment_count + " 个版本"), element("span", batch.mode === "backtest" ? batch.start_date + " 至 " + batch.end_date : "启动于 " + time(batch.started_at)));
            select.addEventListener("click", () => selectBatch(batch.id));
            row.append(select);
            if (batch.status !== "completed") {
                const actions = element("div", null, "lab-batch-actions");
                actions.append(actionButton(batch.status === "running" ? "暂停" : batch.status === "paused" ? "启动" : "恢复", () => changeState(batch.id, batch.status)));
                row.append(actions);
            }
            $("labBatchList").append(row);
        });
    }
    function updatePreview() {
        const combo = $("labMetric").value === "combo";
        $("labCandidatePreview").textContent = (combo ? 39 : 25) + " 个条件 × 2 套结算 = " + (combo ? 78 : 50) + " 个实验版本";
        const historical = $("labBatchForm").elements.mode.value === "backtest";
        $("labDates").hidden = !historical;
        $("labStartDate").required = historical; $("labEndDate").required = historical;
        $("labModeHelp").textContent = historical ? "按现存历史记录重放，与前向模拟分别统计。" : "从启动后的可用信号开始观察，等待条件及开奖。";
    }
    function clearFieldErrors() {
        $("labBatchForm").querySelectorAll("[aria-invalid]").forEach(input => input.removeAttribute("aria-invalid"));
        ["labName", "labPredictor", "labStake", "labStartDate", "labEndDate"].forEach(id => {$(id + "Error").textContent = "";});
    }
    function payloadFromForm() {
        clearFieldErrors();
        const fields = $("labBatchForm").elements;
        const payload = {name:fields.name.value.trim(), predictor_id:Number(fields.predictor_id.value), metric:fields.metric.value,
            mode:fields.mode.value, stake_amount:Number(fields.stake_amount.value), template_pack:"basic_v1"};
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
        return payload;
    }
    function closeDetail(restoreFocus) {
        state.detailRevision++; state.detailId = null; $("labExperimentDetail").hidden = true;
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
        const skipCounts = Object.entries(batch.skip_counts || {}).filter(([, count]) => count > 0).map(([reason, count]) => (reasonNames[reason] || reason) + " " + count);
        $("labSkipCounts").textContent = "未执行记录：" + skipCounts.join("；"); $("labSkipCounts").hidden = !skipCounts.length;
        $("labBatchStateButton").hidden = batch.status === "completed";
        $("labBatchStateButton").textContent = batch.status === "running" ? "暂停模拟" : batch.status === "paused" ? "启动模拟" : "恢复模拟";
        const settled = state.experiments.reduce((sum, item) => sum + item.settled_count, 0);
        $("labResultEmpty").hidden = settled > 0;
        $("labResultEmpty").textContent = "尚无已结算模拟结果。" + (batch.status === "paused" ? "启动批次后开始观察。" : (phaseNames[batch.phase] || "等待可用数据") + "，条件不足时不会按零命中率处理。");
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
            row.append(element("td", conditionName(item.condition), "lab-condition"), element("td", item.settlement_rule_name),
                element("td", item.settled_count, "lab-number"), element("td", money(item.net_profit, true), "lab-number " + profitClass(item.net_profit)),
                element("td", percent(item.roi_percent), "lab-number"), element("td", money(item.max_drawdown), "lab-number"),
                element("td", item.max_loss_streak, "lab-number"), element("td", resultCounts(item), "lab-number"), element("td", item.sample_label, "lab-sample"));
            const cell = element("td"); const button = actionButton("详情", () => openDetail(item, button));
            button.setAttribute("aria-expanded", "false"); button.setAttribute("aria-controls", "labExperimentDetail"); button.dataset.experimentId = String(item.id); cell.append(button); row.append(cell); $("labResultsBody").append(row);
        });
    }
    function denyAccess() {
        state.accessDenied = true; state.selectedId = null; state.batch = null; state.experiments = []; state.overview = null;
        $("labBatchList").replaceChildren(element("p", "请使用有权限的管理员账号登录后刷新。", "lab-empty"));
        $("labWorkerSummary").replaceChildren(); $("labWorkerMessage").textContent = ""; $("labBatchCount").textContent = "—";
        closeDetail(false); renderResults();
    }
    async function selectBatch(id) {
        if (state.mutating || state.loading || state.accessDenied) return;
        state.selectedId = id; state.resultPage = 0; closeDetail(false); $("labSort").value = "paired";
        await refresh(false);
    }
    async function refresh(includeOptions) {
        const revision = ++state.readRevision;
        state.loading = true; updateControls();
        try {
            const calls = [request(base)];
            if (includeOptions !== false || !state.options) calls.push(request(base + "/options"));
            const [overview, options] = await Promise.all(calls);
            if (revision !== state.readRevision) return;
            if (!Array.isArray(overview.batches)) throw responseError("批次数据不完整，请刷新后重试", 0, "invalid_response");
            if (options) {state.options = validateOptions(options); renderOptions();}
            state.accessDenied = false; state.overview = overview; state.readAt = new Date().toISOString();
            if (!overview.batches.some(batch => batch.id === state.selectedId)) state.selectedId = overview.batches.length ? overview.batches[0].id : null;
            renderOverview();
            if (state.selectedId !== null) {
                const detail = await request(base + "/batches/" + state.selectedId);
                if (revision !== state.readRevision) return;
                if (!detail.batch || !Array.isArray(detail.experiments)) throw responseError("实验结果不完整，请刷新后重试", 0, "invalid_response");
                closeDetail(false); state.batch = detail.batch; state.experiments = detail.experiments;
            } else {state.batch = null; state.experiments = [];}
            renderResults(); feedback("statusMessage", "", false);
        } catch (error) {
            if (revision !== state.readRevision) return;
            if (error.status === 401 || error.status === 403) denyAccess();
            feedback("statusMessage", error.message + (state.overview ? "。更新失败，以下为上次读取结果（" + time(state.readAt) + "）。" : "。可点击刷新重试。"), true);
        } finally {if (revision === state.readRevision) {state.loading = false; updateControls();}}
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
    async function changeState(id, status) {
        if (locked()) return;
        state.mutating = true; updateControls();
        try {
            await request(base + "/batches/" + id + "/state", "POST", {state:status === "running" ? "paused" : "running"});
            await refresh(false); feedback("statusMessage", status === "running" ? "已暂停新模拟，已有待结算记录仍会完成结算。" : "已启动模拟，可稍后刷新查看进度与结果。", false);
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
    async function openDetail(item, button) {
        if (state.detailId === item.id && !$("labExperimentDetail").hidden) {closeDetail(true); return;}
        closeDetail(false); state.detailId = item.id; state.detailTrigger = button;
        const revision = ++state.detailRevision, batchId = state.selectedId;
        button.setAttribute("aria-expanded", "true"); $("labExperimentDetail").hidden = false;
        $("labDetailTitle").textContent = conditionName(item.condition) + " · " + item.settlement_rule_name;
        $("labDetailMeta").textContent = "基础条件 v1 · 固定金额单期模拟 · 北京时间";
        $("labDetailStats").replaceChildren(); $("labDailyBody").replaceChildren(); $("labRecentBody").replaceChildren();
        feedback("labDetailFeedback", "读取明细…", false); $("labDetailTitle").focus();
        try {
            const result = await request(base + "/batches/" + batchId + "/experiments/" + item.id);
            if (revision !== state.detailRevision || batchId !== state.selectedId) return;
            if (!result.experiment || !Array.isArray(result.daily) || !Array.isArray(result.recent)) throw responseError("明细数据不完整，请关闭后重试", 0, "invalid_response");
            const exp = result.experiment;
            putSummary($("labDetailStats"), [["累计投注", money(exp.stake_total)], ["累计净收益", money(exp.net_profit, true)], ["已结算期数", exp.settled_count], ["触发 / 跳过", exp.trigger_count + " / " + exp.skipped_count]]);
            tableRows($("labDailyBody"), result.daily, [d=>d.date, d=>d.settled_count, resultCounts, d=>money(d.stake_total), d=>money(d.net_profit,true)], "暂无每日结算记录。");
            tableRows($("labRecentBody"), result.recent, [d=>d.issue_no, d=>time(d.decided_at), d=>d.bet_value, d=>money(d.stake_amount),
                d=>({pending:"待结算",settled:"已结算",skipped:"已跳过"}[d.status] || d.status),
                d=>({hit:"实赔",refund:"退本",miss:"全亏"}[d.result_type] || "—"), d=>money(d.net_profit,true),
                d=>d.refund_reason || reasonNames[d.skip_reason] || d.skip_reason || "—"], "暂无逐期明细。");
            feedback("labDetailFeedback", "", false);
        } catch (error) {if (revision === state.detailRevision) {if (error.status === 401 || error.status === 403) denyAccess(); feedback("labDetailFeedback", error.message + "。关闭后可重新查看。", true);}}
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
    $("labBatchForm").addEventListener("submit", createBatch);
    $("labBatchForm").addEventListener("change", updatePreview);
    $("labRefreshButton").addEventListener("click", () => refresh(true));
    $("labBatchStateButton").addEventListener("click", () => state.batch && changeState(state.batch.id, state.batch.status));
    $("labSort").addEventListener("change", () => {state.resultPage = 0; closeDetail(false); renderResults(); updateControls();});
    $("labResultPrevious").addEventListener("click", () => {if (state.resultPage > 0) {state.resultPage--; closeDetail(false); renderResults(); updateControls();}});
    $("labResultNext").addEventListener("click", () => {if ((state.resultPage + 1) * pageSize < state.experiments.length) {state.resultPage++; closeDetail(false); renderResults(); updateControls();}});
    $("labCloseDetailButton").addEventListener("click", () => closeDetail(true));
    $("labExportButton").addEventListener("click", exportCsv);
    document.addEventListener("platform-auth:changed", () => {state.readRevision++; state.detailRevision++; denyAccess(); state.options = null; refresh(true);});
    updatePreview(); updateControls(); refresh(true);
}());
