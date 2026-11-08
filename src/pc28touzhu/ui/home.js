(function () {
    "use strict";
    const state = {user: null, data: {}, errors: {}, revision: 0, batchBusy: false};
    const $ = function (id) { return document.getElementById(id); };
    const endpoints = {
        sources: ["/api/platform/sources/summary", "方案来源"],
        accounts: ["/api/platform/telegram-accounts", "Telegram 账号"],
        targets: ["/api/platform/delivery-targets/summary", "投递群组"],
        templates: ["/api/platform/message-templates", "消息模板"],
        subscriptions: ["/api/platform/subscriptions/summary", "跟单方案"],
        rules: ["/api/platform/auto-trigger-rules", "自动触发"],
        jobs: ["/api/platform/execution-jobs?limit=20", "执行记录"],
        alerts: ["/api/platform/alerts?limit=6", "异常提醒"],
    };
    const labels = {active: "已启用", inactive: "已停用", standby: "等待触发", pending: "待执行", leased: "执行中", delivered: "已送达", failed: "失败", expired: "已过期", skipped: "已跳过", cancelled: "已取消", sending: "执行中"};
    function escape(value) { return String(value == null ? "" : value).replace(/[&<>"']/g, function (c) { return {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]; }); }
    function items(key) { return (state.data[key] || []).filter(function (item) { return item.status !== "archived"; }); }
    function authorized(account) { return account && (typeof account.is_authorized === "boolean" ? account.is_authorized : (account.auth_state || (account.meta || {}).auth_state) === "authorized"); }
    function status(message, error) { $("statusMessage").textContent = message; $("statusMessage").classList.toggle("is-error", Boolean(error)); }
    function badge(text, kind) { return '<span class="workbench-state ' + (kind || "") + '">' + escape(text) + '</span>'; }
    function link(href, title) { return '<a href="' + escape(href) + '">' + escape(title) + '</a>'; }
    function row(title, description, actions) { return '<article class="workbench-row"><div class="workbench-row-main"><strong>' + escape(title) + '</strong><p>' + escape(description) + '</p></div><div class="workbench-row-actions">' + actions + '</div></article>'; }
    function empty(text) { return '<p class="cell-muted">' + escape(text) + '</p>'; }
    async function request(path, options) {
        const response = await fetch(path, Object.assign({credentials: "same-origin", headers: {"Content-Type": "application/json"}}, options || {}));
        const payload = await response.json().catch(function () { return {}; });
        if (!response.ok) throw new Error(payload.error || "请求失败，请重试");
        return payload;
    }
    function setupSteps() {
        return [
            {title: "导入来源", ready: items("sources").length > 0, href: "/autobet/sources#sourceForm", keys: ["sources"]},
            {title: "授权账号", ready: items("accounts").some(authorized), href: "/autobet/accounts", keys: ["accounts"]},
            {title: "配置群组与模板", ready: items("targets").some(function (target) { return target.template_id && items("templates").some(function (template) { return Number(template.id) === Number(target.template_id); }); }), href: "/autobet/targets", keys: ["targets", "templates"]},
            {title: "建立跟单方案", ready: items("subscriptions").length > 0, href: "/autobet/subscriptions#subscriptionForm", keys: ["subscriptions"]},
        ];
    }
    function chainState(subscription) {
        const source = items("sources").find(function (item) { return Number(item.id) === Number(subscription.source_id); });
        const strategy = subscription.strategy_v2 || {};
        const selected = (strategy.dispatch || {}).delivery_target_ids || (subscription.strategy || {}).delivery_target_ids || [];
        const targets = items("targets").filter(function (item) { return !selected.length || selected.some(function (id) { return Number(id) === Number(item.id); }); });
        const warnings = [];
        if (!source || source.status !== "active") warnings.push("来源未启用或已失效");
        if (!targets.length) warnings.push("缺少投递群组");
        if (selected.some(function (id) { return !targets.some(function (item) { return Number(item.id) === Number(id); }); })) warnings.push("指定群组已失效");
        const readyTargets = targets.filter(function (target) {
            const account = items("accounts").find(function (item) { return Number(item.id) === Number(target.telegram_account_id); });
            const template = items("templates").find(function (item) { return Number(item.id) === Number(target.template_id); });
            return target.status === "active" && account && account.status === "active" && authorized(account) && template && template.status === "active" && ["success", "ok"].includes(target.last_test_status);
        });
        if (targets.length && readyTargets.length < targets.length) warnings.push("部分群组测试、账号或模板待检查");
        const dataIncomplete = ["sources", "targets", "accounts", "templates"].some(function (key) { return state.errors[key]; });
        const runtime = subscription.manual_cycle_state;
        let text = labels[subscription.status] || "配置待检查";
        if (subscription.status === "active") text = warnings.length ? "配置待检查" : "配置就绪";
        const autoRuns = items("rules").flatMap(function (rule) { return rule.cycle_runtime || []; }).filter(function (run) { return Number(run.subscription_id) === Number(subscription.id); });
        if (runtime && runtime.status === "running") text = "手动轮次进行中";
        if (runtime && runtime.pending_issue) text = "有待结算任务";
        if (autoRuns.some(function (run) { return run.status === "running"; })) text = "自动轮次进行中";
        else if (autoRuns.some(function (run) { return run.status === "continuation"; })) text = "等待自动续开";
        if (dataIncomplete) text = "状态未完整加载";
        return {source: source, targets: targets, warnings: warnings, text: text, incomplete: dataIncomplete};
    }
    function render() {
        const subs = items("subscriptions");
        const rules = items("rules");
        const jobs = state.data.jobs || [];
        const alerts = state.data.alerts || [];
        $("workbenchSubscriptions").textContent = state.errors.subscriptions ? "—" : String(subs.length);
        $("workbenchSubscriptionsMeta").textContent = state.errors.subscriptions ? "加载失败，请刷新" : (subs.filter(function (item) { return item.status === "active"; }).length + " 已启用 · " + subs.filter(function (item) { return item.status === "standby"; }).length + " 待触发");
        $("workbenchRules").textContent = state.errors.rules ? "—" : String(rules.filter(function (item) { return item.status === "active"; }).length);
        $("workbenchRulesMeta").textContent = state.errors.rules ? "加载失败，请刷新" : "已启用规则 / 共 " + rules.length + " 条";
        $("workbenchJobs").textContent = state.errors.jobs ? "—" : jobs.filter(function (job) { return job.status === "delivered"; }).length + " / " + jobs.length;
        $("workbenchJobsMeta").textContent = state.errors.jobs ? "加载失败，请刷新" : "最近 " + jobs.length + " 条任务中的送达数";
        $("workbenchAlerts").textContent = state.errors.alerts ? "—" : String(alerts.length);
        $("workbenchAlertsMeta").textContent = state.errors.alerts ? "加载失败，请刷新" : "最近提醒，最多显示 6 条";

        const steps = setupSteps();
        const completed = steps.filter(function (step) { return step.ready && !step.keys.some(function (key) { return state.errors[key]; }); }).length;
        $("guideProgress").textContent = completed + " / 4 已完成";
        $("setupGuide").open = completed < 4 || location.hash === "#setupGuide";
        $("guideSteps").innerHTML = steps.map(function (step) {
            const failed = step.keys.some(function (key) { return state.errors[key]; });
            return '<li><strong>' + escape(step.title) + '</strong><small>' + (failed ? "状态未加载" : step.ready ? "已配置" : "待配置") + '</small>' + link(step.href, step.ready ? "查看配置" : "去配置") + '</li>';
        }).join("");
        const attention = [];
        Object.keys(state.errors).forEach(function (key) { attention.push(row(endpoints[key][1] + "加载失败", state.errors[key], badge("请刷新", "is-warning"))); });
        const next = steps.find(function (step) { return !step.ready && !step.keys.some(function (key) { return state.errors[key]; }); });
        if (next) attention.push(row("下一步：" + next.title, "完成基础配置后，再检查每条方案的实际投递链路。", link(next.href, "继续配置")));
        const broken = subs.filter(function (item) { const chain = chainState(item); return !chain.incomplete && chain.warnings.length > 0; });
        if (broken.length) attention.push(row(broken.length + " 条方案的配置需要检查", "查看方案中的来源、群组、账号授权和模板绑定。", link("/autobet/subscriptions", "检查方案")));
        const failures = jobs.filter(function (item) { return ["failed", "expired"].includes(item.status); });
        if (failures.length) attention.push(row("最近任务中有 " + failures.length + " 条失败或过期", "打开执行记录查看具体原因和可用处理操作。", link("/records", "查看记录")));
        if (alerts.length) attention.push(row("有 " + alerts.length + " 条最近异常提醒", alerts[0].title || "查看异常详情。", link("/alerts", "处理异常")));
        $("workbenchAttention").innerHTML = attention.length ? attention.join("") : empty("当前配置和最近记录中没有发现待处理事项。");

        $("workbenchChains").innerHTML = state.errors.subscriptions ? empty("方案加载失败，请刷新重试。") : subs.slice(0, 5).map(function (sub) {
            const chain = chainState(sub);
            const title = sub.name || (chain.source && chain.source.name) || "未命名方案";
            const targetNames = chain.targets.map(function (item) { return item.target_name || item.target_key; }).filter(Boolean);
            const description = chain.incomplete ? "部分资源未加载，暂时无法判断链路。" : ((chain.source ? chain.source.name : "来源未配置") + " → " + (targetNames.join("、") || "群组未配置") + (chain.warnings.length ? "；" + chain.warnings.join("；") : ""));
            return row(title, description, badge(chain.text, chain.warnings.length && !chain.incomplete ? "is-warning" : "") + link("/autobet/subscriptions?subscription_id=" + encodeURIComponent(sub.id), "查看方案") + link("/auto-triggers?subscription_id=" + encodeURIComponent(sub.id), "关联规则"));
        }).join("") + (subs.length > 5 ? empty("显示前 5 条，共 " + subs.length + " 条方案。") : "") || empty("还没有跟单方案，可以先完成配置向导。");
        $("workbenchRecentJobs").innerHTML = state.errors.jobs ? empty("执行记录加载失败，请刷新重试。") : jobs.slice(0, 5).map(function (job) {
            const href = "/records" + (job.signal_id ? "?signal_id=" + encodeURIComponent(job.signal_id) : "");
            return row((job.issue_no || "—") + " · " + (job.bet_value || job.bet_type || "任务"), (job.target_name || job.target_key || "投递任务") + (job.last_error_message ? " · " + job.last_error_message : ""), badge(labels[job.status] || job.status, job.status === "failed" ? "is-warning" : "") + link(href, "详情"));
        }).join("") || empty("暂无执行记录。");
        $("workbenchRecentAlerts").innerHTML = state.errors.alerts ? empty("异常提醒加载失败，请刷新重试。") : alerts.slice(0, 4).map(function (alert) {
            return row(alert.title || "异常提醒", alert.message || "", link("/alerts", "详情"));
        }).join("") || empty("暂无异常提醒。");
        updateBatchButtons();
    }
    function updateBatchButtons() {
        const unavailable = !state.user || state.batchBusy || ["accounts", "targets", "subscriptions"].some(function (key) { return !state.data[key] || state.errors[key]; });
        $("batchPauseBtn").disabled = unavailable;
        $("batchResumeBtn").disabled = unavailable;
    }
    async function refresh(user) {
        const revision = ++state.revision;
        $("refreshHomeBtn").disabled = true;
        $("workbenchContent").setAttribute("aria-busy", "true");
        status("正在刷新工作台…", false);
        try {
            const currentUser = user === undefined ? (await request("/api/auth/me")).user : user;
            if (revision !== state.revision) return;
            if (!state.user || !currentUser || state.user.id !== currentUser.id) $("batchResult").textContent = "";
            state.user = currentUser || null;
            state.data = {}; state.errors = {};
            window.PlatformAuthPanel.sync({user: state.user, loading: false});
            $("workbenchGuest").hidden = Boolean(state.user);
            $("workbenchContent").hidden = !state.user;
            if (!state.user) { status("", false); return; }
            $("workbenchGreeting").textContent = "查看当前账户的跟单配置、最近执行和待处理事项。";
            const keys = Object.keys(endpoints);
            const results = await Promise.allSettled(keys.map(function (key) { return request(endpoints[key][0]); }));
            if (revision !== state.revision) return;
            keys.forEach(function (key, index) {
                const result = results[index];
                if (result.status === "fulfilled") state.data[key] = result.value.items || [];
                else state.errors[key] = result.reason && result.reason.message || "加载失败，请重试";
            });
            render();
            $("workbenchUpdatedAt").textContent = "更新于 " + new Date().toLocaleTimeString("zh-CN", {hour12: false});
            status(Object.keys(state.errors).length ? "部分数据未能加载，已保留可用结果，请刷新重试。" : "", Boolean(Object.keys(state.errors).length));
            const section = document.getElementById(location.hash.slice(1));
            if (section && !refresh.focused) { section.scrollIntoView(); refresh.focused = true; }
        } catch (error) {
            if (revision !== state.revision) return;
            state.user = null; state.data = {}; state.errors = {};
            $("batchResult").textContent = "";
            window.PlatformAuthPanel.sync({user: null, loading: false});
            $("workbenchContent").hidden = true;
            $("workbenchGuest").hidden = false;
            status(error.message, true);
        } finally {
            if (revision === state.revision) {
                $("refreshHomeBtn").disabled = false;
                $("workbenchContent").setAttribute("aria-busy", "false");
                updateBatchButtons();
            }
        }
    }
    async function batchChange(nextStatus) {
        if (!state.user || state.batchBusy) return;
        const changes = [];
        [["accounts", "telegram-accounts"], ["targets", "delivery-targets"], ["subscriptions", "subscriptions"]].forEach(function (entry) {
            items(entry[0]).filter(function (item) { return item.status !== nextStatus; }).forEach(function (item) {
                changes.push({path: "/api/platform/" + entry[1] + "/" + item.id + "/status", label: item.name || item.label || item.target_name || "配置 #" + item.id});
            });
        });
        if (!changes.length) { $("batchResult").textContent = "没有需要修改的配置。"; return; }
        const action = nextStatus === "active" ? "启用" : "停用";
        const detail = nextStatus === "active" ? "原本单独停用或待命的配置也会被启用。" : "不会撤回已发送的任务。";
        if (!window.confirm("将批量" + action + " " + changes.length + " 项账号、群组和跟单配置。" + detail + "确认继续？")) return;
        const userId = state.user.id;
        state.batchBusy = true; updateBatchButtons();
        try {
            const results = await Promise.allSettled(changes.map(function (item) { return request(item.path, {method: "POST", body: JSON.stringify({status: nextStatus})}); }));
            if (!state.user || state.user.id !== userId) return;
            const failures = results.map(function (result, index) { return result.status === "rejected" ? changes[index].label + "：" + result.reason.message : ""; }).filter(Boolean);
            $("batchResult").textContent = "已" + action + " " + (changes.length - failures.length) + " 项。" + (failures.length ? "失败 " + failures.length + " 项：" + failures.join("；") : "");
            await refresh(state.user);
        } finally { state.batchBusy = false; updateBatchButtons(); }
    }
    $("refreshHomeBtn").addEventListener("click", function () { refresh(); });
    $("batchPauseBtn").addEventListener("click", function () { batchChange("inactive"); });
    $("batchResumeBtn").addEventListener("click", function () { batchChange("active"); });
    document.addEventListener("platform-auth:changed", function (event) { $("batchResult").textContent = ""; refresh(event.detail.user || null); });
    refresh();
}());
