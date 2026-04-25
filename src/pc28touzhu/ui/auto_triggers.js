(function () {
    const state = {
        user: null,
        rules: [],
        events: [],
        subscriptions: [],
        sources: [],
        editingRuleId: "",
    };

    const metricOptions = [
        ["big_small", "大小"],
        ["odd_even", "单双"],
        ["combo", "组合"],
    ];
    const operatorOptions = [
        ["lt", "低于"],
        ["lte", "低于或等于"],
        ["gt", "高于"],
        ["gte", "高于或等于"],
    ];

    function $(id) {
        return document.getElementById(id);
    }

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    async function request(path, options) {
        const response = await fetch(path, {
            credentials: "same-origin",
            headers: {"Content-Type": "application/json"},
            ...(options || {}),
            body: options && options.body && typeof options.body !== "string" ? JSON.stringify(options.body) : (options || {}).body,
        });
        const data = await response.json().catch(function () { return {}; });
        if (!response.ok) {
            throw new Error(data.error || "请求失败");
        }
        return data;
    }

    function setStatus(message, isError) {
        const el = $("statusMessage");
        if (!el) {
            return;
        }
        el.textContent = message || "";
        el.classList.toggle("is-error", Boolean(isError));
    }

    function sourceById(sourceId) {
        return state.sources.find(function (item) { return Number(item.id) === Number(sourceId); }) || null;
    }

    function subscriptionLabel(item) {
        const source = sourceById(item.source_id);
        const name = source ? source.name : ("来源 #" + String(item.source_id));
        const status = item.status === "active" ? "启用" : item.status;
        return "#" + String(item.id) + " " + name + " / " + status;
    }

    function renderSummary() {
        const activeRules = state.rules.filter(function (item) { return item.status === "active"; }).length;
        const activeSubscriptions = state.subscriptions.filter(function (item) { return item.status === "active"; }).length;
        const triggered = state.events.filter(function (item) { return item.status === "triggered"; }).length;
        $("summaryGrid").innerHTML = [
            ["启用规则", activeRules],
            ["可用跟单", activeSubscriptions],
            ["最近触发", triggered],
        ].map(function (item) {
            return '<article class="summary-card"><span>' + escapeHtml(item[0]) + '</span><strong>' + escapeHtml(item[1]) + '</strong></article>';
        }).join("");
    }

    function statusPill(status) {
        const labels = {
            active: "启用",
            inactive: "停用",
            archived: "归档",
            triggered: "已触发",
            skipped: "跳过",
            failed: "失败",
        };
        return '<span class="pill ' + escapeHtml(status) + '">' + escapeHtml(labels[status] || status || "--") + '</span>';
    }

    function conditionText(condition) {
        const metric = metricOptions.find(function (item) { return item[0] === condition.metric; });
        const operator = operatorOptions.find(function (item) { return item[0] === condition.operator; });
        return (metric ? metric[1] : condition.metric) + "近100期命中率" + (operator ? operator[1] : condition.operator) + String(condition.threshold) + "%";
    }

    function actionText(action) {
        const payload = action || {};
        if (payload.play_filter_action === "matched_metric") {
            return "命中后切换到首个命中条件玩法";
        }
        if (payload.play_filter_action === "fixed_metric") {
            const metric = metricOptions.find(function (item) { return item[0] === payload.fixed_metric; });
            return "命中后固定切换到" + (metric ? metric[1] : "指定玩法");
        }
        return "命中后保持原玩法";
    }

    function renderRuleList() {
        const list = $("ruleList");
        if (!state.rules.length) {
            list.innerHTML = '<div class="empty-state">暂无规则。先选择一个已存在跟单方案，再配置命中率条件。</div>';
            return;
        }
        list.innerHTML = state.rules.map(function (rule) {
            const selected = String(rule.id) === String(state.editingRuleId) ? " is-active" : "";
            const scope = rule.scope_mode === "all_subscriptions" ? "全部跟单方案" : ("指定 " + String((rule.subscription_ids || []).length) + " 个方案");
            const conditions = (rule.conditions || []).map(conditionText).join("；") || "--";
            return '' +
                '<article class="rule-card' + selected + '">' +
                    '<div class="rule-card-head"><div><strong>' + escapeHtml(rule.name) + '</strong><p class="meta-line">' + escapeHtml(scope) + '</p></div>' + statusPill(rule.status) + '</div>' +
                    '<p class="meta-line">' + escapeHtml(conditions) + '</p>' +
                    '<p class="meta-line">' + escapeHtml(actionText(rule.action)) + '</p>' +
                    '<p class="meta-line">冷却 <span class="mono">' + escapeHtml(rule.cooldown_issues) + '</span> 期，上次触发 <span class="mono">' + escapeHtml(rule.last_triggered_issue_no || "--") + '</span></p>' +
                    '<div class="rule-actions">' +
                        '<button class="tiny-btn edit-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">编辑</button>' +
                        '<button class="tiny-btn run-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">检查</button>' +
                        '<button class="tiny-btn status-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '" data-status="' + (rule.status === "active" ? "inactive" : "active") + '">' + (rule.status === "active" ? "停用" : "启用") + '</button>' +
                        '<button class="tiny-btn status-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '" data-status="archived">归档</button>' +
                    '</div>' +
                '</article>';
        }).join("");
    }

    function renderEvents() {
        const list = $("eventList");
        if (!state.events.length) {
            list.innerHTML = '<div class="empty-state">暂无触发记录。</div>';
            return;
        }
        list.innerHTML = state.events.map(function (event) {
            const matched = (event.matched_conditions || []).map(conditionText).join("；") || event.reason || "--";
            return '' +
                '<article class="event-card">' +
                    '<div class="event-card-head"><div><strong>' + escapeHtml(event.rule_name || ("规则 #" + event.rule_id)) + '</strong><p class="meta-line">' + escapeHtml(event.source_name || ("来源 #" + event.source_id)) + ' / 跟单 #' + escapeHtml(event.subscription_id || "--") + '</p></div>' + statusPill(event.status) + '</div>' +
                    '<p class="meta-line">期号 <span class="mono">' + escapeHtml(event.latest_issue_no || "--") + '</span>，原因：' + escapeHtml(matched) + '</p>' +
                    '<p class="meta-line">' + escapeHtml(event.created_at || "") + '</p>' +
                '</article>';
        }).join("");
    }

    function renderSubscriptionOptions(selectedIds) {
        const selected = new Set((selectedIds || []).map(function (item) { return String(item); }));
        $("subscriptionSelect").innerHTML = state.subscriptions.map(function (item) {
            return '<option value="' + escapeHtml(item.id) + '"' + (selected.has(String(item.id)) ? " selected" : "") + '>' + escapeHtml(subscriptionLabel(item)) + '</option>';
        }).join("");
    }

    function conditionRowHtml(condition) {
        const item = condition || {metric: "big_small", operator: "lt", threshold: 40, min_sample_count: 100};
        function options(optionsList, current) {
            return optionsList.map(function (option) {
                return '<option value="' + escapeHtml(option[0]) + '"' + (option[0] === current ? " selected" : "") + '>' + escapeHtml(option[1]) + '</option>';
            }).join("");
        }
        return '' +
            '<div class="condition-row">' +
                '<label class="field"><span>玩法</span><select class="text-input condition-metric">' + options(metricOptions, item.metric) + '</select></label>' +
                '<label class="field"><span>比较</span><select class="text-input condition-operator">' + options(operatorOptions, item.operator) + '</select></label>' +
                '<label class="field"><span>阈值 %</span><input class="text-input condition-threshold" type="number" min="0" max="100" step="0.01" value="' + escapeHtml(item.threshold) + '"></label>' +
                '<label class="field"><span>最少样本</span><input class="text-input condition-sample" type="number" min="1" step="1" value="' + escapeHtml(item.min_sample_count || 100) + '"></label>' +
                '<div class="condition-actions">' +
                    '<button class="ghost-btn move-condition-up-btn" type="button">上移</button>' +
                    '<button class="ghost-btn move-condition-down-btn" type="button">下移</button>' +
                    '<button class="ghost-btn remove-condition-btn" type="button">删除</button>' +
                '</div>' +
            '</div>';
    }

    function renderConditions(conditions) {
        $("conditionRows").innerHTML = (conditions && conditions.length ? conditions : [{metric: "big_small", operator: "lt", threshold: 40, min_sample_count: 100}]).map(conditionRowHtml).join("");
    }

    function resetForm(rule) {
        const form = $("ruleForm");
        const item = rule || {};
        state.editingRuleId = item.id ? String(item.id) : "";
        form.elements.rule_id.value = state.editingRuleId;
        form.elements.name.value = item.name || "";
        form.elements.status.value = item.status || "active";
        form.elements.scope_mode.value = item.scope_mode || "selected_subscriptions";
        form.elements.cooldown_issues.value = String(item.cooldown_issues == null ? 10 : item.cooldown_issues);
        form.elements.dispatch_latest_signal.checked = item.action ? item.action.dispatch_latest_signal !== false : true;
        form.elements.play_filter_action.value = item.action && item.action.play_filter_action ? item.action.play_filter_action : "keep";
        form.elements.fixed_metric.value = item.action && item.action.fixed_metric ? item.action.fixed_metric : "big_small";
        renderSubscriptionOptions(item.subscription_ids || []);
        renderConditions(item.conditions || []);
        $("editorTitle").textContent = item.id ? "编辑触发规则" : "新建触发规则";
        $("cancelEditBtn").hidden = !item.id;
        updateScopeVisibility();
        updateActionVisibility();
        renderRuleList();
        setStatus("", false);
    }

    function updateScopeVisibility() {
        const isAll = $("scopeModeSelect").value === "all_subscriptions";
        $("subscriptionSelectField").hidden = isAll;
    }

    function updateActionVisibility() {
        $("fixedMetricField").hidden = $("playFilterActionSelect").value !== "fixed_metric";
    }

    function collectConditions() {
        return Array.from(document.querySelectorAll(".condition-row")).map(function (row) {
            return {
                metric: row.querySelector(".condition-metric").value,
                operator: row.querySelector(".condition-operator").value,
                threshold: Number(row.querySelector(".condition-threshold").value),
                min_sample_count: Number(row.querySelector(".condition-sample").value || 100),
            };
        });
    }

    function collectPayload() {
        const form = $("ruleForm");
        return {
            name: form.elements.name.value.trim(),
            status: form.elements.status.value,
            scope_mode: form.elements.scope_mode.value,
            subscription_ids: Array.from($("subscriptionSelect").selectedOptions).map(function (option) { return Number(option.value); }),
            cooldown_issues: Number(form.elements.cooldown_issues.value || 0),
            condition_mode: "any",
            conditions: collectConditions(),
            action: {
                dispatch_latest_signal: form.elements.dispatch_latest_signal.checked,
                play_filter_action: form.elements.play_filter_action.value,
                fixed_metric: form.elements.fixed_metric.value,
            },
        };
    }

    async function loadData() {
        const auth = await request("/api/auth/me").catch(function () { return {user: null}; });
        state.user = auth.user || null;
        document.documentElement.setAttribute("data-authenticated", state.user ? "true" : "false");
        if (window.PlatformAuthPanel && typeof window.PlatformAuthPanel.sync === "function") {
            window.PlatformAuthPanel.sync({user: state.user});
        }
        if (!state.user) {
            setStatus("请先登录后再配置自动触发规则。", true);
            renderSummary();
            return;
        }
        const payloads = await Promise.all([
            request("/api/platform/sources"),
            request("/api/platform/subscriptions"),
            request("/api/platform/auto-trigger-rules"),
            request("/api/platform/auto-trigger-events?limit=50"),
        ]);
        state.sources = payloads[0].items || [];
        state.subscriptions = payloads[1].items || [];
        state.rules = payloads[2].items || [];
        state.events = payloads[3].items || [];
        renderSummary();
        renderRuleList();
        renderEvents();
        if (!state.editingRuleId) {
            resetForm();
        } else {
            const current = state.rules.find(function (item) { return String(item.id) === state.editingRuleId; });
            resetForm(current);
        }
    }

    async function saveRule(event) {
        event.preventDefault();
        const form = $("ruleForm");
        const ruleId = String(form.elements.rule_id.value || "").trim();
        const button = $("saveRuleBtn");
        button.disabled = true;
        try {
            const payload = collectPayload();
            await request(ruleId ? ("/api/platform/auto-trigger-rules/" + ruleId) : "/api/platform/auto-trigger-rules", {
                method: "POST",
                body: payload,
            });
            setStatus("规则已保存。", false);
            state.editingRuleId = ruleId;
            await loadData();
        } catch (error) {
            setStatus(error.message, true);
        } finally {
            button.disabled = false;
        }
    }

    async function runOnce(ruleId) {
        const body = ruleId ? {rule_id: Number(ruleId)} : {};
        await request("/api/platform/auto-trigger-rules/run-once", {method: "POST", body: body});
        await loadData();
    }

    function attachEvents() {
        $("ruleForm").addEventListener("submit", saveRule);
        $("scopeModeSelect").addEventListener("change", updateScopeVisibility);
        $("playFilterActionSelect").addEventListener("change", updateActionVisibility);
        $("refreshBtn").addEventListener("click", loadData);
        $("newRuleBtn").addEventListener("click", function () { resetForm(); });
        $("cancelEditBtn").addEventListener("click", function () { resetForm(); });
        $("runOnceBtn").addEventListener("click", async function () {
            try {
                await runOnce();
                setStatus("检查完成。", false);
            } catch (error) {
                setStatus(error.message, true);
            }
        });
        $("addConditionBtn").addEventListener("click", function () {
            $("conditionRows").insertAdjacentHTML("beforeend", conditionRowHtml());
        });
        document.addEventListener("click", async function (event) {
            const target = event.target;
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (target.classList.contains("remove-condition-btn")) {
                const rows = document.querySelectorAll(".condition-row");
                if (rows.length > 1) {
                    target.closest(".condition-row").remove();
                }
                return;
            }
            if (target.classList.contains("move-condition-up-btn")) {
                const row = target.closest(".condition-row");
                if (row && row.previousElementSibling) {
                    row.parentElement.insertBefore(row, row.previousElementSibling);
                }
                return;
            }
            if (target.classList.contains("move-condition-down-btn")) {
                const row = target.closest(".condition-row");
                if (row && row.nextElementSibling) {
                    row.parentElement.insertBefore(row.nextElementSibling, row);
                }
                return;
            }
            if (target.classList.contains("edit-rule-btn")) {
                const rule = state.rules.find(function (item) { return String(item.id) === String(target.dataset.id); });
                resetForm(rule);
                window.scrollTo({top: 0, behavior: "smooth"});
                return;
            }
            if (target.classList.contains("run-rule-btn")) {
                try {
                    target.setAttribute("disabled", "disabled");
                    await runOnce(target.dataset.id);
                    setStatus("检查完成。", false);
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    target.removeAttribute("disabled");
                }
                return;
            }
            if (target.classList.contains("status-rule-btn")) {
                try {
                    await request("/api/platform/auto-trigger-rules/" + target.dataset.id + "/status", {
                        method: "POST",
                        body: {status: target.dataset.status},
                    });
                    await loadData();
                } catch (error) {
                    setStatus(error.message, true);
                }
            }
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
            attachEvents();
            loadData();
        });
    } else {
        attachEvents();
        loadData();
    }
})();
