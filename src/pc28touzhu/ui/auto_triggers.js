(function () {
    const state = {
        user: null,
        rules: [],
        events: [],
        subscriptions: [],
        sources: [],
        editingRuleId: "",
        eventLimit: 30,
        eventStatusFilter: "all",
    };

    const metricOptions = [
        ["big_small", "大小"],
        ["odd_even", "单双"],
        ["combo", "组合"],
    ];
    const conditionTypeOptions = [
        ["hit_rate", "命中率"],
        ["miss_streak", "当前连挂"],
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

    function subscriptionStatusText(status) {
        const labels = {
            active: "启用",
            standby: "待命触发",
            inactive: "停用",
            archived: "归档",
        };
        return labels[status] || status || "--";
    }

    function isTriggerableSubscription(item) {
        const status = String(item && item.status || "");
        return status === "active" || status === "standby";
    }

    function subscriptionLabel(item) {
        const source = sourceById(item.source_id);
        const name = source ? source.name : ("来源 #" + String(item.source_id));
        return "#" + String(item.id) + " " + name + " / " + subscriptionStatusText(item.status);
    }

    function renderSummary() {
        const activeRules = state.rules.filter(function (item) { return item.status === "active"; }).length;
        const triggerableSubscriptions = state.subscriptions.filter(isTriggerableSubscription).length;
        $("summaryGrid").innerHTML = [
            ["启用规则", activeRules],
            ["可触发跟单", triggerableSubscriptions],
            ["加载记录", state.events.length],
        ].map(function (item) {
            return '<article class="summary-card"><span>' + escapeHtml(item[0]) + '</span><strong>' + escapeHtml(item[1]) + '</strong></article>';
        }).join("");
    }

    function statusPill(status) {
        const labels = {
            active: "启用",
            standby: "待命触发",
            inactive: "停用",
            archived: "归档",
            triggered: "已触发",
            skipped: "跳过",
            failed: "失败",
        };
        return '<span class="pill ' + escapeHtml(status) + '">' + escapeHtml(labels[status] || status || "--") + '</span>';
    }

    function eventReasonText(event) {
        const matchedText = (event.matched_conditions || []).map(conditionText).join("；");
        if (event.reason === "multiple_metrics_matched") {
            return "当前方案多指标同时命中，已仅跳过该方案" + (matchedText ? "：" + matchedText : "");
        }
        return matchedText || event.reason || "--";
    }

    function conditionText(condition) {
        const type = condition.type || "hit_rate";
        const metric = metricOptions.find(function (item) { return item[0] === condition.metric; });
        const operator = operatorOptions.find(function (item) { return item[0] === condition.operator; });
        if (type === "miss_streak") {
            return (metric ? metric[1] : condition.metric) + "当前连挂" + (operator ? operator[1] : condition.operator) + String(condition.threshold) + "期";
        }
        return (metric ? metric[1] : condition.metric) + "近100期命中率" + (operator ? operator[1] : condition.operator) + String(condition.threshold) + "%";
    }

    function actionText(action) {
        const payload = action || {};
        const guardText = payload.skip_multiple_metrics_matched ? "；单个方案多指标同时命中时仅跳过该方案" : "";
        if (payload.play_filter_action === "matched_metric") {
            return "命中后切换到首个命中条件玩法" + guardText;
        }
        if (payload.play_filter_action === "fixed_metric") {
            const metric = metricOptions.find(function (item) { return item[0] === payload.fixed_metric; });
            return "命中后固定切换到" + (metric ? metric[1] : "指定玩法") + guardText;
        }
        return "命中后保持原玩法" + guardText;
    }

    function dailyRiskText(rule) {
        const payload = rule.daily_risk_control || {};
        if (!payload.enabled) {
            return "";
        }
        const parts = [];
        if (Number(payload.profit_target || 0) > 0) {
            parts.push("止盈 " + String(payload.profit_target));
        }
        if (Number(payload.loss_limit || 0) > 0) {
            parts.push("止损 " + String(payload.loss_limit));
        }
        return parts.length ? ("每日风控：" + parts.join(" / ")) : "";
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
            const guardGroups = (rule.guard_groups || []).map(function (group, index) {
                const groupConditions = (group.conditions || []).map(conditionText).join("；") || "--";
                return "条件区" + String(index + 1) + "：" + groupConditions;
            }).join(" / ");
            return '' +
                '<article class="rule-card' + selected + '">' +
                    '<div class="rule-card-head"><div><strong>' + escapeHtml(rule.name) + '</strong><p class="meta-line">' + escapeHtml(scope) + '</p></div>' + statusPill(rule.status) + '</div>' +
                    '<p class="meta-line">开始条件：' + escapeHtml(conditions) + '</p>' +
                    (guardGroups ? '<p class="meta-line">同时达成：' + escapeHtml(guardGroups) + '</p>' : '') +
                    '<p class="meta-line">' + escapeHtml(actionText(rule.action)) + '</p>' +
                    (dailyRiskText(rule) ? '<p class="meta-line">' + escapeHtml(dailyRiskText(rule)) + '</p>' : '') +
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
        const summary = $("eventSummary");
        const statusCounts = state.events.reduce(function (counts, event) {
            const key = String(event.status || "unknown");
            counts[key] = (counts[key] || 0) + 1;
            return counts;
        }, {});
        summary.innerHTML = [
            ["已触发", statusCounts.triggered || 0, "triggered"],
            ["失败", statusCounts.failed || 0, "failed"],
            ["跳过", statusCounts.skipped || 0, "skipped"],
            ["已加载", state.events.length, "loaded"],
        ].map(function (item) {
            return '<span class="event-summary-pill ' + escapeHtml(item[2]) + '">' + escapeHtml(item[0]) + ' <strong>' + escapeHtml(item[1]) + '</strong></span>';
        }).join("");
        if (!state.events.length) {
            list.innerHTML = '<div class="empty-state">' + (state.eventStatusFilter === "all" ? "暂无触发记录。" : "当前筛选条件下没有触发记录。") + '</div>';
            return;
        }
        list.innerHTML = state.events.map(function (event) {
            const matched = eventReasonText(event);
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
        $("subscriptionSelect").innerHTML = state.subscriptions.filter(function (item) {
            return isTriggerableSubscription(item) || selected.has(String(item.id));
        }).map(function (item) {
            return '<option value="' + escapeHtml(item.id) + '"' + (selected.has(String(item.id)) ? " selected" : "") + '>' + escapeHtml(subscriptionLabel(item)) + '</option>';
        }).join("");
    }

    function conditionRowHtml(condition) {
        const item = condition || {type: "hit_rate", metric: "big_small", operator: "lt", threshold: 40, min_sample_count: 100};
        const conditionType = item.type || "hit_rate";
        const thresholdLabel = conditionType === "miss_streak" ? "连挂期数" : "阈值 %";
        function options(optionsList, current) {
            return optionsList.map(function (option) {
                return '<option value="' + escapeHtml(option[0]) + '"' + (option[0] === current ? " selected" : "") + '>' + escapeHtml(option[1]) + '</option>';
            }).join("");
        }
        return '' +
            '<div class="condition-row">' +
                '<label class="field"><span>条件</span><select class="text-input condition-type">' + options(conditionTypeOptions, conditionType) + '</select></label>' +
                '<label class="field"><span>玩法</span><select class="text-input condition-metric">' + options(metricOptions, item.metric) + '</select></label>' +
                '<label class="field"><span>比较</span><select class="text-input condition-operator">' + options(operatorOptions, item.operator) + '</select></label>' +
                '<label class="field condition-threshold-field"><span class="condition-threshold-label">' + escapeHtml(thresholdLabel) + '</span><input class="text-input condition-threshold" type="number" min="0" max="100" step="0.01" value="' + escapeHtml(item.threshold) + '"></label>' +
                '<label class="field condition-sample-field"><span>最少样本</span><input class="text-input condition-sample" type="number" min="1" step="1" value="' + escapeHtml(item.min_sample_count || 100) + '"></label>' +
                '<div class="condition-actions">' +
                    '<button class="ghost-btn move-condition-up-btn" type="button">上移</button>' +
                    '<button class="ghost-btn move-condition-down-btn" type="button">下移</button>' +
                    '<button class="ghost-btn remove-condition-btn" type="button">删除</button>' +
                '</div>' +
            '</div>';
    }

    function renderConditions(conditions) {
        $("conditionRows").innerHTML = (conditions && conditions.length ? conditions : [{metric: "big_small", operator: "lt", threshold: 40, min_sample_count: 100}]).map(conditionRowHtml).join("");
        $("conditionRows").querySelectorAll(".condition-row").forEach(updateConditionRowVisibility);
    }

    function guardGroupHtml(group, index) {
        const item = group || {};
        const title = item.name || ("条件区 " + String(index + 1));
        const conditions = item.conditions && item.conditions.length ? item.conditions : [{type: "miss_streak", metric: "big_small", operator: "gte", threshold: 5, min_sample_count: 100}];
        return '' +
            '<article class="guard-group-card" data-guard-group-index="' + escapeHtml(index) + '">' +
                '<div class="section-head guard-group-head">' +
                    '<div>' +
                        '<p class="panel-kicker">' + escapeHtml(title) + '</p>' +
                        '<h3>本区任一条件命中即通过</h3>' +
                    '</div>' +
                    '<div class="condition-actions">' +
                        '<button class="secondary-btn add-guard-condition-btn" type="button">添加条件</button>' +
                        '<button class="ghost-btn remove-guard-group-btn" type="button">删除条件区</button>' +
                    '</div>' +
                '</div>' +
                '<div class="condition-rows guard-condition-rows">' + conditions.map(conditionRowHtml).join("") + '</div>' +
            '</article>';
    }

    function renderGuardGroups(groups) {
        $("guardGroupRows").innerHTML = (groups || []).map(guardGroupHtml).join("");
        $("guardGroupRows").querySelectorAll(".condition-row").forEach(updateConditionRowVisibility);
    }

    function updateConditionRowVisibility(row, preferDefaultOperator) {
        const type = row.querySelector(".condition-type").value || "hit_rate";
        const isMissStreak = type === "miss_streak";
        const threshold = row.querySelector(".condition-threshold");
        const operator = row.querySelector(".condition-operator");
        row.querySelector(".condition-threshold-label").textContent = isMissStreak ? "连挂期数" : "阈值 %";
        threshold.max = isMissStreak ? "" : "100";
        threshold.step = isMissStreak ? "1" : "0.01";
        row.querySelector(".condition-sample-field").hidden = isMissStreak;
        if (preferDefaultOperator && isMissStreak && operator.value === "lt") {
            operator.value = "gte";
        }
        if (preferDefaultOperator && isMissStreak) {
            threshold.value = "5";
        }
        if (isMissStreak && !Number(threshold.value || 0)) {
            threshold.value = "5";
        }
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
        form.elements.skip_multiple_metrics_matched.checked = Boolean(item.action && item.action.skip_multiple_metrics_matched);
        form.elements.play_filter_action.value = item.action && item.action.play_filter_action ? item.action.play_filter_action : "keep";
        form.elements.fixed_metric.value = item.action && item.action.fixed_metric ? item.action.fixed_metric : "big_small";
        const dailyRisk = item.daily_risk_control || {};
        form.elements.daily_risk_enabled.checked = Boolean(dailyRisk.enabled);
        form.elements.daily_profit_target.value = String(dailyRisk.profit_target || 0);
        form.elements.daily_loss_limit.value = String(dailyRisk.loss_limit || 0);
        form.elements.daily_cancel_pending_jobs.checked = dailyRisk.cancel_pending_jobs !== false;
        renderSubscriptionOptions(item.subscription_ids || []);
        renderConditions(item.conditions || []);
        renderGuardGroups(item.guard_groups || []);
        $("editorTitle").textContent = item.id ? "编辑触发规则" : "新建触发规则";
        $("cancelEditBtn").hidden = !item.id;
        updateScopeVisibility();
        updateActionVisibility();
        updateDailyRiskVisibility();
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

    function updateDailyRiskVisibility() {
        $("dailyRiskFields").hidden = !Boolean($("dailyRiskEnabledInput").checked);
    }

    function collectConditions(container) {
        return Array.from(container.querySelectorAll(":scope > .condition-row")).map(function (row) {
            return {
                type: row.querySelector(".condition-type").value || "hit_rate",
                metric: row.querySelector(".condition-metric").value,
                operator: row.querySelector(".condition-operator").value,
                threshold: Number(row.querySelector(".condition-threshold").value),
                min_sample_count: Number(row.querySelector(".condition-sample").value || 100),
            };
        });
    }

    function collectGuardGroups() {
        return Array.from(document.querySelectorAll(".guard-group-card")).map(function (group, index) {
            const rows = group.querySelector(".guard-condition-rows");
            return {
                name: "条件区 " + String(index + 1),
                conditions: rows ? collectConditions(rows) : [],
            };
        }).filter(function (group) {
            return group.conditions.length > 0;
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
            conditions: collectConditions($("conditionRows")),
            guard_groups: collectGuardGroups(),
            action: {
                dispatch_latest_signal: form.elements.dispatch_latest_signal.checked,
                play_filter_action: form.elements.play_filter_action.value,
                fixed_metric: form.elements.fixed_metric.value,
                skip_multiple_metrics_matched: form.elements.skip_multiple_metrics_matched.checked,
            },
            daily_risk_control: {
                enabled: form.elements.daily_risk_enabled.checked,
                profit_target: Number(form.elements.daily_profit_target.value || 0),
                loss_limit: Number(form.elements.daily_loss_limit.value || 0),
                timezone: "Asia/Shanghai",
                cancel_pending_jobs: form.elements.daily_cancel_pending_jobs.checked,
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
            request(
                "/api/platform/auto-trigger-events?limit=" + encodeURIComponent(String(state.eventLimit)) +
                (state.eventStatusFilter === "all" ? "" : ("&status=" + encodeURIComponent(state.eventStatusFilter)))
            ),
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
        $("dailyRiskEnabledInput").addEventListener("change", updateDailyRiskVisibility);
        $("refreshBtn").addEventListener("click", loadData);
        $("newRuleBtn").addEventListener("click", function () { resetForm(); });
        $("cancelEditBtn").addEventListener("click", function () { resetForm(); });
        $("eventStatusFilter").addEventListener("change", function (event) {
            state.eventStatusFilter = event.target.value || "all";
            loadData();
        });
        $("eventLimitSelect").addEventListener("change", async function (event) {
            state.eventLimit = Number(event.target.value || 30);
            await loadData();
        });
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
            updateConditionRowVisibility($("conditionRows").lastElementChild);
        });
        $("addGuardGroupBtn").addEventListener("click", function () {
            const index = document.querySelectorAll(".guard-group-card").length;
            $("guardGroupRows").insertAdjacentHTML("beforeend", guardGroupHtml(null, index));
            $("guardGroupRows").lastElementChild.querySelectorAll(".condition-row").forEach(updateConditionRowVisibility);
        });
        $("conditionRows").addEventListener("change", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.classList.contains("condition-type")) {
                const row = target.closest(".condition-row");
                if (row) {
                    updateConditionRowVisibility(row, true);
                }
            }
        });
        $("guardGroupRows").addEventListener("change", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.classList.contains("condition-type")) {
                const row = target.closest(".condition-row");
                if (row) {
                    updateConditionRowVisibility(row, true);
                }
            }
        });
        document.addEventListener("click", async function (event) {
            const target = event.target;
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (target.classList.contains("add-guard-condition-btn")) {
                const group = target.closest(".guard-group-card");
                const rows = group ? group.querySelector(".guard-condition-rows") : null;
                if (rows) {
                    rows.insertAdjacentHTML("beforeend", conditionRowHtml({type: "miss_streak", metric: "big_small", operator: "gte", threshold: 5, min_sample_count: 100}));
                    updateConditionRowVisibility(rows.lastElementChild);
                }
                return;
            }
            if (target.classList.contains("remove-guard-group-btn")) {
                const group = target.closest(".guard-group-card");
                if (group) {
                    group.remove();
                    renderGuardGroups(collectGuardGroups());
                }
                return;
            }
            if (target.classList.contains("remove-condition-btn")) {
                const rows = target.closest(".condition-rows");
                if (rows && rows.querySelectorAll(".condition-row").length > 1) {
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
