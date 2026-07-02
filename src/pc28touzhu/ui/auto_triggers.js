(function () {
    const state = {
        user: null,
        rules: [],
        events: [],
        subscriptions: [],
        sources: [],
        deliveryTargets: [],
        templates: [],
        editingRuleId: "",
        statDate: "",
        eventLimit: 30,
        eventStatusFilter: "all",
    };
    let currentUserLoadToken = 0;

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
    const hitRateWindowOptions = [
        [20, "近20期"],
        [50, "近50期"],
        [100, "近100期"],
    ];
    const settlementRuleOptions = [
        ["pc28_netdisk_regular", "PC28 网盘常规"],
        ["pc28_netdisk_abc", "PC28 网盘 ABC"],
        ["pc28_high_regular", "PC28 高赔常规"],
        ["pc28_high_abc", "PC28 高赔 ABC"],
    ];

    function $(id) {
        return document.getElementById(id);
    }

    function todayDateString() {
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, "0");
        const day = String(now.getDate()).padStart(2, "0");
        return year + "-" + month + "-" + day;
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

    function setCurrentUser(user) {
        state.user = user || null;
        document.documentElement.setAttribute("data-authenticated", state.user ? "true" : "false");
        if (window.PlatformAuthPanel && typeof window.PlatformAuthPanel.sync === "function") {
            window.PlatformAuthPanel.sync({user: state.user});
        }
        const authForm = document.getElementById("authForm");
        const authenticatedPanel = document.getElementById("authenticatedAccountPanel");
        const usernameEl = document.getElementById("currentUsername");
        const metaEl = document.getElementById("currentUserMeta");
        const accountButton = document.getElementById("accountMenuBtn");
        const isAuthenticated = Boolean(state.user);
        if (usernameEl instanceof HTMLElement) {
            usernameEl.textContent = isAuthenticated ? String(state.user.username || "未命名用户") : "未登录";
        }
        if (metaEl instanceof HTMLElement) {
            metaEl.textContent = isAuthenticated
                ? ((state.user.email || "--") + " · " + (state.user.role || "user") + " · " + (state.user.status || "active"))
                : "请先注册或登录";
        }
        if (accountButton instanceof HTMLElement) {
            accountButton.textContent = isAuthenticated ? String(state.user.username || "未命名用户") : "登录";
        }
        if (authForm instanceof HTMLElement) {
            if (isAuthenticated) {
                authForm.hidden = true;
                authForm.style.setProperty("display", "none", "important");
            } else {
                authForm.hidden = false;
                authForm.style.removeProperty("display");
            }
        }
        if (authenticatedPanel instanceof HTMLElement) {
            if (isAuthenticated) {
                authenticatedPanel.hidden = false;
                authenticatedPanel.style.removeProperty("display");
            } else {
                authenticatedPanel.hidden = true;
                authenticatedPanel.style.setProperty("display", "none", "important");
            }
        }
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

    function targetLabel(item) {
        const name = item.target_name || item.target_key || ("投递群组 #" + String(item.id));
        const account = item.telegram_account_id ? (" / 账号 #" + String(item.telegram_account_id)) : "";
        return "#" + String(item.id) + " " + name + account;
    }

    function templateLabel(item) {
        return "#" + String(item.id) + " " + (item.name || item.template_text || "消息模板");
    }

    function renderSummary() {
        const activeRules = state.rules.filter(function (item) { return item.status === "active"; }).length;
        const triggerableSubscriptions = state.subscriptions.filter(isTriggerableSubscription).length;
        $("summaryGrid").innerHTML = [
            ["启用规则", activeRules],
            ["可触发跟单", triggerableSubscriptions],
            ["统计日期", state.statDate || "--"],
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

    function subscriptionStatusText(status) {
        const labels = {
            active: "启用",
            standby: "待命触发",
            inactive: "停用",
            archived: "归档",
        };
        return labels[status] || status || "--";
    }

    function eventReasonText(event) {
        const matchedText = (event.matched_conditions || []).map(conditionText).join("；");
        if (event.reason === "multiple_metrics_matched") {
            return "当前方案多指标同时命中，已仅跳过该方案" + (matchedText ? "：" + matchedText : "");
        }
        if (event.reason === "subscription_not_ready_for_restart") {
            const restartState = (event.snapshot && event.snapshot.restart_state) || {};
            const statDate = event.snapshot && event.snapshot.stat_date ? String(event.snapshot.stat_date) : "";
            const prefix = statDate ? ("统计日 " + statDate + "：") : "";
            const thresholdStatus = String(restartState.threshold_status || "").trim();
            if (thresholdStatus === "profit_target_hit") {
                return prefix + "当前方案已触发止盈，但未处于待命触发，已跳过自动开始新一轮。";
            }
            if (thresholdStatus === "loss_limit_hit") {
                return prefix + "当前方案已触发止损，但未处于待命触发，已跳过自动开始新一轮。";
            }
            return prefix + "当前方案状态为" + subscriptionStatusText(String(restartState.subscription_status || "")) + "，且未触发止盈/止损重开条件，已跳过自动开始新一轮。";
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
        const windowSize = Number(condition.window_size || String(condition.window || "").replace("recent_", "") || 100);
        const sampleText = condition.min_sample_count ? "（样本>=" + String(condition.min_sample_count) + "）" : "";
        return (metric ? metric[1] : condition.metric) + "近" + String(windowSize || 100) + "期命中率" + (operator ? operator[1] : condition.operator) + String(condition.threshold) + "%" + sampleText;
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

    function formatMoney(value, withSign) {
        const amount = Number(value || 0);
        const text = amount.toFixed(2);
        if (!withSign) {
            return text;
        }
        return amount > 0 ? ("+" + text) : text;
    }

    function ruleDailyStat(rule) {
        return rule && rule.daily_stat && typeof rule.daily_stat === "object" ? rule.daily_stat : null;
    }

    function dailyProfitText(rule) {
        const stat = ruleDailyStat(rule);
        if (!stat) {
            return "当日盈亏：--";
        }
        return String(stat.stat_date || "--") + "：净盈亏 " + formatMoney(stat.net_profit, true) +
            "（赢 " + formatMoney(stat.profit_amount, false) + " / 亏 " + formatMoney(stat.loss_amount, false) + "）";
    }

    function dailySettlementText(rule) {
        const stat = ruleDailyStat(rule);
        if (!stat) {
            return "";
        }
        return "已结算 " + String(stat.settled_event_count || 0) + " 单，命中 " + String(stat.hit_count || 0) +
            " / 未中 " + String(stat.miss_count || 0) + " / 回本 " + String(stat.refund_count || 0);
    }

    function dailyStatusText(rule) {
        const stat = ruleDailyStat(rule);
        if (!stat || stat.status !== "stopped") {
            return "";
        }
        return String(stat.stat_date || "--") + " 状态：已停止触发" + (stat.stopped_reason ? "（" + String(stat.stopped_reason) + "）" : "");
    }

    function isDailyRiskStopped(rule) {
        const stat = ruleDailyStat(rule);
        return Boolean(stat && stat.status === "stopped");
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
            const routeCount = (rule.routes || []).filter(function (route) { return route.status !== "archived"; }).length;
            const stoppedRouteCount = (rule.routes || []).filter(function (route) {
                return route.daily_stat && route.daily_stat.status === "stopped";
            }).length;
            return '' +
                '<article class="rule-card' + selected + '">' +
                    '<div class="rule-card-head"><div><strong>' + escapeHtml(rule.name) + '</strong><p class="meta-line">' + escapeHtml(scope) + '</p></div>' + statusPill(rule.status) + '</div>' +
                    '<p class="meta-line">开始条件：' + escapeHtml(conditions) + '</p>' +
                    (guardGroups ? '<p class="meta-line">同时达成：' + escapeHtml(guardGroups) + '</p>' : '') +
                    '<p class="meta-line">' + escapeHtml(actionText(rule.action)) + '</p>' +
                    (routeCount ? '<p class="meta-line">投注路由：' + escapeHtml(routeCount) + ' 条' + (stoppedRouteCount ? '，今日已停 ' + escapeHtml(stoppedRouteCount) + ' 条' : '') + '</p>' : '') +
                    (dailyRiskText(rule) ? '<p class="meta-line">' + escapeHtml(dailyRiskText(rule)) + '</p>' : '') +
                    '<p class="meta-line">' + escapeHtml(dailyProfitText(rule)) + '</p>' +
                    '<p class="meta-line">' + escapeHtml(dailySettlementText(rule)) + '</p>' +
                    (dailyStatusText(rule) ? '<p class="meta-line">' + escapeHtml(dailyStatusText(rule)) + '</p>' : '') +
                    '<p class="meta-line">冷却 <span class="mono">' + escapeHtml(rule.cooldown_issues) + '</span> 期，上次触发 <span class="mono">' + escapeHtml(rule.last_triggered_issue_no || "--") + '</span></p>' +
                    '<div class="rule-actions">' +
                        '<button class="tiny-btn edit-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">编辑</button>' +
                        '<button class="tiny-btn run-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">检查</button>' +
                        (isDailyRiskStopped(rule) ? '<button class="tiny-btn resume-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">继续今日触发</button>' : '') +
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

    function options(optionsList, current) {
        return optionsList.map(function (option) {
            return '<option value="' + escapeHtml(option[0]) + '"' + (String(option[0]) === String(current) ? " selected" : "") + '>' + escapeHtml(option[1]) + '</option>';
        }).join("");
    }

    function conditionRowHtml(condition) {
        const item = condition || {type: "hit_rate", metric: "big_small", operator: "lt", threshold: 40, min_sample_count: 100};
        const conditionType = item.type || "hit_rate";
        const thresholdLabel = conditionType === "miss_streak" ? "连挂期数" : "阈值 %";
        const windowSize = Number(item.window_size || String(item.window || "").replace("recent_", "") || 100);
        return '' +
            '<div class="condition-row">' +
                '<label class="field"><span>条件</span><select class="text-input condition-type">' + options(conditionTypeOptions, conditionType) + '</select></label>' +
                '<label class="field"><span>玩法</span><select class="text-input condition-metric">' + options(metricOptions, item.metric) + '</select></label>' +
                '<label class="field condition-window-field"><span>统计期数</span><select class="text-input condition-window">' + options(hitRateWindowOptions, windowSize) + '</select></label>' +
                '<label class="field"><span>比较</span><select class="text-input condition-operator">' + options(operatorOptions, item.operator) + '</select></label>' +
                '<label class="field condition-threshold-field"><span class="condition-threshold-label">' + escapeHtml(thresholdLabel) + '</span><input class="text-input condition-threshold" type="number" min="0" max="100" step="0.01" value="' + escapeHtml(item.threshold) + '"></label>' +
                '<label class="field condition-sample-field"><span>样本下限</span><input class="text-input condition-sample" type="number" min="1" step="1" value="' + escapeHtml(item.min_sample_count || 100) + '"></label>' +
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

    function selectOptions(items, selectedValue, labeler) {
        return (items || []).map(function (item) {
            return '<option value="' + escapeHtml(item.id) + '"' + (String(item.id) === String(selectedValue || "") ? " selected" : "") + '>' +
                escapeHtml(labeler(item)) +
                '</option>';
        }).join("");
    }

    function routeRowHtml(route, index) {
        const item = route || {};
        const routeRiskMode = item.route_risk_mode || (item.risk_mode === "override" ? "override" : (item.risk_mode === "disabled" ? "disabled" : "inherit_rule"));
        const subscriptionRiskMode = item.subscription_risk_mode || (item.risk_mode === "disabled" ? "disabled" : (item.risk_mode ? "inherit_subscription" : "inherit_rule"));
        const settlementMode = item.settlement_mode || "inherit";
        const stakingMode = item.staking_mode || "inherit";
        const playFilterMode = item.play_filter_mode || "inherit";
        const templateMode = item.template_mode || "target_default";
        const routeRiskControl = item.route_risk_control || item.risk_control || {};
        const subscriptionRiskControl = item.subscription_risk_control || {};
        const settlementPolicy = item.settlement_policy || {};
        const stakingPolicy = item.staking_policy || {};
        const playFilter = item.play_filter || {};
        return '' +
            '<article class="route-card" data-route-index="' + escapeHtml(index) + '">' +
                '<input type="hidden" class="route-id" value="' + escapeHtml(item.id || "") + '">' +
                '<div class="route-card-head">' +
                    '<strong>路由 ' + escapeHtml(index + 1) + '</strong>' +
                    '<button class="ghost-btn remove-route-btn" type="button">删除</button>' +
                '</div>' +
                '<div class="route-grid">' +
                    '<label class="field"><span>投递群组</span><select class="text-input route-target" required>' + selectOptions(state.deliveryTargets, item.delivery_target_id, targetLabel) + '</select></label>' +
                    '<label class="field"><span>名称</span><input class="text-input route-name" type="text" value="' + escapeHtml(item.name || "") + '" placeholder="例如：正式群"></label>' +
                    '<label class="field"><span>状态</span><select class="text-input route-status">' + options([["active", "启用"], ["inactive", "停用"], ["archived", "归档"]], item.status || "active") + '</select></label>' +
                    '<label class="field"><span>路由风控</span><select class="text-input route-risk-mode">' + options([["inherit_rule", "继承规则默认"], ["override", "单独设置"], ["disabled", "关闭"]], routeRiskMode) + '</select></label>' +
                    '<div class="route-risk-fields">' +
                        '<label class="field"><span>路由止盈</span><input class="text-input route-profit-target" type="number" min="0" step="0.01" value="' + escapeHtml(routeRiskControl.profit_target || 0) + '"></label>' +
                        '<label class="field"><span>路由止损</span><input class="text-input route-loss-limit" type="number" min="0" step="0.01" value="' + escapeHtml(routeRiskControl.loss_limit || 0) + '"></label>' +
                    '</div>' +
                    '<label class="field"><span>方案风控</span><select class="text-input route-subscription-risk-mode">' + options([["inherit_rule", "继承规则默认"], ["inherit_subscription", "继承跟单方案"], ["override", "单独设置"], ["disabled", "关闭"]], subscriptionRiskMode) + '</select></label>' +
                    '<div class="route-subscription-risk-fields">' +
                        '<label class="field"><span>方案止盈</span><input class="text-input route-subscription-profit-target" type="number" min="0" step="0.01" value="' + escapeHtml(subscriptionRiskControl.profit_target || 0) + '"></label>' +
                        '<label class="field"><span>方案止损</span><input class="text-input route-subscription-loss-limit" type="number" min="0" step="0.01" value="' + escapeHtml(subscriptionRiskControl.loss_limit || 0) + '"></label>' +
                    '</div>' +
                    '<label class="field"><span>结算</span><select class="text-input route-settlement-mode">' + options([["inherit", "继承跟单方案"], ["override", "单独设置"]], settlementMode) + '</select></label>' +
                    '<label class="field route-settlement-field"><span>结算规则</span><select class="text-input route-settlement-rule">' + options(settlementRuleOptions, settlementPolicy.settlement_rule_id || "pc28_netdisk_regular") + '</select></label>' +
                    '<label class="field"><span>投注</span><select class="text-input route-staking-mode">' + options([["inherit", "继承跟单方案"], ["override", "固定金额"]], stakingMode) + '</select></label>' +
                    '<label class="field route-staking-field"><span>固定金额</span><input class="text-input route-fixed-amount" type="number" min="0.01" step="0.01" value="' + escapeHtml(stakingPolicy.fixed_amount || 10) + '"></label>' +
                    '<label class="field"><span>玩法</span><select class="text-input route-play-filter-mode">' + options([["inherit", "继承规则动作"], ["keep", "保持跟单方案"], ["matched_metric", "命中玩法"], ["fixed_metric", "固定玩法"]], playFilterMode) + '</select></label>' +
                    '<label class="field route-fixed-metric-field"><span>固定玩法</span><select class="text-input route-fixed-metric">' + options(metricOptions, playFilter.fixed_metric || "big_small") + '</select></label>' +
                    '<label class="field"><span>模板</span><select class="text-input route-template-mode">' + options([["target_default", "群组默认"], ["override", "单独模板"]], templateMode) + '</select></label>' +
                    '<label class="field route-template-field"><span>消息模板</span><select class="text-input route-template">' + selectOptions(state.templates, item.template_id, templateLabel) + '</select></label>' +
                '</div>' +
            '</article>';
    }

    function updateRouteRowVisibility(card) {
        const riskMode = card.querySelector(".route-risk-mode").value;
        const subscriptionRiskMode = card.querySelector(".route-subscription-risk-mode").value;
        const settlementMode = card.querySelector(".route-settlement-mode").value;
        const stakingMode = card.querySelector(".route-staking-mode").value;
        const playFilterMode = card.querySelector(".route-play-filter-mode").value;
        const templateMode = card.querySelector(".route-template-mode").value;
        card.querySelector(".route-risk-fields").hidden = riskMode !== "override";
        card.querySelector(".route-subscription-risk-fields").hidden = subscriptionRiskMode !== "override";
        card.querySelector(".route-settlement-field").hidden = settlementMode !== "override";
        card.querySelector(".route-staking-field").hidden = stakingMode !== "override";
        card.querySelector(".route-fixed-metric-field").hidden = playFilterMode !== "fixed_metric";
        card.querySelector(".route-template-field").hidden = templateMode !== "override";
    }

    function renderRoutes(routes) {
        const items = routes || [];
        $("routeRows").innerHTML = items.map(routeRowHtml).join("");
        $("routeRows").querySelectorAll(".route-card").forEach(updateRouteRowVisibility);
    }

    function updateConditionRowVisibility(row, preferDefaultOperator) {
        const type = row.querySelector(".condition-type").value || "hit_rate";
        const isMissStreak = type === "miss_streak";
        const threshold = row.querySelector(".condition-threshold");
        const operator = row.querySelector(".condition-operator");
        const windowInput = row.querySelector(".condition-window");
        const sampleInput = row.querySelector(".condition-sample");
        row.querySelector(".condition-threshold-label").textContent = isMissStreak ? "连挂期数" : "阈值 %";
        threshold.max = isMissStreak ? "" : "100";
        threshold.step = isMissStreak ? "1" : "0.01";
        row.querySelector(".condition-window-field").hidden = isMissStreak;
        row.querySelector(".condition-sample-field").hidden = isMissStreak;
        if (!isMissStreak && Number(sampleInput.value || 0) > Number(windowInput.value || 100)) {
            sampleInput.value = String(windowInput.value || 100);
        }
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
        renderRoutes(item.routes || []);
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
            const type = row.querySelector(".condition-type").value || "hit_rate";
            const windowSize = Number(row.querySelector(".condition-window").value || 100);
            return {
                type: type,
                metric: row.querySelector(".condition-metric").value,
                operator: row.querySelector(".condition-operator").value,
                threshold: Number(row.querySelector(".condition-threshold").value),
                window_size: type === "hit_rate" ? windowSize : 0,
                window: type === "hit_rate" ? ("recent_" + String(windowSize)) : "current",
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

    function collectRoutes() {
        return Array.from(document.querySelectorAll(".route-card")).map(function (card, index) {
            const riskMode = card.querySelector(".route-risk-mode").value;
            const subscriptionRiskMode = card.querySelector(".route-subscription-risk-mode").value;
            const settlementMode = card.querySelector(".route-settlement-mode").value;
            const stakingMode = card.querySelector(".route-staking-mode").value;
            const playFilterMode = card.querySelector(".route-play-filter-mode").value;
            const templateMode = card.querySelector(".route-template-mode").value;
            const route = {
                id: card.querySelector(".route-id").value ? Number(card.querySelector(".route-id").value) : undefined,
                delivery_target_id: Number(card.querySelector(".route-target").value),
                name: card.querySelector(".route-name").value.trim(),
                status: card.querySelector(".route-status").value,
                sort_order: index,
                route_risk_mode: riskMode,
                subscription_risk_mode: subscriptionRiskMode,
                settlement_mode: settlementMode,
                staking_mode: stakingMode,
                play_filter_mode: playFilterMode,
                template_mode: templateMode,
            };
            if (riskMode === "override") {
                route.route_risk_control = {
                    enabled: true,
                    profit_target: Number(card.querySelector(".route-profit-target").value || 0),
                    loss_limit: Number(card.querySelector(".route-loss-limit").value || 0),
                    timezone: "Asia/Shanghai",
                    cancel_pending_jobs: true,
                };
            }
            if (subscriptionRiskMode === "override") {
                route.subscription_risk_control = {
                    enabled: true,
                    profit_target: Number(card.querySelector(".route-subscription-profit-target").value || 0),
                    loss_limit: Number(card.querySelector(".route-subscription-loss-limit").value || 0),
                    timezone: "Asia/Shanghai",
                    cancel_pending_jobs: false,
                };
            }
            if (settlementMode === "override") {
                route.settlement_policy = {
                    settlement_rule_id: card.querySelector(".route-settlement-rule").value,
                    fallback_profit_ratio: 1,
                };
            }
            if (stakingMode === "override") {
                route.staking_policy = {
                    mode: "fixed",
                    fixed_amount: Number(card.querySelector(".route-fixed-amount").value || 0),
                };
            }
            if (playFilterMode === "fixed_metric") {
                route.play_filter = {fixed_metric: card.querySelector(".route-fixed-metric").value};
            }
            if (templateMode === "override") {
                route.template_id = Number(card.querySelector(".route-template").value);
            }
            return route;
        }).filter(function (route) {
            return Number(route.delivery_target_id || 0) > 0;
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
            routes: collectRoutes(),
        };
    }

    async function loadData(options) {
        const normalized = options || {};
        if (!normalized.skipCurrentUserReload) {
            const token = ++currentUserLoadToken;
            const auth = await request("/api/auth/me").catch(function () { return {user: null}; });
            if (token !== currentUserLoadToken) {
                return;
            }
            setCurrentUser(auth.user || null);
        }
        if (!state.user) {
            setStatus("请先登录后再配置自动触发规则。", true);
            renderSummary();
            return;
        }
        const statDateQuery = state.statDate ? ("?stat_date=" + encodeURIComponent(state.statDate)) : "";
        const payloads = await Promise.all([
            request("/api/platform/sources"),
            request("/api/platform/subscriptions/summary"),
            request("/api/platform/delivery-targets"),
            request("/api/platform/message-templates"),
            request("/api/platform/auto-trigger-rules" + statDateQuery),
            request(
                "/api/platform/auto-trigger-events?limit=" + encodeURIComponent(String(state.eventLimit)) +
                (state.eventStatusFilter === "all" ? "" : ("&status=" + encodeURIComponent(state.eventStatusFilter)))
            ),
        ]);
        state.sources = payloads[0].items || [];
        state.subscriptions = payloads[1].items || [];
        state.deliveryTargets = (payloads[2].items || []).filter(function (item) { return item.status !== "archived"; });
        state.templates = (payloads[3].items || []).filter(function (item) { return item.status !== "archived"; });
        state.rules = payloads[4].items || [];
        state.events = payloads[5].items || [];
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

    function syncStatDateControls() {
        if ($("statDateInput")) {
            $("statDateInput").value = state.statDate || "";
        }
    }

    document.addEventListener("platform-auth:changed", async function (event) {
        const detail = event.detail || {};
        setCurrentUser(detail.user || null);
        await loadData({skipCurrentUserReload: true});
        if (detail.message) {
            setStatus(detail.message, false);
        }
    });

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
        $("statDateInput").addEventListener("change", async function (event) {
            state.statDate = event.target.value || todayDateString();
            syncStatDateControls();
            await loadData();
        });
        $("statDateTodayBtn").addEventListener("click", async function () {
            state.statDate = todayDateString();
            syncStatDateControls();
            await loadData();
        });
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
        $("addRouteBtn").addEventListener("click", function () {
            const index = document.querySelectorAll(".route-card").length;
            $("routeRows").insertAdjacentHTML("beforeend", routeRowHtml(null, index));
            updateRouteRowVisibility($("routeRows").lastElementChild);
        });
        $("routeRows").addEventListener("change", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.closest(".route-card")) {
                updateRouteRowVisibility(target.closest(".route-card"));
            }
        });
        $("conditionRows").addEventListener("change", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && (target.classList.contains("condition-type") || target.classList.contains("condition-window"))) {
                const row = target.closest(".condition-row");
                if (row) {
                    updateConditionRowVisibility(row, true);
                }
            }
        });
        $("guardGroupRows").addEventListener("change", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && (target.classList.contains("condition-type") || target.classList.contains("condition-window"))) {
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
            if (target.classList.contains("remove-route-btn")) {
                const route = target.closest(".route-card");
                if (route) {
                    route.remove();
                    renderRoutes(collectRoutes());
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
            if (target.classList.contains("resume-rule-btn")) {
                try {
                    target.setAttribute("disabled", "disabled");
                    await request("/api/platform/auto-trigger-rules/" + target.dataset.id + "/resume-daily-risk", {
                        method: "POST",
                        body: {stat_date: state.statDate || todayDateString()},
                    });
                    await loadData();
                    setStatus("已恢复今日自动触发。", false);
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

    state.statDate = todayDateString();

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
            syncStatDateControls();
            attachEvents();
            loadData();
        });
    } else {
        syncStatDateControls();
        attachEvents();
        loadData();
    }
})();
