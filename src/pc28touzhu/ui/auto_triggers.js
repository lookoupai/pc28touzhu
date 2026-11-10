(function () {
    const state = {
        supportsExplicitStart: false,
        user: null,
        rules: [],
        events: [],
        subscriptions: [],
        sources: [],
        deliveryTargets: [],
        templates: [],
        editingRuleId: new URLSearchParams(window.location.search).get("rule_id") || "",
        statDate: "",
        eventLimit: 30,
        eventStatusFilter: "all",
        linkedSubscriptionId: new URLSearchParams(location.search).get("subscription_id") || "",
        editingCyclePolicy: null,
    };
    let currentUserLoadToken = 0;
    let dataLoadToken = 0;

    const metricOptions = [
        ["big_small", "大小"],
        ["odd_even", "单双"],
        ["combo", "组合"],
        ["double_group", "双组"],
        ["kill_group", "杀组"],
    ];
    const conditionTypeOptions = [
        ["hit_rate", "命中率"],
        ["hit_streak", "当前连中"],
        ["miss_streak", "当前连挂"],
        ["draw_streak", "连续开出"],
        ["draw_absence", "连续不出"],
    ];
    const drawConditionTypes = ["draw_streak", "draw_absence"];
    const predictionStreakTypes = ["hit_streak", "miss_streak"];
    const drawValuesByMetric = {
        big_small: ["大", "小"],
        odd_even: ["单", "双"],
        combo: ["大单", "大双", "小单", "小双"],
        double_group: ["大单+大双", "大单+小单", "大单+小双", "大双+小单", "大双+小双", "小单+小双"],
        kill_group: ["大单", "大双", "小单", "小双"],
    };
    const cycleActionLabels = {
        wait_condition: "等待下次条件满足",
        new_cycle: "下一可投注期直接续开",
        close_day: "当天收工",
    };
    const cycleStatusLabels = {
        waiting: "等待触发", running: "进行中", continuation: "等待直接续开",
        day_closed: "当天收工", blocked: "暂停，需核对",
    };
    const cycleReasonLabels = {
        cycle_balance_depleted: "本轮资金耗尽", balance_depleted: "本轮资金耗尽",
        cycle_target_hit: "已达到本轮过关目标", win_streak_target: "已达到本轮过关目标",
        cycle_daily_limit: "今日启动次数已满", daily_start_limit: "今日启动次数已满",
        daily_limit_reached: "今日启动次数已满", cycle_start_limit_reached: "今日启动次数已满",
        max_starts_per_day: "今日启动次数已满", manual_stop: "已人工结束本轮",
        profit_target_hit: "规则日止盈已触发", loss_limit_hit: "规则日止损已触发",
        daily_hit_rate_low: "规则日命中率熔断已触发",
    };
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
        ["pc28_netdisk_regular", "OK游戏网盘"],
        ["pc28_netdisk_abc", "OK游戏网盘 ABC"],
        ["pc28_high_regular", "OK游戏高赔常规"],
        ["pc28_high_abc", "OK游戏高赔 ABC"],
        ["pc28_fullpay_netdisk_regular", "彩28网盘"],
        ["pc28_fullpay_2_0_regular", "彩28满赔2.0"],
        ["pc28_fullpay_2_8_regular", "彩28满赔2.8"],
        ["pc28_fullpay_3_2_regular", "彩28满赔3.2"],
    ];
    const SETTLEMENT_RULE_UI_PRESETS = {
        pc28_netdisk_regular: {bigSmall: 1.98, comboSmallOddBigEven: 3.6, comboBigOddSmallEven: 4.2, refundHint: "无回本。OK游戏网盘"},
        pc28_netdisk_abc: {bigSmall: 1.98, comboSmallOddBigEven: 4.8, comboBigOddSmallEven: 3.1, refundHint: "无回本。OK游戏网盘 ABC"},
        pc28_high_regular: {bigSmall: 2.846, comboSmallOddBigEven: 6.78, comboBigOddSmallEven: 6.33, refundHint: "中奖遇 13/14、对子、顺子、豹子退本（旧高赔 2.846）"},
        pc28_high_abc: {bigSmall: 1.98, comboSmallOddBigEven: 4.9, comboBigOddSmallEven: 3.1, refundHint: "中奖遇 13/14、对子、顺子、豹子退本"},
        pc28_fullpay_netdisk_regular: {bigSmall: 1.99, comboSmallOddBigEven: 3.71, comboBigOddSmallEven: 4.32, refundHint: "大小单双中奖遇 0/27 退本，未中全亏；组合不退"},
        pc28_fullpay_2_0_regular: {bigSmall: 2, comboSmallOddBigEven: 4.76, comboBigOddSmallEven: 4.32, refundHint: "大小单双中奖遇 0/27 退本，未中全亏；组合中奖 13/14 退本；大小 13/14 注>2001 降赔 1.98"},
        pc28_fullpay_2_8_regular: {bigSmall: 2.84, comboSmallOddBigEven: 6.79, comboBigOddSmallEven: 6.33, refundHint: "中奖遇 13/14、对子、顺子、豹子退本"},
        pc28_fullpay_3_2_regular: {bigSmall: 3.2, comboSmallOddBigEven: 7, comboBigOddSmallEven: 6.5, refundHint: "中奖遇 13/14 退本；中奖且 ABC 含 0/9 退本"},
    };

    function $(id) {
        return document.getElementById(id);
    }

    function todayDateString() {
        const parts = new Intl.DateTimeFormat("en-CA", {
            timeZone: "Asia/Shanghai",
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
        }).formatToParts(new Date());
        const values = Object.fromEntries(parts.map(function (part) { return [part.type, part.value]; }));
        return values.year + "-" + values.month + "-" + values.day;
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
        const changesLaunch = options?.method === 'POST' && /^\/api\/platform\/(subscriptions|auto-trigger-rules)(\/|$)/.test(path);
        const pausesOnly = path.endsWith('/status') && ['inactive', 'archived'].includes(options?.body?.status);
        if (changesLaunch && !pausesOnly && !state.supportsExplicitStart) {
            throw new Error('保存与启动分离尚未在服务端生效，请重启跟单服务后刷新页面。');
        }
        const response = await fetch(path, {
            credentials: "same-origin",
            headers: {"Content-Type": "application/json"},
            ...(options || {}),
            body: options && options.body && typeof options.body !== "string" ? JSON.stringify(options.body) : (options || {}).body,
        });
        const data = await response.json().catch(function () { return {}; });
        if (!response.ok) {
            if (!data || !Object.keys(data).length) {
                throw new Error("服务器返回了非预期的响应（HTTP " + response.status + "），请求可能被网站防火墙拦截，请联系管理员处理。");
            }
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
        const name = (item.name && String(item.name).trim()) || (source ? source.name : ("来源 #" + String(item.source_id)));
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
        const snapshot = event.snapshot || {};
        if (event.reason === "schedule_day_already_started" && (
            snapshot.cross_day_block || (snapshot.existing_day_run && snapshot.existing_day_run.status === "blocked")
        )) {
            return "因上一轮跨日延续被拦截，当天未新开轮次。";
        }
        const scheduleReasons = {
            cycle_continued: "按本规则设置开启下一轮过关，已计入每日启动次数。",
            cycle_daily_limit: "本规则下该方案今日启动次数已满。",
            cycle_day_closed: "本规则下该方案今日已收工。",
            cycle_balance_depleted: "本轮滚仓资金耗尽，按规则结束设置处理。",
            cycle_target_hit: "已达到本轮过关目标，按规则结束设置处理。",
            cycle_running: "本规则下该方案的过关轮次仍在运行，不重复启动。",
            cycle_has_open_run: "本规则下该方案的过关轮次仍在运行，不重复启动。",
            cycle_blocked: "本规则下该方案暂停，需核对状态后恢复待命。",
            cycle_needs_new_issue: "等待本轮结束之后的新一期重新判断条件。",
            cycle_waiting_new_issue: "等待本轮结束之后的新一期重新判断条件。",
            cycle_signal_not_dispatched: "尚未创建新的投注任务，本次未消耗启动次数。",
            cycle_no_jobs_created: "尚未创建新的投注任务，本次未消耗启动次数。",
            manual_stop: "本规则的当前轮次已人工结束，需恢复该方案待命后再判断条件。",
            draw_clock_unavailable: "暂时无法确认最新开奖期号，等待下一次检查。",
            draw_history_not_current: "开奖记录尚未同步到最新一期，等待下一次检查。",
            draw_issue_already_triggered: "该期开奖已触发过此方案，不重复启动。",
            subscription_not_active: "跟单方案已停用，规则未触发。",
            source_not_active: "信号来源已停用，规则未触发。",
            outside_schedule_window: "当前不在定时窗口内。",
            schedule_weekday_blocked: "今天未启用定时触发。",
            schedule_day_already_started: "该定时窗口今日已经启动过一轮。",
            schedule_previous_run_active: "上一轮仍在运行（尚未达到停止条件），后续窗口不再重复触发；结束后可在尚未结束的窗口开新一轮。",
            schedule_signal_not_found: "定时窗口内没有可用的新信号。",
            schedule_signal_stale: "最新信号已超过允许延迟。",
            schedule_signal_before_window: "最新信号在窗口开始前就已发布，等窗口内新发布的信号。",
            schedule_subscription_open: "当前跟单方案仍有未结算轮次或待执行任务。",
            schedule_daily_risk_stopped: "今日已被每日风控停止。",
            schedule_signal_not_dispatched: "信号未成功派发，可在窗口内重试。",
            schedule_started: "定时窗口已启动一轮。",
        };
        if (scheduleReasons[event.reason]) {
            return scheduleReasons[event.reason];
        }
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
        const actualCount = condition["actual_" + type];
        const actual = actualCount == null ? "" : ("（当前" + (condition.streak_complete === false ? "至少" : "") + String(actualCount) + "期）");
        if (drawConditionTypes.includes(type)) {
            return groupDirectionLabel(condition.metric, condition.bet_value || "") + (type === "draw_absence" ? "连续不出" : "连续开出") + (operator ? operator[1] : condition.operator) + String(condition.threshold) + "期" + actual;
        }
        if (predictionStreakTypes.includes(type)) {
            return (metric ? metric[1] : condition.metric) + "（" + (condition.prediction_value ? "预测" + groupDirectionLabel(condition.metric, condition.prediction_value) : "不限方向") + "）" + (type === "hit_streak" ? "当前连中" : "当前连挂") + (operator ? operator[1] : condition.operator) + String(condition.threshold) + "期" + actual;
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
        const gate = payload.hit_rate_gate || {};
        if (gate.enabled) {
            parts.push("命中率 ≥" + String(gate.threshold) + "%（满" + String(gate.min_settled_count) + "单）");
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
        const reasonText = {
            daily_hit_rate_low: "当日命中率低于熔断阈值",
            profit_target_hit: "止盈",
            loss_limit_hit: "止损",
        }[String(stat.stopped_reason || "")] || String(stat.stopped_reason || "");
        return String(stat.stat_date || "--") + " 状态：已停止触发（" + reasonText + "）";
    }

    function isDailyRiskStopped(rule) {
        const stat = ruleDailyStat(rule);
        return Boolean(stat && stat.status === "stopped");
    }

    function ruleActiveRuns(rule) {
        return rule && Array.isArray(rule.active_runs) ? rule.active_runs : [];
    }

    function activeRunText(rule) {
        const runs = ruleActiveRuns(rule);
        if (!runs.length) {
            return "";
        }
        const today = String(rule.stat_date || "");
        const dates = runs.map(function (run) { return String(run.stat_date || "--"); });
        // 轮次多时只留首尾，避免一行铺满几十个日期
        const shown = dates.length > 5 ? dates.slice(0, 2).concat(["…"], dates.slice(-2)) : dates;
        const head = "在跑轮次 " + String(dates.length) + " 个：" + shown.join("、");
        const staleCount = dates.filter(function (date) { return date < today; }).length;
        // 每日开轮额度只对定时触发生效，条件触发不受跨日闸约束，别在那边报警
        if (staleCount) {
            return head + "（其中 " + String(staleCount) + " 个是上一日启动，当前仍在运行；不占今天的新开额度）";
        }
        return head;
    }

    function stopCurrentRunText(result, rule) {
        if (!result || !result.stopped) {
            return "这条规则当前没有在跑的轮次。";
        }
        const runCount = Array.isArray(result.stopped_runs) ? result.stopped_runs.length : (Array.isArray(result.runs) ? result.runs.length : 0);
        const parts = ["已结束本规则 " + String(runCount) + " 个在跑轮次"];
        if (result.cancelled_event_count) {
            parts.push("取消排队 " + String(result.cancelled_event_count) + " 单");
        }
        if (result.expired_job_count) {
            parts.push("跳过未执行任务 " + String(result.expired_job_count) + " 个");
        }
        const summary = parts.join("，") + "。";
        const nextStep = rule && Array.isArray(rule.cycle_runtime) && rule.cycle_runtime.length
            ? "过关方案可在运行详情中恢复待命，已用次数和历史盈亏保留。"
            : (rule && rule.trigger_mode === "condition_schedule" ? "后续启动仍需同时满足允许时段、触发条件和风控。" : (rule && rule.trigger_mode === "schedule" ? "后续启动仍需满足允许的定时窗口和风控。" : "后续启动仍需满足新的触发条件和风控。"));
        if (result.pending_settlement_count) {
            return summary + "仍有 " + String(result.pending_settlement_count) +
                " 单已下注待结算，已发送单不会撤销。" + nextStep;
        }
        return summary + nextStep;
    }

    function isRollSubscription(item) {
        const strategy = item.strategy_v2 || item.strategy || {};
        return Boolean((strategy.staking_policy || {}).mode === "win_streak_roll" ||
            (item.strategy && item.strategy.win_streak_roll));
    }

    function cycleNumber(value, money) {
        if (value === null || value === undefined || value === "" || !Number.isFinite(Number(value))) return "—";
        return money ? Number(value).toFixed(2) : String(value);
    }

    function cycleResumeBlock(rule, runtime) {
        if (runtime.outside_scope) return "已移出规则范围，仅保留运行记录";
        if (rule.status !== "active") return "规则未启用";
        const canResumeLegacyCycle = (rule.cycle_policy || {}).mode === "rule" &&
            ["cycle_target_hit", "cycle_balance_depleted"].includes(String((ruleDailyStat(rule) || {}).stopped_reason || ""));
        if (isDailyRiskStopped(rule) && !canResumeLegacyCycle) return "规则当天仍处于停止状态，请先处理规则日风控";
        if (!Array.isArray(runtime.routes) || !Object.hasOwn(runtime, "starts_today")) return "过关状态不可用";
        if (runtime.routes.some(function (route) { return Boolean(route.pending_issue); })) return "还有已发送单待结算";
        if (["running", "continuation"].includes(runtime.status)) return "当前仍在运行或等待续开";
        if (runtime.status === "waiting") return "已经处于等待触发状态";
        if (!Object.hasOwn(cycleStatusLabels, runtime.status)) return "过关状态不可用";
        if (runtime.status === "blocked" && runtime.stop_reason !== "manual_stop") return "历史过关状态需核对，自动续投已暂停";
        const starts = Number(runtime.starts_today);
        const limit = Number(runtime.max_starts_per_day);
        if (Number.isFinite(limit) && limit > 0 && starts >= limit) return "今日启动次数已满，调整上限后再恢复待命";
        const item = state.subscriptions.find(function (sub) { return Number(sub.id) === Number(runtime.subscription_id); });
        if (!item || !isTriggerableSubscription(item)) return "关联跟单方案未就绪";
        const source = sourceById(item.source_id);
        if (!source || source.status !== "active") return "信号来源未启用";
        return "";
    }

    function cycleRuntimeHtml(rule) {
        if (!Array.isArray(rule.cycle_runtime)) {
            return '<p class="meta-line">过关状态不可用，请刷新后重试。</p>';
        }
        if (!rule.cycle_runtime.length) return '';
        const historical = state.statDate !== todayDateString();
        const cards = rule.cycle_runtime.map(function (runtime) {
            const routes = Array.isArray(runtime.routes) ? runtime.routes : [];
            const blockReason = cycleResumeBlock(rule, runtime);
            const name = runtime.subscription_name || ("方案 " + runtime.subscription_id);
            const routeMarkup = routes.map(function (route) {
                const inherited = !(rule.routes || []).length;
                const routeName = inherited ? ("继承方案投递" + (route.route_name ? " · " + route.route_name : "")) : (route.route_name || "投递群组");
                return '<li><strong>' + escapeHtml(routeName) + '</strong><p class="meta-line">余额 ' + escapeHtml(cycleNumber(route.balance, true)) +
                    ' · 滚仓注额 ' + escapeHtml(cycleNumber(route.cycle_stake, true)) + ' · 连续盈利 ' +
                    escapeHtml(cycleNumber(route.win_streak)) + ' / ' + escapeHtml(cycleNumber(route.target_streak)) + ' 关</p>' +
                    (route.pending_issue ? '<p class="meta-line">第 <span class="mono">' + escapeHtml(route.pending_issue) + '</span> 期待结算</p>' : '') + '</li>';
            }).join("");
            const startDate = runtime.start_stat_date;
            const reason = runtime.stop_reason ? (cycleReasonLabels[runtime.stop_reason] || (runtime.status === "blocked" ? "历史过关状态需核对，自动续投已暂停" : "当前暂不可启动")) : "";
            return '<section class="cycle-runtime-card" tabindex="-1" data-cycle-subscription="' + escapeHtml(runtime.subscription_id) + '">' +
                '<div class="cycle-runtime-head"><strong>' + escapeHtml(name) + '</strong><span class="cycle-state">' + escapeHtml(cycleStatusLabels[runtime.status] || "状态未知") + '</span></div>' +
                (runtime.outside_scope ? '<p class="meta-line">已移出规则范围，仅保留运行记录</p>' : '') +
                '<p class="meta-line">' + escapeHtml(runtime.stat_date || state.statDate) + ' · 所选日已启动 ' + escapeHtml(cycleNumber(runtime.starts_today)) + ' / ' + escapeHtml(cycleNumber(runtime.max_starts_per_day)) + ' 轮</p>' +
                (startDate ? '<p class="meta-line">当前或最近一轮启动日：' + escapeHtml(startDate) + (runtime.status === "running" && startDate < todayDateString() ? ' · 上一日启动，当前仍在运行' : '') + '</p>' : '') +
                (reason ? '<p class="meta-line">' + escapeHtml(reason) + '</p>' : '') +
                (routes.length ? '<ul class="cycle-route-states">' + routeMarkup + '</ul>' : '<p class="meta-line">尚未启动过关轮次。</p>') +
                (runtime.status === "running" && !routes.some(function (route) { return route.pending_issue; }) ? '<p class="meta-line">等待下一期信号。</p>' : '') +
                (runtime.status === "continuation" ? '<p class="meta-line">等待下一可投注信号，不重新等待触发条件。</p>' : '') +
                (!historical && ["day_closed", "blocked"].includes(runtime.status) ? '<div class="cycle-runtime-actions"><button class="tiny-btn resume-cycle-btn" type="button" data-id="' + escapeHtml(rule.id) + '" data-subscription-id="' + escapeHtml(runtime.subscription_id) + '" aria-label="' + escapeHtml('恢复规则“' + rule.name + '”下方案“' + name + '”待命') + '"' + (blockReason ? ' disabled' : '') + '>恢复本方案待命</button>' + (blockReason ? '<p class="meta-line">' + escapeHtml(blockReason) + '</p>' : '') + '</div>' : '') + '</section>';
        });
        const content = cards.slice(0, 3).join("") + (cards.length > 3 ? '<details class="cycle-runtime-more"><summary>展开其余 ' + (cards.length - 3) + ' 个方案</summary>' + cards.slice(3).join("") + '</details>' : '');
        return '<details class="cycle-runtime" data-cycle-rule="' + escapeHtml(rule.id) + '"><summary>过关运行 · ' + cards.length + ' 个方案</summary>' + content +
            (historical ? '<button class="ghost-btn cycle-today-btn" type="button">切回今天操作</button>' : '') + '</details>';
    }

    function ruleLaunchSummary(rule) {
        const subs = state.subscriptions.filter(item => rule.scope_mode === 'all_subscriptions' ? item.status !== 'archived' : (rule.subscription_ids || []).includes(Number(item.id)));
        const labels = Object.fromEntries(settlementRuleOptions);
        return subs.map(sub => {
            const routes = (rule.routes || []).filter(route => route.status === 'active');
            const details = (rule.routes?.length ? routes : [null]).map(route => {
                const stakingOverride = route?.staking_mode === 'override';
                const staking = stakingOverride ? route.staking_policy || {} : sub.strategy_v2?.staking_policy || {};
                const base = staking.base_amount || staking.fixed_amount || staking.base_stake;
                const cycle = staking.mode === 'win_streak_roll' ? '过 ' + staking.target_streak + ' 关 · ' : '';
                const amount = base != null ? '起始 ' + base : '跟随来源金额';
                const override = route?.settlement_mode === 'override';
                const policy = override ? route.settlement_policy || {} : sub.strategy_v2?.settlement_policy || {};
                const text = policy.rule_source === 'follow_signal' || !policy.settlement_rule_id ? '跟随来源赔率' : labels[policy.settlement_rule_id] || policy.settlement_rule_id;
                const custom = Object.keys(policy.odds_overrides || {}).length ? '（含自定义赔率）' : '';
                return (route?.name ? route.name + '：' : '') + cycle + amount + (stakingOverride ? '（路由金额）' : '') +
                    ' · 新轮结算：' + text + custom + (override ? '（路由覆盖）' : '（继承方案）');
            }).join('；');
            return (sub.name || '方案 #' + sub.id) + ' · ' + (details || '尚无启用的投递路由');
        }).join('\n');
    }

    function renderLaunchPreview(rule) {
        const box = $('ruleLaunchPreview');
        const button = $('enableSavedRuleBtn');
        if (!box || !button) return;
        button.hidden = !rule?.id || rule.status === 'active' || rule.status === 'archived';
        button.disabled = false;
        if (!rule?.id) { box.textContent = '保存规则不会启动。保存后可启用规则并让关联方案待命。'; return; }
        const windows = rule.trigger_mode === 'condition' ? '' : (rule.schedule?.windows || []).map(w => w.start + '–' + w.end).join(' / ');
        const timing = windows ? ('北京时间 ' + windows) : '满足配置条件时';
        const hasRoll = state.subscriptions.some(sub => (rule.scope_mode === 'all_subscriptions' || (rule.subscription_ids || []).includes(Number(sub.id))) && sub.strategy_v2?.staking_policy?.mode === 'win_streak_roll');
        const quota = hasRoll && rule.cycle_policy?.mode === 'rule' ? ' · 过关每个方案每日最多 ' + rule.cycle_policy.max_starts_per_day + ' 轮。' : ' · 按配置的条件、时段和冷却启动。';
        box.textContent = ruleLaunchSummary(rule) + '\n' + timing + quota +
            (rule.status === 'active' ? '规则已启用；保存编辑不会重新启动当前轮次。' : '启用时会同时将关联方案设为自动待命；若当前时段和条件满足，可能立即开始。');
    }

    async function enableRule(ruleId) {
        await request('/api/platform/auto-trigger-rules/' + ruleId + '/status', {method:'POST',body:{status:'active'}});
        await loadData();
        setStatus('规则已启用，关联方案已进入自动待命；后续只由规则启动。', false);
    }

    function renderRuleList() {
        const list = $("ruleList");
        const subscriptionFilter = $("ruleSubscriptionFilter");
        if (subscriptionFilter) {
            subscriptionFilter.innerHTML = '<option value="">全部方案</option>' + state.subscriptions.map(function (item) {
                return '<option value="' + escapeHtml(item.id) + '">' + escapeHtml(subscriptionLabel(item)) + '</option>';
            }).join("");
            subscriptionFilter.value = state.linkedSubscriptionId;
        }
        const visibleRules = state.rules.filter(function (rule) {
            return !state.linkedSubscriptionId || rule.scope_mode === "all_subscriptions" || (rule.subscription_ids || []).some(function (id) { return String(id) === state.linkedSubscriptionId; });
        });
        if (!visibleRules.length) {
            list.innerHTML = '<div class="empty-state">当前范围暂无规则。可以新建规则并关联跟单方案。</div>';
            return;
        }
        list.innerHTML = visibleRules.map(function (rule) {
            const selected = String(rule.id) === String(state.editingRuleId) ? " is-active" : "";
            const scope = rule.scope_mode === "all_subscriptions" ? "全部跟单方案" : ("指定 " + String((rule.subscription_ids || []).length) + " 个方案");
            const isSchedule = String(rule.trigger_mode || "condition") === "schedule";
            const isCombined = rule.trigger_mode === "condition_schedule";
            const hasSchedule = isSchedule || isCombined;
            const conditions = (rule.conditions || []).map(conditionText).join("；") || "--";
            const schedule = rule.schedule || {};
            const scheduleText = hasSchedule ? ((isCombined ? "条件检查时段：" : "定时触发：") + ((schedule.windows || []).map(function (item) {
                return String(item.start || "") + "-" + String(item.end || "");
            }).join(" / ") || "未配置") + (isCombined ? "；时段内满足条件才开轮" : ((rule.cycle_policy || {}).mode === "rule" ? "；过关轮次按每日上限控制" : "，每个窗口每天最多1轮"))) : "";
            const guardGroups = (rule.guard_groups || []).map(function (group, index) {
                const groupConditions = (group.conditions || []).map(conditionText).join("；") || "--";
                return "条件区" + String(index + 1) + "：" + groupConditions;
            }).join(" / ");
            const routeCount = (rule.routes || []).filter(function (route) { return route.status !== "archived"; }).length;
            const stoppedRouteCount = (rule.routes || []).filter(function (route) {
                return route.daily_stat && route.daily_stat.status === "stopped";
            }).length;
            const blockedSubscriptions = state.subscriptions.filter(function (item) {
                return (rule.subscription_ids || []).some(function (id) { return Number(id) === Number(item.id); }) &&
                    (!isTriggerableSubscription(item) || !sourceById(item.source_id) || sourceById(item.source_id).status !== "active");
            }).map(function (item) {
                const source = sourceById(item.source_id);
                return subscriptionLabel(item) + (source && source.status !== "active" ? " / 来源" + subscriptionStatusText(source.status) : "");
            });
            return '' +
                '<article class="rule-card' + selected + '">' +
                    '<div class="rule-card-head"><div><strong>' + escapeHtml(rule.name) + '</strong><p class="meta-line">' + escapeHtml(scope) + '</p></div>' + statusPill(rule.status) + '</div>' +
                    '<p class="meta-line">' + (rule.scope_mode === "all_subscriptions" ? '<a href="/autobet/subscriptions">查看全部跟单方案</a>' : (rule.subscription_ids || []).map(function (id) {
                        const sub = state.subscriptions.find(function (item) { return Number(item.id) === Number(id); });
                        return '<a href="/autobet/subscriptions?subscription_id=' + encodeURIComponent(id) + '">' + escapeHtml(sub ? subscriptionLabel(sub) : "方案 #" + id) + '</a>';
                    }).join(" · ")) + '</p>' +
                    (hasSchedule ? '<p class="meta-line">' + escapeHtml(scheduleText) + '</p>' : '') +
                    (!isSchedule ? '<p class="meta-line">' + escapeHtml("开始条件：" + conditions) + '</p>' : '') +
                    (blockedSubscriptions.length ? '<p class="meta-line">未就绪：' + escapeHtml(blockedSubscriptions.join("；")) + '</p>' : '') +
                    (hasSchedule && rule.schedule_status ? '<p class="meta-line">' + escapeHtml(isCombined ? "当前" : (rule.stat_date || "当前统计日")) + ' 定时状态：' + escapeHtml(rule.schedule_status) + '</p>' : '') +
                    (guardGroups ? '<p class="meta-line">同时达成：' + escapeHtml(guardGroups) + '</p>' : '') +
                    '<p class="meta-line rule-settlement-summary">' + escapeHtml(ruleLaunchSummary(rule)).replace(/\n/g, '<br>') + '</p>' +
                    '<p class="meta-line">' + escapeHtml(actionText(rule.action)) + '</p>' +
                    (routeCount ? '<p class="meta-line">投注路由：' + escapeHtml(routeCount) + ' 条' + (stoppedRouteCount ? '，今日已停 ' + escapeHtml(stoppedRouteCount) + ' 条' : '') + '</p>' : '') +
                    (dailyRiskText(rule) ? '<p class="meta-line">' + escapeHtml(dailyRiskText(rule)) + '</p>' : '') +
                    '<p class="meta-line">' + escapeHtml(dailyProfitText(rule)) + '</p>' +
                    '<p class="meta-line">' + escapeHtml(dailySettlementText(rule)) + '</p>' +
                    (dailyStatusText(rule) ? '<p class="meta-line">' + escapeHtml(dailyStatusText(rule)) + '</p>' : '') +
                    (activeRunText(rule) ? '<p class="meta-line">' + escapeHtml(activeRunText(rule)) + '</p>' : '') +
                    '<p class="meta-line">冷却 <span class="mono">' + escapeHtml(rule.cooldown_issues) + '</span> 期，上次触发 <span class="mono">' + escapeHtml(rule.last_triggered_issue_no || "--") + '</span></p>' +
                    cycleRuntimeHtml(rule) +
                    '<div class="rule-actions">' +
                        '<button class="tiny-btn edit-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">编辑</button>' +
                        '<button class="tiny-btn copy-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">复制为未启用规则</button>' +
                        '<button class="tiny-btn run-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">检查</button>' +
                        (isDailyRiskStopped(rule) || stoppedRouteCount ? '<button class="tiny-btn resume-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">恢复规则日风控</button>' : '') +
                        (ruleActiveRuns(rule).length ? '<button class="tiny-btn stop-run-btn" type="button" data-id="' + escapeHtml(rule.id) + '">结束本规则当前轮次</button>' : '') +
                        '<button class="tiny-btn status-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '" data-status="' + (rule.status === "active" ? "inactive" : "active") + '">' + (rule.status === "active" ? "暂停规则" : "启用规则并让方案待命") + '</button>' +
                        (rule.status === "archived" ? '' : '<button class="tiny-btn status-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '" data-status="archived">归档</button>') +
                        (rule.status === "archived" ? '<button class="tiny-btn danger-btn delete-rule-btn" type="button" data-id="' + escapeHtml(rule.id) + '">删除</button>' : '') +
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
            return item.status !== "archived" || selected.has(String(item.id));
        }).map(function (item) {
            return '<option value="' + escapeHtml(item.id) + '"' + (selected.has(String(item.id)) ? " selected" : "") + '>' + escapeHtml(subscriptionLabel(item)) + '</option>';
        }).join("");
    }

    function options(optionsList, current) {
        return optionsList.map(function (option) {
            return '<option value="' + escapeHtml(option[0]) + '"' + (String(option[0]) === String(current) ? " selected" : "") + '>' + escapeHtml(option[1]) + '</option>';
        }).join("");
    }

    function conditionValueOptions(type, metric) {
        const values = (drawValuesByMetric[metric] || drawValuesByMetric.big_small).map(function (value) { return [value, groupDirectionLabel(metric, value)]; });
        return drawConditionTypes.includes(type) ? values : [["", "不限"]].concat(values);
    }

    function groupDirectionLabel(metric, value) {
        if (metric === "kill_group") return "杀" + value + "（其余三组）";
        if (metric === "double_group") return String(value).replace("+", "＋");
        return value;
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
                '<label class="field condition-draw-value-field"><span class="condition-value-label">' + (drawConditionTypes.includes(conditionType) ? '开奖结果' : '预测方向') + '</span><select class="text-input condition-draw-value">' + options(conditionValueOptions(conditionType, item.metric), drawConditionTypes.includes(conditionType) ? item.bet_value : (item.prediction_value || "")) + '</select></label>' +
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

    function settlementRulePreset(ruleId) {
        return SETTLEMENT_RULE_UI_PRESETS[String(ruleId || "")] || SETTLEMENT_RULE_UI_PRESETS.pc28_netdisk_regular;
    }

    function sameOddsValue(left, right) {
        return Math.round(Number(left) * 10000) === Math.round(Number(right) * 10000);
    }

    function parseOddsInputValue(raw) {
        const text = String(raw == null ? "" : raw).trim();
        if (!text) {
            return null;
        }
        const odds = Number(text);
        if (!Number.isFinite(odds) || odds <= 1 || odds > 1000) {
            throw new Error("自定义赔率必须大于 1 且不超过 1000");
        }
        return Math.round(odds * 10000) / 10000;
    }

    function comboOverrideValue(overrides, names, fallback) {
        const payload = overrides && typeof overrides === "object" ? overrides : {};
        const combo = payload.combo && typeof payload.combo === "object" ? payload.combo : {};
        for (let index = 0; index < names.length; index += 1) {
            if (combo[names[index]] != null && String(combo[names[index]]).trim() !== "") {
                return combo[names[index]];
            }
        }
        return fallback;
    }

    function routeOddsValues(ruleId, overrides) {
        const preset = settlementRulePreset(ruleId);
        const payload = overrides && typeof overrides === "object" ? overrides : {};
        return {
            bigSmall: payload.big_small != null ? payload.big_small : (payload.odd_even != null ? payload.odd_even : preset.bigSmall),
            comboSmallOddBigEven: comboOverrideValue(payload, ["小单", "大双"], payload.combo_small_odd_big_even != null ? payload.combo_small_odd_big_even : preset.comboSmallOddBigEven),
            comboBigOddSmallEven: comboOverrideValue(payload, ["大单", "小双"], payload.combo_big_odd_small_even != null ? payload.combo_big_odd_small_even : preset.comboBigOddSmallEven),
            refundHint: preset.refundHint,
        };
    }

    function fillRouteOddsInputs(card, ruleId, overrides) {
        const values = routeOddsValues(ruleId, overrides);
        const bigSmall = card.querySelector(".route-odds-big-small");
        const comboA = card.querySelector(".route-odds-combo-a");
        const comboB = card.querySelector(".route-odds-combo-b");
        const hint = card.querySelector(".route-settlement-hint");
        if (bigSmall) {
            bigSmall.value = values.bigSmall;
        }
        if (comboA) {
            comboA.value = values.comboSmallOddBigEven;
        }
        if (comboB) {
            comboB.value = values.comboBigOddSmallEven;
        }
        if (hint) {
            hint.textContent = values.refundHint;
        }
    }

    function collectRouteOddsOverrides(card, ruleId) {
        const preset = settlementRulePreset(ruleId);
        const overrides = {};
        const bigSmall = parseOddsInputValue(card.querySelector(".route-odds-big-small") && card.querySelector(".route-odds-big-small").value);
        if (bigSmall != null && !sameOddsValue(bigSmall, preset.bigSmall)) {
            overrides.big_small = bigSmall;
            overrides.odd_even = bigSmall;
        }
        const comboA = parseOddsInputValue(card.querySelector(".route-odds-combo-a") && card.querySelector(".route-odds-combo-a").value);
        if (comboA != null && !sameOddsValue(comboA, preset.comboSmallOddBigEven)) {
            overrides.combo = overrides.combo || {};
            overrides.combo["小单"] = comboA;
            overrides.combo["大双"] = comboA;
        }
        const comboB = parseOddsInputValue(card.querySelector(".route-odds-combo-b") && card.querySelector(".route-odds-combo-b").value);
        if (comboB != null && !sameOddsValue(comboB, preset.comboBigOddSmallEven)) {
            overrides.combo = overrides.combo || {};
            overrides.combo["大单"] = comboB;
            overrides.combo["小双"] = comboB;
        }
        return overrides;
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
        const oddsValues = routeOddsValues(settlementPolicy.settlement_rule_id || "pc28_netdisk_regular", settlementPolicy.odds_overrides);
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
                        '<label class="field"><span>命中率熔断阈值(%)</span><input class="text-input route-hit-rate-threshold" type="number" min="1" max="99" step="0.1" value="' + escapeHtml((routeRiskControl.hit_rate_gate || {}).threshold || 50) + '"></label>' +
                        '<label class="field"><span>熔断最小结算单数</span><input class="text-input route-hit-rate-min-settled" type="number" min="1" step="1" value="' + escapeHtml((routeRiskControl.hit_rate_gate || {}).min_settled_count || 20) + '"></label>' +
                        '<label class="check-row"><input class="route-hit-rate-gate-enabled" type="checkbox"' + ((routeRiskControl.hit_rate_gate || {}).enabled ? " checked" : "") + '><span>当日命中率低于阈值时熔断该路由</span></label>' +
                    '</div>' +
                    '<label class="field"><span>方案风控</span><select class="text-input route-subscription-risk-mode">' + options([["inherit_rule", "继承规则默认"], ["inherit_subscription", "继承跟单方案"], ["override", "单独设置"], ["disabled", "关闭"]], subscriptionRiskMode) + '</select></label>' +
                    '<div class="route-subscription-risk-fields">' +
                        '<label class="field"><span>方案止盈</span><input class="text-input route-subscription-profit-target" type="number" min="0" step="0.01" value="' + escapeHtml(subscriptionRiskControl.profit_target || 0) + '"></label>' +
                        '<label class="field"><span>方案止损</span><input class="text-input route-subscription-loss-limit" type="number" min="0" step="0.01" value="' + escapeHtml(subscriptionRiskControl.loss_limit || 0) + '"></label>' +
                    '</div>' +
                    '<label class="field"><span>结算</span><select class="text-input route-settlement-mode">' + options([["inherit", "继承跟单方案"], ["override", "单独设置"]], settlementMode) + '</select></label>' +
                    '<label class="field route-settlement-field"><span>结算规则</span><select class="text-input route-settlement-rule">' + options(settlementRuleOptions, settlementPolicy.settlement_rule_id || "pc28_netdisk_regular") + '</select></label>' +
                    '<div class="route-settlement-field route-odds-fields">' +
                        '<label class="field"><span>大小/单双</span><input class="text-input route-odds-big-small" type="number" min="1.0001" max="1000" step="0.0001" value="' + escapeHtml(oddsValues.bigSmall) + '"></label>' +
                        '<label class="field"><span>大双/小单</span><input class="text-input route-odds-combo-a" type="number" min="1.0001" max="1000" step="0.0001" value="' + escapeHtml(oddsValues.comboSmallOddBigEven) + '"></label>' +
                        '<label class="field"><span>大单/小双</span><input class="text-input route-odds-combo-b" type="number" min="1.0001" max="1000" step="0.0001" value="' + escapeHtml(oddsValues.comboBigOddSmallEven) + '"></label>' +
                        '<p class="field-hint route-settlement-hint">' + escapeHtml(oddsValues.refundHint) + '</p>' +
                        '<button class="ghost-btn reset-route-odds-btn" type="button">恢复预设</button>' +
                    '</div>' +
                    '<label class="field"><span>投注</span><select class="text-input route-staking-mode">' + options([["inherit", "继承跟单方案"], ["override", "固定金额"]], stakingMode) + '</select></label>' +
                    '<label class="field route-staking-field"><span>固定金额（双组/杀组按每组合）</span><input class="text-input route-fixed-amount" type="number" min="0.01" step="0.01" value="' + escapeHtml(stakingPolicy.fixed_amount || 10) + '"></label>' +
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
        card.querySelectorAll(".route-settlement-field").forEach(function (field) {
            field.hidden = settlementMode !== "override";
        });
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
        const previousType = row.dataset.conditionType;
        row.dataset.conditionType = type;
        const isPredictionStreak = predictionStreakTypes.includes(type);
        const isDrawStreak = drawConditionTypes.includes(type);
        const isStreak = isPredictionStreak || isDrawStreak;
        const threshold = row.querySelector(".condition-threshold");
        const operator = row.querySelector(".condition-operator");
        const windowInput = row.querySelector(".condition-window");
        const sampleInput = row.querySelector(".condition-sample");
        row.querySelector(".condition-threshold-label").textContent = isDrawStreak ? "连续期数" : (isPredictionStreak ? (type === "hit_streak" ? "连中期数" : "连挂期数") : "阈值 %");
        threshold.min = isDrawStreak ? "1" : "0";
        threshold.max = isPredictionStreak ? "" : "100";
        threshold.step = isStreak ? "1" : "0.01";
        row.querySelector(".condition-window-field").hidden = isStreak;
        row.querySelector(".condition-sample-field").hidden = isStreak;
        row.querySelector(".condition-draw-value-field").hidden = !isStreak;
        row.querySelector(".condition-value-label").textContent = isDrawStreak ? "开奖结果" : "预测方向";
        const drawValue = row.querySelector(".condition-draw-value");
        const resetDirection = previousType && drawConditionTypes.includes(previousType) !== isDrawStreak;
        drawValue.innerHTML = options(conditionValueOptions(type, row.querySelector(".condition-metric").value), resetDirection ? "" : drawValue.value);
        if (!isStreak && Number(sampleInput.value || 0) > Number(windowInput.value || 100)) {
            sampleInput.value = String(windowInput.value || 100);
        }
        if (preferDefaultOperator === true && isStreak && operator.value === "lt") {
            operator.value = "gte";
        }
        if (preferDefaultOperator === true && isStreak) {
            threshold.value = isDrawStreak ? "2" : "5";
        }
    }

    function scheduleWindowRowHtml(item) {
        const row = item || {};
        return '' +
            '<div class="schedule-window-row"' + (row.id ? ' data-window-id="' + escapeHtml(row.id) + '"' : '') + '>' +
                '<input class="text-input schedule-window-start" type="time" value="' + escapeHtml(row.start || "18:00") + '" aria-label="窗口开始时间">' +
                '<span>至</span>' +
                '<input class="text-input schedule-window-end" type="time" value="' + escapeHtml(row.end === "24:00" ? "" : (row.end || "18:30")) + '" aria-label="窗口结束时间，留空表示当天24点">' +
                '<button class="ghost-btn remove-schedule-window-btn" type="button">删除</button>' +
            '</div>';
    }

    function renderScheduleWindowRows(windows) {
        const list = (windows || []).length ? windows : [
            {id: "primary", start: "18:00", end: "18:30"},
            {id: "fallback", start: "20:00", end: "20:30"},
        ];
        $("scheduleWindowRows").innerHTML = list.map(scheduleWindowRowHtml).join("");
    }

    function collectScheduleWindows() {
        const rows = Array.from(document.querySelectorAll("#scheduleWindowRows .schedule-window-row"));
        const usedIds = new Set(rows.map(function (row) { return String(row.dataset.windowId || ""); }).filter(Boolean));
        let nextIndex = 1;
        return rows.map(function (row) {
            let id = String(row.dataset.windowId || "");
            if (!id) {
                while (usedIds.has("w" + nextIndex)) {
                    nextIndex += 1;
                }
                id = "w" + nextIndex;
                usedIds.add(id);
            }
            return {
                id: id,
                start: row.querySelector(".schedule-window-start").value,
                end: row.querySelector(".schedule-window-end").value || "24:00",
            };
        });
    }

    function resetForm(rule) {
        const form = $("ruleForm");
        const item = rule || {};
        state.editingRuleId = item.id ? String(item.id) : "";
        form.elements.rule_id.value = state.editingRuleId;
        form.elements.name.value = item.name || "";
        form.elements.status.value = item.status || "inactive";
        form.elements.scope_mode.value = item.scope_mode || "selected_subscriptions";
        form.elements.cooldown_issues.value = String(item.cooldown_issues == null ? 10 : item.cooldown_issues);
        form.elements.trigger_mode.value = item.trigger_mode || "condition";
        const cyclePolicy = item.cycle_policy && typeof item.cycle_policy === "object" ? item.cycle_policy : (item.id ? {mode: "legacy"} : {mode: "rule"});
        state.editingCyclePolicy = cyclePolicy;
        form.elements.cycle_mode.innerHTML = options(cyclePolicy.mode === "legacy" ? [["legacy", "沿用原有结束设置"], ["rule", "由本规则控制"]] : [["rule", "由本规则控制"]], cyclePolicy.mode || "rule");
        form.elements.cycle_max_starts.value = String(cyclePolicy.max_starts_per_day == null ? 1 : cyclePolicy.max_starts_per_day);
        form.elements.cycle_on_target.value = cyclePolicy.on_target || "wait_condition";
        form.elements.cycle_on_bust.value = cyclePolicy.on_bust || "wait_condition";
        form.elements.cycle_max_starts.removeAttribute("aria-invalid");
        $("cyclePolicyError").hidden = true;
        const schedule = item.schedule || {};
        form.elements.schedule_timezone.value = schedule.timezone || "Asia/Shanghai";
        form.elements.schedule_signal_max_age_seconds.value = String(schedule.signal_max_age_seconds || 300);
        const weekdays = new Set(schedule.weekdays || ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]);
        form.querySelectorAll('input[name="schedule_weekday"]').forEach(function (input) {
            input.checked = weekdays.has(input.value);
        });
        renderScheduleWindowRows(schedule.windows || []);
        form.elements.dispatch_latest_signal.checked = item.action ? item.action.dispatch_latest_signal !== false : true;
        form.elements.skip_multiple_metrics_matched.checked = Boolean(item.action && item.action.skip_multiple_metrics_matched);
        form.elements.play_filter_action.value = item.action && item.action.play_filter_action ? item.action.play_filter_action : "keep";
        form.elements.fixed_metric.value = item.action && item.action.fixed_metric ? item.action.fixed_metric : "big_small";
        const dailyRisk = item.daily_risk_control || {};
        const dailyGate = dailyRisk.hit_rate_gate || {};
        form.elements.daily_risk_enabled.checked = Boolean(dailyRisk.enabled);
        form.elements.daily_profit_target.value = String(dailyRisk.profit_target || 0);
        form.elements.daily_loss_limit.value = String(dailyRisk.loss_limit || 0);
        form.elements.daily_hit_rate_gate_enabled.checked = Boolean(dailyGate.enabled);
        form.elements.daily_hit_rate_threshold.value = String(dailyGate.threshold || 50);
        form.elements.daily_hit_rate_min_settled.value = String(dailyGate.min_settled_count || 20);
        form.elements.daily_cancel_pending_jobs.checked = dailyRisk.cancel_pending_jobs !== false;
        renderSubscriptionOptions(item.subscription_ids || (state.linkedSubscriptionId ? [Number(state.linkedSubscriptionId)] : []));
        renderConditions(item.conditions || []);
        renderGuardGroups(item.guard_groups || []);
        renderRoutes(item.routes || []);
        $("editorTitle").textContent = item.id ? "编辑触发规则" : "新建触发规则";
        $("cancelEditBtn").hidden = !item.id;
        updateScopeVisibility();
        updateTriggerModeVisibility();
        updateActionVisibility();
        updateDailyRiskVisibility();
        updateCyclePolicyVisibility();
        renderRuleList();
        renderLaunchPreview(item);
        setStatus("", false);
        if (window.PlatformNavigation) {
            window.PlatformNavigation.markClean(form);
            window.PlatformNavigation.setQuery({rule_id: state.editingRuleId});
        }
    }

    function updateTriggerModeVisibility() {
        const isSchedule = $("triggerModeSelect").value === "schedule";
        const isCombined = $("triggerModeSelect").value === "condition_schedule";
        $("scheduleFields").hidden = !(isSchedule || isCombined);
        $("conditionPanel").hidden = isSchedule;
        $("guardPanel").hidden = isSchedule;
        updateCyclePolicyVisibility();
    }

    function updateScopeVisibility() {
        const isAll = $("scopeModeSelect").value === "all_subscriptions";
        $("subscriptionSelectField").hidden = isAll;
        updateCyclePolicyVisibility();
    }

    function updateActionVisibility() {
        $("fixedMetricField").hidden = $("playFilterActionSelect").value !== "fixed_metric";
    }

    function updateDailyRiskVisibility() {
        $("dailyRiskFields").hidden = !Boolean($("dailyRiskEnabledInput").checked);
    }

    function updateCyclePolicyVisibility() {
        const form = $("ruleForm");
        const legacy = form.elements.cycle_mode.value === "legacy";
        $("cyclePolicyPanel").querySelectorAll(".cycle-rule-field").forEach(function (field) { field.hidden = legacy; });
        ["cycle_max_starts", "cycle_on_target", "cycle_on_bust"].forEach(function (name) { form.elements[name].disabled = legacy; });
        $("cyclePolicyHint").textContent = legacy
            ? "沿用原有结束设置。切换为由本规则控制后，新的轮次将使用本区设置；保存不会解除已收工状态。"
            : "本规则的每日次数与结束处理独立管理，不影响手动方案或其他规则。";
        const selectedIds = new Set(Array.from($("subscriptionSelect").selectedOptions).map(function (option) { return Number(option.value); }));
        const scoped = state.subscriptions.filter(function (item) { return form.elements.scope_mode.value === "all_subscriptions" || selectedIds.has(Number(item.id)); });
        $("cycleScopeHint").textContent = scoped.some(isRollSubscription) ? "" : "当前范围没有过关方案；这些设置将在选用过关方案时生效。";
        const isSchedule = form.elements.trigger_mode.value === "schedule";
        const isCombined = form.elements.trigger_mode.value === "condition_schedule";
        const hints = {
            wait_condition: isSchedule
                ? "本轮结束后等待新一期，且处于允许的定时窗口；同一窗口可再次启动。"
                : "本轮结束后回到待命，使用结束后的新一期重新判断条件，仍受冷却期数限制。" + (isCombined ? "只能在允许时段内重新开轮。" : ""),
            new_cycle: "不重新等待开始条件和触发冷却，按方案起始金额开启下一轮；仍计入每日上限并受日风控限制。" + (isSchedule || isCombined ? "直接续开也必须处于允许时段。" : ""),
            close_day: "本轮结束后停止本规则下该方案当天的过关启动。",
        };
        [["cycle_on_target", "cycleOnTargetHint"], ["cycle_on_bust", "cycleOnBustHint"]].forEach(function (entry) {
            const action = form.elements[entry[0]].value;
            $(entry[1]).textContent = (hints[action] || "结束处理不可用，请重新选择。") +
                (Number(form.elements.cycle_max_starts.value) === 1 && action !== "close_day" ? " 当前每天最多 1 轮，本轮结束后会因额度用完收工。" : "");
        });
        $("scheduleModeHint").textContent = isCombined
            ? "只有在所选星期和时段内才检查开始条件与附加条件区；到点后条件未满足会继续等待。时段仅限制开新轮，已开始的轮次继续按原停止条件运行。再次开轮仍受冷却、风控和过关每日次数限制，不额外限制每个窗口一轮；不会补发窗口外旧信号。"
            : !legacy
            ? "过关方案在上轮结束后，可用新一期在同一允许窗口再次启动，受本规则每日次数限制。普通均注、倍投仍是每个窗口每天最多一轮。所有方案均不会补发窗口外旧信号。"
            : "每个定时任务在窗口开始后尝试启动一轮，每个窗口每天最多一轮；上一轮未结束时不重复开轮。窗口结束后不会补发旧信号。";
    }

    function collectCyclePolicy() {
        const form = $("ruleForm");
        if (form.elements.cycle_mode.value === "legacy") {
            return {...(state.editingCyclePolicy || {}), mode: "legacy"};
        }
        const input = form.elements.cycle_max_starts;
        const value = String(input.value).trim();
        const limit = Number(value);
        if (!/^\d+$/.test(value) || !Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
            input.setAttribute("aria-invalid", "true");
            $("cyclePolicyError").textContent = "每日启动轮数请输入 1–1000 的整数。";
            $("cyclePolicyError").hidden = false;
            input.focus();
            throw new Error("每日启动轮数请输入 1–1000 的整数。");
        }
        const onTarget = form.elements.cycle_on_target.value;
        const onBust = form.elements.cycle_on_bust.value;
        if (!cycleActionLabels[onTarget] || !cycleActionLabels[onBust]) {
            throw new Error("请选择有效的过关结束处理方式。");
        }
        input.removeAttribute("aria-invalid");
        $("cyclePolicyError").hidden = true;
        return {mode: "rule", max_starts_per_day: limit, on_target: onTarget, on_bust: onBust};
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
                ...(drawConditionTypes.includes(type) ? {bet_value: row.querySelector(".condition-draw-value").value} : {}),
                ...(predictionStreakTypes.includes(type) ? {prediction_value: row.querySelector(".condition-draw-value").value} : {}),
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
                    hit_rate_gate: {
                        enabled: card.querySelector(".route-hit-rate-gate-enabled").checked,
                        threshold: Number(card.querySelector(".route-hit-rate-threshold").value || 0),
                        min_settled_count: Number(card.querySelector(".route-hit-rate-min-settled").value || 20),
                    },
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
                const settlementRuleId = card.querySelector(".route-settlement-rule").value;
                route.settlement_policy = {
                    settlement_rule_id: settlementRuleId,
                    fallback_profit_ratio: 1,
                    odds_overrides: collectRouteOddsOverrides(card, settlementRuleId),
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

            scope_mode: form.elements.scope_mode.value,
            subscription_ids: Array.from($("subscriptionSelect").selectedOptions).map(function (option) { return Number(option.value); }),
            cooldown_issues: Number(form.elements.cooldown_issues.value || 0),
            trigger_mode: form.elements.trigger_mode.value,
            schedule: {
                timezone: form.elements.schedule_timezone.value,
                weekdays: Array.from(form.querySelectorAll('input[name="schedule_weekday"]:checked')).map(function (input) { return input.value; }),
                windows: collectScheduleWindows(),
                signal_max_age_seconds: Number(form.elements.schedule_signal_max_age_seconds.value || 300),
            },
            condition_mode: "any",
            cycle_policy: collectCyclePolicy(),
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
                hit_rate_gate: {
                    enabled: form.elements.daily_hit_rate_gate_enabled.checked,
                    threshold: Number(form.elements.daily_hit_rate_threshold.value || 0),
                    min_settled_count: Number(form.elements.daily_hit_rate_min_settled.value || 20),
                },
                timezone: "Asia/Shanghai",
                cancel_pending_jobs: form.elements.daily_cancel_pending_jobs.checked,
            },
            routes: collectRoutes(),
        };
    }

    async function loadData(options) {
        const dataToken = ++dataLoadToken;
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
        if (dataToken !== dataLoadToken) return;
        state.sources = payloads[0].items || [];
        state.subscriptions = payloads[1].items || [];
        state.supportsExplicitStart = payloads[1].launch_workflow === "explicit_start_v1";
        state.deliveryTargets = (payloads[2].items || []).filter(function (item) { return item.status !== "archived"; });
        state.templates = (payloads[3].items || []).filter(function (item) { return item.status !== "archived"; });
        state.rules = payloads[4].items || [];
        state.events = payloads[5].items || [];
        renderSummary();
        renderRuleList();
        renderEvents();
        if (window.PlatformNavigation && window.PlatformNavigation.isDirty($("ruleForm"))) return;
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
        currentUserLoadToken += 1;
        dataLoadToken += 1;
        state.rules = []; state.events = []; state.subscriptions = [];
        state.sources = []; state.deliveryTargets = []; state.templates = [];
        setCurrentUser(detail.user || null);
        resetForm(); renderRuleList(); renderEvents(); renderSummary();
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
        button.textContent = "保存中…";
        button.setAttribute("aria-busy", "true");
        try {
            const payload = collectPayload();
            const saved = await request(ruleId ? ("/api/platform/auto-trigger-rules/" + ruleId) : "/api/platform/auto-trigger-rules", {
                method: "POST",
                body: payload,
            });
            state.editingRuleId = String((saved.item || {}).id || ruleId);
            if (window.PlatformNavigation) window.PlatformNavigation.markClean(form);
            await loadData();
            setStatus(ruleId ? "规则配置已保存，启用状态和在途轮次保持原设置。" : "规则已保存，尚未启用。核对下方启动摘要后，可启用规则并让方案待命。", false);
        } catch (error) {
            setStatus(error.message, true);
        } finally {
            button.disabled = false;
            button.textContent = "保存规则";
            button.removeAttribute("aria-busy");
        }
    }

    async function runOnce(ruleId) {
        const body = ruleId ? {rule_id: Number(ruleId)} : {};
        await request("/api/platform/auto-trigger-rules/run-once", {method: "POST", body: body});
        await loadData();
    }

    function attachEvents() {
        $('enableSavedRuleBtn').addEventListener('click', async function () {
            if (!state.editingRuleId) return;
            this.disabled = true;
            try { await enableRule(state.editingRuleId); } catch (error) { setStatus(error.message, true); this.disabled = false; }
        });
        ['input', 'change'].forEach(name => $('ruleForm').addEventListener(name, () => {
            $('enableSavedRuleBtn').disabled = true;
            if (!$('enableSavedRuleBtn').hidden) $('ruleLaunchPreview').textContent = '配置已修改，请先保存后再启用。';
        }));

        if ($("ruleSubscriptionFilter")) $("ruleSubscriptionFilter").addEventListener("change", function () {
            state.linkedSubscriptionId = this.value;
            if (window.PlatformNavigation) window.PlatformNavigation.setQuery({subscription_id: state.linkedSubscriptionId});
            renderRuleList();
        });
        $("cycleModeSelect").addEventListener("change", function () {
            if (this.value === "rule" && (state.editingCyclePolicy || {}).mode === "legacy") {
                const form = $("ruleForm");
                form.elements.cycle_max_starts.value = "1";
                form.elements.cycle_on_target.value = "wait_condition";
                form.elements.cycle_on_bust.value = "wait_condition";
            }
            updateCyclePolicyVisibility();
        });
        ["cycleMaxStartsInput", "cycleOnTargetSelect", "cycleOnBustSelect", "subscriptionSelect"].forEach(function (id) {
            $(id).addEventListener("change", updateCyclePolicyVisibility);
        });
        $("ruleForm").addEventListener("submit", saveRule);
        $("scopeModeSelect").addEventListener("change", updateScopeVisibility);
        $("triggerModeSelect").addEventListener("change", updateTriggerModeVisibility);
        $("playFilterActionSelect").addEventListener("change", updateActionVisibility);
        $("dailyRiskEnabledInput").addEventListener("change", updateDailyRiskVisibility);
        $("statDateInput").addEventListener("change", async function (event) {
            state.statDate = event.target.value || todayDateString();
            if (window.PlatformNavigation) window.PlatformNavigation.setQuery({stat_date: state.statDate});
            syncStatDateControls();
            await loadData();
        });
        $("statDateTodayBtn").addEventListener("click", async function () {
            state.statDate = todayDateString();
            if (window.PlatformNavigation) window.PlatformNavigation.setQuery({stat_date: state.statDate});
            syncStatDateControls();
            await loadData();
        });
        $("refreshBtn").addEventListener("click", loadData);
        $("newRuleBtn").addEventListener("click", function () { if (!window.PlatformNavigation || window.PlatformNavigation.confirmLeaveForm($("ruleForm"))) resetForm(); });
        $("cancelEditBtn").addEventListener("click", function () { if (!window.PlatformNavigation || window.PlatformNavigation.confirmLeaveForm($("ruleForm"))) resetForm(); });
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
        $("addScheduleWindowBtn").addEventListener("click", function () {
            $("scheduleWindowRows").insertAdjacentHTML("beforeend", scheduleWindowRowHtml(null));
        });
        $("routeRows").addEventListener("change", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.closest(".route-card")) {
                const card = target.closest(".route-card");
                updateRouteRowVisibility(card);
                if (target.classList.contains("route-settlement-rule")) {
                    fillRouteOddsInputs(card, target.value, {});
                }
            }
        });
        $("routeRows").addEventListener("click", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.classList.contains("reset-route-odds-btn")) {
                const card = target.closest(".route-card");
                const ruleSelect = card ? card.querySelector(".route-settlement-rule") : null;
                if (card && ruleSelect) {
                    fillRouteOddsInputs(card, ruleSelect.value, {});
                }
            }
        });
        $("conditionRows").addEventListener("change", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && (target.classList.contains("condition-type") || target.classList.contains("condition-window") || target.classList.contains("condition-metric"))) {
                const row = target.closest(".condition-row");
                if (row) {
                    updateConditionRowVisibility(row, target.classList.contains("condition-type"));
                }
            }
        });
        $("guardGroupRows").addEventListener("change", function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && (target.classList.contains("condition-type") || target.classList.contains("condition-window") || target.classList.contains("condition-metric"))) {
                const row = target.closest(".condition-row");
                if (row) {
                    updateConditionRowVisibility(row, target.classList.contains("condition-type"));
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
            if (target.classList.contains("remove-schedule-window-btn")) {
                const windowRows = $("scheduleWindowRows");
                if (windowRows && windowRows.querySelectorAll(".schedule-window-row").length > 1) {
                    const row = target.closest(".schedule-window-row");
                    if (row) {
                        row.remove();
                    }
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
                if (window.PlatformNavigation && !window.PlatformNavigation.confirmLeaveForm($("ruleForm"))) return;
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
                    setStatus("已处理规则日风控恢复。过关方案的每日次数与收工状态仍按各自设置检查。", false);
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    target.removeAttribute("disabled");
                }
                return;
            }
            if (target.classList.contains("cycle-today-btn")) {
                $("statDateTodayBtn").click();
                return;
            }
            if (target.classList.contains("resume-cycle-btn")) {
                const ruleId = target.dataset.id;
                const subscriptionId = target.dataset.subscriptionId;
                try {
                    target.disabled = true;
                    target.textContent = "恢复中…";
                    target.setAttribute("aria-busy", "true");
                    await request("/api/platform/auto-trigger-rules/" + ruleId + "/resume-cycle", {
                        method: "POST", body: {subscription_id: Number(subscriptionId)},
                    });
                    await loadData();
                    const detail = document.querySelector('.cycle-runtime[data-cycle-rule="' + ruleId + '"]');
                    if (detail) {
                        detail.open = true;
                        const card = detail.querySelector('[data-cycle-subscription="' + subscriptionId + '"]');
                        if (card) card.focus({preventScroll: true});
                    }
                    setStatus("已恢复本规则下该方案的待命状态，仍需满足触发条件。日次数和历史盈亏已保留。", false);
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    target.disabled = false;
                    target.textContent = "恢复本方案待命";
                    target.removeAttribute("aria-busy");
                }
                return;
            }
            if (target.classList.contains("stop-run-btn")) {
                const rule = state.rules.find(function (item) { return String(item.id) === target.dataset.id; });
                if (!window.confirm("结束本规则当前在跑的轮次？已发送的单仍会结算。此操作不清空已用次数或历史盈亏；后续需按本规则的待命、条件和风控限制重新启动。")) {
                    return;
                }
                try {
                    target.setAttribute("disabled", "disabled");
                    const result = await request("/api/platform/auto-trigger-rules/" + target.dataset.id + "/stop-current-run", {
                        method: "POST",
                        body: {note: "前端手动结束当前轮次"},
                    });
                    await loadData();
                    setStatus(stopCurrentRunText(result, rule), false);
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    target.removeAttribute("disabled");
                }
                return;
            }
            if (target.classList.contains("delete-rule-btn")) {
                if (!window.confirm("删除后规则配置和运行统计将无法恢复；已结算的历史盈亏记录会保留。确认删除吗？")) {
                    return;
                }
                try {
                    target.setAttribute("disabled", "disabled");
                    await request("/api/platform/auto-trigger-rules/" + target.dataset.id + "/delete", {
                        method: "POST",
                        body: {},
                    });
                    if (String(state.editingRuleId || "") === String(target.dataset.id)) {
                        state.editingRuleId = "";
                    }
                    setStatus("规则已删除。", false);
                    await loadData();
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    target.removeAttribute("disabled");
                }
                return;
            }
            if (target.classList.contains('copy-rule-btn')) {
                const original = state.rules.find(rule => String(rule.id) === target.dataset.id);
                if (!original) return;
                resetForm({...original, id:null, name:original.name + ' 副本', status:'inactive',
                    routes:(original.routes || []).map(route => ({...route,id:undefined}))});
                setStatus('已复制到表单。保存后仍为未启用。', false);
                return;
            }
            if (target.classList.contains("status-rule-btn")) {
                try {
                    if (target.dataset.status === 'active') {
                        await enableRule(target.dataset.id);
                    } else {
                        await request("/api/platform/auto-trigger-rules/" + target.dataset.id + "/status", {
                            method: "POST", body: {status: target.dataset.status},
                        });
                    }
                    await loadData();
                } catch (error) {
                    setStatus(error.message, true);
                }
            }
        });
    }

    state.statDate = new URLSearchParams(window.location.search).get("stat_date") || todayDateString();

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
