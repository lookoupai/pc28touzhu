(function () {
    const state = {
        currentUser: null,
        sources: [],
        accounts: [],
        subscriptions: [],
        targets: [],
        jobs: [],
        alerts: [],
    };

    const statusMessage = document.getElementById("statusMessage");
    const checklist = document.getElementById("checklist");
    const jobTableBody = document.getElementById("jobTableBody");
    const jobCardList = document.getElementById("jobCardList");
    const alertList = document.getElementById("alertList");
    const refreshHomeBtn = document.getElementById("refreshHomeBtn");
    const workspaceLinks = Array.from(document.querySelectorAll(".workspace-link"));
    const importSourceForm = document.getElementById("importSourceForm");
    const importSourceBtn = document.getElementById("importSourceBtn");
    const importSourceNameInput = document.getElementById("importSourceName");
    const importSourceUrlInput = document.getElementById("importSourceUrl");
    const fillImportExampleBtn = document.getElementById("fillImportExampleBtn");
    const nextStepTitle = document.getElementById("nextStepTitle");
    const nextStepBadge = document.getElementById("nextStepBadge");
    const nextStepDescription = document.getElementById("nextStepDescription");
    const nextStepAction = document.getElementById("nextStepAction");
    const nextStepSecondaryAction = document.getElementById("nextStepSecondaryAction");

    function setStatus(message, isError) {
        statusMessage.textContent = message || "";
        statusMessage.classList.toggle("is-error", Boolean(isError));
    }

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function labelStatus(value) {
        return window.PlatformUiText.labelStatus(value);
    }

    function formatDateTime(value) {
        const text = String(value || "").trim();
        if (!text) {
            return "--";
        }
        return text.replace("T", " ").replace("Z", " UTC");
    }

    function renderPill(text, className) {
        return '<span class="' + className + '">' + escapeHtml(labelStatus(text)) + "</span>";
    }

    function setActiveWorkspaceLink(targetHash) {
        workspaceLinks.forEach(function (link) {
            link.classList.toggle("is-active", link.getAttribute("href") === targetHash);
        });
    }

    function setButtonBusy(button, busy, busyText) {
        if (!(button instanceof HTMLElement)) {
            return;
        }
        if (!button.hasAttribute("data-idle-text")) {
            button.setAttribute("data-idle-text", button.textContent || "");
        }
        button.toggleAttribute("disabled", Boolean(busy));
        button.textContent = busy ? busyText : (button.getAttribute("data-idle-text") || "");
    }

    function deriveAiSourceName(urlObject) {
        const match = urlObject.pathname.match(/\/(?:api\/export|public|api\/public)\/predictors\/(\d+)(?:\/signals)?$/);
        if (match) {
            return "AITradingSimulator 方案 #" + match[1];
        }
        return "AITradingSimulator 来源";
    }

    function normalizeAiSourceUrl(rawUrl) {
        const text = String(rawUrl || "").trim();
        if (!text) {
            throw new Error("导出链接不能为空");
        }

        let parsed;
        try {
            parsed = new URL(text);
        } catch (error) {
            throw new Error("导出链接格式不正确");
        }

        if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
            throw new Error("导出链接必须使用 http 或 https");
        }

        const publicMatch = parsed.pathname.match(/^\/public\/predictors\/(\d+)$/);
        if (publicMatch) {
            parsed.pathname = "/api/export/predictors/" + publicMatch[1] + "/signals";
            parsed.search = "";
            parsed.searchParams.set("view", "execution");
        } else {
            const publicApiMatch = parsed.pathname.match(/^\/api\/public\/predictors\/(\d+)$/);
            if (publicApiMatch) {
                parsed.pathname = "/api/export/predictors/" + publicApiMatch[1] + "/signals";
                parsed.search = "";
                parsed.searchParams.set("view", "execution");
            }
        }

        const validPath = (
            /^\/api\/export\/predictors\/\d+\/signals$/.test(parsed.pathname) ||
            parsed.pathname === "/api/export/signals/pc28"
        );
        if (!validPath) {
            throw new Error("当前只支持 AITradingSimulator 的公开方案页链接或 signals 导出地址");
        }

        if (parsed.pathname !== "/api/export/signals/pc28") {
            const view = parsed.searchParams.get("view");
            if (!view) {
                parsed.searchParams.set("view", "execution");
            } else if (view !== "execution") {
                throw new Error("当前只支持 execution 视图导入");
            }
        }

        return {
            url: parsed.toString(),
            suggestedName: deriveAiSourceName(parsed),
        };
    }

    function findImportedAiSourceByUrl(normalizedUrl) {
        return state.sources.find(function (item) {
            const fetchConfig = item && item.config && item.config.fetch ? item.config.fetch : {};
            return item.source_type === "ai_trading_simulator_export" && String(fetchConfig.url || "") === normalizedUrl;
        }) || null;
    }

    function summarizeStrategy(strategy) {
        const payload = strategy && typeof strategy === "object" ? strategy : {};
        if (payload.base_stake != null && payload.multiplier != null && payload.max_steps != null) {
            return "策略 " + [payload.mode || "flat", "基础注 " + payload.base_stake, "倍数 " + payload.multiplier, "追手 " + payload.max_steps].join(" · ");
        }
        if (payload.stake_amount) {
            return "固定金额 " + payload.stake_amount;
        }
        if (payload.mode) {
            return "模式 " + payload.mode;
        }
        const keys = Object.keys(payload);
        if (keys.length) {
            return "已配置 " + keys.length + " 项规则";
        }
        return "已建立基础跟单关系";
    }

    function accountAuthState(account) {
        return String((account && (account.auth_state || (account.meta && account.meta.auth_state))) || "pending").trim() || "pending";
    }

    function isAuthorizedAccount(account) {
        if (!account) {
            return false;
        }
        if (typeof account.is_authorized === "boolean") {
            return account.is_authorized;
        }
        return accountAuthState(account) === "authorized";
    }

    function accountAuthLabel(account) {
        const authState = accountAuthState(account);
        const authMode = String((account && (account.auth_mode || (account.meta && account.meta.auth_mode))) || "phone_login").trim() || "phone_login";
        if (authState === "authorized") {
            return authMode === "session_import" ? "已导入" : "已授权";
        }
        if (authState === "code_sent") {
            return "待验证码";
        }
        if (authState === "password_required") {
            return "待二次密码";
        }
        if (authState === "pending_import") {
            return "待导入";
        }
        return "待授权";
    }

    function nextStepState() {
        if (!state.currentUser) {
            return {
                title: "先登录平台账号",
                description: "登录后，首页才能展示当前账号的自动投注总览，后续再进入总控台完成具体配置。",
                badgeText: "第 1 步",
                badgeClass: "step-pill is-pending",
                actionHref: "#accountSection",
                actionText: "去登录",
                secondaryHref: "/autobet",
                secondaryText: "查看总控台",
            };
        }

        const aiSourceCount = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        }).length;
        if (!aiSourceCount) {
            return {
                title: "先导入 AITradingSimulator 方案",
                description: "首页仍保留快捷导入，但导入完成后应直接进入总控台继续配置账号、群组、模板和跟单。",
                badgeText: "第 2 步",
                badgeClass: "step-pill is-pending",
                actionHref: "#importSection",
                actionText: "快捷导入",
                secondaryHref: "/autobet#autobetQuickstartSection",
                secondaryText: "去总控台继续",
            };
        }

        const authorizedAccounts = state.accounts.filter(isAuthorizedAccount);
        if (!authorizedAccounts.length) {
            return {
                title: "进入账号工作区完成授权",
                description: "方案已经导入成功。下一步不要停留在首页，直接进入账号工作区完成托管账号授权。",
                badgeText: "第 3 步",
                badgeClass: "step-pill is-pending",
                actionHref: "/autobet/accounts#accountsSection",
                actionText: "去账号工作区",
                secondaryHref: "/autobet",
                secondaryText: "进入总控台",
            };
        }

        if (!state.targets.length) {
            return {
                title: "进入群组工作区配置投递群组",
                description: "托管账号已经存在，但还没有执行落点。需要在群组工作区里绑定群组并完成测试发送。",
                badgeText: "第 4 步",
                badgeClass: "step-pill is-pending",
                actionHref: "/autobet/targets#targetsSection",
                actionText: "去群组工作区",
                secondaryHref: "/autobet",
                secondaryText: "回总控台",
            };
        }

        if (!state.subscriptions.length) {
            return {
                title: "进入跟单工作区建立策略",
                description: "账号和群组都已准备好，下一步在跟单工作区里建立策略并核对执行链路。",
                badgeText: "第 5 步",
                badgeClass: "step-pill is-pending",
                actionHref: "/autobet/subscriptions#subscriptionsSection",
                actionText: "去跟单工作区",
                secondaryHref: "/autobet/targets#targetsSection",
                secondaryText: "去群组工作区",
            };
        }

        return {
            title: "自动投注基础条件已就绪",
            description: "首页现在只负责汇总状态；如果要继续调整账号、群组、模板或跟单，请进入总控台或对应工作区。",
            badgeText: "已就绪",
            badgeClass: "step-pill is-done",
            actionHref: "/autobet",
            actionText: "进入总控台",
            secondaryHref: "/records",
            secondaryText: "查看完整执行记录",
        };
    }

    function renderNextStep() {
        const statePayload = nextStepState();
        nextStepTitle.textContent = statePayload.title;
        nextStepDescription.textContent = statePayload.description;
        nextStepBadge.className = statePayload.badgeClass;
        nextStepBadge.textContent = statePayload.badgeText;
        nextStepAction.setAttribute("href", statePayload.actionHref);
        nextStepAction.textContent = statePayload.actionText;
        nextStepSecondaryAction.setAttribute("href", statePayload.secondaryHref);
        nextStepSecondaryAction.textContent = statePayload.secondaryText;
    }

    function focusNextStep() {
        const nextStepSection = document.getElementById("nextStepSection");
        if (!(nextStepSection instanceof HTMLElement)) {
            return;
        }
        setActiveWorkspaceLink("#nextStepSection");
        nextStepSection.scrollIntoView({
            behavior: "smooth",
            block: "start",
        });
    }

    function setCurrentUser(user) {
        state.currentUser = user || null;
        window.PlatformAuthPanel.sync({
            user: state.currentUser,
            heroUsernameId: "heroUsername",
            heroUserMetaId: "heroUserMeta",
            heroLoggedInText: "当前会话已就绪，可先看总览，再进入总控台继续配置。",
            heroLoggedOutText: "登录后查看自动投注总览和最近执行结果。",
            panelTitleLoggedIn: "当前账户",
            panelTitleLoggedOut: "登录与账户",
        });
    }

    async function request(path, options) {
        const response = await fetch(path, {
            method: options && options.method ? options.method : "GET",
            credentials: "same-origin",
            headers: {
                "Content-Type": "application/json",
            },
            body: options && options.body ? JSON.stringify(options.body) : undefined,
        });
        const payload = await response.json();
        if (!response.ok) {
            throw new Error(payload.error || "请求失败");
        }
        return payload;
    }

    async function loadCurrentUser() {
        const response = await fetch("/api/auth/me", {
            method: "GET",
            credentials: "same-origin",
            headers: {
                "Content-Type": "application/json",
            },
        });

        const payload = await response.json();
        if (response.status === 401) {
            setCurrentUser(null);
            return null;
        }
        if (!response.ok) {
            throw new Error(payload.error || "加载当前用户失败");
        }
        setCurrentUser(payload.user || null);
        return state.currentUser;
    }

    function resetCollections() {
        state.sources = [];
        state.accounts = [];
        state.subscriptions = [];
        state.targets = [];
        state.jobs = [];
        state.alerts = [];
        renderMetrics();
        renderNextStep();
        renderChecklist();
        renderCurrentSetup();
        renderJobs();
        renderAlerts();
    }

    function sortedJobs() {
        return state.jobs.slice().sort(function (left, right) {
            return Number(right.id || 0) - Number(left.id || 0);
        });
    }

    function sourceById(sourceId) {
        return state.sources.find(function (item) {
            return Number(item.id) === Number(sourceId);
        }) || null;
    }

    function primarySource() {
        const subscription = primarySubscription();
        if (subscription && subscription.source_id != null) {
            const matchedSource = sourceById(subscription.source_id);
            if (matchedSource) {
                return matchedSource;
            }
        }
        const aiSource = state.sources.find(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        return aiSource || state.sources[0] || null;
    }

    function primarySubscription() {
        return state.subscriptions[0] || null;
    }

    function primaryTarget() {
        return state.targets[0] || null;
    }

    function primaryAccount() {
        const target = primaryTarget();
        if (target && target.telegram_account_id != null) {
            const matched = state.accounts.find(function (item) {
                return Number(item.id) === Number(target.telegram_account_id);
            });
            if (matched) {
                return matched;
            }
        }
        return state.accounts.find(isAuthorizedAccount) || state.accounts[0] || null;
    }

    function readinessState() {
        const aiSources = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        const missing = [];
        if (!state.currentUser) {
            missing.push("登录");
        }
        if (!aiSources.length) {
            missing.push("AI 方案");
        }
        if (!state.accounts.filter(isAuthorizedAccount).length) {
            missing.push("已授权托管账号");
        }
        if (!state.targets.length) {
            missing.push("投递群组");
        }
        if (!state.subscriptions.length) {
            missing.push("跟单关系");
        }
        return {
            ready: missing.length === 0,
            missing: missing,
        };
    }

    function renderMetrics() {
        const aiSourceCount = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        }).length;
        const authorizedAccountCount = state.accounts.filter(isAuthorizedAccount).length;
        const deliveredCount = state.jobs.filter(function (item) { return item.status === "delivered"; }).length;
        const issueCount = state.jobs.filter(function (item) {
            return item.status === "failed" || item.status === "expired" || item.status === "skipped";
        }).length;
        const pendingCount = state.jobs.filter(function (item) { return item.status === "pending"; }).length;
        const readiness = readinessState();

        document.getElementById("aiSourceCount").textContent = String(aiSourceCount);
        document.getElementById("accountCount").textContent = String(authorizedAccountCount);
        document.getElementById("subscriptionCount").textContent = String(state.subscriptions.length);
        document.getElementById("targetCount").textContent = String(state.targets.length);
        document.getElementById("alertCount").textContent = String(state.alerts.length);
        document.getElementById("jobHealthSummary").textContent = String(deliveredCount) + " / " + String(issueCount);
        document.getElementById("jobHealthDetail").textContent = "成功 " + deliveredCount + "，异常 " + issueCount + "，待执行 " + pendingCount;
        document.getElementById("readinessHeadline").textContent = readiness.ready ? "已具备条件" : "待配置";
        document.getElementById("readinessDetail").textContent = readiness.ready
            ? "方案、账号、群组和跟单关系都已存在。后续调整统一进入自动投注总控台。"
            : "还缺少：" + (readiness.missing.join("、") || "基础配置");
    }

    function renderChecklist() {
        const items = [
            {
                title: "登录平台账号",
                complete: Boolean(state.currentUser),
                description: "先建立会话，后续方案、托管账号和执行记录都会与当前账号绑定。",
                actionHref: "#authForm",
                actionText: state.currentUser ? "已登录" : "先登录",
            },
            {
                title: "导入 AITradingSimulator 方案",
                complete: state.sources.some(function (item) { return item.source_type === "ai_trading_simulator_export"; }),
                description: "首页保留快捷导入；如果需要继续配置后续步骤，请直接进入自动投注总控台。",
                actionHref: "#importSection",
                actionText: "快捷导入",
            },
            {
                title: "绑定托管账号和群组",
                complete: state.accounts.filter(isAuthorizedAccount).length > 0 && state.targets.length > 0,
                description: "自动执行至少需要一个已授权托管账号和一个已通过测试发送的目标群组。",
                actionHref: state.accounts.filter(isAuthorizedAccount).length > 0 ? "/autobet/targets#targetsSection" : "/autobet/accounts#accountsSection",
                actionText: "去对应工作区",
            },
            {
                title: "建立跟单关系",
                complete: state.subscriptions.length > 0,
                description: "跟单、下注模板和群组绑定关系统一在总控台和对应工作区维护。",
                actionHref: "/autobet/subscriptions#subscriptionsSection",
                actionText: "去跟单工作区",
            },
        ];

        checklist.innerHTML = items.map(function (item) {
            return [
                '<article class="checklist-item">',
                '<div class="checklist-head">',
                '<strong class="checklist-title">' + escapeHtml(item.title) + "</strong>",
                renderPill(item.complete ? "已完成" : "待处理", "step-pill " + (item.complete ? "is-done" : "is-pending")),
                "</div>",
                '<p class="checklist-body">' + escapeHtml(item.description) + "</p>",
                '<div class="checklist-foot">',
                '<span class="cell-muted">' + (item.complete ? "当前已完成" : "当前仍需处理") + "</span>",
                '<a class="panel-link" href="' + escapeHtml(item.actionHref) + '">' + escapeHtml(item.actionText) + "</a>",
                "</div>",
                "</article>",
            ].join("");
        }).join("");
    }

    function renderCurrentSetup() {
        const source = primarySource();
        const account = primaryAccount();
        const target = primaryTarget();
        const subscription = primarySubscription();

        document.getElementById("currentSourceName").textContent = source ? source.name : "未导入方案";
        document.getElementById("currentSourceMeta").textContent = source
            ? ((source.source_type || "--") + " · " + (source.visibility || "private"))
            : "先从 AITradingSimulator 复制公开方案页链接或导出地址，再到平台导入。";

        document.getElementById("currentAccountName").textContent = account ? account.label : "未绑定";
        document.getElementById("currentAccountMeta").textContent = account
            ? ((account.phone || "--") + " · " + accountAuthLabel(account))
            : "当前没有可用于执行的 Telegram 托管账号。";

        document.getElementById("currentTargetName").textContent = target ? (target.target_name || target.target_key) : "未配置";
        document.getElementById("currentTargetMeta").textContent = target
            ? ((target.target_key || "--") + " · " + (target.executor_type || "--"))
            : "需要先绑定投递群组，任务才有实际执行落点。";

        document.getElementById("currentStrategyName").textContent = subscription ? "已建立订阅" : "未建立";
        document.getElementById("currentStrategyMeta").textContent = subscription
            ? summarizeStrategy(subscription.strategy || {})
            : "建立订阅后，平台才会把标准信号展开成执行任务。";
    }

    function renderJobs() {
        const items = sortedJobs().slice(0, 6);
        if (!items.length) {
            jobTableBody.innerHTML = '<tr class="empty-row"><td colspan="6">暂无执行记录，先在管理端完成抓取、标准化与派发。</td></tr>';
            jobCardList.innerHTML = '<article class="job-card is-empty"><strong>暂无执行记录</strong><p>先在管理端完成抓取、标准化与派发，或回到自动投注总控台补齐配置。</p></article>';
            return;
        }

        jobTableBody.innerHTML = items.map(function (item) {
            const errorText = item.last_error_message || item.error_message || "--";
            const pillClass = "job-pill is-" + escapeHtml(String(item.status || "pending"));
            return [
                "<tr>",
                "<td><div class=\"cell-stack\"><strong>" + escapeHtml(item.issue_no || "--") + "</strong><span class=\"cell-muted\">" + escapeHtml((item.bet_type || "--") + " / " + (item.bet_value || "--")) + "</span></div></td>",
                "<td><div class=\"cell-stack\"><span class=\"mono-text\">" + escapeHtml(item.target_key || "--") + "</span><span class=\"cell-muted\">" + escapeHtml(item.target_name || "--") + "</span></div></td>",
                "<td><div class=\"cell-stack\"><span>" + escapeHtml(item.telegram_account_label || "--") + "</span><span class=\"cell-muted\">ID " + escapeHtml(item.telegram_account_id == null ? "--" : String(item.telegram_account_id)) + "</span></div></td>",
                "<td>" + renderPill(item.status || "--", pillClass) + "</td>",
                "<td><span class=\"" + (errorText === "--" ? "cell-muted" : "error-text") + "\">" + escapeHtml(errorText) + "</span></td>",
                "<td><div class=\"cell-stack\"><span>" + escapeHtml(formatDateTime(item.updated_at)) + "</span><span class=\"cell-muted\">" + escapeHtml(item.last_executed_at ? ("上次执行 " + formatDateTime(item.last_executed_at)) : "--") + "</span></div></td>",
                "</tr>",
            ].join("");
        }).join("");

        jobCardList.innerHTML = items.map(function (item) {
            const statusClass = "job-pill is-" + escapeHtml(String(item.status || "pending"));
            const errorText = item.last_error_message || item.error_message || "--";
            return [
                '<article class="job-card">',
                '<div class="job-card-head"><div class="cell-stack"><strong>' + escapeHtml(item.issue_no || "--") + '</strong><span class="cell-muted">' + escapeHtml((item.bet_type || "--") + " / " + (item.bet_value || "--")) + "</span></div>" + renderPill(item.status || "--", statusClass) + "</div>",
                '<div class="job-card-grid">',
                '<div class="job-card-item"><span class="job-card-label">投递群组</span><strong>' + escapeHtml(item.target_name || item.target_key || "--") + '</strong><span class="mono-text">' + escapeHtml(item.target_key || "--") + "</span></div>",
                '<div class="job-card-item"><span class="job-card-label">托管账号</span><strong>' + escapeHtml(item.telegram_account_label || "--") + '</strong><span class="cell-muted">ID ' + escapeHtml(item.telegram_account_id == null ? "--" : String(item.telegram_account_id)) + "</span></div>",
                '<div class="job-card-item"><span class="job-card-label">最近更新</span><strong>' + escapeHtml(formatDateTime(item.updated_at)) + '</strong><span class="cell-muted">' + escapeHtml(item.last_executed_at ? ("上次执行 " + formatDateTime(item.last_executed_at)) : "--") + "</span></div>",
                "</div>",
                '<p class="' + (errorText === "--" ? "cell-muted" : "error-text") + '">' + escapeHtml(errorText) + "</p>",
                '<div class="job-card-actions"><a class="panel-link" href="/records">去记录页查看</a></div>',
                "</article>",
            ].join("");
        }).join("");
    }

    function renderAlerts() {
        if (!state.alerts.length) {
            alertList.innerHTML = '<article class="alert-card"><div class="alert-title-row"><strong>暂无需要处理的异常</strong>' + renderPill("INFO", "alert-pill is-info") + '</div><p class="cell-muted">执行器状态、失败任务和通知记录暂时都比较平稳。</p></article>';
            return;
        }

        alertList.innerHTML = state.alerts.slice(0, 6).map(function (item) {
            const severity = String(item.severity || "warning");
            return [
                '<article class="alert-card">',
                '<div class="alert-title-row">',
                '<strong>' + escapeHtml(item.title || "--") + "</strong>",
                renderPill(severity, "alert-pill is-" + escapeHtml(severity)),
                "</div>",
                '<p>' + escapeHtml(item.message || "--") + "</p>",
                '<div class="checklist-foot">',
                '<span class="cell-muted">' + escapeHtml(item.alert_type || "--") + "</span>",
                '<a class="panel-link" href="/alerts">查看全部</a>',
                "</div>",
                "</article>",
            ].join("");
        }).join("");
    }

    async function loadPlatformData() {
        const results = await Promise.all([
            request("/api/platform/sources"),
            request("/api/platform/telegram-accounts"),
            request("/api/platform/subscriptions"),
            request("/api/platform/delivery-targets"),
            request("/api/platform/execution-jobs?limit=20"),
            request("/api/platform/alerts?limit=6"),
        ]);

        state.sources = results[0].items || [];
        state.accounts = results[1].items || [];
        state.subscriptions = results[2].items || [];
        state.targets = results[3].items || [];
        state.jobs = results[4].items || [];
        state.alerts = results[5].items || [];
    }

    async function refreshAll() {
        setButtonBusy(refreshHomeBtn, true, "刷新中...");
        try {
            const user = await loadCurrentUser();
            if (!user) {
                resetCollections();
                setStatus("登录后可查看当前方案、托管账号和最近执行结果。", false);
                return;
            }

            await loadPlatformData();
            renderMetrics();
            renderNextStep();
            renderChecklist();
            renderCurrentSetup();
            renderJobs();
            renderAlerts();

            if (readinessState().ready) {
                setStatus("当前账号已具备自动执行基础条件。首页负责看总览，后续调整统一进入自动投注总控台。", false);
            } else {
                setStatus("还未完成全部托管配置。首页先看缺口，具体操作统一在自动投注总控台和对应工作区完成。", false);
            }
        } catch (error) {
            resetCollections();
            setStatus(error.message || "页面加载失败", true);
        } finally {
            setButtonBusy(refreshHomeBtn, false, "刷新状态");
        }
    }

    function initWorkspaceNav() {
        workspaceLinks.forEach(function (link) {
            link.addEventListener("click", function () {
                const targetHash = link.getAttribute("href");
                if (targetHash) {
                    setActiveWorkspaceLink(targetHash);
                }
            });
        });

        if (!("IntersectionObserver" in window)) {
            return;
        }

        const sections = workspaceLinks.map(function (link) {
            const href = link.getAttribute("href") || "";
            if (!href.startsWith("#")) {
                return null;
            }
            return document.querySelector(href);
        }).filter(Boolean);

        const observer = new IntersectionObserver(function (entries) {
            const visible = entries.filter(function (entry) {
                return entry.isIntersecting;
            }).sort(function (left, right) {
                return left.boundingClientRect.top - right.boundingClientRect.top;
            })[0];

            if (!visible || !visible.target || !visible.target.id) {
                return;
            }
            setActiveWorkspaceLink("#" + visible.target.id);
        }, {
            rootMargin: "-20% 0px -60% 0px",
            threshold: [0.1, 0.3, 0.6],
        });

        sections.forEach(function (section) {
            observer.observe(section);
        });
    }

    document.getElementById("registerBtn").addEventListener("click", async function () {
        try {
            const payload = await request("/api/auth/register", {
                method: "POST",
                body: {
                    username: document.getElementById("authUsername").value,
                    email: document.getElementById("authEmail").value,
                    password: document.getElementById("authPassword").value,
                },
            });
            setCurrentUser(payload.user || null);
            await refreshAll();
            setStatus("注册并登录成功。", false);
        } catch (error) {
            setStatus(error.message, true);
        }
    });

    document.getElementById("loginBtn").addEventListener("click", async function () {
        try {
            const payload = await request("/api/auth/login", {
                method: "POST",
                body: {
                    username: document.getElementById("authUsername").value,
                    password: document.getElementById("authPassword").value,
                },
            });
            setCurrentUser(payload.user || null);
            await refreshAll();
            setStatus("登录成功。", false);
        } catch (error) {
            setStatus(error.message, true);
        }
    });

    document.getElementById("logoutBtn").addEventListener("click", async function () {
        try {
            await request("/api/auth/logout", {
                method: "POST",
                body: {},
            });
            setCurrentUser(null);
            resetCollections();
            setStatus("已退出登录。", false);
        } catch (error) {
            setStatus(error.message, true);
        }
    });

    refreshHomeBtn.addEventListener("click", function () {
        refreshAll();
    });

    fillImportExampleBtn.addEventListener("click", function () {
        importSourceUrlInput.value = "https://your-ai-platform.example.com/public/predictors/12";
        if (!String(importSourceNameInput.value || "").trim()) {
            importSourceNameInput.value = "AITradingSimulator 方案 #12";
        }
        setStatus("已填入 AITradingSimulator 示例公开方案链接。", false);
    });

    importSourceForm.addEventListener("submit", async function (event) {
        event.preventDefault();
        try {
            if (!state.currentUser) {
                throw new Error("请先登录，再导入方案");
            }

            setButtonBusy(importSourceBtn, true, "导入中...");
            const normalized = normalizeAiSourceUrl(importSourceUrlInput.value);
            const existingSource = findImportedAiSourceByUrl(normalized.url);
            if (existingSource) {
                setStatus("该方案已导入，无需重复创建来源。下一步请继续完成账号授权并配置群组。", false);
                focusNextStep();
                return;
            }

            const rawName = String(importSourceNameInput.value || "").trim();
            const payload = await request("/api/platform/sources", {
                method: "POST",
                body: {
                    source_type: "ai_trading_simulator_export",
                    name: rawName || normalized.suggestedName,
                    visibility: "private",
                    config: {
                        fetch: {
                            url: normalized.url,
                            headers: {
                                Accept: "application/json",
                            },
                            timeout: 10,
                        },
                    },
                },
            });
            importSourceForm.reset();
            await refreshAll();
            setStatus("方案导入成功，已创建来源 #" + payload.item.id + "，下一步请继续完成账号授权并配置群组。", false);
            focusNextStep();
        } catch (error) {
            setStatus(error.message || "导入方案失败", true);
        } finally {
            setButtonBusy(importSourceBtn, false, "导入方案");
        }
    });

    initWorkspaceNav();
    refreshAll();
}());
