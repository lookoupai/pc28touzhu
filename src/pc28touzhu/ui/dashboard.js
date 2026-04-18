(function () {
    const ROUTES = {
        "/admin": {
            key: "overview",
            section: "值班总览",
            eyebrow: "后台控制台",
            title: "值班总览",
            description: "只看平台健康、执行链路和待处理事项，不再把对象 CRUD 与调试操作堆在一个首页里。",
            load: loadOverviewData,
            render: renderOverviewPage,
        },
        "/admin/sources": {
            key: "sources",
            section: "来源接入中心",
            eyebrow: "来源接入",
            title: "来源接入中心",
            description: "承接来源管理、抓取、原始内容和标准化链路，默认先列表、筛选和详情，再进入高级录入。",
            load: loadSourcesData,
            render: renderSourcesPage,
        },
        "/admin/signals": {
            key: "signals",
            section: "信号中心",
            eyebrow: "信号管理",
            title: "信号中心",
            description: "查看标准信号、派发结果和人工重派，标准信号录入改为次级入口而不是首页主视觉。",
            load: loadSignalsData,
            render: renderSignalsPage,
        },
        "/admin/execution": {
            key: "execution",
            section: "执行中心",
            eyebrow: "执行监控",
            title: "执行中心",
            description: "集中查看执行任务、失败回看、执行器状态和人工重试入口，服务值班排障。",
            load: loadExecutionData,
            render: renderExecutionPage,
        },
        "/admin/alerts": {
            key: "alerts",
            section: "告警中心",
            eyebrow: "告警处理",
            title: "告警中心",
            description: "按级别、通知状态和类型处理告警，不再和其他对象混排。",
            load: loadAlertsData,
            render: renderAlertsPage,
        },
        "/admin/telegram": {
            key: "telegram",
            section: "Telegram 配置",
            eyebrow: "Telegram 运行配置",
            title: "Telegram 配置中心",
            description: "统一维护告警通知、收益查询 Bot 和日报推送配置。保存后对应 worker 会在下一轮自动读取新配置。",
            load: loadTelegramData,
            render: renderTelegramSettingsPage,
        },
        "/admin/support": {
            key: "support",
            section: "支持查询页",
            eyebrow: "支持查询",
            title: "支持查询页",
            description: "从排障和支持视角查询用户、托管账号、群组、模板和跟单配置，配置动作继续回到 /autobet。",
            load: loadSupportData,
            render: renderSupportPage,
        },
    };
    const LEGACY_HASH_REDIRECTS = {
        "#sourcesSection": "/admin/sources",
        "#signalsSection": "/admin/signals",
        "#rawItemsSection": "/admin/sources",
        "#executionJobsSection": "/admin/execution",
        "#executorsSection": "/admin/execution",
        "#failuresSection": "/admin/execution",
        "#alertsSection": "/admin/alerts",
        "#accountsSection": "/admin/support",
        "#subscriptionsSection": "/admin/support",
        "#targetsSection": "/admin/support",
    };
    const SOURCE_TEMPLATES = {
        ai_trading_simulator_export: {
            fetch: {
                url: "https://your-ai-platform.example.com/api/export/predictors/12/signals?view=execution",
                headers: {
                    Accept: "application/json",
                },
                timeout: 10,
            },
        },
        http_json: {
            fetch: {
                url: "https://example.com/feed.json",
                issue_no_path: "data.issue_no",
                external_item_id_path: "data.id",
                published_at_path: "meta.published_at",
            },
        },
    };
    const DEFAULT_FILTERS = {
        overview: {},
        sources: {
            sourceType: "",
            visibility: "",
            status: "",
            rawSourceId: "",
            parseStatus: "",
        },
        signals: {
            sourceId: "",
            issueQuery: "",
        },
        execution: {
            status: "",
            query: "",
        },
        alerts: {
            severity: "",
            notificationStatus: "",
            query: "",
        },
        telegram: {},
        support: {
            userId: "",
            query: "",
        },
    };
    const state = {
        currentUser: null,
        currentRoute: null,
        pageData: null,
        filters: cloneFilters(DEFAULT_FILTERS),
    };

    const statusMessage = document.getElementById("statusMessage");
    const adminContent = document.getElementById("adminContent");
    const adminPageEyebrow = document.getElementById("adminPageEyebrow");
    const adminPageTitle = document.getElementById("adminPageTitle");
    const adminPageDescription = document.getElementById("adminPageDescription");
    const adminCurrentSection = document.getElementById("adminCurrentSection");
    const adminLastRefreshAt = document.getElementById("adminLastRefreshAt");
    const refreshButton = document.getElementById("adminRefreshBtn");
    const routeLinks = Array.from(document.querySelectorAll("[data-admin-link]"));

    function cloneFilters(value) {
        return JSON.parse(JSON.stringify(value));
    }

    function normalizePath(pathname) {
        const text = String(pathname || "").trim();
        if (!text || text === "/") {
            return "/";
        }
        return text.endsWith("/") ? text.slice(0, -1) : text;
    }

    function resolveRoute(pathname) {
        const path = normalizePath(pathname);
        return ROUTES[path] ? path : "/admin";
    }

    function maybeRedirectLegacyHash() {
        const currentPath = resolveRoute(window.location.pathname);
        const target = LEGACY_HASH_REDIRECTS[window.location.hash || ""];
        if (!target || currentPath !== "/admin") {
            return false;
        }
        window.location.replace(target);
        return true;
    }

    function setStatus(message, isError) {
        statusMessage.textContent = message || "";
        statusMessage.classList.toggle("is-error", Boolean(isError));
    }

    function setCurrentUser(user) {
        state.currentUser = user || null;
        window.PlatformAuthPanel.sync({
            user: state.currentUser,
            heroUsernameId: "adminSessionUsername",
            heroUserMetaId: "adminSessionMeta",
            heroLoggedInText: "后台会话已就绪，可以查看值班与排障信息。",
            heroLoggedOutText: "当前后台会话不可用，请重新登录。",
            panelTitleLoggedIn: "当前会话",
            panelTitleLoggedOut: "平台认证",
        });
    }

    function ensureAuthenticated() {
        if (!state.currentUser) {
            throw new Error("后台会话已失效，请重新登录");
        }
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

    function loadCurrentUser() {
        return request("/api/auth/me").then(function (payload) {
            setCurrentUser(payload.user || null);
        });
    }

    function formatDateTime(value) {
        const text = String(value || "").trim();
        if (!text) {
            return "--";
        }
        return text.replace("T", " ").replace("Z", " UTC");
    }

    function formatNow() {
        return new Date().toLocaleString("zh-CN", { hour12: false });
    }

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function formatJson(value) {
        return JSON.stringify(value, null, 2);
    }

    function formatJsonPreview(value, fallback) {
        if (!value || typeof value !== "object") {
            return escapeHtml(fallback || "--");
        }
        const text = JSON.stringify(value);
        return escapeHtml(text.length > 180 ? (text.slice(0, 177) + "...") : text);
    }

    function truncateText(value, length) {
        const text = String(value == null ? "" : value);
        if (text.length <= length) {
            return text;
        }
        return text.slice(0, Math.max(0, length - 3)) + "...";
    }

    function labelStatus(value) {
        return window.PlatformUiText.labelStatus(value);
    }

    function labelSourceType(value) {
        return window.PlatformUiText.labelSourceType(value);
    }

    function renderStatusPill(status) {
        const raw = String(status || "").trim() || "--";
        return '<span class="status-pill is-' + escapeHtml(raw.toLowerCase()) + '">' + escapeHtml(labelStatus(raw)) + "</span>";
    }

    function renderRows(items, columns, emptyText) {
        if (!items.length) {
            return '<tr class="empty-row"><td colspan="' + columns.length + '">' + escapeHtml(emptyText || "暂无数据") + "</td></tr>";
        }
        return items.map(function (item) {
            return "<tr>" + columns.map(function (column) {
                return "<td>" + column(item) + "</td>";
            }).join("") + "</tr>";
        }).join("");
    }

    function renderTable(columns, rowsHtml) {
        return '<div class="table-wrap"><table class="data-table"><thead><tr>' + columns.map(function (item) {
            return "<th>" + escapeHtml(item) + "</th>";
        }).join("") + "</tr></thead><tbody>" + rowsHtml + "</tbody></table></div>";
    }

    function renderSummaryList(items) {
        if (!items.length) {
            return '<div class="empty-state"><strong>暂无待关注摘要</strong><p>当前没有需要额外说明的聚合状态。</p></div>';
        }
        return '<ul class="summary-list">' + items.map(function (item) {
            return "<li><span>" + escapeHtml(item.label) + "</span><strong>" + escapeHtml(String(item.value)) + "</strong></li>";
        }).join("") + "</ul>";
    }

    function renderMetricCard(config) {
        return '' +
            '<article class="metric-card">' +
                '<span class="metric-label">' + escapeHtml(config.label) + '</span>' +
                '<div class="metric-value-row">' +
                    '<strong class="metric-value">' + escapeHtml(String(config.value)) + '</strong>' +
                    renderStatusPill(config.status || "ok") +
                '</div>' +
                '<p class="metric-meta">' + escapeHtml(config.meta || "--") + "</p>" +
                '<div class="metric-foot"><span>' + escapeHtml(config.footLabel || "--") + '</span><strong>' + escapeHtml(String(config.footValue || "--")) + "</strong></div>" +
            "</article>";
    }

    function renderTodoList(items) {
        if (!items.length) {
            return '<div class="empty-state"><strong>暂无待处理事项</strong><p>当前没有需要人工立即跟进的后台事件。</p></div>';
        }
        return '<ul class="todo-list">' + items.map(function (item) {
            return '' +
                '<li class="todo-item is-' + escapeHtml(item.severity) + '">' +
                    '<div class="todo-meta">' +
                        renderStatusPill(item.severity) +
                        '<span class="cell-muted">' + escapeHtml(item.meta) + "</span>" +
                    "</div>" +
                    '<strong class="todo-title">' + escapeHtml(item.title) + "</strong>" +
                    '<p class="helper-text">' + escapeHtml(item.detail) + "</p>" +
                    '<div class="todo-actions"><a class="todo-link" href="' + escapeHtml(item.href) + '">' + escapeHtml(item.linkText) + "</a></div>" +
                "</li>";
        }).join("") + "</ul>";
    }

    function renderEmptyState(title, message, actionHtml) {
        return '<div class="empty-state"><strong>' + escapeHtml(title) + "</strong><p>" + escapeHtml(message) + "</p>" + (actionHtml || "") + "</div>";
    }

    function renderToolbar(groups, actions) {
        return '<div class="toolbar">' + groups.join("") + '<div class="toolbar-actions">' + (actions || []).join("") + "</div></div>";
    }

    function renderSelectField(label, key, value, options) {
        return '' +
            '<label class="toolbar-group">' +
                '<span>' + escapeHtml(label) + "</span>" +
                '<select class="inline-select" data-filter-key="' + escapeHtml(key) + '">' +
                    options.map(function (option) {
                        const optionValue = String(option.value == null ? "" : option.value);
                        return '<option value="' + escapeHtml(optionValue) + '"' + (optionValue === String(value || "") ? " selected" : "") + ">" + escapeHtml(option.label) + "</option>";
                    }).join("") +
                "</select>" +
            "</label>";
    }

    function renderInputField(label, key, value, placeholder) {
        return '' +
            '<label class="toolbar-group">' +
                '<span>' + escapeHtml(label) + "</span>" +
                '<input class="search-input" data-filter-key="' + escapeHtml(key) + '" data-filter-live="true" type="text" value="' + escapeHtml(value || "") + '" placeholder="' + escapeHtml(placeholder || "") + '">' +
            "</label>";
    }

    function countBy(items, getter) {
        return items.reduce(function (result, item) {
            const key = String(getter(item) || "").trim() || "--";
            result[key] = (result[key] || 0) + 1;
            return result;
        }, {});
    }

    function getFilterState() {
        const routeConfig = ROUTES[state.currentRoute] || ROUTES["/admin"];
        return state.filters[routeConfig.key] || {};
    }

    function resetCurrentRouteFilters() {
        const routeConfig = ROUTES[state.currentRoute] || ROUTES["/admin"];
        state.filters[routeConfig.key] = cloneFilters(DEFAULT_FILTERS[routeConfig.key] || {});
    }

    function applyRouteMeta(routePath) {
        const config = ROUTES[routePath];
        adminPageEyebrow.textContent = config.eyebrow;
        adminPageTitle.textContent = config.title;
        adminPageDescription.textContent = config.description;
        adminCurrentSection.textContent = config.section;
        document.title = "pc28touzhu · " + config.title;
        routeLinks.forEach(function (link) {
            link.classList.toggle("is-active", link.getAttribute("data-admin-link") === routePath);
        });
    }

    function updateRefreshTimestamp() {
        adminLastRefreshAt.textContent = formatNow();
    }

    function renderLoadingState() {
        adminContent.innerHTML = '' +
            '<section class="panel loading-panel">' +
                '<div class="panel-header"><div><p class="panel-kicker">数据加载中</p><h2>正在读取后台数据</h2></div></div>' +
                '<div class="skeleton-grid"><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div><div class="skeleton-card"></div></div>' +
            "</section>";
    }

    function renderErrorState(message) {
        adminContent.innerHTML = '' +
            '<section class="panel">' +
                '<div class="panel-header"><div><p class="panel-kicker">加载失败</p><h2>后台页面暂时不可用</h2></div></div>' +
                '<div class="empty-state"><strong>本页数据未能成功加载</strong><p>' + escapeHtml(message || "请稍后重试") + '</p></div>' +
            "</section>";
    }

    function rerenderCurrentRoute() {
        if (!state.currentRoute || !state.pageData) {
            return;
        }
        ROUTES[state.currentRoute].render(state.pageData);
    }

    function parseJsonField(text, fieldName) {
        const raw = String(text || "").trim();
        if (!raw) {
            return {};
        }
        try {
            const value = JSON.parse(raw);
            if (value && typeof value === "object" && !Array.isArray(value)) {
                return value;
            }
        } catch (error) {
            throw new Error(fieldName + " 必须是合法 JSON 对象");
        }
        throw new Error(fieldName + " 必须是合法 JSON 对象");
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
            throw new Error("公开方案页链接或导出地址不能为空");
        }

        let parsed;
        try {
            parsed = new URL(text);
        } catch (error) {
            throw new Error("公开方案页链接或导出地址格式不正确");
        }

        if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
            throw new Error("公开方案页链接或导出地址必须使用 http 或 https");
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

    function buildAiSourceConfig(rawUrl) {
        const normalized = normalizeAiSourceUrl(rawUrl);
        return {
            normalizedUrl: normalized.url,
            suggestedName: normalized.suggestedName,
            config: {
                fetch: {
                    url: normalized.url,
                    headers: {
                        Accept: "application/json",
                    },
                    timeout: 10,
                },
            },
        };
    }

    function accountAuthState(item) {
        const meta = item && item.meta && typeof item.meta === "object" ? item.meta : {};
        return String(item && item.auth_state ? item.auth_state : (meta.auth_state || "new")).trim() || "new";
    }

    function accountAuthMode(item) {
        const meta = item && item.meta && typeof item.meta === "object" ? item.meta : {};
        return String(item && item.auth_mode ? item.auth_mode : (meta.auth_mode || "phone_login")).trim() || "phone_login";
    }

    function accountAuthLabel(item) {
        const authState = accountAuthState(item);
        const authMode = accountAuthMode(item);
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
        return "待处理";
    }

    function sourceNameMap(items) {
        const result = {};
        (items || []).forEach(function (item) {
            result[String(item.id)] = item.name || ("#" + item.id);
        });
        return result;
    }

    function loadOverviewData() {
        ensureAuthenticated();
        return Promise.all([
            request("/api/platform/sources"),
            request("/api/platform/signals"),
            request("/api/platform/execution-jobs?limit=80"),
            request("/api/platform/executors?limit=20"),
            request("/api/platform/alerts?limit=50"),
            request("/api/platform/execution-failures?limit=20"),
        ]).then(function (payloads) {
            return {
                sources: payloads[0].items || [],
                signals: payloads[1].items || [],
                jobs: payloads[2].items || [],
                executors: payloads[3].items || [],
                alerts: payloads[4].items || [],
                failures: payloads[5].items || [],
            };
        });
    }

    function loadSourcesData() {
        ensureAuthenticated();
        return Promise.all([
            request("/api/platform/sources"),
            request("/api/platform/raw-items"),
            request("/api/platform/signals"),
        ]).then(function (payloads) {
            return {
                sources: payloads[0].items || [],
                rawItems: payloads[1].items || [],
                signals: payloads[2].items || [],
            };
        });
    }

    function loadSignalsData() {
        ensureAuthenticated();
        return Promise.all([
            request("/api/platform/sources"),
            request("/api/platform/signals"),
            request("/api/platform/execution-jobs?limit=100"),
        ]).then(function (payloads) {
            return {
                sources: payloads[0].items || [],
                signals: payloads[1].items || [],
                jobs: payloads[2].items || [],
            };
        });
    }

    function loadExecutionData() {
        ensureAuthenticated();
        return Promise.all([
            request("/api/platform/execution-jobs?limit=100"),
            request("/api/platform/executors?limit=20"),
            request("/api/platform/execution-failures?limit=30"),
        ]).then(function (payloads) {
            return {
                jobs: payloads[0].items || [],
                executors: payloads[1].items || [],
                failures: payloads[2].items || [],
            };
        });
    }

    function loadAlertsData() {
        ensureAuthenticated();
        return Promise.all([
            request("/api/platform/alerts?limit=100"),
            request("/api/platform/execution-failures?limit=30"),
        ]).then(function (payloads) {
            return {
                alerts: payloads[0].items || [],
                failures: payloads[1].items || [],
            };
        });
    }

    function loadTelegramData() {
        ensureAuthenticated();
        return request("/api/platform/admin/telegram-settings");
    }

    function loadSupportData() {
        ensureAuthenticated();
        return request("/api/platform/admin/support");
    }

    function deriveOverviewState(payload) {
        const sources = payload.sources || [];
        const signals = payload.signals || [];
        const jobs = payload.jobs || [];
        const executors = payload.executors || [];
        const alerts = payload.alerts || [];
        const failures = payload.failures || [];

        const executorBuckets = countBy(executors, function (item) {
            return item.heartbeat_status || item.status || "--";
        });
        const alertBuckets = countBy(alerts, function (item) {
            return item.severity || "--";
        });
        const jobBuckets = countBy(jobs, function (item) {
            return item.status || "--";
        });
        const retryBuckets = countBy(failures, function (item) {
            return item.auto_retry_state || "--";
        });

        const offlineExecutors = executors.filter(function (item) {
            return String(item.heartbeat_status || "").toLowerCase() === "offline";
        });
        const staleExecutors = executors.filter(function (item) {
            return String(item.heartbeat_status || "").toLowerCase() === "stale";
        });
        const criticalAlerts = alerts.filter(function (item) {
            return String(item.severity || "").toLowerCase() === "critical";
        });
        const warningAlerts = alerts.filter(function (item) {
            return String(item.severity || "").toLowerCase() === "warning";
        });
        const unsentAlerts = alerts.filter(function (item) {
            return String(item.notification_status || "").toLowerCase() !== "sent";
        });
        const exhaustedFailures = failures.filter(function (item) {
            return String(item.auto_retry_state || "").toLowerCase() === "exhausted";
        });
        const pendingFailures = failures.filter(function (item) {
            return ["due", "retrying", "manual_only"].indexOf(String(item.auto_retry_state || "").toLowerCase()) >= 0;
        });

        let healthStatus = "ok";
        let healthText = "运行稳定";
        let healthMeta = "当前未发现需要立即升级处理的后台事件。";
        if (criticalAlerts.length || offlineExecutors.length || exhaustedFailures.length) {
            healthStatus = "critical";
            healthText = "需要立即处理";
            healthMeta = "存在严重告警、离线执行器或重试耗尽任务。";
        } else if (warningAlerts.length || staleExecutors.length || pendingFailures.length) {
            healthStatus = "warning";
            healthText = "需要关注";
            healthMeta = "存在延迟执行器、警告告警或待处理失败任务。";
        }

        const todoItems = [];
        criticalAlerts.slice(0, 3).forEach(function (item) {
            todoItems.push({
                severity: "critical",
                title: item.title || "严重告警",
                meta: "告警中心 · " + labelStatus(item.notification_status || "pending"),
                detail: item.message || "请进入告警中心确认原因并处理通知状态。",
                href: "/admin/alerts",
                linkText: "打开告警中心",
            });
        });
        offlineExecutors.slice(0, 3).forEach(function (item) {
            todoItems.push({
                severity: "critical",
                title: "执行器离线: " + (item.executor_id || "--"),
                meta: "执行中心 · 最近心跳 " + formatDateTime(item.last_seen_at),
                detail: item.last_failure_error_message || "需要确认节点是否失联、进程是否退出或凭据是否异常。",
                href: "/admin/execution",
                linkText: "打开执行中心",
            });
        });
        exhaustedFailures.slice(0, 3).forEach(function (item) {
            todoItems.push({
                severity: "warning",
                title: "失败任务已耗尽: #" + (item.job_id || "--"),
                meta: "执行中心 · 自动重试 " + labelStatus(item.auto_retry_state || "exhausted"),
                detail: item.error_message || "请检查目标、执行器或信号内容后决定是否人工重试。",
                href: "/admin/execution",
                linkText: "查看失败记录",
            });
        });
        unsentAlerts.slice(0, 2).forEach(function (item) {
            todoItems.push({
                severity: "warning",
                title: "告警未完成通知: " + (item.title || "--"),
                meta: "告警中心 · " + labelStatus(item.notification_status || "pending"),
                detail: item.message || "请确认通知渠道、发送错误和当前处理状态。",
                href: "/admin/alerts",
                linkText: "处理通知状态",
            });
        });

        return {
            sources: sources,
            signals: signals,
            jobs: jobs,
            executors: executors,
            alerts: alerts,
            failures: failures,
            healthStatus: healthStatus,
            healthText: healthText,
            healthMeta: healthMeta,
            executorBuckets: executorBuckets,
            alertBuckets: alertBuckets,
            jobBuckets: jobBuckets,
            retryBuckets: retryBuckets,
            unsentAlerts: unsentAlerts,
            todoItems: todoItems.slice(0, 6),
        };
    }

    function renderOverviewPage(data) {
        const normalized = deriveOverviewState(data);
        adminContent.innerHTML = '' +
            '<div class="overview-layout">' +
                '<section class="metrics-grid">' +
                    renderMetricCard({
                        label: "平台健康",
                        value: normalized.healthText,
                        status: normalized.healthStatus,
                        meta: normalized.healthMeta,
                        footLabel: "告警 / 失败",
                        footValue: String(normalized.alerts.length) + " / " + String(normalized.failures.length),
                    }) +
                    renderMetricCard({
                        label: "执行器状态",
                        value: String(normalized.executors.length),
                        status: normalized.executorBuckets.offline ? "critical" : (normalized.executorBuckets.stale ? "warning" : "online"),
                        meta: "在线 " + String(normalized.executorBuckets.online || 0) + "，延迟 " + String(normalized.executorBuckets.stale || 0) + "，离线 " + String(normalized.executorBuckets.offline || 0),
                        footLabel: "最近失败",
                        footValue: String(normalized.executors.filter(function (item) { return item.last_failure_at; }).length),
                    }) +
                    renderMetricCard({
                        label: "失败任务摘要",
                        value: String(normalized.failures.length),
                        status: normalized.retryBuckets.exhausted ? "critical" : (normalized.retryBuckets.due || normalized.retryBuckets.retrying ? "warning" : "ok"),
                        meta: "待重试 " + String((normalized.retryBuckets.due || 0) + (normalized.retryBuckets.retrying || 0)) + "，已耗尽 " + String(normalized.retryBuckets.exhausted || 0),
                        footLabel: "任务总量",
                        footValue: String(normalized.jobs.length),
                    }) +
                    renderMetricCard({
                        label: "告警摘要",
                        value: String(normalized.alerts.length),
                        status: normalized.alertBuckets.critical ? "critical" : (normalized.alertBuckets.warning ? "warning" : "ok"),
                        meta: "严重 " + String(normalized.alertBuckets.critical || 0) + "，警告 " + String(normalized.alertBuckets.warning || 0) + "，待通知 " + String(normalized.unsentAlerts.length),
                        footLabel: "来源 / 信号",
                        footValue: String(normalized.sources.length) + " / " + String(normalized.signals.length),
                    }) +
                "</section>" +
                '<section class="content-grid">' +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">待处理事项</p><h2>值班队列</h2></div></div>' +
                        renderTodoList(normalized.todoItems) +
                    "</article>" +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">摘要</p><h2>当前分布</h2></div></div>' +
                        '<div class="status-card"><div class="status-head"><strong>任务状态</strong>' + renderStatusPill(normalized.jobBuckets.failed ? "warning" : "ok") + "</div>" +
                            renderSummaryList([
                                { label: "待执行", value: normalized.jobBuckets.pending || 0 },
                                { label: "已送达", value: normalized.jobBuckets.delivered || 0 },
                                { label: "失败", value: normalized.jobBuckets.failed || 0 },
                                { label: "已跳过/过期", value: (normalized.jobBuckets.skipped || 0) + (normalized.jobBuckets.expired || 0) },
                            ]) +
                        "</div>" +
                        '<div class="status-card"><div class="status-head"><strong>告警级别</strong>' + renderStatusPill(normalized.alertBuckets.critical ? "critical" : "ok") + "</div>" +
                            renderSummaryList([
                                { label: "严重", value: normalized.alertBuckets.critical || 0 },
                                { label: "警告", value: normalized.alertBuckets.warning || 0 },
                                { label: "提示", value: normalized.alertBuckets.info || 0 },
                                { label: "未完成通知", value: normalized.unsentAlerts.length },
                            ]) +
                        "</div>" +
                    "</article>" +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">执行器状态</p><h2>执行器心跳与最近表现</h2></div><a class="ghost-btn link-btn" href="/admin/execution">进入执行中心</a></div>' +
                    renderTable(["执行器", "心跳", "版本", "投递", "失败", "能力"], renderRows(normalized.executors.slice(0, 8), [
                        function (item) {
                            return '<div class="cell-stack"><strong>' + escapeHtml(item.executor_id || "--") + '</strong><span class="cell-muted">' + escapeHtml(labelStatus(item.status || "--")) + "</span></div>";
                        },
                        function (item) {
                            const age = item.heartbeat_age_seconds == null ? "--" : String(item.heartbeat_age_seconds) + "s";
                            return '<div class="cell-stack">' + renderStatusPill(item.heartbeat_status || "--") + '<span class="cell-muted">' + escapeHtml(formatDateTime(item.last_seen_at)) + " · " + escapeHtml(age) + "</span></div>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><span>' + escapeHtml(item.version || "--") + '</span><span class="cell-muted">' + escapeHtml(formatDateTime(item.last_executed_at)) + "</span></div>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><strong>' + escapeHtml(String(item.total_attempt_count || 0)) + '</strong><span class="cell-muted">已送达 ' + escapeHtml(String(item.delivered_attempt_count || 0)) + "</span></div>";
                        },
                        function (item) {
                            const failureText = item.last_failure_error_message || "--";
                            return '<div class="cell-stack"><strong class="' + (failureText === "--" ? "cell-muted" : "error-text") + '">' + escapeHtml(String(item.failed_attempt_count || 0)) + '</strong><span class="' + (failureText === "--" ? "cell-muted" : "error-text") + '">' + escapeHtml(failureText) + "</span></div>";
                        },
                        function (item) {
                            return '<span class="mono-text">' + formatJsonPreview(item.capabilities || {}, "--") + "</span>";
                        },
                    ], "暂无执行器状态")) +
                "</section>" +
                '<section class="content-grid">' +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">最近失败</p><h2>失败任务摘要</h2></div><a class="ghost-btn link-btn" href="/admin/execution">查看执行详情</a></div>' +
                        renderTable(["任务", "信号", "执行器", "目标", "重试状态", "错误", "执行时间"], renderRows(normalized.failures.slice(0, 8), [
                            function (item) {
                                return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.job_id || "--")) + '</strong><span class="cell-muted">attempt #' + escapeHtml(String(item.attempt_no || 0)) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(String(item.signal_id || "--")) + '</strong><span class="cell-muted">' + escapeHtml((item.issue_no || "--") + " · " + (item.bet_value || "--")) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><span class="mono-text">' + escapeHtml(item.executor_instance_id || "--") + '</span><span class="cell-muted">' + escapeHtml(item.telegram_account_label || "--") + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><span class="mono-text">' + escapeHtml(item.target_key || "--") + '</span><span class="cell-muted">' + escapeHtml(item.target_name || "--") + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack">' + renderStatusPill(item.auto_retry_state || "--") + '<span class="cell-muted">' + escapeHtml(item.next_retry_at ? formatDateTime(item.next_retry_at) : "--") + "</span></div>";
                            },
                            function (item) {
                                return '<span class="error-text">' + escapeHtml(item.error_message || "--") + "</span>";
                            },
                            function (item) {
                                return escapeHtml(formatDateTime(item.executed_at));
                            },
                        ], "暂无失败任务")) +
                    "</article>" +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">告警摘要</p><h2>最新告警</h2></div><a class="ghost-btn link-btn" href="/admin/alerts">进入告警中心</a></div>' +
                        renderTable(["级别", "类型", "标题", "通知"], renderRows(normalized.alerts.slice(0, 8), [
                            function (item) {
                                return renderStatusPill(item.severity || "--");
                            },
                            function (item) {
                                return '<span class="mono-text">' + escapeHtml(item.alert_type || "--") + "</span>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(item.title || "--") + '</strong><span class="cell-muted">' + escapeHtml(item.message || "--") + "</span></div>";
                            },
                            function (item) {
                                const notification = item.notification || {};
                                const sentText = notification.last_sent_at ? formatDateTime(notification.last_sent_at) : "--";
                                const errorText = notification.last_error || "--";
                                return '<div class="cell-stack">' + renderStatusPill(item.notification_status || "pending") + '<span class="cell-muted">' + escapeHtml(sentText) + '</span><span class="' + (errorText === "--" ? "cell-muted" : "error-text") + '">' + escapeHtml(errorText) + "</span></div>";
                            },
                        ], "暂无告警")) +
                    "</article>" +
                "</section>" +
            "</div>";
        setStatus("已刷新后台值班总览。", false);
    }

    function renderSourcesPage(data) {
        const filters = getFilterState();
        const rawCounts = countBy(data.rawItems || [], function (item) { return item.parse_status || "--"; });
        const signalCountBySource = countBy(data.signals || [], function (item) { return item.source_id; });
        const rawCountBySource = countBy(data.rawItems || [], function (item) { return item.source_id; });
        const sourceOptions = [{ value: "", label: "全部来源" }].concat((data.sources || []).map(function (item) {
            return { value: String(item.id), label: "#" + item.id + " · " + item.name };
        }));
        const sourceTypeOptions = [{ value: "", label: "全部类型" }].concat(Array.from(new Set((data.sources || []).map(function (item) {
            return item.source_type;
        }))).map(function (value) {
            return { value: value, label: labelSourceType(value) };
        }));
        const visibilityOptions = [
            { value: "", label: "全部可见性" },
            { value: "private", label: "私有" },
            { value: "public", label: "公开" },
        ];
        const statusOptions = [
            { value: "", label: "全部状态" },
            { value: "active", label: "已启用" },
            { value: "inactive", label: "已停用" },
            { value: "archived", label: "已归档" },
        ];
        const parseStatusOptions = [
            { value: "", label: "全部解析状态" },
            { value: "pending", label: "待处理" },
            { value: "parsed", label: "已解析" },
            { value: "failed", label: "失败" },
        ];
        const filteredSources = (data.sources || []).filter(function (item) {
            return (!filters.sourceType || item.source_type === filters.sourceType) &&
                (!filters.visibility || item.visibility === filters.visibility) &&
                (!filters.status || item.status === filters.status);
        });
        const filteredRawItems = (data.rawItems || []).filter(function (item) {
            return (!filters.rawSourceId || String(item.source_id) === filters.rawSourceId) &&
                (!filters.parseStatus || String(item.parse_status || "") === filters.parseStatus);
        });
        adminContent.innerHTML = '' +
            '<div class="overview-layout">' +
                '<section class="metrics-grid">' +
                    renderMetricCard({
                        label: "来源总数",
                        value: String((data.sources || []).length),
                        status: filteredSources.length !== (data.sources || []).length ? "warning" : "ok",
                        meta: "当前筛选命中 " + String(filteredSources.length) + " 个来源。",
                        footLabel: "启用中",
                        footValue: String((data.sources || []).filter(function (item) { return item.status === "active"; }).length),
                    }) +
                    renderMetricCard({
                        label: "待标准化",
                        value: String(rawCounts.pending || 0),
                        status: rawCounts.pending ? "warning" : "ok",
                        meta: "原始内容已入库但尚未进入标准化链路。",
                        footLabel: "已失败",
                        footValue: String(rawCounts.failed || 0),
                    }) +
                    renderMetricCard({
                        label: "原始内容",
                        value: String((data.rawItems || []).length),
                        status: rawCounts.failed ? "warning" : "ok",
                        meta: "用于排查抓取质量和标准化失败原因。",
                        footLabel: "已解析",
                        footValue: String(rawCounts.parsed || 0),
                    }) +
                    renderMetricCard({
                        label: "标准化产出",
                        value: String((data.signals || []).length),
                        status: (data.signals || []).length ? "ok" : "warning",
                        meta: "原始内容进入信号中心前，先在这里确认链路是否通畅。",
                        footLabel: "关联来源",
                        footValue: String(Object.keys(signalCountBySource).length),
                    }) +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">来源列表</p><h2>来源接入与抓取排障</h2></div></div>' +
                    renderToolbar([
                        renderSelectField("来源类型", "sourceType", filters.sourceType, sourceTypeOptions),
                        renderSelectField("可见性", "visibility", filters.visibility, visibilityOptions),
                        renderSelectField("状态", "status", filters.status, statusOptions),
                    ], [
                        '<button class="ghost-btn" type="button" data-action="reset-filters">重置筛选</button>',
                    ]) +
                    renderTable(["来源", "类型", "可见性", "状态", "内容规模", "配置摘要", "操作"], renderRows(filteredSources, [
                        function (item) {
                            return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + " · " + escapeHtml(item.name || "--") + '</strong><span class="cell-muted">拥有者 #' + escapeHtml(String(item.owner_user_id == null ? "--" : item.owner_user_id)) + " · 更新于 " + escapeHtml(formatDateTime(item.updated_at)) + "</span></div>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><span>' + escapeHtml(labelSourceType(item.source_type || "--")) + '</span><span class="cell-muted">' + escapeHtml(item.source_type || "--") + "</span></div>";
                        },
                        function (item) { return renderStatusPill(item.visibility || "--"); },
                        function (item) { return renderStatusPill(item.status || "--"); },
                        function (item) {
                            return '<div class="cell-stack"><strong>raw ' + escapeHtml(String(rawCountBySource[item.id] || 0)) + '</strong><span class="cell-muted">signal ' + escapeHtml(String(signalCountBySource[item.id] || 0)) + "</span></div>";
                        },
                        function (item) {
                            const fetchConfig = item.config && item.config.fetch ? item.config.fetch : {};
                            return '<div class="cell-stack"><span class="mono-text">' + escapeHtml(truncateText(fetchConfig.url || "--", 80)) + '</span><span class="cell-muted">' + escapeHtml(formatJsonPreview(item.config || {}, "--")) + "</span></div>";
                        },
                        function (item) {
                            return '<button class="ghost-btn table-action-btn" type="button" data-action="fetch-source" data-id="' + escapeHtml(String(item.id)) + '">手动抓取</button>';
                        },
                    ], "当前筛选下没有来源")) +
                "</section>" +
                '<section class="content-grid">' +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">原始内容</p><h2>抓取结果与标准化入口</h2></div></div>' +
                        renderToolbar([
                            renderSelectField("来源过滤", "rawSourceId", filters.rawSourceId, sourceOptions),
                            renderSelectField("解析状态", "parseStatus", filters.parseStatus, parseStatusOptions),
                        ], []) +
                        renderTable(["Raw Item", "来源", "期号/发布时间", "解析状态", "错误", "负载预览", "操作"], renderRows(filteredRawItems, [
                            function (item) {
                                return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + '</strong><span class="cell-muted">外部 ID ' + escapeHtml(item.external_item_id || "--") + "</span></div>";
                            },
                            function (item) {
                                const source = (data.sources || []).find(function (entry) { return Number(entry.id) === Number(item.source_id); }) || null;
                                return '<div class="cell-stack"><strong>' + escapeHtml(source ? source.name : ("#" + item.source_id)) + '</strong><span class="cell-muted">source #' + escapeHtml(String(item.source_id)) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(item.issue_no || "--") + '</strong><span class="cell-muted">' + escapeHtml(formatDateTime(item.published_at)) + "</span></div>";
                            },
                            function (item) { return renderStatusPill(item.parse_status || "--"); },
                            function (item) {
                                return item.parse_error ? '<span class="error-text">' + escapeHtml(item.parse_error) + "</span>" : '<span class="cell-muted">--</span>';
                            },
                            function (item) {
                                return '<span class="mono-text">' + formatJsonPreview(item.raw_payload || {}, "--") + "</span>";
                            },
                            function (item) {
                                return '<button class="ghost-btn table-action-btn" type="button" data-action="normalize-raw" data-id="' + escapeHtml(String(item.id)) + '">标准化</button>';
                            },
                        ], "当前筛选下没有原始内容")) +
                    "</article>" +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">链路提示</p><h2>来源接入策略</h2></div></div>' +
                        '<div class="status-card"><div class="status-head"><strong>当前页面职责</strong>' + renderStatusPill("current") + '</div><p class="helper-text">本页负责来源登记、抓取回看和原始内容标准化，不再承担托管用户的账号、群组和模板日常配置。</p></div>' +
                        '<div class="status-card"><div class="status-head"><strong>高风险动作</strong>' + renderStatusPill("warning") + '</div><p class="helper-text">抓取和标准化会直接推进链路，已与普通浏览区分到操作列和折叠的高级入口中。</p></div>' +
                        '<div class="status-card"><div class="status-head"><strong>后续分工</strong>' + renderStatusPill("info") + '</div><p class="helper-text">标准信号的人工派发和执行结果不再留在本页，统一转到信号中心和执行中心处理。</p></div>' +
                    "</article>" +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">高级录入</p><h2>保留原能力，但默认收起</h2></div></div>' +
                    renderSourceForms(data.sources || []) +
                "</section>" +
            "</div>";
        setStatus("已刷新来源接入中心。", false);
    }

    function renderSourceForms(sourceItems) {
        const options = (sourceItems || []).map(function (item) {
            return '<option value="' + escapeHtml(String(item.id)) + '">#' + escapeHtml(String(item.id)) + " · " + escapeHtml(item.name) + "</option>";
        }).join("");
        return '' +
            '<details class="detail-block">' +
                '<summary>新增来源</summary>' +
                '<div class="detail-content">' +
                    '<form id="sourceManagementForm" class="form-grid">' +
                        '<label><span>来源类型</span><select class="inline-select" name="source_type">' +
                            '<option value="ai_trading_simulator_export">AITradingSimulator 导出</option>' +
                            '<option value="http_json">HTTP JSON</option>' +
                            '<option value="internal_ai">内部方案</option>' +
                            '<option value="telegram_channel">Telegram 频道</option>' +
                            '<option value="website_feed">网站 Feed</option>' +
                        "</select></label>" +
                        '<label><span>来源名称</span><input class="search-input" name="name" type="text" placeholder="例如：主策略 A" required></label>' +
                        '<label><span>可见性</span><select class="inline-select" name="visibility"><option value="private">私有</option><option value="public">公开</option></select></label>' +
                        '<label class="wide"><span>公开方案页链接或导出地址</span><input class="search-input" name="source_url" type="url" placeholder="https://your-ai-platform.example.com/public/predictors/12"></label>' +
                        '<label class="wide"><span>配置 JSON</span><textarea class="text-area" name="config" rows="6" placeholder=\'{"fetch":{"url":"https://example.com/feed.json"}}\'></textarea></label>' +
                        '<div class="form-actions wide">' +
                            '<button class="ghost-btn" type="button" data-action="fill-ai-source-template">填充 AITradingSimulator 模板</button>' +
                            '<button class="ghost-btn" type="button" data-action="generate-ai-source-config">按链接生成配置</button>' +
                            '<button class="primary-btn" type="submit">新增来源</button>' +
                        "</div>" +
                    "</form>" +
                "</div>" +
            "</details>" +
            '<details class="detail-block">' +
                '<summary>手工录入原始内容</summary>' +
                '<div class="detail-content">' +
                    '<form id="rawItemManagementForm" class="form-grid">' +
                        '<label><span>来源</span><select class="inline-select" name="source_id" required><option value="">请选择来源</option>' + options + "</select></label>" +
                        '<label><span>期号</span><input class="search-input" name="issue_no" type="text" placeholder="20260408001"></label>' +
                        '<label class="wide"><span>原始负载 JSON</span><textarea class="text-area" name="raw_payload" rows="8" placeholder=\'{"signals":[{"lottery_type":"pc28","issue_no":"20260408001","bet_type":"big_small","bet_value":"大"}]}\' required></textarea></label>' +
                        '<div class="form-actions wide"><button class="primary-btn" type="submit">录入原始内容</button></div>' +
                    "</form>" +
                "</div>" +
            "</details>";
    }

    function renderSignalsPage(data) {
        const filters = getFilterState();
        const sourceMap = sourceNameMap(data.sources || []);
        const jobCountBySignal = countBy(data.jobs || [], function (item) {
            return item.signal_id;
        });
        const dispatchedSignalIds = {};
        (data.jobs || []).forEach(function (item) {
            dispatchedSignalIds[String(item.signal_id)] = true;
        });
        const sourceOptions = [{ value: "", label: "全部来源" }].concat((data.sources || []).map(function (item) {
            return { value: String(item.id), label: "#" + item.id + " · " + item.name };
        }));
        const filteredSignals = (data.signals || []).filter(function (item) {
            const issueQuery = String(filters.issueQuery || "").trim().toLowerCase();
            return (!filters.sourceId || String(item.source_id) === filters.sourceId) &&
                (!issueQuery || String(item.issue_no || "").toLowerCase().indexOf(issueQuery) >= 0 || String(item.bet_value || "").toLowerCase().indexOf(issueQuery) >= 0);
        });
        adminContent.innerHTML = '' +
            '<div class="overview-layout">' +
                '<section class="metrics-grid">' +
                    renderMetricCard({
                        label: "信号总数",
                        value: String((data.signals || []).length),
                        status: (data.signals || []).length ? "ok" : "warning",
                        meta: "当前筛选命中 " + String(filteredSignals.length) + " 条信号。",
                        footLabel: "关联来源",
                        footValue: String(Object.keys(countBy(data.signals || [], function (item) { return item.source_id; })).length),
                    }) +
                    renderMetricCard({
                        label: "待派发",
                        value: String((data.signals || []).filter(function (item) { return !dispatchedSignalIds[String(item.id)]; }).length),
                        status: (data.signals || []).length ? "warning" : "ok",
                        meta: "尚未展开成执行任务的标准信号。",
                        footLabel: "已派发",
                        footValue: String(Object.keys(dispatchedSignalIds).length),
                    }) +
                    renderMetricCard({
                        label: "关联 Raw Item",
                        value: String((data.signals || []).filter(function (item) { return item.source_raw_item_id != null; }).length),
                        status: "ok",
                        meta: "便于从标准信号回溯到来源原始内容。",
                        footLabel: "人工录入",
                        footValue: String((data.signals || []).filter(function (item) { return item.source_raw_item_id == null; }).length),
                    }) +
                    renderMetricCard({
                        label: "派发任务",
                        value: String((data.jobs || []).length),
                        status: (data.jobs || []).length ? "ok" : "warning",
                        meta: "用于判断标准信号是否已成功展开到执行中心。",
                        footLabel: "失败任务",
                        footValue: String((data.jobs || []).filter(function (item) { return item.status === "failed"; }).length),
                    }) +
                "</section>" +
                '<section class="content-grid">' +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">标准信号</p><h2>查看与人工派发</h2></div></div>' +
                        renderToolbar([
                            renderSelectField("来源过滤", "sourceId", filters.sourceId, sourceOptions),
                            renderInputField("期号 / 投注值", "issueQuery", filters.issueQuery, "输入期号或投注值"),
                        ], [
                            '<button class="ghost-btn" type="button" data-action="reset-filters">重置筛选</button>',
                        ]) +
                        renderTable(["信号", "来源", "玩法", "置信度", "状态", "派发结果", "负载摘要", "操作"], renderRows(filteredSignals, [
                            function (item) {
                                return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + " · " + escapeHtml(item.issue_no || "--") + '</strong><span class="cell-muted">raw #' + escapeHtml(String(item.source_raw_item_id == null ? "--" : item.source_raw_item_id)) + " · " + escapeHtml(formatDateTime(item.created_at)) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(sourceMap[String(item.source_id)] || ("#" + item.source_id)) + '</strong><span class="cell-muted">source #' + escapeHtml(String(item.source_id)) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(item.bet_type || "--") + '</strong><span class="cell-muted">' + escapeHtml(item.bet_value || "--") + "</span></div>";
                            },
                            function (item) {
                                return item.confidence == null ? '<span class="cell-muted">--</span>' : escapeHtml(String(item.confidence));
                            },
                            function (item) {
                                return renderStatusPill(item.status || "--");
                            },
                            function (item) {
                                const count = Number(jobCountBySignal[item.id] || 0);
                                return '<div class="cell-stack"><strong>' + escapeHtml(String(count)) + '</strong><span class="cell-muted">' + escapeHtml(count ? "已生成执行任务" : "尚未派发") + "</span></div>";
                            },
                            function (item) {
                                return '<span class="mono-text">' + formatJsonPreview(item.normalized_payload || {}, "--") + "</span>";
                            },
                            function (item) {
                                return '<button class="ghost-btn table-action-btn" type="button" data-action="dispatch-signal" data-id="' + escapeHtml(String(item.id)) + '">人工派发</button>';
                            },
                        ], "当前筛选下没有标准信号")) +
                    "</article>" +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">派发规则</p><h2>信号中心使用说明</h2></div></div>' +
                        '<div class="status-card"><div class="status-head"><strong>页面定位</strong>' + renderStatusPill("current") + '</div><p class="helper-text">这里用于看标准信号是否正常生成，以及是否已经正确展开到执行任务。策略、群组和模板编辑仍回到 /autobet。</p></div>' +
                        '<div class="status-card"><div class="status-head"><strong>人工派发</strong>' + renderStatusPill("warning") + '</div><p class="helper-text">人工派发是高风险动作，只保留在信号列表的最后一步，不与普通浏览和筛选混在首页。</p></div>' +
                        '<div class="status-card"><div class="status-head"><strong>排障路径</strong>' + renderStatusPill("info") + '</div><p class="helper-text">如果信号已生成但任务异常，请直接进入执行中心；如果信号根本没有生成，请返回来源接入中心看 raw item 标准化。</p></div>' +
                    "</article>" +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">高级录入</p><h2>保留手工信号录入能力</h2></div></div>' +
                    renderSignalForm(data.sources || []) +
                "</section>" +
            "</div>";
        setStatus("已刷新信号中心。", false);
    }

    function renderSignalForm(sourceItems) {
        const options = (sourceItems || []).map(function (item) {
            return '<option value="' + escapeHtml(String(item.id)) + '">#' + escapeHtml(String(item.id)) + " · " + escapeHtml(item.name) + "</option>";
        }).join("");
        return '' +
            '<details class="detail-block">' +
                '<summary>手工录入标准信号</summary>' +
                '<div class="detail-content">' +
                    '<form id="signalManagementForm" class="form-grid">' +
                        '<label><span>来源</span><select class="inline-select" name="source_id" required><option value="">请选择来源</option>' + options + "</select></label>" +
                        '<label><span>彩种</span><input class="search-input" name="lottery_type" type="text" value="pc28" required></label>' +
                        '<label><span>期号</span><input class="search-input" name="issue_no" type="text" placeholder="20260408001" required></label>' +
                        '<label><span>玩法</span><input class="search-input" name="bet_type" type="text" placeholder="big_small" required></label>' +
                        '<label><span>投注值</span><input class="search-input" name="bet_value" type="text" placeholder="大" required></label>' +
                        '<label><span>置信度</span><input class="search-input" name="confidence" type="number" min="0" max="1" step="0.01" placeholder="0.80"></label>' +
                        '<label class="wide"><span>标准化负载 JSON</span><textarea class="text-area" name="normalized_payload" rows="8" placeholder=\'{"message_text":"大10","stake_amount":10}\' required></textarea></label>' +
                        '<div class="form-actions wide"><button class="primary-btn" type="submit">录入信号</button></div>' +
                    "</form>" +
                "</div>" +
            "</details>";
    }

    function renderExecutionPage(data) {
        const filters = getFilterState();
        const executorBuckets = countBy(data.executors || [], function (item) {
            return item.heartbeat_status || item.status || "--";
        });
        const searchText = String(filters.query || "").trim().toLowerCase();
        const filteredJobs = (data.jobs || []).filter(function (item) {
            return (!filters.status || item.status === filters.status) &&
                (!searchText ||
                    String(item.issue_no || "").toLowerCase().indexOf(searchText) >= 0 ||
                    String(item.target_name || "").toLowerCase().indexOf(searchText) >= 0 ||
                    String(item.target_key || "").toLowerCase().indexOf(searchText) >= 0 ||
                    String(item.telegram_account_label || "").toLowerCase().indexOf(searchText) >= 0);
        });
        const filteredFailures = (data.failures || []).filter(function (item) {
            return !searchText ||
                String(item.issue_no || "").toLowerCase().indexOf(searchText) >= 0 ||
                String(item.target_name || "").toLowerCase().indexOf(searchText) >= 0 ||
                String(item.error_message || "").toLowerCase().indexOf(searchText) >= 0;
        });
        adminContent.innerHTML = '' +
            '<div class="overview-layout">' +
                '<section class="metrics-grid">' +
                    renderMetricCard({
                        label: "执行任务",
                        value: String((data.jobs || []).length),
                        status: (data.jobs || []).some(function (item) { return item.status === "failed"; }) ? "warning" : "ok",
                        meta: "当前筛选命中 " + String(filteredJobs.length) + " 条任务。",
                        footLabel: "失败",
                        footValue: String((data.jobs || []).filter(function (item) { return item.status === "failed"; }).length),
                    }) +
                    renderMetricCard({
                        label: "待执行",
                        value: String((data.jobs || []).filter(function (item) { return item.status === "pending"; }).length),
                        status: (data.jobs || []).some(function (item) { return item.status === "pending"; }) ? "warning" : "ok",
                        meta: "等待执行器消费或等待人工处理的任务。",
                        footLabel: "已送达",
                        footValue: String((data.jobs || []).filter(function (item) { return item.status === "delivered"; }).length),
                    }) +
                    renderMetricCard({
                        label: "失败回看",
                        value: String((data.failures || []).length),
                        status: (data.failures || []).some(function (item) { return item.auto_retry_state === "exhausted"; }) ? "critical" : "warning",
                        meta: "用于定位自动重试、失败堆积和人工介入点。",
                        footLabel: "可重试",
                        footValue: String((data.failures || []).filter(function (item) { return item.can_retry; }).length),
                    }) +
                    renderMetricCard({
                        label: "执行器",
                        value: String((data.executors || []).length),
                        status: executorBuckets.offline ? "critical" : (executorBuckets.stale ? "warning" : "online"),
                        meta: "在线 " + String(executorBuckets.online || 0) + "，延迟 " + String(executorBuckets.stale || 0) + "，离线 " + String(executorBuckets.offline || 0),
                        footLabel: "连续失败",
                        footValue: String((data.executors || []).filter(function (item) { return Number(item.recent_failure_streak || 0) > 0; }).length),
                    }) +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">执行任务</p><h2>任务状态与人工重试</h2></div></div>' +
                    renderToolbar([
                        renderSelectField("任务状态", "status", filters.status, [
                            { value: "", label: "全部状态" },
                            { value: "pending", label: "待执行" },
                            { value: "delivered", label: "已送达" },
                            { value: "failed", label: "失败" },
                            { value: "skipped", label: "已跳过" },
                            { value: "expired", label: "已过期" },
                        ]),
                        renderInputField("任务检索", "query", filters.query, "期号 / 群组 / 账号 / target_key"),
                    ], [
                        '<button class="ghost-btn" type="button" data-action="reset-filters">重置筛选</button>',
                    ]) +
                    renderTable(["任务", "信号", "目标", "账号", "状态", "尝试", "错误", "最近执行", "操作"], renderRows(filteredJobs, [
                        function (item) {
                            return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + '</strong><span class="cell-muted">signal #' + escapeHtml(String(item.signal_id || "--")) + " · " + escapeHtml(item.lottery_type || "--") + "</span></div>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><strong>' + escapeHtml(item.issue_no || "--") + '</strong><span class="cell-muted">' + escapeHtml((item.bet_type || "--") + " / " + (item.bet_value || "--")) + "</span></div>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><span class="mono-text">' + escapeHtml(item.target_key || "--") + '</span><span class="cell-muted">' + escapeHtml(item.target_name || "--") + "</span></div>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><span>' + escapeHtml(item.telegram_account_label || "--") + '</span><span class="cell-muted">ID ' + escapeHtml(String(item.telegram_account_id == null ? "--" : item.telegram_account_id)) + "</span></div>";
                        },
                        function (item) {
                            return renderStatusPill(item.status || "--");
                        },
                        function (item) {
                            return '<div class="cell-stack"><strong>' + escapeHtml(String(item.attempt_count || 0)) + '</strong><span class="cell-muted">' + escapeHtml(item.last_attempt_no == null ? "未执行" : ("最近第 " + item.last_attempt_no + " 次")) + "</span></div>";
                        },
                        function (item) {
                            const message = item.last_error_message || item.error_message || "--";
                            return message === "--" ? '<span class="cell-muted">--</span>' : '<span class="error-text">' + escapeHtml(message) + "</span>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><span>' + escapeHtml(formatDateTime(item.updated_at)) + '</span><span class="cell-muted">' + escapeHtml(item.last_executed_at ? ("上次执行 " + formatDateTime(item.last_executed_at)) : "--") + "</span></div>";
                        },
                        function (item) {
                            if (!item.can_retry) {
                                return '<span class="cell-muted">--</span>';
                            }
                            return '<button class="ghost-btn table-action-btn" type="button" data-action="retry-job" data-id="' + escapeHtml(String(item.id)) + '">人工重试</button>';
                        },
                    ], "当前筛选下没有执行任务")) +
                "</section>" +
                '<section class="content-grid">' +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">最近失败</p><h2>失败记录与自动重试状态</h2></div></div>' +
                        renderTable(["任务", "执行器", "目标", "交付状态", "自动重试", "错误", "执行时间"], renderRows(filteredFailures, [
                            function (item) {
                                return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.job_id || "--")) + '</strong><span class="cell-muted">' + escapeHtml((item.issue_no || "--") + " · " + (item.bet_value || "--")) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><span class="mono-text">' + escapeHtml(item.executor_instance_id || "--") + '</span><span class="cell-muted">' + escapeHtml(item.telegram_account_label || "--") + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><span class="mono-text">' + escapeHtml(item.target_key || "--") + '</span><span class="cell-muted">' + escapeHtml(item.target_name || "--") + "</span></div>";
                            },
                            function (item) {
                                return renderStatusPill(item.delivery_status || item.job_status || "--");
                            },
                            function (item) {
                                return '<div class="cell-stack">' + renderStatusPill(item.auto_retry_state || "--") + '<span class="cell-muted">' + escapeHtml(item.next_retry_at ? formatDateTime(item.next_retry_at) : "--") + "</span></div>";
                            },
                            function (item) {
                                return '<span class="error-text">' + escapeHtml(item.error_message || "--") + "</span>";
                            },
                            function (item) {
                                return escapeHtml(formatDateTime(item.executed_at));
                            },
                        ], "当前筛选下没有失败记录")) +
                    "</article>" +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">执行器状态</p><h2>节点健康与最近失败</h2></div></div>' +
                        renderTable(["执行器", "心跳", "版本", "投递统计", "失败摘要", "能力"], renderRows(data.executors || [], [
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(item.executor_id || "--") + '</strong><span class="cell-muted">连续失败 ' + escapeHtml(String(item.recent_failure_streak || 0)) + "</span></div>";
                            },
                            function (item) {
                                const age = item.heartbeat_age_seconds == null ? "--" : String(item.heartbeat_age_seconds) + "s";
                                return '<div class="cell-stack">' + renderStatusPill(item.heartbeat_status || "--") + '<span class="cell-muted">' + escapeHtml(formatDateTime(item.last_seen_at)) + " · " + escapeHtml(age) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><span>' + escapeHtml(item.version || "--") + '</span><span class="cell-muted">' + escapeHtml(formatDateTime(item.last_executed_at)) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(String(item.total_attempt_count || 0)) + '</strong><span class="cell-muted">已送达 ' + escapeHtml(String(item.delivered_attempt_count || 0)) + "</span></div>";
                            },
                            function (item) {
                                const failureText = item.last_failure_error_message || "--";
                                return failureText === "--"
                                    ? '<span class="cell-muted">--</span>'
                                    : '<div class="cell-stack"><strong class="error-text">' + escapeHtml(String(item.failed_attempt_count || 0)) + '</strong><span class="error-text">' + escapeHtml(failureText) + "</span></div>";
                            },
                            function (item) {
                                return '<span class="mono-text">' + formatJsonPreview(item.capabilities || {}, "--") + "</span>";
                            },
                        ], "暂无执行器状态")) +
                    "</article>" +
                "</section>" +
            "</div>";
        setStatus("已刷新执行中心。", false);
    }

    function renderAlertsPage(data) {
        const filters = getFilterState();
        const searchText = String(filters.query || "").trim().toLowerCase();
        const filteredAlerts = (data.alerts || []).filter(function (item) {
            return (!filters.severity || item.severity === filters.severity) &&
                (!filters.notificationStatus || item.notification_status === filters.notificationStatus) &&
                (!searchText ||
                    String(item.title || "").toLowerCase().indexOf(searchText) >= 0 ||
                    String(item.alert_type || "").toLowerCase().indexOf(searchText) >= 0 ||
                    String(item.message || "").toLowerCase().indexOf(searchText) >= 0);
        });
        const alertBuckets = countBy(data.alerts || [], function (item) { return item.severity || "--"; });
        adminContent.innerHTML = '' +
            '<div class="overview-layout">' +
                '<section class="metrics-grid">' +
                    renderMetricCard({
                        label: "告警总数",
                        value: String((data.alerts || []).length),
                        status: alertBuckets.critical ? "critical" : (alertBuckets.warning ? "warning" : "ok"),
                        meta: "当前筛选命中 " + String(filteredAlerts.length) + " 条告警。",
                        footLabel: "严重",
                        footValue: String(alertBuckets.critical || 0),
                    }) +
                    renderMetricCard({
                        label: "警告告警",
                        value: String(alertBuckets.warning || 0),
                        status: alertBuckets.warning ? "warning" : "ok",
                        meta: "需要值班关注，但通常还未到完全阻塞。",
                        footLabel: "提示",
                        footValue: String(alertBuckets.info || 0),
                    }) +
                    renderMetricCard({
                        label: "待通知",
                        value: String((data.alerts || []).filter(function (item) { return item.notification_status !== "sent"; }).length),
                        status: (data.alerts || []).some(function (item) { return item.notification_status === "failed"; }) ? "critical" : "warning",
                        meta: "未成功发送或尚未发送的告警通知。",
                        footLabel: "已通知",
                        footValue: String((data.alerts || []).filter(function (item) { return item.notification_status === "sent"; }).length),
                    }) +
                    renderMetricCard({
                        label: "关联失败",
                        value: String((data.failures || []).length),
                        status: (data.failures || []).some(function (item) { return item.auto_retry_state === "exhausted"; }) ? "warning" : "ok",
                        meta: "用于确认告警背后的失败任务是否已进入执行中心。",
                        footLabel: "重试耗尽",
                        footValue: String((data.failures || []).filter(function (item) { return item.auto_retry_state === "exhausted"; }).length),
                    }) +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">告警列表</p><h2>按级别和通知状态处理告警</h2></div></div>' +
                    renderToolbar([
                        renderSelectField("级别", "severity", filters.severity, [
                            { value: "", label: "全部级别" },
                            { value: "critical", label: "严重" },
                            { value: "warning", label: "警告" },
                            { value: "info", label: "提示" },
                        ]),
                        renderSelectField("通知状态", "notificationStatus", filters.notificationStatus, [
                            { value: "", label: "全部通知状态" },
                            { value: "pending", label: "待通知" },
                            { value: "sent", label: "已通知" },
                            { value: "failed", label: "通知失败" },
                        ]),
                        renderInputField("检索", "query", filters.query, "标题 / 类型 / 内容"),
                    ], [
                        '<button class="ghost-btn" type="button" data-action="reset-filters">重置筛选</button>',
                    ]) +
                    renderTable(["级别", "类型", "标题与内容", "通知状态", "关联对象", "处理入口"], renderRows(filteredAlerts, [
                        function (item) {
                            return renderStatusPill(item.severity || "--");
                        },
                        function (item) {
                            return '<span class="mono-text">' + escapeHtml(item.alert_type || "--") + "</span>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><strong>' + escapeHtml(item.title || "--") + '</strong><span class="cell-muted">' + escapeHtml(item.message || "--") + "</span></div>";
                        },
                        function (item) {
                            const notification = item.notification || {};
                            const sentText = notification.last_sent_at ? formatDateTime(notification.last_sent_at) : "--";
                            const errorText = notification.last_error || "--";
                            return '<div class="cell-stack">' + renderStatusPill(item.notification_status || "pending") + '<span class="cell-muted">最近发送 ' + escapeHtml(sentText) + '</span><span class="' + (errorText === "--" ? "cell-muted" : "error-text") + '">' + escapeHtml(errorText) + "</span></div>";
                        },
                        function (item) {
                            const metadata = item.metadata || {};
                            if (metadata.job_id) {
                                return '<div class="cell-stack"><strong>job #' + escapeHtml(String(metadata.job_id)) + '</strong><span class="cell-muted">signal #' + escapeHtml(String(metadata.signal_id || "--")) + "</span></div>";
                            }
                            if (metadata.executor_id) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(String(metadata.executor_id)) + '</strong><span class="cell-muted">执行器相关</span></div>';
                            }
                            return '<span class="cell-muted">--</span>';
                        },
                        function (item) {
                            const metadata = item.metadata || {};
                            if (metadata.job_id) {
                                return '<a class="todo-link" href="/admin/execution">去执行中心</a>';
                            }
                            if (metadata.executor_id) {
                                return '<a class="todo-link" href="/admin/execution">查看执行器</a>';
                            }
                            return '<span class="cell-muted">--</span>';
                        },
                    ], "当前筛选下没有告警")) +
                "</section>" +
            "</div>";
        setStatus("已刷新告警中心。", false);
    }

    function renderTelegramSettingsPage(payload) {
        const item = payload && payload.item ? payload.item : {};
        const alertSettings = item.alert || {};
        const botSettings = item.bot || {};
        const reportSettings = item.report || {};
        const autoSettlementSettings = item.auto_settlement || {};
        const autoSettlementRuntime = autoSettlementSettings.runtime_state || {};
        adminContent.innerHTML = '' +
            '<div class="overview-layout">' +
                '<section class="metrics-grid">' +
                    renderMetricCard({
                        label: "告警通知",
                        value: alertSettings.enabled ? "已启用" : "已停用",
                        status: alertSettings.enabled ? "ok" : "warning",
                        meta: "发送到 " + String(alertSettings.target_chat_id || "未配置"),
                        footLabel: "刷新周期",
                        footValue: String(alertSettings.interval_seconds || 0) + " 秒",
                    }) +
                    renderMetricCard({
                        label: "收益查询 Bot",
                        value: botSettings.enabled ? "已启用" : "已停用",
                        status: botSettings.enabled ? "ok" : "warning",
                        meta: botSettings.has_bot_token ? ("当前 Token: " + String(botSettings.bot_token_masked || "--")) : "当前未配置 Token",
                        footLabel: "长轮询超时",
                        footValue: String(botSettings.poll_interval_seconds || 0) + " 秒",
                    }) +
                    renderMetricCard({
                        label: "日报推送",
                        value: reportSettings.enabled ? "已启用" : "已停用",
                        status: reportSettings.enabled ? "ok" : "warning",
                        meta: "目标 " + String(reportSettings.target_chat_id || "未配置"),
                        footLabel: "发送时刻",
                        footValue: String(reportSettings.send_hour || 0) + ":" + String(reportSettings.send_minute || 0).padStart(2, "0"),
                    }) +
                    renderMetricCard({
                        label: "配置来源",
                        value: item.source === "database" ? "网页覆盖" : "环境默认",
                        status: item.source === "database" ? "ok" : "warning",
                        meta: "保存后后台 worker 无需重启，下一轮自动读取。",
                        footLabel: "最近更新",
                        footValue: formatDateTime(item.updated_at),
                    }) +
                    renderMetricCard({
                        label: "PC28 自动结算",
                        value: autoSettlementSettings.enabled ? "已启用" : "已停用",
                        status: autoSettlementSettings.enabled ? "ok" : "warning",
                        meta: autoSettlementRuntime.last_status
                            ? ("最近状态 " + String(autoSettlementRuntime.last_status || "--"))
                            : "尚未运行",
                        footLabel: "最近执行",
                        footValue: formatDateTime(autoSettlementRuntime.updated_at || autoSettlementRuntime.last_run_at),
                    }) +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">后台 Worker 配置</p><h2>网页保存后自动热更新</h2></div></div>' +
                    '<div class="status-card"><div class="status-head"><strong>热更新说明</strong>' + renderStatusPill("ok") + '</div><p class="helper-text">本页保存的是平台侧后台 worker 运行配置。`alert`、`bot`、`report`、`auto_settlement` 四个独立 worker 会在下一轮循环自动读取数据库中的最新配置，不需要重启进程。</p></div>' +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">配置表单</p><h2>告警、Bot、日报推送、自动结算</h2></div></div>' +
                    '<form id="telegramSettingsForm" class="settings-form">' +
                        '<div class="settings-grid">' +
                            '<section class="detail-card">' +
                                '<div class="detail-card-head"><strong>告警通知</strong>' + renderStatusPill(alertSettings.enabled ? "active" : "inactive") + '</div>' +
                                '<p class="helper-text">平台异常提醒推送。Bot Token 留空表示保持当前值。</p>' +
                                '<div class="settings-form-grid">' +
                                    '<label class="settings-toggle"><input type="checkbox" name="alert_enabled"' + (alertSettings.enabled ? " checked" : "") + '><span>启用告警通知</span></label>' +
                                    '<label><span class="field-label">Bot Token</span><input class="text-input" type="password" name="alert_bot_token" value="" placeholder="' + escapeHtml(alertSettings.bot_token_masked ? ("当前: " + alertSettings.bot_token_masked + "，留空保持不变") : "留空表示未配置") + '"></label>' +
                                    '<label><span class="field-label">目标 chat_id</span><input class="text-input" type="text" name="alert_target_chat_id" value="' + escapeHtml(alertSettings.target_chat_id || "") + '" placeholder="-1001234567890"></label>' +
                                    '<label><span class="field-label">重复通知间隔（秒）</span><input class="text-input" type="number" min="60" name="alert_repeat_interval_seconds" value="' + escapeHtml(String(alertSettings.repeat_interval_seconds || 1800)) + '"></label>' +
                                    '<label><span class="field-label">轮询间隔（秒）</span><input class="text-input" type="number" min="5" name="alert_interval_seconds" value="' + escapeHtml(String(alertSettings.interval_seconds || 30)) + '"></label>' +
                                '</div>' +
                            '</section>' +
                            '<section class="detail-card">' +
                                '<div class="detail-card-head"><strong>收益查询 Bot</strong>' + renderStatusPill(botSettings.enabled ? "active" : "inactive") + '</div>' +
                                '<p class="helper-text">负责 `/bind`、`/profit`、`/plan` 命令。这里配置的是 Telegram `getUpdates` 的长轮询超时；有新消息时会立即返回，不会固定等满这段时间。Bot Token 留空表示保持当前值。</p>' +
                                '<div class="settings-form-grid">' +
                                    '<label class="settings-toggle"><input type="checkbox" name="bot_enabled"' + (botSettings.enabled ? " checked" : "") + '><span>启用收益查询 Bot</span></label>' +
                                    '<label><span class="field-label">Bot Token</span><input class="text-input" type="password" name="bot_bot_token" value="" placeholder="' + escapeHtml(botSettings.bot_token_masked ? ("当前: " + botSettings.bot_token_masked + "，留空保持不变") : "输入新的 Bot Token") + '"></label>' +
                                    '<label><span class="field-label">长轮询超时（秒）</span><input class="text-input" type="number" min="1" name="bot_poll_interval_seconds" value="' + escapeHtml(String(botSettings.poll_interval_seconds || 30)) + '"></label>' +
                                    '<label><span class="field-label">绑定码有效期（秒）</span><input class="text-input" type="number" min="60" name="bot_bind_token_ttl_seconds" value="' + escapeHtml(String(botSettings.bind_token_ttl_seconds || 600)) + '"></label>' +
                                '</div>' +
                            '</section>' +
                            '<section class="detail-card">' +
                                '<div class="detail-card-head"><strong>日报推送</strong>' + renderStatusPill(reportSettings.enabled ? "active" : "inactive") + '</div>' +
                                '<p class="helper-text">日报推送复用“收益查询 Bot”的 Bot Token。</p>' +
                                '<div class="settings-form-grid">' +
                                    '<label class="settings-toggle"><input type="checkbox" name="report_enabled"' + (reportSettings.enabled ? " checked" : "") + '><span>启用日报推送</span></label>' +
                                    '<label><span class="field-label">目标 chat_id</span><input class="text-input" type="text" name="report_target_chat_id" value="' + escapeHtml(reportSettings.target_chat_id || "") + '" placeholder="-1001234567890"></label>' +
                                    '<label><span class="field-label">循环检查间隔（秒）</span><input class="text-input" type="number" min="5" name="report_interval_seconds" value="' + escapeHtml(String(reportSettings.interval_seconds || 30)) + '"></label>' +
                                    '<label><span class="field-label">发送小时</span><input class="text-input" type="number" min="0" max="23" name="report_send_hour" value="' + escapeHtml(String(reportSettings.send_hour || 9)) + '"></label>' +
                                    '<label><span class="field-label">发送分钟</span><input class="text-input" type="number" min="0" max="59" name="report_send_minute" value="' + escapeHtml(String(reportSettings.send_minute || 0)) + '"></label>' +
                                    '<label><span class="field-label">榜单 Top N</span><input class="text-input" type="number" min="1" max="100" name="report_top_n" value="' + escapeHtml(String(reportSettings.top_n || 10)) + '"></label>' +
                                    '<label><span class="field-label">时区</span><select class="inline-select" name="report_timezone"><option value="Asia/Shanghai"' + (String(reportSettings.timezone || "") === "Asia/Shanghai" ? " selected" : "") + '>Asia/Shanghai</option><option value="UTC"' + (String(reportSettings.timezone || "") === "UTC" ? " selected" : "") + '>UTC</option></select></label>' +
                                '</div>' +
                            '</section>' +
                            '<section class="detail-card">' +
                                '<div class="detail-card-head"><strong>PC28 自动结算</strong>' + renderStatusPill(autoSettlementSettings.enabled ? "active" : "inactive") + '</div>' +
                                '<p class="helper-text">后台自动拉取最近开奖并批量结算已发出的待结算 PC28 记录。当前状态：' + escapeHtml(String(autoSettlementRuntime.last_status || "尚未运行")) + (autoSettlementRuntime.last_error ? ("；最近错误：" + String(autoSettlementRuntime.last_error)) : "") + '</p>' +
                                '<div class="settings-form-grid">' +
                                    '<label class="settings-toggle"><input type="checkbox" name="auto_settlement_enabled"' + (autoSettlementSettings.enabled ? " checked" : "") + '><span>启用 PC28 自动结算</span></label>' +
                                    '<label><span class="field-label">轮询间隔（秒）</span><input class="text-input" type="number" min="5" name="auto_settlement_interval_seconds" value="' + escapeHtml(String(autoSettlementSettings.interval_seconds || 30)) + '"></label>' +
                                    '<label><span class="field-label">最近开奖拉取条数</span><input class="text-input" type="number" min="10" max="100" name="auto_settlement_draw_limit" value="' + escapeHtml(String(autoSettlementSettings.draw_limit || 60)) + '"></label>' +
                                    '<label><span class="field-label">最近执行</span><input class="text-input" type="text" value="' + escapeHtml(formatDateTime(autoSettlementRuntime.updated_at || autoSettlementRuntime.last_run_at)) + '" disabled></label>' +
                                '</div>' +
                            '</section>' +
                        '</div>' +
                        '<div class="form-actions wide"><button class="primary-btn" type="submit">保存后台 Worker 配置</button><button class="ghost-btn" type="button" data-action="reload-telegram-settings">重新加载当前值</button></div>' +
                    '</form>' +
                "</section>" +
            "</div>";
        setStatus("已刷新后台 Worker 配置中心。", false);
    }

    function renderSupportPage(data) {
        const filters = getFilterState();
        const query = String(filters.query || "").trim().toLowerCase();
        const selectedUserId = String(filters.userId || "");
        const sourcesById = {};
        (data.sources || []).forEach(function (item) {
            sourcesById[String(item.id)] = item;
        });
        const accountsByUser = countBy(data.accounts || [], function (item) { return item.user_id; });
        const targetsByUser = countBy(data.targets || [], function (item) { return item.user_id; });
        const templatesByUser = countBy(data.templates || [], function (item) { return item.user_id; });
        const subscriptionsByUser = countBy(data.subscriptions || [], function (item) { return item.user_id; });
        const visibleUsers = (data.users || []).filter(function (item) {
            return (!selectedUserId || String(item.id) === selectedUserId) &&
                (!query ||
                    String(item.username || "").toLowerCase().indexOf(query) >= 0 ||
                    String(item.email || "").toLowerCase().indexOf(query) >= 0);
        });
        function inScope(item) {
            return !selectedUserId || String(item.user_id || item.id) === selectedUserId;
        }
        function matchQuery(values) {
            if (!query) {
                return true;
            }
            return values.some(function (value) {
                return String(value || "").toLowerCase().indexOf(query) >= 0;
            });
        }
        const visibleAccounts = (data.accounts || []).filter(function (item) {
            return inScope(item) && matchQuery([item.user_username, item.label, item.phone, item.session_path, accountAuthLabel(item)]);
        });
        const visibleTargets = (data.targets || []).filter(function (item) {
            return inScope(item) && matchQuery([item.user_username, item.target_name, item.target_key, item.last_test_status, item.recent_execution_status]);
        });
        const visibleSubscriptions = (data.subscriptions || []).filter(function (item) {
            return inScope(item) && matchQuery([item.user_username, item.source_name, formatJson(item.strategy || {}), item.status]);
        });
        const visibleTemplates = (data.templates || []).filter(function (item) {
            return inScope(item) && matchQuery([item.user_username, item.name, item.template_text, item.status]);
        });
        const scopedAccounts = (data.accounts || []).filter(inScope);
        const scopedTemplates = (data.templates || []).filter(inScope);
        const userOptions = [{ value: "", label: "全部用户" }].concat((data.users || []).map(function (item) {
            return { value: String(item.id), label: "#" + item.id + " · " + item.username };
        }));
        adminContent.innerHTML = '' +
            '<div class="overview-layout">' +
                '<section class="metrics-grid">' +
                    renderMetricCard({
                        label: "用户数",
                        value: String((data.users || []).length),
                        status: "ok",
                        meta: "当前筛选命中 " + String(visibleUsers.length) + " 个用户。",
                        footLabel: "账号",
                        footValue: String(visibleAccounts.length),
                    }) +
                    renderMetricCard({
                        label: "待授权账号",
                        value: String(visibleAccounts.filter(function (item) { return accountAuthState(item) !== "authorized"; }).length),
                        status: visibleAccounts.some(function (item) { return accountAuthState(item) !== "authorized"; }) ? "warning" : "ok",
                        meta: "用于支持排查账号为什么无法承接发送任务。",
                        footLabel: "群组",
                        footValue: String(visibleTargets.length),
                    }) +
                    renderMetricCard({
                        label: "群组测试失败",
                        value: String(visibleTargets.filter(function (item) { return item.last_test_status === "failed"; }).length),
                        status: visibleTargets.some(function (item) { return item.last_test_status === "failed"; }) ? "warning" : "ok",
                        meta: "快速定位最近测试发送失败的群组。",
                        footLabel: "跟单策略",
                        footValue: String(visibleSubscriptions.length),
                    }) +
                    renderMetricCard({
                        label: "模板与策略",
                        value: String(visibleTemplates.length),
                        status: visibleTemplates.length ? "ok" : "warning",
                        meta: "支持查询页默认只做关系诊断，不直接承载编辑。",
                        footLabel: "未激活策略",
                        footValue: String(visibleSubscriptions.filter(function (item) { return item.status !== "active"; }).length),
                    }) +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">支持查询</p><h2>跨对象关系诊断</h2></div></div>' +
                    renderToolbar([
                        renderSelectField("用户过滤", "userId", filters.userId, userOptions),
                        renderInputField("全局检索", "query", filters.query, "用户名 / 手机号 / 群组 / 模板 / 期号"),
                    ], [
                        '<a class="ghost-btn link-btn" href="/admin/telegram">去 Telegram 配置</a>',
                        '<a class="ghost-btn link-btn" href="/autobet/accounts">去账号工作区</a>',
                        '<a class="ghost-btn link-btn" href="/autobet/targets">去群组工作区</a>',
                        '<a class="ghost-btn link-btn" href="/autobet/templates">去模板工作区</a>',
                        '<a class="ghost-btn link-btn" href="/autobet/subscriptions">去跟单工作区</a>',
                    ]) +
                    '<div class="status-card"><div class="status-head"><strong>支持页原则</strong>' + renderStatusPill("current") + '</div><p class="helper-text">本页只做排障和查询。账号授权、群组测试发送、模板编辑、跟单配置依旧统一回到 /autobet，避免后台重新变成用户配置入口。</p></div>' +
                "</section>" +
                '<section class="panel">' +
                    '<div class="panel-header"><div><p class="panel-kicker">用户</p><h2>用户与会话概况</h2></div></div>' +
                    renderTable(["用户", "角色与状态", "来源", "账号", "群组", "模板/订阅"], renderRows(visibleUsers, [
                        function (item) {
                            return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + " · " + escapeHtml(item.username || "--") + '</strong><span class="cell-muted">' + escapeHtml(item.email || "--") + "</span></div>";
                        },
                        function (item) {
                            return '<div class="cell-stack"><span>' + escapeHtml(item.role || "--") + '</span><span class="cell-muted">' + escapeHtml(labelStatus(item.status || "--")) + "</span></div>";
                        },
                        function (item) {
                            return escapeHtml(String((data.sources || []).filter(function (entry) { return Number(entry.user_id || entry.owner_user_id) === Number(item.id); }).length));
                        },
                        function (item) {
                            return escapeHtml(String(accountsByUser[item.id] || 0));
                        },
                        function (item) {
                            return escapeHtml(String(targetsByUser[item.id] || 0));
                        },
                        function (item) {
                            return '<div class="cell-stack"><strong>' + escapeHtml(String(templatesByUser[item.id] || 0)) + '</strong><span class="cell-muted">订阅 ' + escapeHtml(String(subscriptionsByUser[item.id] || 0)) + "</span></div>";
                        },
                    ], "当前筛选下没有用户")) +
                "</section>" +
                '<section class="content-grid">' +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">托管账号</p><h2>授权与接入状态</h2></div><a class="ghost-btn link-btn" href="/autobet/accounts">去配置账号</a></div>' +
                        renderTable(["账号", "用户", "授权", "状态", "Session 路径", "承接群组"], renderRows(visibleAccounts, [
                            function (item) {
                                return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + " · " + escapeHtml(item.label || "--") + '</strong><span class="cell-muted">' + escapeHtml(item.phone || "--") + "</span></div>";
                            },
                            function (item) {
                                return escapeHtml(item.user_username || "--");
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(accountAuthLabel(item)) + '</strong><span class="cell-muted">' + escapeHtml(accountAuthMode(item) === "session_import" ? "导入 Session" : "手机号登录") + "</span></div>";
                            },
                            function (item) {
                                return renderStatusPill(item.status || "--");
                            },
                            function (item) {
                                return '<span class="mono-text">' + escapeHtml(truncateText(item.session_path || "--", 56)) + "</span>";
                            },
                            function (item) {
                                return escapeHtml(String((data.targets || []).filter(function (target) { return Number(target.telegram_account_id) === Number(item.id); }).length));
                            },
                        ], "当前筛选下没有托管账号")) +
                    "</article>" +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">投递群组</p><h2>测试与执行状态</h2></div><a class="ghost-btn link-btn" href="/autobet/targets">去配置群组</a></div>' +
                        renderTable(["群组", "用户", "绑定账号", "模板", "测试状态", "最近执行", "命中策略"], renderRows(visibleTargets, [
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(item.target_name || "--") + '</strong><span class="cell-muted mono-text">' + escapeHtml(item.target_key || "--") + "</span></div>";
                            },
                            function (item) {
                                return escapeHtml(item.user_username || "--");
                            },
                            function (item) {
                                const account = scopedAccounts.find(function (entry) { return Number(entry.id) === Number(item.telegram_account_id); }) || null;
                                return account ? escapeHtml(account.label || "--") : '<span class="cell-muted">--</span>';
                            },
                            function (item) {
                                const template = scopedTemplates.find(function (entry) { return Number(entry.id) === Number(item.template_id); }) || null;
                                return template ? escapeHtml(template.name || "--") : '<span class="cell-muted">--</span>';
                            },
                            function (item) {
                                return item.last_test_status ? '<div class="cell-stack">' + renderStatusPill(item.last_test_status) + '<span class="cell-muted">' + escapeHtml(item.last_test_message || "--") + "</span></div>" : '<span class="cell-muted">未测试</span>';
                            },
                            function (item) {
                                const recentStatus = item.recent_execution_status || "--";
                                return '<div class="cell-stack"><span>' + escapeHtml(formatDateTime(item.recent_execution_at)) + '</span><span class="cell-muted">' + escapeHtml(labelStatus(recentStatus)) + "</span></div>";
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(String(item.active_matched_subscription_count || 0)) + '</strong><span class="cell-muted">总计 ' + escapeHtml(String(item.matched_subscription_count || 0)) + "</span></div>";
                            },
                        ], "当前筛选下没有群组")) +
                    "</article>" +
                "</section>" +
                '<section class="content-grid">' +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">跟单策略</p><h2>来源与策略摘要</h2></div><a class="ghost-btn link-btn" href="/autobet/subscriptions">去配置跟单</a></div>' +
                        renderTable(["策略", "用户", "来源", "状态", "策略 JSON"], renderRows(visibleSubscriptions, [
                            function (item) {
                                return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + '</strong><span class="cell-muted">source #' + escapeHtml(String(item.source_id || "--")) + "</span></div>";
                            },
                            function (item) {
                                return escapeHtml(item.user_username || "--");
                            },
                            function (item) {
                                return '<div class="cell-stack"><strong>' + escapeHtml(item.source_name || "--") + '</strong><span class="cell-muted">' + escapeHtml(labelSourceType(item.source_type || (sourcesById[String(item.source_id)] || {}).source_type || "--")) + "</span></div>";
                            },
                            function (item) {
                                return renderStatusPill(item.status || "--");
                            },
                            function (item) {
                                return '<span class="mono-text">' + formatJsonPreview(item.strategy || {}, "--") + "</span>";
                            },
                        ], "当前筛选下没有跟单策略")) +
                    "</article>" +
                    '<article class="panel">' +
                        '<div class="panel-header"><div><p class="panel-kicker">消息模板</p><h2>模板绑定关系</h2></div><a class="ghost-btn link-btn" href="/autobet/templates">去配置模板</a></div>' +
                        renderTable(["模板", "用户", "状态", "已绑定群组", "活跃群组", "模板摘要"], renderRows(visibleTemplates, [
                            function (item) {
                                return '<div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + " · " + escapeHtml(item.name || "--") + '</strong><span class="cell-muted">' + escapeHtml(item.bet_type || "*") + " · " + escapeHtml(item.lottery_type || "--") + "</span></div>";
                            },
                            function (item) {
                                return escapeHtml(item.user_username || "--");
                            },
                            function (item) {
                                return renderStatusPill(item.status || "--");
                            },
                            function (item) {
                                return escapeHtml(String(item.usage_count || 0));
                            },
                            function (item) {
                                return escapeHtml(String(item.active_target_count || 0));
                            },
                            function (item) {
                                return '<span class="mono-text">' + escapeHtml(truncateText(item.template_text || "--", 48)) + "</span>";
                            },
                        ], "当前筛选下没有消息模板")) +
                    "</article>" +
                "</section>" +
            "</div>";
        setStatus("已刷新支持查询页。", false);
    }

    async function loadRoute() {
        state.currentRoute = resolveRoute(window.location.pathname);
        applyRouteMeta(state.currentRoute);
        renderLoadingState();
        try {
            await loadCurrentUser();
            state.pageData = await ROUTES[state.currentRoute].load();
            ROUTES[state.currentRoute].render(state.pageData);
            updateRefreshTimestamp();
        } catch (error) {
            setCurrentUser(null);
            renderErrorState(error.message || "后台页面加载失败");
            setStatus(error.message || "后台页面加载失败", true);
        }
    }

    async function refreshCurrentRoute(message) {
        await loadRoute();
        if (message) {
            setStatus(message, false);
        }
    }

    function updateFilterValue(filterKey, value) {
        const routeConfig = ROUTES[state.currentRoute];
        if (!routeConfig) {
            return;
        }
        if (!state.filters[routeConfig.key]) {
            state.filters[routeConfig.key] = {};
        }
        state.filters[routeConfig.key][filterKey] = value;
        rerenderCurrentRoute();
    }

    function handleSourceTemplateFill() {
        const form = document.getElementById("sourceManagementForm");
        if (!(form instanceof HTMLFormElement)) {
            return;
        }
        form.elements.source_type.value = "ai_trading_simulator_export";
        if (!String(form.elements.source_url.value || "").trim()) {
            form.elements.source_url.value = "https://your-ai-platform.example.com/public/predictors/12";
        }
        form.elements.config.value = formatJson(SOURCE_TEMPLATES.ai_trading_simulator_export);
        if (!String(form.elements.name.value || "").trim()) {
            form.elements.name.value = "AITradingSimulator 来源";
        }
        setStatus("已填充 AITradingSimulator 来源模板。", false);
    }

    function handleAiSourceConfigGenerate() {
        const form = document.getElementById("sourceManagementForm");
        if (!(form instanceof HTMLFormElement)) {
            return;
        }
        const payload = buildAiSourceConfig(form.elements.source_url.value);
        form.elements.source_type.value = "ai_trading_simulator_export";
        form.elements.visibility.value = "private";
        form.elements.config.value = formatJson(payload.config);
        if (!String(form.elements.name.value || "").trim()) {
            form.elements.name.value = payload.suggestedName;
        }
        setStatus("已根据来源链接生成配置：" + payload.normalizedUrl, false);
    }

    async function handleContentClick(event) {
        const actionTarget = event.target.closest("[data-action]");
        if (!actionTarget) {
            return;
        }
        const action = actionTarget.getAttribute("data-action") || "";
        const id = actionTarget.getAttribute("data-id") || "";
        try {
            if (action === "fetch-source") {
                await request("/api/platform/sources/" + encodeURIComponent(id) + "/fetch", { method: "POST", body: {} });
                await refreshCurrentRoute("来源抓取已执行。");
                return;
            }
            if (action === "normalize-raw") {
                const payload = await request("/api/platform/raw-items/" + encodeURIComponent(id) + "/normalize", { method: "POST", body: {} });
                await refreshCurrentRoute("原始内容标准化完成，新增 " + String(payload.created_count || 0) + " 条信号。");
                return;
            }
            if (action === "dispatch-signal") {
                const payload = await request("/api/platform/signals/" + encodeURIComponent(id) + "/dispatch", { method: "POST", body: {} });
                await refreshCurrentRoute("信号派发完成，新增 " + String(payload.created_count || 0) + " 条执行任务。");
                return;
            }
            if (action === "retry-job") {
                await request("/api/platform/execution-jobs/" + encodeURIComponent(id) + "/retry", { method: "POST", body: {} });
                await refreshCurrentRoute("执行任务已重置为待执行。");
                return;
            }
            if (action === "reset-filters") {
                resetCurrentRouteFilters();
                rerenderCurrentRoute();
                setStatus("已重置当前页筛选条件。", false);
                return;
            }
            if (action === "fill-ai-source-template") {
                handleSourceTemplateFill();
                return;
            }
            if (action === "generate-ai-source-config") {
                handleAiSourceConfigGenerate();
                return;
            }
            if (action === "reload-telegram-settings") {
                await refreshCurrentRoute("已重新加载 Telegram 当前配置。");
                return;
            }
        } catch (error) {
            setStatus(error.message || "操作失败", true);
        }
    }

    function handleContentChange(event) {
        const filterTarget = event.target.closest("[data-filter-key]");
        if (filterTarget) {
            updateFilterValue(filterTarget.getAttribute("data-filter-key"), filterTarget.value || "");
            return;
        }
        const sourceTypeField = event.target.closest('#sourceManagementForm [name="source_type"]');
        if (sourceTypeField) {
            const form = document.getElementById("sourceManagementForm");
            if (form instanceof HTMLFormElement && !String(form.elements.config.value || "").trim()) {
                const template = SOURCE_TEMPLATES[sourceTypeField.value];
                if (template) {
                    form.elements.config.value = formatJson(template);
                }
            }
        }
    }

    function handleContentInput(event) {
        const filterTarget = event.target.closest("[data-filter-key][data-filter-live]");
        if (filterTarget) {
            updateFilterValue(filterTarget.getAttribute("data-filter-key"), filterTarget.value || "");
        }
    }

    async function handleContentSubmit(event) {
        const form = event.target;
        if (!(form instanceof HTMLFormElement)) {
            return;
        }
        event.preventDefault();
        try {
            if (form.id === "sourceManagementForm") {
                let config = parseJsonField(form.elements.config.value, "config");
                if (form.elements.source_type.value === "ai_trading_simulator_export" && String(form.elements.source_url.value || "").trim()) {
                    config = buildAiSourceConfig(form.elements.source_url.value).config;
                    form.elements.config.value = formatJson(config);
                }
                await request("/api/platform/sources", {
                    method: "POST",
                    body: {
                        source_type: form.elements.source_type.value,
                        name: form.elements.name.value,
                        visibility: form.elements.visibility.value,
                        config: config,
                    },
                });
                form.reset();
                form.elements.source_type.value = "ai_trading_simulator_export";
                form.elements.visibility.value = "private";
                await refreshCurrentRoute("来源已创建。");
                return;
            }
            if (form.id === "rawItemManagementForm") {
                await request("/api/platform/raw-items", {
                    method: "POST",
                    body: {
                        source_id: Number(form.elements.source_id.value),
                        issue_no: form.elements.issue_no.value,
                        raw_payload: parseJsonField(form.elements.raw_payload.value, "raw_payload"),
                    },
                });
                form.reset();
                await refreshCurrentRoute("原始内容已录入。");
                return;
            }
            if (form.id === "signalManagementForm") {
                await request("/api/platform/signals", {
                    method: "POST",
                    body: {
                        source_id: Number(form.elements.source_id.value),
                        lottery_type: form.elements.lottery_type.value,
                        issue_no: form.elements.issue_no.value,
                        bet_type: form.elements.bet_type.value,
                        bet_value: form.elements.bet_value.value,
                        confidence: form.elements.confidence.value ? Number(form.elements.confidence.value) : null,
                        normalized_payload: parseJsonField(form.elements.normalized_payload.value, "normalized_payload"),
                    },
                });
                form.reset();
                form.elements.lottery_type.value = "pc28";
                await refreshCurrentRoute("标准信号已录入。");
                return;
            }
            if (form.id === "telegramSettingsForm") {
                await request("/api/platform/admin/telegram-settings", {
                    method: "POST",
                    body: {
                        alert: {
                            enabled: Boolean(form.elements.alert_enabled.checked),
                            bot_token: form.elements.alert_bot_token.value,
                            target_chat_id: form.elements.alert_target_chat_id.value,
                            repeat_interval_seconds: Number(form.elements.alert_repeat_interval_seconds.value),
                            interval_seconds: Number(form.elements.alert_interval_seconds.value),
                        },
                        bot: {
                            enabled: Boolean(form.elements.bot_enabled.checked),
                            bot_token: form.elements.bot_bot_token.value,
                            poll_interval_seconds: Number(form.elements.bot_poll_interval_seconds.value),
                            bind_token_ttl_seconds: Number(form.elements.bot_bind_token_ttl_seconds.value),
                        },
                        report: {
                            enabled: Boolean(form.elements.report_enabled.checked),
                            target_chat_id: form.elements.report_target_chat_id.value,
                            interval_seconds: Number(form.elements.report_interval_seconds.value),
                            send_hour: Number(form.elements.report_send_hour.value),
                            send_minute: Number(form.elements.report_send_minute.value),
                            top_n: Number(form.elements.report_top_n.value),
                            timezone: form.elements.report_timezone.value,
                        },
                        auto_settlement: {
                            enabled: Boolean(form.elements.auto_settlement_enabled.checked),
                            interval_seconds: Number(form.elements.auto_settlement_interval_seconds.value),
                            draw_limit: Number(form.elements.auto_settlement_draw_limit.value),
                        },
                    },
                });
                await refreshCurrentRoute("后台 Worker 配置已保存，对应 worker 将在下一轮自动生效。");
                return;
            }
        } catch (error) {
            setStatus(error.message || "提交失败", true);
        }
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
            await loadRoute();
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
            await loadRoute();
            setStatus("登录成功。", false);
        } catch (error) {
            setStatus(error.message, true);
        }
    });

    document.getElementById("logoutBtn").addEventListener("click", async function () {
        try {
            await request("/api/auth/logout", { method: "POST", body: {} });
            setCurrentUser(null);
            window.location.href = "/";
        } catch (error) {
            setStatus(error.message, true);
        }
    });

    refreshButton.addEventListener("click", function () {
        loadRoute();
    });

    adminContent.addEventListener("click", function (event) {
        handleContentClick(event);
    });
    adminContent.addEventListener("change", function (event) {
        handleContentChange(event);
    });
    adminContent.addEventListener("input", function (event) {
        handleContentInput(event);
    });
    adminContent.addEventListener("submit", function (event) {
        handleContentSubmit(event);
    });

    if (!maybeRedirectLegacyHash()) {
        loadRoute();
    }
})();
