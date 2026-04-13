(function () {
    const state = {
        currentUser: null,
        alerts: [],
    };

    const statusMessage = document.getElementById("statusMessage");
    const refreshAlertsBtn = document.getElementById("refreshAlertsBtn");
    const alertsFilterForm = document.getElementById("alertsFilterForm");
    const resetAlertsFilterBtn = document.getElementById("resetAlertsFilterBtn");
    const alertsList = document.getElementById("alertsList");
    const workspaceLinks = Array.from(document.querySelectorAll(".workspace-link"));

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

    function renderPill(text, className) {
        return '<span class="' + className + '">' + escapeHtml(labelStatus(text)) + "</span>";
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

    function setActiveWorkspaceLink(targetHash) {
        workspaceLinks.forEach(function (link) {
            link.classList.toggle("is-active", link.getAttribute("href") === targetHash);
        });
    }

    function initWorkspaceNav() {
        workspaceLinks.forEach(function (link) {
            link.addEventListener("click", function () {
                const href = link.getAttribute("href") || "";
                if (href.startsWith("#")) {
                    setActiveWorkspaceLink(href);
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

    function setCurrentUser(user) {
        state.currentUser = user || null;
        window.PlatformAuthPanel.sync({
            user: state.currentUser,
            heroUsernameId: "alertsHeroUsername",
            heroUserMetaId: "alertsHeroUserMeta",
            heroLoggedInText: "当前会话已就绪，可查看异常提醒并处理可操作告警。",
            heroLoggedOutText: "登录后查看完整异常提醒和通知状态。",
            panelTitleLoggedIn: "当前账户",
            panelTitleLoggedOut: "登录与账户",
        });
    }

    function getFilterState() {
        return {
            severity: String(alertsFilterForm.elements.severity.value || "").trim(),
            alertType: String(alertsFilterForm.elements.alert_type.value || "").trim().toLowerCase(),
        };
    }

    function applyFilterStateToForm() {
        const params = new URLSearchParams(window.location.search);
        alertsFilterForm.elements.severity.value = params.get("severity") || "";
        alertsFilterForm.elements.alert_type.value = params.get("alert_type") || "";
    }

    function syncFilterStateToUrl() {
        const filterState = getFilterState();
        const params = new URLSearchParams();
        if (filterState.severity) {
            params.set("severity", filterState.severity);
        }
        if (filterState.alertType) {
            params.set("alert_type", filterState.alertType);
        }
        const nextUrl = window.location.pathname + (params.toString() ? "?" + params.toString() : "");
        window.history.replaceState({}, "", nextUrl);
    }

    function alertMatchesFilter(item) {
        const filterState = getFilterState();
        if (filterState.severity && String(item.severity || "") !== filterState.severity) {
            return false;
        }
        if (filterState.alertType && String(item.alert_type || "").toLowerCase().indexOf(filterState.alertType) === -1) {
            return false;
        }
        return true;
    }

    function filteredAlerts() {
        return state.alerts.filter(alertMatchesFilter);
    }

    function renderFilterSummary() {
        const filterState = getFilterState();
        document.getElementById("alertsFilterSummary").textContent = filterState.severity ? labelStatus(filterState.severity) : "全部告警";
        document.getElementById("alertsFilterDetail").textContent = filterState.alertType
            ? ("类型包含 " + filterState.alertType + " · 默认最多加载最近 50 条")
            : "默认最多加载最近 50 条告警";
    }

    function renderSummary() {
        const items = filteredAlerts();
        const criticalCount = items.filter(function (item) { return item.severity === "critical"; }).length;
        const warningCount = items.filter(function (item) { return item.severity === "warning"; }).length;
        const pendingCount = items.filter(function (item) { return item.notification_status === "pending"; }).length;
        const failedCount = items.filter(function (item) { return item.notification_status === "failed"; }).length;
        const actionableCount = items.filter(function (item) {
            const metadata = item.metadata || {};
            return Boolean(metadata.job_id || metadata.signal_id);
        }).length;

        document.getElementById("alertsCount").textContent = String(items.length);
        document.getElementById("alertsCriticalCount").textContent = String(criticalCount);
        document.getElementById("alertsWarningCount").textContent = String(warningCount);
        document.getElementById("alertsPendingCount").textContent = String(pendingCount);
        document.getElementById("alertsFailedCount").textContent = String(failedCount);
        document.getElementById("alertsActionableCount").textContent = String(actionableCount);
    }

    function notificationStatusPill(status) {
        const normalized = String(status || "pending");
        return renderPill(normalized, "alert-pill is-" + escapeHtml(normalized));
    }

    function buildAlertActions(item) {
        const metadata = item.metadata || {};
        const actions = [];
        if (metadata.job_id) {
            actions.push('<button class="ghost-btn retry-job-btn" type="button" data-job-id="' + escapeHtml(String(metadata.job_id)) + '">重试相关任务</button>');
        }
        if (metadata.signal_id) {
            actions.push('<a class="panel-link" href="/records?signal_id=' + encodeURIComponent(String(metadata.signal_id)) + '">查看相关记录</a>');
        }
        actions.push('<a class="panel-link" href="/admin#alertsSection">去管理端处理</a>');
        return actions.join("");
    }

    function renderMetadata(item) {
        const metadata = item.metadata || {};
        const parts = [];
        if (metadata.executor_id) {
            parts.push('<div class="alert-supporting-item"><strong>执行器</strong><span class="cell-muted mono-text">' + escapeHtml(String(metadata.executor_id)) + "</span></div>");
        }
        if (metadata.job_id) {
            parts.push('<div class="alert-supporting-item"><strong>任务 ID</strong><span class="cell-muted mono-text">' + escapeHtml(String(metadata.job_id)) + "</span></div>");
        }
        if (metadata.signal_id) {
            parts.push('<div class="alert-supporting-item"><strong>信号 ID</strong><span class="cell-muted mono-text">' + escapeHtml(String(metadata.signal_id)) + "</span></div>");
        }
        if (metadata.recent_failure_streak != null) {
            parts.push('<div class="alert-supporting-item"><strong>连续失败</strong><span class="cell-muted">' + escapeHtml(String(metadata.recent_failure_streak)) + " 次</span></div>");
        }
        const notification = item.notification || {};
        if (notification.last_sent_at || notification.last_error) {
            parts.push(
                '<div class="alert-supporting-item"><strong>通知状态</strong><span class="cell-muted">' +
                escapeHtml(notification.last_sent_at ? ("上次发送 " + notification.last_sent_at.replace("T", " ").replace("Z", " UTC")) : "未发送") +
                "</span>" +
                (notification.last_error ? '<span class="error-text">' + escapeHtml(notification.last_error) + "</span>" : "") +
                "</div>"
            );
        }
        return parts.join("");
    }

    function renderAlerts() {
        const items = filteredAlerts();
        if (!items.length) {
            alertsList.innerHTML = '<article class="alert-detail-card"><div class="alert-meta-row"><strong class="alert-title">当前筛选下没有异常提醒</strong>' + renderPill("INFO", "alert-pill is-info") + '</div><p class="alert-message">可以尝试清空筛选条件，或稍后重新刷新。</p></article>';
            return;
        }

        alertsList.innerHTML = items.map(function (item) {
            return [
                '<article class="alert-detail-card">',
                '<div class="alert-meta-row">',
                '<strong class="alert-title">' + escapeHtml(item.title || "--") + "</strong>",
                '<div class="alert-actions-inline">' + renderPill(item.severity || "--", "alert-pill is-" + escapeHtml(String(item.severity || "warning"))) + notificationStatusPill(item.notification_status) + "</div>",
                "</div>",
                '<p class="alert-message">' + escapeHtml(item.message || "--") + "</p>",
                '<div class="alert-supporting">',
                '<div class="alert-supporting-item"><strong>告警类型</strong><span class="cell-muted mono-text">' + escapeHtml(item.alert_type || "--") + "</span></div>",
                renderMetadata(item),
                "</div>",
                '<div class="alert-actions">' + buildAlertActions(item) + "</div>",
                "</article>",
            ].join("");
        }).join("");
    }

    function resetCollections() {
        state.alerts = [];
        renderFilterSummary();
        renderSummary();
        renderAlerts();
    }

    async function loadPageData() {
        const payload = await request("/api/platform/alerts?limit=50");
        state.alerts = payload.items || [];
    }

    async function refreshAll() {
        setButtonBusy(refreshAlertsBtn, true, "刷新中...");
        try {
            const user = await loadCurrentUser();
            renderFilterSummary();
            if (!user) {
                resetCollections();
                setStatus("登录后可查看完整异常提醒、通知状态和相关处理动作。", false);
                return;
            }

            syncFilterStateToUrl();
            renderFilterSummary();
            await loadPageData();
            renderSummary();
            renderAlerts();
            setStatus("异常提醒已刷新。", false);
        } catch (error) {
            resetCollections();
            setStatus(error.message || "异常提醒页面加载失败", true);
        } finally {
            setButtonBusy(refreshAlertsBtn, false, "刷新告警");
        }
    }

    alertsFilterForm.addEventListener("submit", function (event) {
        event.preventDefault();
        refreshAll();
    });

    resetAlertsFilterBtn.addEventListener("click", function () {
        alertsFilterForm.reset();
        refreshAll();
    });

    alertsList.addEventListener("click", async function (event) {
        const target = event.target;
        if (!(target instanceof HTMLElement) || !target.classList.contains("retry-job-btn")) {
            return;
        }
        const jobId = target.getAttribute("data-job-id");
        if (!jobId) {
            return;
        }
        try {
            await request("/api/platform/execution-jobs/" + jobId + "/retry", {
                method: "POST",
                body: {},
            });
            await refreshAll();
            setStatus("关联任务 " + jobId + " 已重置为待执行。", false);
        } catch (error) {
            setStatus(error.message, true);
        }
    });

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

    refreshAlertsBtn.addEventListener("click", function () {
        refreshAll();
    });

    applyFilterStateToForm();
    initWorkspaceNav();
    refreshAll();
}());
