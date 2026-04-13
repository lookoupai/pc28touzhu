(function () {
    const state = {
        currentUser: null,
        jobs: [],
        alerts: [],
    };

    const statusMessage = document.getElementById("statusMessage");
    const refreshRecordsBtn = document.getElementById("refreshRecordsBtn");
    const recordsFilterForm = document.getElementById("recordsFilterForm");
    const resetRecordsFilterBtn = document.getElementById("resetRecordsFilterBtn");
    const recordsTableBody = document.getElementById("recordsTableBody");
    const recordsCardList = document.getElementById("recordsCardList");
    const recordsAlertList = document.getElementById("recordsAlertList");
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
            heroUsernameId: "recordsHeroUsername",
            heroUserMetaId: "recordsHeroUserMeta",
            heroLoggedInText: "当前会话已就绪，可筛选记录并处理可重试任务。",
            heroLoggedOutText: "登录后查看完整执行记录和可重试任务。",
            panelTitleLoggedIn: "当前账户",
            panelTitleLoggedOut: "登录与账户",
        });
    }

    function getFilterState() {
        const status = String(recordsFilterForm.elements.status.value || "").trim();
        const signalId = String(recordsFilterForm.elements.signal_id.value || "").trim();
        return {
            status: status,
            signalId: signalId,
        };
    }

    function applyFilterStateToForm() {
        const params = new URLSearchParams(window.location.search);
        recordsFilterForm.elements.status.value = params.get("status") || "";
        recordsFilterForm.elements.signal_id.value = params.get("signal_id") || "";
    }

    function syncFilterStateToUrl() {
        const filterState = getFilterState();
        const params = new URLSearchParams();
        if (filterState.status) {
            params.set("status", filterState.status);
        }
        if (filterState.signalId) {
            params.set("signal_id", filterState.signalId);
        }
        const nextUrl = window.location.pathname + (params.toString() ? "?" + params.toString() : "");
        window.history.replaceState({}, "", nextUrl);
    }

    function renderFilterSummary() {
        const filterState = getFilterState();
        const statusText = filterState.status ? labelStatus(filterState.status) : "全部状态";
        const signalText = filterState.signalId ? ("signal #" + filterState.signalId) : "全部信号";
        document.getElementById("recordsFilterSummary").textContent = statusText;
        document.getElementById("recordsFilterDetail").textContent = signalText + " · 默认最多加载最近 100 条";
    }

    function renderSummary() {
        const totalCount = state.jobs.length;
        const deliveredCount = state.jobs.filter(function (item) { return item.status === "delivered"; }).length;
        const pendingCount = state.jobs.filter(function (item) { return item.status === "pending"; }).length;
        const issueCount = state.jobs.filter(function (item) {
            return item.status === "failed" || item.status === "expired" || item.status === "skipped";
        }).length;
        const retryableCount = state.jobs.filter(function (item) { return Boolean(item.can_retry); }).length;

        document.getElementById("recordsCount").textContent = String(totalCount);
        document.getElementById("recordsDeliveredCount").textContent = String(deliveredCount);
        document.getElementById("recordsPendingCount").textContent = String(pendingCount);
        document.getElementById("recordsIssueCount").textContent = String(issueCount);
        document.getElementById("recordsRetryableCount").textContent = String(retryableCount);
        document.getElementById("recordsAlertCount").textContent = String(state.alerts.length);
    }

    function renderJobs() {
        if (!state.jobs.length) {
            recordsTableBody.innerHTML = '<tr class="empty-row"><td colspan="9">当前筛选下暂无执行记录。</td></tr>';
            recordsCardList.innerHTML = '<article class="records-job-card is-empty"><strong>当前筛选下暂无执行记录</strong><p>可以调整状态或 signal 条件后重新查看。</p></article>';
            return;
        }

        recordsTableBody.innerHTML = state.jobs.map(function (item) {
            const statusClass = "job-pill is-" + escapeHtml(String(item.status || "pending"));
            const errorText = item.last_error_message || item.error_message || "--";
            const retryCell = item.can_retry
                ? '<button class="ghost-btn retry-job-btn" type="button" data-job-id="' + item.id + '">重试</button>'
                : '<span class="cell-muted">--</span>';

            return [
                "<tr>",
                "<td>" + escapeHtml(String(item.id)) + "</td>",
                "<td><div class=\"cell-stack\"><strong>" + escapeHtml(item.issue_no || "--") + "</strong><span class=\"cell-muted\">" + escapeHtml((item.bet_type || "--") + " / " + (item.bet_value || "--")) + "</span></div></td>",
                "<td><div class=\"cell-stack\"><span class=\"mono-text\">" + escapeHtml(item.target_key || "--") + "</span><span class=\"cell-muted\">" + escapeHtml(item.target_name || "--") + "</span></div></td>",
                "<td><div class=\"cell-stack\"><span>" + escapeHtml(item.telegram_account_label || "--") + '</span><span class="cell-muted">ID ' + escapeHtml(item.telegram_account_id == null ? "--" : String(item.telegram_account_id)) + "</span></div></td>",
                "<td>" + renderPill(item.status || "--", statusClass) + "</td>",
                "<td><div class=\"cell-stack\"><strong>" + escapeHtml(String(item.attempt_count || 0)) + '</strong><span class="cell-muted">' + escapeHtml(item.last_attempt_no == null ? "未执行" : ("最近第 " + item.last_attempt_no + " 次")) + "</span></div></td>",
                "<td><span class=\"" + (errorText === "--" ? "cell-muted" : "error-text") + "\">" + escapeHtml(errorText) + "</span></td>",
                "<td><div class=\"cell-stack\"><span>" + escapeHtml(formatDateTime(item.updated_at)) + '</span><span class="cell-muted">' + escapeHtml(item.last_executed_at ? ("上次执行 " + formatDateTime(item.last_executed_at)) : "--") + "</span></div></td>",
                "<td>" + retryCell + "</td>",
                "</tr>",
            ].join("");
        }).join("");

        recordsCardList.innerHTML = state.jobs.map(function (item) {
            const statusClass = "job-pill is-" + escapeHtml(String(item.status || "pending"));
            const errorText = item.last_error_message || item.error_message || "--";
            const retryAction = item.can_retry
                ? '<button class="ghost-btn retry-job-btn" type="button" data-job-id="' + item.id + '">重试任务</button>'
                : '<span class="cell-muted">当前不可重试</span>';
            return [
                '<article class="records-job-card">',
                '<div class="records-job-head"><div class="cell-stack"><strong>#' + escapeHtml(String(item.id)) + " · " + escapeHtml(item.issue_no || "--") + '</strong><span class="cell-muted">' + escapeHtml((item.bet_type || "--") + " / " + (item.bet_value || "--")) + "</span></div>" + renderPill(item.status || "--", statusClass) + "</div>",
                '<div class="records-job-grid">',
                '<div class="records-job-item"><span class="records-job-label">投递群组</span><strong>' + escapeHtml(item.target_name || item.target_key || "--") + '</strong><span class="mono-text">' + escapeHtml(item.target_key || "--") + "</span></div>",
                '<div class="records-job-item"><span class="records-job-label">托管账号</span><strong>' + escapeHtml(item.telegram_account_label || "--") + '</strong><span class="cell-muted">ID ' + escapeHtml(item.telegram_account_id == null ? "--" : String(item.telegram_account_id)) + "</span></div>",
                '<div class="records-job-item"><span class="records-job-label">尝试次数</span><strong>' + escapeHtml(String(item.attempt_count || 0)) + '</strong><span class="cell-muted">' + escapeHtml(item.last_attempt_no == null ? "未执行" : ("最近第 " + item.last_attempt_no + " 次")) + "</span></div>",
                '<div class="records-job-item"><span class="records-job-label">最近更新</span><strong>' + escapeHtml(formatDateTime(item.updated_at)) + '</strong><span class="cell-muted">' + escapeHtml(item.last_executed_at ? ("上次执行 " + formatDateTime(item.last_executed_at)) : "--") + "</span></div>",
                "</div>",
                '<p class="' + (errorText === "--" ? "cell-muted" : "error-text") + '">' + escapeHtml(errorText) + "</p>",
                '<div class="records-job-actions">' + retryAction + "</div>",
                "</article>",
            ].join("");
        }).join("");
    }

    function renderAlerts() {
        if (!state.alerts.length) {
            recordsAlertList.innerHTML = '<article class="alert-card"><div class="alert-title-row"><strong>当前没有异常提醒</strong>' + renderPill("INFO", "alert-pill is-info") + '</div><p class="cell-muted">如果后续出现执行器离线、任务重试耗尽等问题，会在这里提示。</p></article>';
            return;
        }

        recordsAlertList.innerHTML = state.alerts.map(function (item) {
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

    function resetCollections() {
        state.jobs = [];
        state.alerts = [];
        renderFilterSummary();
        renderSummary();
        renderJobs();
        renderAlerts();
    }

    async function loadPageData() {
        const filterState = getFilterState();
        const params = new URLSearchParams();
        params.set("limit", "100");
        if (filterState.status) {
            params.set("status", filterState.status);
        }
        if (filterState.signalId) {
            params.set("signal_id", filterState.signalId);
        }

        const results = await Promise.all([
            request("/api/platform/execution-jobs?" + params.toString()),
            request("/api/platform/alerts?limit=6"),
        ]);

        state.jobs = results[0].items || [];
        state.alerts = results[1].items || [];
    }

    async function refreshAll() {
        setButtonBusy(refreshRecordsBtn, true, "刷新中...");
        try {
            const user = await loadCurrentUser();
            renderFilterSummary();
            if (!user) {
                resetCollections();
                setStatus("登录后可查看完整执行记录、失败原因和可重试任务。", false);
                return;
            }

            syncFilterStateToUrl();
            renderFilterSummary();
            await loadPageData();
            renderSummary();
            renderJobs();
            renderAlerts();
            setStatus("执行记录和异常摘要已刷新。", false);
        } catch (error) {
            resetCollections();
            setStatus(error.message || "执行记录页面加载失败", true);
        } finally {
            setButtonBusy(refreshRecordsBtn, false, "刷新记录");
        }
    }

    recordsFilterForm.addEventListener("submit", function (event) {
        event.preventDefault();
        refreshAll();
    });

    resetRecordsFilterBtn.addEventListener("click", function () {
        recordsFilterForm.reset();
        refreshAll();
    });

    recordsTableBody.addEventListener("click", async function (event) {
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
            setStatus("执行任务 " + jobId + " 已重置为待执行。", false);
        } catch (error) {
            setStatus(error.message, true);
        }
    });

    recordsCardList.addEventListener("click", async function (event) {
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
            setStatus("执行任务 " + jobId + " 已重置为待执行。", false);
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

    refreshRecordsBtn.addEventListener("click", function () {
        refreshAll();
    });

    applyFilterStateToForm();
    initWorkspaceNav();
    refreshAll();
}());
