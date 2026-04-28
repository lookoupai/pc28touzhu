(function () {
    if (!document.documentElement.hasAttribute("data-authenticated")) {
        document.documentElement.setAttribute("data-authenticated", "false");
    }

    const state = {
        user: null,
        passwordPanelOpen: false,
        initialized: false,
        bootstrapping: true,
        authRevision: 0,
    };

    function byId(id) {
        return document.getElementById(id);
    }

    function setVisible(element, visible) {
        if (!(element instanceof HTMLElement)) {
            return;
        }
        element.hidden = !visible;
        if (visible) {
            element.style.removeProperty("display");
        } else {
            element.style.setProperty("display", "none", "important");
        }
    }

    function resolveFieldContainer(element) {
        if (!(element instanceof HTMLElement)) {
            return null;
        }
        return element.closest("label") || element;
    }

    function setRequired(input, required) {
        if (!(input instanceof HTMLInputElement)) {
            return;
        }
        input.required = Boolean(required);
    }

    function setText(element, value) {
        if (!(element instanceof HTMLElement)) {
            return;
        }
        element.textContent = value;
    }

    function announceStatus(message, isError) {
        const statusEl = byId("statusMessage");
        if (!(statusEl instanceof HTMLElement)) {
            return;
        }
        statusEl.textContent = String(message || "");
        statusEl.classList.toggle("is-error", Boolean(isError));
    }

    function dispatchAuthEvent(name, detail) {
        document.dispatchEvent(new CustomEvent(name, {detail: detail || {}}));
    }

    function markAuthRevision() {
        state.authRevision += 1;
    }

    function ensureStyles() {
        if (byId("platformAuthPanelStyles")) {
            return;
        }
        const style = document.createElement("style");
        style.id = "platformAuthPanelStyles";
        style.textContent = [
            ".auth-inline-actions{display:flex;gap:10px;flex-wrap:wrap;align-items:center}",
            ".auth-password-panel{display:grid;gap:12px;padding:14px;border:1px solid rgba(20,37,45,.12);border-radius:16px;background:rgba(15,23,42,.03)}",
            ".auth-password-panel[hidden]{display:none!important}",
            ".auth-password-panel label{display:grid;gap:8px;font-size:.88rem;font-weight:700}",
            ".auth-helper-text{margin:0;font-size:.84rem;line-height:1.6;color:rgba(15,23,42,.72)}",
        ].join("");
        document.head.appendChild(style);
    }

    function ensureEnhancements() {
        const authForm = byId("authForm");
        if (!(authForm instanceof HTMLElement)) {
            return;
        }
        ensureStyles();
        const accountCard = byId("currentUsername") && byId("currentUsername").closest(".account-card");
        if (accountCard instanceof HTMLElement && !byId("authenticatedAccountPanel")) {
            const panel = document.createElement("div");
            panel.id = "authenticatedAccountPanel";
            panel.className = "auth-password-panel";
            panel.hidden = true;
            panel.innerHTML = [
                '<p class="auth-helper-text">当前会话已建立。这里统一处理密码维护和退出登录。</p>',
                '<div class="auth-inline-actions">',
                '<button id="changePasswordToggleBtn" class="ghost-btn" type="button">修改密码</button>',
                '<button id="authenticatedLogoutBtn" class="ghost-btn danger-btn" type="button">退出登录</button>',
                "</div>",
            ].join("");
            accountCard.insertAdjacentElement("afterend", panel);
        }

        const authenticatedPanel = byId("authenticatedAccountPanel");
        if (!(authenticatedPanel instanceof HTMLElement)) {
            return;
        }

        if (!byId("changePasswordForm")) {
            const panel = document.createElement("div");
            panel.id = "changePasswordForm";
            panel.className = "auth-password-panel";
            panel.hidden = true;
            panel.innerHTML = [
                '<p class="auth-helper-text">修改密码后，当前浏览器会自动保留新会话，其他旧会话会立即失效。</p>',
                '<label><span>当前密码</span><input id="authCurrentPassword" class="text-input" type="password" autocomplete="current-password"></label>',
                '<label><span>新密码</span><input id="authNewPassword" class="text-input" type="password" autocomplete="new-password"></label>',
                '<label><span>确认新密码</span><input id="authConfirmPassword" class="text-input" type="password" autocomplete="new-password"></label>',
                '<div class="auth-inline-actions">',
                '<button id="submitPasswordChangeBtn" class="primary-btn" type="button">确认修改</button>',
                '<button id="cancelPasswordChangeBtn" class="ghost-btn" type="button">取消</button>',
                "</div>",
            ].join("");
            authenticatedPanel.appendChild(panel);
        } else if (byId("changePasswordForm").parentElement !== authenticatedPanel) {
            authenticatedPanel.appendChild(byId("changePasswordForm"));
        }
    }

    function resetPasswordForm() {
        ["authCurrentPassword", "authNewPassword", "authConfirmPassword"].forEach(function (id) {
            const input = byId(id);
            if (input instanceof HTMLInputElement) {
                input.value = "";
            }
        });
    }

    function togglePasswordPanel(visible) {
        state.passwordPanelOpen = Boolean(visible);
        setVisible(byId("changePasswordForm"), state.passwordPanelOpen && Boolean(state.user));
    }

    async function requestAuth(path, body) {
        const response = await fetch(path, {
            method: "POST",
            credentials: "same-origin",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(body || {}),
        });
        const payload = await response.json().catch(function () { return {}; });
        if (!response.ok) {
            throw new Error(payload.error || "请求失败");
        }
        return payload;
    }

    function setButtonBusy(button, busy, busyText, idleText) {
        if (!(button instanceof HTMLButtonElement)) {
            return;
        }
        button.disabled = Boolean(busy);
        button.textContent = busy ? busyText : idleText;
    }

    async function handleRegister(button) {
        setButtonBusy(button, true, "注册中...", "注册");
        try {
            const payload = await requestAuth("/api/auth/register", {
                username: byId("authUsername").value,
                email: byId("authEmail").value,
                password: byId("authPassword").value,
            });
            markAuthRevision();
            sync({user: payload.user || null});
            closeAccountDialog();
            announceStatus(payload.message || "注册并登录成功。", false);
            dispatchAuthEvent("platform-auth:changed", {
                action: "register",
                user: payload.user || null,
                message: payload.message || "注册并登录成功。",
            });
        } catch (error) {
            announceStatus(error.message, true);
            dispatchAuthEvent("platform-auth:error", {
                action: "register",
                message: error.message,
            });
        } finally {
            setButtonBusy(button, false, "注册中...", "注册");
        }
    }

    async function handleLogin(button) {
        setButtonBusy(button, true, "登录中...", "登录");
        try {
            const payload = await requestAuth("/api/auth/login", {
                username: byId("authUsername").value,
                password: byId("authPassword").value,
            });
            markAuthRevision();
            sync({user: payload.user || null});
            closeAccountDialog();
            announceStatus("登录成功。", false);
            dispatchAuthEvent("platform-auth:changed", {
                action: "login",
                user: payload.user || null,
                message: "登录成功。",
            });
        } catch (error) {
            announceStatus(error.message, true);
            dispatchAuthEvent("platform-auth:error", {
                action: "login",
                message: error.message,
            });
        } finally {
            setButtonBusy(button, false, "登录中...", "登录");
        }
    }

    async function handleLogout(button) {
        const idleText = button.id === "authenticatedLogoutBtn" ? "退出登录" : "退出";
        setButtonBusy(button, true, "退出中...", idleText);
        try {
            await requestAuth("/api/auth/logout", {});
            markAuthRevision();
            sync({user: null});
            closeAccountDialog();
            announceStatus("已退出登录。", false);
            dispatchAuthEvent("platform-auth:changed", {
                action: "logout",
                user: null,
                message: "已退出登录。",
            });
        } catch (error) {
            announceStatus(error.message, true);
            dispatchAuthEvent("platform-auth:error", {
                action: "logout",
                message: error.message,
            });
        } finally {
            setButtonBusy(button, false, "退出中...", idleText);
        }
    }

    async function handleSubmitPasswordChange(button) {
        setButtonBusy(button, true, "提交中...", "确认修改");
        try {
            const payload = await requestAuth("/api/auth/change-password", {
                current_password: byId("authCurrentPassword").value,
                new_password: byId("authNewPassword").value,
                confirm_password: byId("authConfirmPassword").value,
            });
            markAuthRevision();
            sync({user: payload.user || state.user});
            togglePasswordPanel(false);
            closeAccountDialog();
            announceStatus(payload.message || "密码已更新。", false);
            dispatchAuthEvent("platform-auth:changed", {
                action: "change-password",
                user: payload.user || state.user,
                message: payload.message || "密码已更新。",
            });
        } catch (error) {
            announceStatus(error.message, true);
            dispatchAuthEvent("platform-auth:error", {
                action: "change-password",
                message: error.message,
            });
        } finally {
            setButtonBusy(button, false, "提交中...", "确认修改");
        }
    }

    function bindButtonAction(id, handler) {
        const button = byId(id);
        if (!(button instanceof HTMLButtonElement) || button.dataset.authBound === "true") {
            return;
        }
        button.dataset.authBound = "true";
        button.addEventListener("click", function (event) {
            event.preventDefault();
            handler(button);
        });
    }

    function wireAuthButtons() {
        bindButtonAction("registerBtn", handleRegister);
        bindButtonAction("loginBtn", handleLogin);
        bindButtonAction("logoutBtn", handleLogout);
        bindButtonAction("authenticatedLogoutBtn", handleLogout);
        bindButtonAction("changePasswordToggleBtn", function () {
            togglePasswordPanel(!state.passwordPanelOpen);
        });
        bindButtonAction("cancelPasswordChangeBtn", function () {
            togglePasswordPanel(false);
            resetPasswordForm();
        });
        bindButtonAction("submitPasswordChangeBtn", handleSubmitPasswordChange);
    }

    function closeAccountDialog() {
        if (window.PlatformAccountDialog && typeof window.PlatformAccountDialog.close === "function") {
            window.PlatformAccountDialog.close();
        }
    }

    function sync(options) {
        ensureEnhancements();
        wireAuthButtons();

        const normalized = options || {};
        const isBootstrapping = normalized.loading === undefined ? state.bootstrapping : Boolean(normalized.loading);
        state.bootstrapping = isBootstrapping;
        const currentUser = normalized.user || null;
        state.user = currentUser;
        const username = currentUser ? String(currentUser.username || "未命名用户") : "未登录";
        const role = currentUser ? String(currentUser.role || "user") : "";
        const status = currentUser ? String(currentUser.status || "active") : "";
        const meta = currentUser
            ? ((currentUser.email || "--") + " · " + role + " · " + status)
            : "请先注册或登录";
        const isAuthenticated = Boolean(currentUser);
        document.documentElement.setAttribute("data-authenticated", isAuthenticated ? "true" : "false");

        setText(byId("currentUsername"), username);
        setText(byId("currentUserMeta"), meta);

        const accountButton = document.querySelector("[data-auth-account-label]");
        if (accountButton instanceof HTMLElement) {
            accountButton.textContent = isAuthenticated ? username : "登录";
            accountButton.setAttribute("title", meta);
            accountButton.classList.toggle("is-authenticated", isAuthenticated);
        }

        if (normalized.heroUsernameId) {
            setText(byId(normalized.heroUsernameId), username);
        }
        if (normalized.heroUserMetaId) {
            setText(
                byId(normalized.heroUserMetaId),
                isAuthenticated ? String(normalized.heroLoggedInText || "") : String(normalized.heroLoggedOutText || ""),
            );
        }

        const panelTitle = document.querySelector("[data-auth-panel-title]");
        if (panelTitle instanceof HTMLElement) {
            panelTitle.textContent = isAuthenticated
                ? String(normalized.panelTitleLoggedIn || panelTitle.textContent || "当前账户")
                : String(normalized.panelTitleLoggedOut || panelTitle.textContent || "登录与账户");
        }

        const navLabel = document.querySelector("[data-auth-nav-label]");
        if (navLabel instanceof HTMLElement) {
            navLabel.textContent = isAuthenticated
                ? String(normalized.navLabelLoggedIn || normalized.panelTitleLoggedIn || "当前账户")
                : String(normalized.navLabelLoggedOut || normalized.panelTitleLoggedOut || "登录与账户");
        }

        const authForm = byId("authForm");
        if (authForm instanceof HTMLElement) {
            authForm.classList.toggle("is-authenticated", isAuthenticated);
            authForm.classList.toggle("is-bootstrapping", isBootstrapping);
            authForm.setAttribute("data-auth-state", isAuthenticated ? "authenticated" : "anonymous");
        }
        setVisible(authForm, !isAuthenticated && !isBootstrapping);
        setVisible(byId("authenticatedAccountPanel"), isAuthenticated);

        const usernameInput = byId("authUsername");
        const emailInput = byId("authEmail");
        const passwordInput = byId("authPassword");

        const shouldShowAnonymousFields = !isAuthenticated && !isBootstrapping;

        setVisible(resolveFieldContainer(usernameInput), shouldShowAnonymousFields);
        setVisible(resolveFieldContainer(emailInput), shouldShowAnonymousFields);
        setVisible(resolveFieldContainer(passwordInput), shouldShowAnonymousFields);

        setRequired(usernameInput, shouldShowAnonymousFields);
        setRequired(passwordInput, shouldShowAnonymousFields);

        if (usernameInput instanceof HTMLInputElement) {
            usernameInput.value = isAuthenticated ? username : "";
        }
        if (emailInput instanceof HTMLInputElement) {
            emailInput.value = isAuthenticated ? String(currentUser.email || "") : "";
        }
        if (passwordInput instanceof HTMLInputElement) {
            passwordInput.value = "";
        }

        setVisible(byId("registerBtn"), shouldShowAnonymousFields);
        setVisible(byId("loginBtn"), shouldShowAnonymousFields);
        setVisible(byId("logoutBtn"), false);
        setVisible(byId("changePasswordToggleBtn"), isAuthenticated);
        setVisible(byId("authenticatedLogoutBtn"), isAuthenticated);

        if (!isAuthenticated) {
            togglePasswordPanel(false);
        } else {
            togglePasswordPanel(state.passwordPanelOpen);
        }
        resetPasswordForm();

        if (isAuthenticated) {
            try {
                const pending = sessionStorage.getItem("post_login_redirect");
                if (pending && pending.indexOf("/admin") === 0) {
                    sessionStorage.removeItem("post_login_redirect");
                    const current = window.location.pathname + window.location.search + window.location.hash;
                    if (current !== pending) {
                        window.location.href = pending;
                    }
                }
            } catch (error) {
                // ignore storage errors
            }
        }
    }

    function bindAuthActions() {
        if (state.initialized) {
            wireAuthButtons();
            return;
        }
        ensureEnhancements();
        state.initialized = true;
        wireAuthButtons();
    }

    async function bootstrapCurrentUser() {
        const revisionAtStart = state.authRevision;
        sync({user: state.user, loading: true});
        try {
            const response = await fetch("/api/auth/me", {
                method: "GET",
                credentials: "same-origin",
                headers: {
                    "Content-Type": "application/json",
                },
            });
            const payload = await response.json().catch(function () { return {}; });
            if (revisionAtStart !== state.authRevision) {
                return;
            }
            if (response.status === 401) {
                sync({user: null, loading: false});
                return;
            }
            if (!response.ok) {
                sync({user: null, loading: false});
                return;
            }
            sync({user: payload.user || null, loading: false});
        } catch (error) {
            sync({user: null, loading: false});
        }
    }

    function boot() {
        bindAuthActions();
        bootstrapCurrentUser();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }

    window.PlatformAuthPanel = {
        bind: bindAuthActions,
        sync: sync,
        close: closeAccountDialog,
        announceStatus: announceStatus,
    };
})();
