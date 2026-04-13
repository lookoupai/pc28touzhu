(function () {
    if (!document.documentElement.hasAttribute("data-authenticated")) {
        document.documentElement.setAttribute("data-authenticated", "false");
    }

    function byId(id) {
        return document.getElementById(id);
    }

    function setVisible(element, visible) {
        if (!(element instanceof HTMLElement)) {
            return;
        }
        element.hidden = !visible;
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

    function sync(options) {
        const normalized = options || {};
        const currentUser = normalized.user || null;
        const username = currentUser ? String(currentUser.username || "未命名用户") : "未登录";
        const meta = currentUser
            ? ((currentUser.email || "--") + " · " + (currentUser.role || "user"))
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
            authForm.setAttribute("data-auth-state", isAuthenticated ? "authenticated" : "anonymous");
        }

        const usernameInput = byId("authUsername");
        const emailInput = byId("authEmail");
        const passwordInput = byId("authPassword");

        setVisible(resolveFieldContainer(usernameInput), !isAuthenticated);
        setVisible(resolveFieldContainer(emailInput), !isAuthenticated);
        setVisible(resolveFieldContainer(passwordInput), !isAuthenticated);

        setRequired(usernameInput, !isAuthenticated);
        setRequired(passwordInput, !isAuthenticated);

        if (usernameInput instanceof HTMLInputElement && isAuthenticated) {
            usernameInput.value = username;
        }
        if (emailInput instanceof HTMLInputElement && isAuthenticated) {
            emailInput.value = String(currentUser.email || "");
        }
        if (passwordInput instanceof HTMLInputElement) {
            passwordInput.value = "";
        }

        setVisible(byId("registerBtn"), !isAuthenticated);
        setVisible(byId("loginBtn"), !isAuthenticated);
        setVisible(byId("logoutBtn"), isAuthenticated);

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

    window.PlatformAuthPanel = {
        sync: sync,
    };
})();
