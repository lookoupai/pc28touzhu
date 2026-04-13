(function () {
    function isAuthenticated() {
        return document.documentElement.getAttribute("data-authenticated") === "true";
    }

    function openAccountDialog() {
        const dialog = document.getElementById("accountDialog");
        const openButton = document.getElementById("accountMenuBtn");
        if (openButton instanceof HTMLElement) {
            openButton.click();
            return true;
        }
        if (!(dialog instanceof HTMLElement)) {
            return false;
        }
        if (typeof dialog.showModal === "function") {
            dialog.showModal();
            return true;
        }
        dialog.setAttribute("open", "");
        return true;
    }

    function setStatusHint(message) {
        const statusEl = document.getElementById("statusMessage");
        if (!(statusEl instanceof HTMLElement)) {
            return;
        }
        statusEl.textContent = message;
        statusEl.classList.remove("is-error");
    }

    function attachAuthGuards() {
        const guardedLinks = Array.from(document.querySelectorAll("a[data-requires-auth]"));
        if (!guardedLinks.length) {
            return;
        }
        guardedLinks.forEach(function (link) {
            link.addEventListener("click", function (event) {
                if (isAuthenticated()) {
                    return;
                }
                const href = link.getAttribute("href") || "";
                if (!href) {
                    return;
                }
                event.preventDefault();
                try {
                    if (href.indexOf("/admin") === 0) {
                        sessionStorage.setItem("post_login_redirect", href);
                    }
                } catch (error) {
                    // ignore storage errors
                }
                setStatusHint("请先登录后再进入管理控制台。");
                openAccountDialog();
            });
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", attachAuthGuards);
    } else {
        attachAuthGuards();
    }
})();
