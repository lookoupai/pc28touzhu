(function () {
    "use strict";
    const dataNode = document.getElementById("appNavigationData");
    if (!dataNode) return;
    const page = JSON.parse(dataNode.textContent);
    const legacyTarget = page.legacy[window.location.hash];
    if (legacyTarget) {
        const destination = new URL(legacyTarget, window.location.origin);
        destination.search = window.location.search;
        if (destination.pathname !== window.location.pathname) {
            window.location.replace(destination.pathname + destination.search + destination.hash);
        } else {
            window.history.replaceState(null, "", destination.pathname + destination.search + destination.hash);
        }
    }
    document.title = page.title + " · pc28touzhu";
    if (document.body.hasAttribute("data-admin-access-denied")) {
        document.addEventListener("click", function (event) {
            if (!event.target.closest("[data-account-open], #accountMenuBtn")) return;
            try { sessionStorage.setItem("post_login_redirect", location.pathname + location.search + location.hash); } catch (error) { /* Storage may be disabled. */ }
        });
    }
    document.body.classList.add("has-app-shell");

    const sidebar = document.getElementById("appSidebar");
    const toggle = document.getElementById("navigationToggle");
    const close = document.getElementById("navigationClose");
    const backdrop = document.getElementById("navigationBackdrop");
    const main = document.getElementById("mainContent");
    const header = document.querySelector(".app-header");
    const mobile = window.matchMedia("(max-width: 960px)");
    let opened = false;

    function setOpen(value, restoreFocus) {
        opened = Boolean(value && mobile.matches);
        document.body.classList.toggle("navigation-open", opened);
        toggle.setAttribute("aria-expanded", String(opened));
        backdrop.hidden = !opened;
        sidebar.inert = mobile.matches && !opened;
        if (main) main.inert = opened;
        header.inert = opened;
        if (opened) {
            sidebar.setAttribute("role", "dialog");
            sidebar.setAttribute("aria-modal", "true");
            close.focus();
        } else {
            sidebar.removeAttribute("role");
            sidebar.removeAttribute("aria-modal");
            if (restoreFocus) toggle.focus();
        }
    }
    toggle.addEventListener("click", function () { setOpen(!opened, true); });
    close.addEventListener("click", function () { setOpen(false, true); });
    backdrop.addEventListener("click", function () { setOpen(false, true); });
    mobile.addEventListener("change", function () { setOpen(false, false); });
    document.addEventListener("keydown", function (event) {
        if (!opened) return;
        if (event.key === "Escape") { event.preventDefault(); setOpen(false, true); }
        if (event.key !== "Tab") return;
        const items = Array.from(sidebar.querySelectorAll("a, button")).filter(function (item) {
            return !item.closest("[hidden]") && !item.disabled;
        });
        const first = items[0];
        const last = items[items.length - 1];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    });
    setOpen(false, false);

    function updatePermissions() {
        const admin = document.documentElement.getAttribute("data-auth-role") === "admin";
        document.querySelectorAll("[data-admin-only]").forEach(function (element) { element.hidden = !admin; });
    }
    document.addEventListener("platform-auth:synced", updatePermissions);
    new MutationObserver(updatePermissions).observe(document.body, {childList: true, subtree: true});
    updatePermissions();

    // Only real configuration forms participate; filters and authentication do not.
    const originals = new WeakMap();
    const edited = new Set();
    let leaving = false;
    function snapshot(form) {
        return JSON.stringify(Array.from(form.querySelectorAll("input, select, textarea")).map(function (field) {
            if (field.type === "file") return [field.name, Array.from(field.files).map(function (file) { return [file.name, file.size, file.lastModified]; })];
            return [field.name, field.id, field.value, field.checked, field.multiple ? Array.from(field.selectedOptions).map(function (option) { return option.value; }) : null];
        }));
    }
    function markClean(form) {
        if (!form) return;
        originals.set(form, snapshot(form));
        edited.delete(form);
    }
    function markDirty(form) {
        originals.delete(form);
        edited.add(form);
    }
    function isDirty(form) {
        return form.isConnected && edited.has(form) && originals.get(form) !== snapshot(form);
    }
    function confirmLeaveForm(form) {
        return !form || !isDirty(form) || window.confirm("当前修改尚未保存，确定离开吗？");
    }
    document.addEventListener("focusin", function (event) {
        const form = event.target.closest("form[data-track-changes]");
        if (form && !edited.has(form)) markClean(form);
    });
    ["input", "change"].forEach(function (name) {
        document.addEventListener(name, function (event) {
            const form = event.target.closest("form[data-track-changes]");
            if (form) edited.add(form);
        });
    });
    document.addEventListener("click", function (event) {
        const button = event.target.closest("button[type=button]");
        const form = button && button.closest("form[data-track-changes]");
        if (form) {
            if (!originals.has(form)) markClean(form);
            edited.add(form);
        }
    }, true);
    document.addEventListener("reset", function (event) {
        if (event.target.matches("form[data-track-changes]")) {
            window.setTimeout(function () { markClean(event.target); }, 0);
        }
    }, true);
    document.addEventListener("click", function (event) {
        const link = event.target.closest("a[href]");
        if (!link || event.defaultPrevented || event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey || link.target === "_blank" || link.hasAttribute("download")) return;
        const destination = new URL(link.href, window.location.href);
        const changesPage = destination.origin !== location.origin || destination.pathname !== location.pathname || destination.search !== location.search;
        if (changesPage && Array.from(edited).some(isDirty)) {
            if (!window.confirm("当前修改尚未保存，确定离开吗？")) { event.preventDefault(); return; }
            leaving = true;
            window.setTimeout(function () { leaving = false; }, 1500);
        }
        if (link.closest(".app-navigation")) setOpen(false, false);
    });
    window.addEventListener("beforeunload", function (event) {
        if (leaving || !Array.from(edited).some(isDirty)) return;
        event.preventDefault();
        event.returnValue = "";
    });
    window.addEventListener("pageshow", function () { leaving = false; setOpen(false, false); });

    function setQuery(values) {
        const url = new URL(window.location.href);
        Object.keys(values).forEach(function (key) {
            if (values[key] == null || values[key] === "") url.searchParams.delete(key);
            else url.searchParams.set(key, String(values[key]));
        });
        window.history.replaceState(null, "", url.pathname + url.search + url.hash);
    }
    window.PlatformNavigation = {markClean: markClean, markDirty: markDirty, isDirty: isDirty, confirmLeaveForm: confirmLeaveForm, setQuery: setQuery};
}());
