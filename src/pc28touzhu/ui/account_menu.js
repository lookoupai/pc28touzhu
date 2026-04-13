(function () {
    function byId(id) {
        return document.getElementById(id);
    }

    function isDialogSupported(dialog) {
        return dialog && typeof dialog.showModal === "function" && typeof dialog.close === "function";
    }

    function setupAccountDialog() {
        const dialog = byId("accountDialog");
        const openButton = byId("accountMenuBtn");
        const closeButton = byId("accountDialogCloseBtn");
        const extraOpenButtons = Array.from(document.querySelectorAll("[data-account-open]"));

        if (!(dialog instanceof HTMLElement) || !(openButton instanceof HTMLElement)) {
            return;
        }

        let lastFocus = null;

        function openDialog() {
            lastFocus = document.activeElement;
            if (dialog.hasAttribute("open")) {
                return;
            }
            if (isDialogSupported(dialog)) {
                dialog.showModal();
                return;
            }
            dialog.setAttribute("open", "");
        }

        function closeDialog() {
            if (isDialogSupported(dialog)) {
                dialog.close();
            } else {
                dialog.removeAttribute("open");
            }
            if (lastFocus && typeof lastFocus.focus === "function") {
                lastFocus.focus();
            } else if (typeof openButton.focus === "function") {
                openButton.focus();
            }
        }

        openButton.addEventListener("click", openDialog);
        extraOpenButtons.forEach(function (btn) {
            btn.addEventListener("click", openDialog);
        });

        if (closeButton instanceof HTMLElement) {
            closeButton.addEventListener("click", closeDialog);
        }

        dialog.addEventListener("click", function (event) {
            if (event.target === dialog) {
                closeDialog();
            }
        });

        dialog.addEventListener("cancel", function () {
            if (!isDialogSupported(dialog)) {
                closeDialog();
            }
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", setupAccountDialog);
    } else {
        setupAccountDialog();
    }
})();
