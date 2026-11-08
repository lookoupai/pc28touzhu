(function () {
    "use strict";
    const $ = function (id) { return document.getElementById(id); };
    let user = null;
    let binding = null;
    let revision = 0;
    let busy = false;
    function status(message, error) { $("statusMessage").textContent = message; $("statusMessage").classList.toggle("is-error", Boolean(error)); }
    async function request(path, method) {
        const response = await fetch(path, {method: method || "GET", credentials: "same-origin", headers: {"Content-Type": "application/json"}, body: method === "POST" ? "{}" : undefined});
        const payload = await response.json().catch(function () { return {}; });
        if (!response.ok) throw new Error(payload.error || "请求失败，请重试");
        return payload;
    }
    function tokenActive() {
        const timestamp = Date.parse(binding && binding.bind_token_expire_at || "");
        return Boolean(binding && binding.bind_token && Number.isFinite(timestamp) && timestamp > Date.now());
    }
    function render() {
        const bound = binding && binding.is_bound;
        const active = tokenActive();
        $("botBindingSummary").textContent = !user ? "登录后生成绑定码，再到 Telegram 私聊收益查询 Bot 完成绑定。" : bound ? "当前已绑定 " + (binding.telegram_username ? "@" + binding.telegram_username : "Telegram 用户 " + binding.telegram_user_id) : "生成绑定码后，到 Telegram 私聊收益查询 Bot 完成绑定。";
        $("botBindingMetrics").textContent = "";
        $("botBindingCommand").textContent = active ? "/bind " + binding.bind_token : "/bind <绑定码>";
        $("botBindingCommandMeta").textContent = active ? "有效期至 " + new Date(binding.bind_token_expire_at).toLocaleString("zh-CN") : binding && binding.bind_token ? "绑定码已过期，请重新生成。" : "点击生成绑定码后，复制完整命令。";
        $("botBindingActions").innerHTML = '<button id="generateBindCodeBtn" class="primary-btn" type="button">' + (busy ? "处理中…" : active || bound ? "重新生成绑定码" : "生成绑定码") + '</button><button id="copyBindCommandBtn" class="ghost-btn" type="button">复制绑定命令</button><button id="clearTelegramBindingBtn" class="ghost-btn" type="button">解除绑定 / 清空绑定码</button>';
        $("generateBindCodeBtn").disabled = !user || busy;
        $("copyBindCommandBtn").disabled = !active || busy;
        $("clearTelegramBindingBtn").disabled = !user || busy || !(bound || binding && binding.bind_token);
        $("botBindingInstructions").innerHTML = '<p>1. 在本页生成绑定码并复制命令。</p><p>2. 在 Telegram 私聊收益查询 Bot，发送 /start，再发送绑定命令。</p><p>3. 绑定后可使用 /profit 查询收益，/plan 查看方案，/rule 查看自动规则。</p><p>未找到 Bot 或没有响应时，请联系管理员确认 Bot 配置。</p>';
    }
    async function refresh(nextUser) {
        const token = ++revision;
        $("refreshBindingBtn").disabled = true;
        try {
            const current = nextUser === undefined ? (await request("/api/auth/me")).user : nextUser;
            if (token !== revision) return;
            user = current || null; binding = null;
            window.PlatformAuthPanel.sync({user: user, loading: false});
            render();
            const payload = user ? await request("/api/platform/telegram-binding") : {};
            if (token !== revision) return;
            binding = payload.item || null; render(); status("", false);
        } catch (error) { if (token === revision) { binding = null; render(); status(error.message, true); } }
        finally { if (token === revision) $("refreshBindingBtn").disabled = false; }
    }
    $("botBindingActions").addEventListener("click", async function (event) {
        const button = event.target.closest("button");
        if (!button || button.disabled || busy) return;
        if (button.id === "copyBindCommandBtn") {
            if (!tokenActive()) { render(); return; }
            try { await navigator.clipboard.writeText($("botBindingCommand").textContent); status("绑定命令已复制。", false); }
            catch (error) { status("无法自动复制，请选中上方绑定命令手动复制。", true); }
            return;
        }
        const remove = button.id === "clearTelegramBindingBtn";
        if (remove && !window.confirm("解除后，当前 Telegram 账号将无法查询本账户收益。确定继续？")) return;
        const token = revision;
        busy = true; render();
        try {
            const result = await request("/api/platform/telegram-binding/" + (remove ? "unbind" : "token"), "POST");
            if (token !== revision) return;
            binding = result.item || null;
            status(remove ? "绑定已解除，绑定码已清空。" : "绑定码已生成，请在有效期内使用。", false);
        } catch (error) { if (token === revision) status(error.message, true); }
        finally { busy = false; render(); }
    });
    $("refreshBindingBtn").addEventListener("click", function () { refresh(); });
    document.addEventListener("platform-auth:changed", function (event) { refresh(event.detail.user || null); });
    render(); refresh();
}());
