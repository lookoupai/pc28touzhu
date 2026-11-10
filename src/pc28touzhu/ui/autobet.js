(function () {
    const state = {
        supportsExplicitStart: false,
        currentUser: null,
        sources: [],
        rawItems: [],
        signals: [],
        accounts: [],
        targets: [],
        templates: [],
        subscriptions: [],
        jobs: [],
        failures: [],
        subscriptionStatDate: "",
        subscriptionDailySummary: null,
        expandedSubscriptionId: null,
        subscriptionDetailTabs: {},
        subscriptionAllDailyHistories: {},
        subscriptionRuntimeHistories: {},
        showArchivedTargets: false,
        scopeFocused: false,
    };
    let currentUserLoadToken = 0;
    let pipelineDataLoadToken = 0;

    const statusMessage = document.getElementById("statusMessage");
    const refreshAutobetBtn = document.getElementById("refreshAutobetBtn");
    const sourceCards = document.getElementById("sourceCards");
    const sourceList = document.getElementById("sourceList");
    const sourceForm = document.getElementById("sourceForm");
    const sourceUrlError = document.getElementById("sourceUrlError");
    const accountCards = document.getElementById("accountList");
    const targetCards = document.getElementById("targetList");
    const subscriptionCards = document.getElementById("subscriptionList");
    const accountForm = document.getElementById("accountForm");
    const targetForm = document.getElementById("targetForm");
    const messageTemplateForm = document.getElementById("messageTemplateForm");
    const subscriptionForm = document.getElementById("subscriptionForm");
    const toggleArchivedTargetsBtn = document.getElementById("toggleArchivedTargetsBtn");
    const batchResolveSubscriptionsBtn = document.getElementById("batchResolveSubscriptionsBtn");
    const subscriptionStatDateInput = document.getElementById("subscriptionStatDateInput");
    const subscriptionStatTodayBtn = document.getElementById("subscriptionStatTodayBtn");
    const cancelAccountEditBtn = document.getElementById("cancelAccountEditBtn");
    const cancelTargetEditBtn = document.getElementById("cancelTargetEditBtn");
    const cancelMessageTemplateEditBtn = document.getElementById("cancelMessageTemplateEditBtn");
    const cancelSubscriptionEditBtn = document.getElementById("cancelSubscriptionEditBtn");
    const accountVerifyForm = document.getElementById("accountVerifyForm");
    const accountModeButtons = Array.from(document.querySelectorAll("[data-account-auth-mode]"));
    const targetKeyPreview = document.getElementById("targetKeyPreview");
    const sourceNameInput = document.getElementById("sourceNameInput");
    const sourceUrlInput = document.getElementById("sourceUrlInput");
    const fillSourceExampleBtn = document.getElementById("fillSourceExampleBtn");
    const createSourceBtn = document.getElementById("createSourceBtn");
    const cancelSourceEditBtn = document.getElementById("cancelSourceEditBtn");
    const subscriptionBetFilterModeSelect = document.getElementById("subscriptionBetFilterModeSelect");
    const subscriptionBetFilterHint = document.getElementById("subscriptionBetFilterHint");
    const subscriptionBetFilterFields = document.getElementById("subscriptionBetFilterFields");
    const subscriptionBetFilterSelectionSummary = document.getElementById("subscriptionBetFilterSelectionSummary");
    const subscriptionTargetSelect = document.getElementById("subscriptionTargetSelect");
    const subscriptionTargetHint = document.getElementById("subscriptionTargetHint");
    const subscriptionStrategyModeSelect = document.getElementById("subscriptionStrategyModeSelect");
    const subscriptionStrategyModeHint = document.getElementById("subscriptionStrategyModeHint");
    const subscriptionSettlementRuleSourceSelect = document.getElementById("subscriptionSettlementRuleSourceSelect");
    const subscriptionSettlementRuleHint = document.getElementById("subscriptionSettlementRuleHint");
    const subscriptionRiskControlEnabledCheckbox = document.getElementById("subscriptionRiskControlEnabledCheckbox");
    const toggleSubscriptionAdvancedBtn = document.getElementById("toggleSubscriptionAdvancedBtn");
    const subscriptionAdvancedFields = document.getElementById("subscriptionAdvancedFields");
    const messageTemplateCards = document.getElementById("messageTemplateList");
    const templatePreviewBetType = document.getElementById("templatePreviewBetType");
    const templatePreviewBetValue = document.getElementById("templatePreviewBetValue");
    const templatePreviewAmount = document.getElementById("templatePreviewAmount");
    const templatePreviewIssueNo = document.getElementById("templatePreviewIssueNo");

    const SUBSCRIPTION_PLAY_FILTER_LABELS = {
        "big_small:大": "大",
        "big_small:小": "小",
        "odd_even:单": "单",
        "odd_even:双": "双",
        "combo:大单": "大单",
        "combo:大双": "大双",
        "combo:小单": "小单",
        "combo:小双": "小双",
    };
    const SUBSCRIPTION_PLAY_FILTER_PRESETS = {
        number: Array.from({length:28}, (_, i) => "number:" + i),
        big_small: ["big_small:大", "big_small:小"],
        odd_even: ["odd_even:单", "odd_even:双"],
        combo: ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
        double_group: ["大单+大双", "大单+小单", "大单+小双", "大双+小单", "大双+小双", "小单+小双"].map(function (value) { return "double_group:" + value; }),
        kill_group: ["大单", "大双", "小单", "小双"].map(function (value) { return "kill_group:" + value; }),
    };
    SUBSCRIPTION_PLAY_FILTER_PRESETS.double_group.forEach(function (key) { SUBSCRIPTION_PLAY_FILTER_LABELS[key] = "双组 " + key.split(":")[1].replace("+", "＋"); });
    SUBSCRIPTION_PLAY_FILTER_PRESETS.kill_group.forEach(function (key) { SUBSCRIPTION_PLAY_FILTER_LABELS[key] = "杀" + key.split(":")[1]; });
    SUBSCRIPTION_PLAY_FILTER_PRESETS.number.forEach(key => { SUBSCRIPTION_PLAY_FILTER_LABELS[key] = "号码 " + key.split(":")[1]; });
    const SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS = {
        big_small: {
            label: "大小",
            profit: "risk_big_small_profit_target",
            loss: "risk_big_small_loss_limit",
        },
        odd_even: {
            label: "单双",
            profit: "risk_odd_even_profit_target",
            loss: "risk_odd_even_loss_limit",
        },
        combo: {
            label: "组合",
            profit: "risk_combo_profit_target",
            loss: "risk_combo_loss_limit",
        },
        double_group: {label: "双组", profit: "risk_double_group_profit_target", loss: "risk_double_group_loss_limit"},
        kill_group: {label: "杀组", profit: "risk_kill_group_profit_target", loss: "risk_kill_group_loss_limit"},
    };
    const SUBSCRIPTION_SETTLEMENT_RULE_LABELS = {
        follow_signal: "跟随来源规则",
        subscription_fixed: "固定规则",
        pc28_netdisk_regular: "OK游戏网盘",
        pc28_netdisk_abc: "OK游戏网盘 ABC",
        pc28_high_regular: "OK游戏高赔常规",
        pc28_high_abc: "OK游戏高赔 ABC",
        pc28_fullpay_netdisk_regular: "彩28网盘",
        pc28_fullpay_2_0_regular: "彩28满赔2.0",
        pc28_fullpay_2_8_regular: "彩28满赔2.8",
        pc28_fullpay_3_2_regular: "彩28满赔3.2",
    };
    const SETTLEMENT_RULE_UI_PRESETS = {
        pc28_netdisk_regular: {bigSmall: 1.98, comboSmallOddBigEven: 3.6, comboBigOddSmallEven: 4.2, refundHint: "无回本。OK游戏网盘 1.98 / 3.6 / 4.2。"},
        pc28_netdisk_abc: {bigSmall: 1.98, comboSmallOddBigEven: 4.8, comboBigOddSmallEven: 3.1, refundHint: "无回本。OK游戏网盘 ABC 目录。"},
        pc28_high_regular: {bigSmall: 2.846, comboSmallOddBigEven: 6.78, comboBigOddSmallEven: 6.33, refundHint: "中奖时遇 13/14、对子、顺子、豹子退本。这是 OK游戏高赔常规 2.846 / 6.78 / 6.33，与彩28满赔2.8 的 2.84 / 6.79 不是同一套。"},
        pc28_high_abc: {bigSmall: 1.98, comboSmallOddBigEven: 4.9, comboBigOddSmallEven: 3.1, refundHint: "中奖时遇 13/14、对子、顺子、豹子退本。OK游戏高赔 ABC 目录。"},
        pc28_fullpay_netdisk_regular: {bigSmall: 1.99, comboSmallOddBigEven: 3.71, comboBigOddSmallEven: 4.32, refundHint: "大小单双中奖遇 0/27 退本，未中全亏；组合不退。顺子不含 890/190。"},
        pc28_fullpay_2_0_regular: {bigSmall: 2, comboSmallOddBigEven: 4.76, comboBigOddSmallEven: 4.32, refundHint: "大小单双中奖遇 0/27 退本，未中全亏。组合中奖遇 13/14 退本。大小单双中奖 13/14：≤2001 赔 2，>2001 降赔 1.98。"},
        pc28_fullpay_2_8_regular: {bigSmall: 2.84, comboSmallOddBigEven: 6.79, comboBigOddSmallEven: 6.33, refundHint: "大小单双和组合中奖时遇 13/14、对子、顺子、豹子退本。顺子含 890/190。"},
        pc28_fullpay_3_2_regular: {bigSmall: 3.2, comboSmallOddBigEven: 7, comboBigOddSmallEven: 6.5, refundHint: "中奖时遇 13/14 退本；中奖时 ABC 任球为 0 或 9 退本。不对子、顺子、豹子单独退。"},
    };
    const RECENT_PIPELINE_LIMIT = 200;
    const templatePreviewPayload = document.getElementById("templatePreviewPayload");
    const templatePreviewOutput = document.getElementById("templatePreviewOutput");
    const templatePreviewMeta = document.getElementById("templatePreviewMeta");
    const subscriptionWorkspaceSummary = document.getElementById("subscriptionWorkspaceSummary");
    const subscriptionWorkspaceSummaryText = document.getElementById("subscriptionWorkspaceSummaryText");
    const sourceWorkspaceSummary = document.getElementById("sourceWorkspaceSummary");
    const sourceWorkspaceSummaryText = document.getElementById("sourceWorkspaceSummaryText");
    const accountWorkspaceSummary = document.getElementById("accountWorkspaceSummary");
    const accountWorkspaceSummaryText = document.getElementById("accountWorkspaceSummaryText");
    const templateWorkspaceSummary = document.getElementById("templateWorkspaceSummary");
    const templateWorkspaceSummaryText = document.getElementById("templateWorkspaceSummaryText");
    const templatePresetButtons = Array.from(document.querySelectorAll(".template-preset-btn"));
    const templateSampleButtons = Array.from(document.querySelectorAll(".template-sample-btn"));
    const accountModeGuideTitle = document.getElementById("accountModeGuideTitle");
    const accountModeGuideSummary = document.getElementById("accountModeGuideSummary");
    const accountModeGuideSteps = document.getElementById("accountModeGuideSteps");
    const accountModeBadge = document.getElementById("accountModeBadge");
    const accountModeHeadline = document.getElementById("accountModeHeadline");
    const accountPostActionTitle = document.getElementById("accountPostActionTitle");
    const accountPostActionBody = document.getElementById("accountPostActionBody");
    const accountPostActionHint = document.getElementById("accountPostActionHint");
    const templateRuleBuilder = document.getElementById("templateRuleBuilder");
    const templateRuleBuilderContent = document.getElementById("templateRuleBuilderContent");
    const toggleTemplateJsonBtn = document.getElementById("toggleTemplateJsonBtn");
    const addCustomTemplateRuleBtn = document.getElementById("addCustomTemplateRuleBtn");
    const templateConfigJsonField = document.querySelector(".template-config-json-field");

    const templateRuleCatalog = [
        {
            betType: "big_small",
            label: "大小单双",
            description: "大 / 小 / 单 / 双",
            defaultFormat: "{{bet_value}}{{amount}}",
            values: ["大", "小", "单", "双"],
        },
        {
            betType: "combo",
            label: "组合",
            description: "大单 / 大双 / 小单 / 小双",
            defaultFormat: "{{bet_value}}{{amount}}",
            values: ["大单", "大双", "小单", "小双"],
        },
        {
            betType: "extreme",
            label: "极值",
            description: "极大 / 极小",
            defaultFormat: "{{bet_value}}{{amount}}",
            values: ["极大", "极小"],
        },
        {
            betType: "special",
            label: "特殊词",
            description: "对子 / 顺子 / 豹子",
            defaultFormat: "{{bet_value}}{{amount}}",
            values: ["对子", "顺子", "豹子"],
        },
        {
            betType: "edge",
            label: "边中",
            description: "小边 / 大边 / 边 / 中",
            defaultFormat: "{{bet_value}}{{amount}}",
            values: ["小边", "大边", "边", "中"],
        },
        {
            betType: "number_sum",
            label: "和值",
            description: "例如 0/100",
            defaultFormat: "{{bet_value}}/{{amount}}",
            values: [],
        },
        {
            betType: "abc",
            label: "ABC 定位",
            description: "例如 A大10，依赖 {{position}} 额外字段",
            defaultFormat: "{{position}}{{bet_value}}{{amount}}",
            values: [],
        },
    ];

    function todayDateString() {
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, "0");
        const day = String(now.getDate()).padStart(2, "0");
        return year + "-" + month + "-" + day;
    }

    let templateBuilderExtraConfig = {};
    let templateBuilderCustomRules = [];
    let templateCustomRuleSeq = 1;
    let templateJsonAdvancedVisible = false;

    const accountModeConfig = {
        phone_login: {
            badge: "手机号登录",
            headline: "先填手机号，再通过验证码完成授权",
            description: "系统会先创建托管账号，再发送 Telegram 验证码。",
            title: "通过手机号接入 Telegram",
            summary: "适合没有现成 Session 文件的账号。系统会发送验证码，你在下方继续完成授权。",
            steps: [
                "填写账号标签和手机号",
                "点击“创建并发送验证码”",
                "在下方继续输入验证码或二次密码",
            ],
            createText: "创建并发送验证码",
            editText: "重新发送验证码",
            postActionTitle: "提交后继续完成账号授权",
            postActionBody: "发送验证码后，下方会出现验证码输入框。登录过期时请先重新发码，不要继续填二次密码。",
        },
        session_import: {
            badge: "导入 Session",
            headline: "直接导入现成 Session，避免验证码流程",
            description: "直接上传现成的 Telethon Session 文件，系统会自动接管并校验。",
            title: "通过 Session 文件快速接入",
            summary: "适合已经在本地或服务端登录过的 Telegram 账号。导入成功后通常可以直接用于后续群组绑定。",
            steps: [
                "填写账号标签并选择 .session 文件",
                "点击“创建并导入 Session”",
                "系统校验通过后即可继续绑定群组",
            ],
            createText: "创建并导入 Session",
            editText: "保存并重新导入 Session",
            postActionTitle: "导入后不再需要验证码",
            postActionBody: "Session 校验通过后，账号会直接进入可用状态；如果需要替换 Session，可重新选择文件后再次保存。",
        },
    };

    function currentAutobetScope() {
        return window.location.pathname.replace(/\/$/, "").split("/").pop();
    }

    function currentUserStorageKey(name) {
        const userId = state.currentUser && state.currentUser.id != null ? String(state.currentUser.id) : "guest";
        return "pc28touzhu.autobet." + name + "." + userId;
    }

    function readStoredValue(name) {
        try {
            return window.localStorage.getItem(currentUserStorageKey(name)) || "";
        } catch (error) {
            return "";
        }
    }

    function writeStoredValue(name, value) {
        try {
            const storageKey = currentUserStorageKey(name);
            const normalized = String(value || "").trim();
            if (!normalized) {
                window.localStorage.removeItem(storageKey);
                return;
            }
            window.localStorage.setItem(storageKey, normalized);
        } catch (error) {
            // noop
        }
    }

    function deriveAiSourceName(urlObject) {
        const fieldNames = {
            odd_even: "单双",
            big_small: "大小",
            combo: "组合",
            double_group: "双组",
            kill_group: "杀组",
        };
        if (urlObject.pathname === "/api/export/consensus/pc28/best-pair-signals") {
            const ids = urlObject.searchParams.get("predictor_ids");
            return ids ? "PC28 最佳组合共识 (" + ids + ")" : "PC28 最佳组合共识";
        }
        if (urlObject.pathname === "/api/export/consensus/pc28/signals") {
            const ids = urlObject.searchParams.get("predictor_ids");
            const field = urlObject.searchParams.get("field");
            const name = "PC28 多方案共识" + (field ? " " + (fieldNames[field] || field) : "");
            return ids ? name + " (" + ids + ")" : name;
        }
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
            parsed.pathname === "/api/export/signals/pc28" ||
            parsed.pathname === "/api/export/consensus/pc28/signals" ||
            parsed.pathname === "/api/export/consensus/pc28/best-pair-signals"
        );
        if (!validPath) {
            throw new Error("当前只支持 AITradingSimulator 的公开方案页链接或 signals/consensus 导出地址");
        }

        if (/^\/api\/export\/predictors\/\d+\/signals$/.test(parsed.pathname)) {
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

    function findImportedAiSourceByUrl(normalizedUrl, excludeSourceId) {
        return aiSources().find(function (item) {
            const fetchConfig = item && item.config && item.config.fetch ? item.config.fetch : {};
            return String(item.id) !== String(excludeSourceId || "") &&
                String(fetchConfig.url || "") === normalizedUrl;
        }) || null;
    }

    function focusElement(element) {
        if (!(element instanceof HTMLElement) || typeof element.focus !== "function") {
            return;
        }
        window.setTimeout(function () {
            element.focus({preventScroll: true});
        }, 180);
    }

    function targetProgressToken(item) {
        if (!item) {
            return "";
        }
        return [
            String(item.id || ""),
            String(item.updated_at || ""),
            String(item.target_key || ""),
        ].join("::");
    }

    function statusText(status) {
        const normalized = String(status || "").trim().toLowerCase();
        const statusMap = {
            success: "测试成功",
            ok: "测试成功",
            warning: "警告",
        };
        return statusMap[normalized] || window.PlatformUiText.labelStatus(normalized);
    }

    function formatErrorMessage(message) {
        const rawMessage = String(message || "").trim() || "操作失败";
        if (rawMessage.includes('保存与启动分离尚未在服务端生效')) return rawMessage;
        if (rawMessage.indexOf("发生了什么：") >= 0) {
            return rawMessage;
        }
        const lower = rawMessage.toLowerCase();
        let reason = "当前请求没有通过系统校验。";
        let nextStep = "请先检查输入信息，再重试一次。";

        if (rawMessage.indexOf("请先登录") >= 0 || lower.indexOf("401") >= 0) {
            reason = "当前会话不存在或已过期。";
            nextStep = "请先在“登录与账户”完成登录，再回到当前步骤继续。";
        } else if (rawMessage.indexOf("待验证码") >= 0 || rawMessage.indexOf("二次密码") >= 0 || rawMessage.indexOf("未授权") >= 0) {
            reason = "托管账号还未完成授权，系统无法继续发送或绑定。";
            nextStep = "进入“托管账号”完成验证码或二次密码验证，然后重试。";
        } else if (rawMessage.indexOf("已归档") >= 0) {
            reason = "归档项不会参与当前自动投注流程。";
            nextStep = "先取消归档后再执行当前操作。";
        } else if (rawMessage.indexOf("Telethon") >= 0) {
            reason = "当前平台进程没有加载到 Telethon 依赖，常见原因是服务没有使用装过依赖的 Python 解释器。";
            nextStep = "检查当前服务实际使用的 Python 路径，必要时切到项目虚拟环境后重启服务。";
        } else if (rawMessage.indexOf("邀请链接") >= 0 || rawMessage.indexOf("格式") >= 0 || rawMessage.indexOf("群组标识") >= 0) {
            reason = "群组标识格式不符合平台可识别规则。";
            nextStep = "改用 @username 或 -100... Chat ID，并重新提交。";
        } else if (rawMessage.indexOf("不存在") >= 0) {
            reason = "对应配置可能已删除或不属于当前账号。";
            nextStep = "点击“刷新配置”后重试，必要时重新创建该配置。";
        } else if (rawMessage.indexOf("无法测试发送") >= 0 || rawMessage.indexOf("测试") >= 0) {
            reason = "当前群组不可达或账号权限不足，测试发送未成功。";
            nextStep = "确认账号已入群且可发言，再点击“测试发送”。";
        }

        return "发生了什么：" + rawMessage + "；为什么：" + reason + "；下一步：" + nextStep;
    }

    function setStatus(message, isError) {
        statusMessage.textContent = isError ? formatErrorMessage(message) : (message || "");
        statusMessage.classList.toggle("is-error", Boolean(isError));
    }

    function setSourceUrlError(message) {
        if (!(sourceUrlError instanceof HTMLElement)) {
            return;
        }
        sourceUrlError.textContent = message || "";
        sourceUrlError.classList.toggle("is-error", Boolean(message));
        sourceUrlError.hidden = !message;
    }

    function reportUiError(scope, error) {
        const message = error && error.message ? error.message : String(error || "未知错误");
        if (window.console && typeof window.console.error === "function") {
            window.console.error("[autobet-ui][" + scope + "]", error);
        }
        setStatus("页面渲染异常：" + scope + " · " + message, true);
    }

    function safeRender(scope, handler) {
        try {
            handler();
            return true;
        } catch (error) {
            reportUiError(scope, error);
            return false;
        }
    }

    function escapeHtml(value) {
        return String(value == null ? "" : value)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function renderStatusPill(status) {
        const normalized = String(status || "inactive").trim() || "inactive";
        return '<span class="config-status-pill is-' + escapeHtml(normalized) + '">' + escapeHtml(statusText(normalized)) + "</span>";
    }

    function renderStatusPillWithLabel(status, label) {
        const normalized = String(status || "inactive").trim() || "inactive";
        return '<span class="config-status-pill is-' + escapeHtml(normalized) + '">' + escapeHtml(label || statusText(normalized)) + "</span>";
    }

    function formatDateTime(value) {
        const text = String(value || "").trim();
        if (!text) {
            return "--";
        }
        return text.replace("T", " ").replace("Z", " UTC");
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

    function setFormEditingState(form, submitButton, cancelButton, editing, submitText) {
        if (!(form instanceof HTMLFormElement)) {
            return;
        }
        form.elements.edit_id.value = editing && form.elements.edit_id.value ? form.elements.edit_id.value : "";
        if (submitButton instanceof HTMLElement) {
            submitButton.textContent = submitText;
        }
        if (cancelButton instanceof HTMLElement) {
            cancelButton.hidden = !editing;
        }
        if (window.PlatformNavigation) window.PlatformNavigation.markClean(form);
    }

    function applyAutobetScope() {
        const scope = currentAutobetScope();
        document.body.setAttribute("data-autobet-scope", scope);
        const copy = {
            sources: "导入、维护方案来源，并查看来源的信号链路。",
            accounts: "管理 Telegram 账号授权与可用状态。",
            targets: "绑定投递群组、选择模板并测试可达性。",
            templates: "维护消息格式、玩法映射和发送预览。",
            subscriptions: "选择来源、玩法和投递群组，管理跟单方案。",
        };
        document.getElementById("autobetHeroText").textContent = copy[scope] || "";
        document.getElementById("autobetHeroEyebrow").textContent = scope === "subscriptions" ? "策略管理" : "资源配置";
    }

    function scrollToHashTarget(hash) {
        const normalized = String(hash || "").trim();
        if (!normalized || normalized.charAt(0) !== "#") {
            return false;
        }
        let id;
        try { id = decodeURIComponent(normalized.slice(1)); } catch (error) { return false; }
        const target = document.getElementById(id);
        if (!(target instanceof HTMLElement)) {
            return false;
        }
        target.scrollIntoView({behavior: "smooth", block: "start"});
        if (window.history && typeof window.history.replaceState === "function") {
            window.history.replaceState({}, "", window.location.pathname + window.location.search + normalized);
        }
        return true;
    }

    async function request(path, options) {
        const changesLaunch = options?.method === 'POST' && /^\/api\/platform\/(subscriptions|auto-trigger-rules)(\/|$)/.test(path);
        const pausesOnly = path.endsWith('/status') && ['inactive', 'archived'].includes(options?.body?.status);
        if (changesLaunch && !pausesOnly && !state.supportsExplicitStart) {
            throw new Error('保存与启动分离尚未在服务端生效，请重启跟单服务后刷新页面。');
        }
        const response = await fetch(path, {
            method: options && options.method ? options.method : "GET",
            credentials: "same-origin",
            headers: {
                "Content-Type": "application/json",
            },
            body: options && options.body ? JSON.stringify(options.body) : undefined,
        });
        let payload;
        try {
            payload = await response.json();
        } catch (error) {
            if (!response.ok) {
                throw new Error("服务器返回了非预期的响应（HTTP " + response.status + "），请求可能被网站防火墙拦截，请联系管理员处理。");
            }
            throw error;
        }
        if (!response.ok) {
            const reason = String(payload.why || "").trim();
            const nextStep = String(payload.next_step || "").trim();
            const error = new Error(
                reason || nextStep
                    ? ("发生了什么：" + (payload.error || "请求失败") + "；为什么：" + (reason || "当前请求没有通过系统校验。") + "；下一步：" + (nextStep || "请先检查输入信息，再重试一次。"))
                    : (payload.error || "请求失败")
            );
            error.payload = payload;
            throw error;
        }
        return payload;
    }

    function settlementStatusMessage(item, successPrefix) {
        const thresholdStatus = item && item.financial ? String(item.financial.threshold_status || "") : "";
        if (thresholdStatus === "profit_target_hit") {
            return (successPrefix || "当前待结算记录已处理，") + "策略达到止盈阈值，当前轮次已停止。";
        }
        if (thresholdStatus === "loss_limit_hit") {
            return (successPrefix || "当前待结算记录已处理，") + "策略达到止损阈值，当前轮次已停止。";
        }
        return (successPrefix || "当前待结算记录已处理。");
    }

    function pendingPlacedSubscriptionCount() {
        return state.subscriptions.filter(function (item) {
            const progression = item && item.progression && typeof item.progression === "object" ? item.progression : null;
            return Boolean(progression && progression.pending_event_id && String(progression.pending_status || "") === "placed");
        }).length;
    }

    function syncBatchResolveButtonState() {
        if (!(batchResolveSubscriptionsBtn instanceof HTMLElement)) {
            return;
        }
        const pendingCount = pendingPlacedSubscriptionCount();
        batchResolveSubscriptionsBtn.toggleAttribute("disabled", pendingCount <= 0);
        batchResolveSubscriptionsBtn.textContent = pendingCount > 0
            ? ("批量自动结算待结算记录（" + pendingCount + "）")
            : "当前没有待结算记录";
    }

    async function loadCurrentUser() {
        const token = ++currentUserLoadToken;
        const response = await fetch("/api/auth/me", {
            method: "GET",
            credentials: "same-origin",
            headers: {
                "Content-Type": "application/json",
            },
        });
        const payload = await response.json();
        if (token !== currentUserLoadToken) {
            return state.currentUser;
        }
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
            heroUsernameId: "autobetHeroUsername",
            heroUserMetaId: "autobetHeroUserMeta",
            heroLoggedInText: "当前会话已就绪，可集中在总控台和工作区完成自动投注配置。",
            heroLoggedOutText: "登录后查看当前配置和缺失项。",
            panelTitleLoggedIn: "当前账户",
            panelTitleLoggedOut: "登录与账户",
        });
        const authForm = document.getElementById("authForm");
        const authenticatedPanel = document.getElementById("authenticatedAccountPanel");
        const usernameEl = document.getElementById("currentUsername");
        const metaEl = document.getElementById("currentUserMeta");
        const accountButton = document.getElementById("accountMenuBtn");
        const isAuthenticated = Boolean(state.currentUser);
        if (usernameEl instanceof HTMLElement) {
            usernameEl.textContent = isAuthenticated ? String(state.currentUser.username || "未命名用户") : "未登录";
        }
        if (metaEl instanceof HTMLElement) {
            metaEl.textContent = isAuthenticated
                ? ((state.currentUser.email || "--") + " · " + (state.currentUser.role || "user") + " · " + (state.currentUser.status || "active"))
                : "请先注册或登录";
        }
        if (accountButton instanceof HTMLElement) {
            accountButton.textContent = isAuthenticated ? String(state.currentUser.username || "未命名用户") : "登录";
        }
        if (authForm instanceof HTMLElement) {
            if (isAuthenticated) {
                authForm.hidden = true;
                authForm.style.setProperty("display", "none", "important");
            } else {
                authForm.hidden = false;
                authForm.style.removeProperty("display");
            }
        }
        if (authenticatedPanel instanceof HTMLElement) {
            if (isAuthenticated) {
                authenticatedPanel.hidden = false;
                authenticatedPanel.style.removeProperty("display");
            } else {
                authenticatedPanel.hidden = true;
                authenticatedPanel.style.setProperty("display", "none", "important");
            }
        }
    }

    function sourceById(sourceId) {
        return state.sources.find(function (item) {
            return Number(item.id) === Number(sourceId);
        }) || null;
    }

    function accountById(accountId) {
        return state.accounts.find(function (item) {
            return Number(item.id) === Number(accountId);
        }) || null;
    }

    function targetById(targetId) {
        return state.targets.find(function (item) {
            return Number(item.id) === Number(targetId);
        }) || null;
    }

    function subscriptionDispatchTargetIds(item) {
        const strategyV2 = item && item.strategy_v2 && typeof item.strategy_v2 === "object" ? item.strategy_v2 : {};
        const dispatchV2 = strategyV2.dispatch && typeof strategyV2.dispatch === "object" ? strategyV2.dispatch : {};
        const strategy = item && item.strategy && typeof item.strategy === "object" ? item.strategy : {};
        const rawIds = Array.isArray(dispatchV2.delivery_target_ids)
            ? dispatchV2.delivery_target_ids
            : (Array.isArray(strategy.delivery_target_ids) ? strategy.delivery_target_ids : []);
        const seen = {};
        return rawIds.map(function (value) {
            return Number(value);
        }).filter(function (value) {
            if (!Number.isInteger(value) || value <= 0 || seen[String(value)]) {
                return false;
            }
            seen[String(value)] = true;
            return true;
        });
    }

    function templateById(templateId) {
        return state.templates.find(function (item) {
            return Number(item.id) === Number(templateId);
        }) || null;
    }

    function subscriptionById(subscriptionId) {
        return state.subscriptions.find(function (item) {
            return Number(item.id) === Number(subscriptionId);
        }) || null;
    }

    function isArchivedItem(item) {
        return Boolean(item) && String(item.status || "") === "archived";
    }

    function accountAuthMode(item) {
        return String((item && (item.auth_mode || (item.meta && item.meta.auth_mode))) || "phone_login").trim() || "phone_login";
    }

    function accountAuthState(item) {
        return String((item && (item.auth_state || (item.meta && item.meta.auth_state))) || "pending").trim() || "pending";
    }

    function isAccountAuthorized(item) {
        if (!item) {
            return false;
        }
        if (typeof item.is_authorized === "boolean") {
            return item.is_authorized;
        }
        return accountAuthState(item) === "authorized";
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
        if (authState === "login_expired") {
            return "登录已过期";
        }
        if (authState === "pending_import") {
            return "待导入";
        }
        return "不可用";
    }

    function accountAuthDescription(item) {
        const authState = accountAuthState(item);
        const authMode = accountAuthMode(item);
        if (authState === "authorized") {
            return authMode === "session_import"
                ? "现有 Session 已通过校验，可以直接用于发送。"
                : "账号已完成登录授权，可以直接用于发送。";
        }
        if (authState === "code_sent") {
            return "验证码已发送，请继续填写验证码完成授权。";
        }
        if (authState === "password_required") {
            return "该账号开启了二次密码，请继续完成验证。";
        }
        if (authState === "login_expired") {
            return "登录会话已过期。请重新发送验证码，不要继续提交二次密码。";
        }
        if (authMode === "session_import") {
            return "请上传商家附带的 Telethon Session 文件。";
        }
        return "请先发送验证码并完成登录验证。";
    }

    function accountDeletionSummary(item) {
        const deletion = item && item.deletion && typeof item.deletion === "object" ? item.deletion : {};
        return {
            targetCount: Math.max(0, Number(deletion.target_count || 0)),
            activeTargetCount: Math.max(0, Number(deletion.active_target_count || 0)),
            executionJobCount: Math.max(0, Number(deletion.execution_job_count || 0)),
            pendingJobCount: Math.max(0, Number(deletion.pending_job_count || 0)),
        };
    }

    function accountDeletionDescription(item) {
        const deletion = accountDeletionSummary(item);
        if (deletion.activeTargetCount || deletion.pendingJobCount) {
            return "请先停用关联群组并等待待执行任务完成后再删除。";
        }
        if (deletion.targetCount || deletion.executionJobCount) {
            return "删除后会清除手机号和 Session，历史执行记录保留，关联的停用/归档群组会解除绑定。";
        }
        return "删除后会清除手机号和 Session，当前没有需要保留的关联记录。";
    }

    function availableAccountsForTargets() {
        return visibleAccounts().filter(function (item) {
            return isAccountAuthorized(item);
        });
    }

    function readFileAsBase64(file) {
        return new Promise(function (resolve, reject) {
            if (!(file instanceof File)) {
                reject(new Error("请选择 Session 文件"));
                return;
            }
            const reader = new FileReader();
            reader.onerror = function () {
                reject(new Error("读取 Session 文件失败"));
            };
            reader.onload = function () {
                const result = String(reader.result || "");
                const marker = "base64,";
                const index = result.indexOf(marker);
                resolve(index >= 0 ? result.slice(index + marker.length) : result);
            };
            reader.readAsDataURL(file);
        });
    }

    function normalizeTelegramTargetKey(rawValue) {
        const text = String(rawValue || "").trim();
        if (!text) {
            throw new Error("群组标识不能为空");
        }

        const normalizedText = text.replace(/\s+/g, "");

        if (normalizedText.startsWith("@")) {
            const username = normalizedText.slice(1);
            if (!/^[a-zA-Z0-9_]{5,32}$/.test(username)) {
                throw new Error("群组 @username 格式不正确");
            }
            return "@" + username;
        }

        if (/^-100\d{6,}$/.test(normalizedText)) {
            return normalizedText;
        }

        if (/^-?\d+$/.test(normalizedText)) {
            return normalizedText;
        }

        const linkCandidate = (
            normalizedText.startsWith("http://") ||
            normalizedText.startsWith("https://") ||
            normalizedText.startsWith("t.me/") ||
            normalizedText.startsWith("telegram.me/") ||
            normalizedText.startsWith("www.t.me/") ||
            normalizedText.startsWith("www.telegram.me/")
        )
            ? normalizedText
            : "";

        if (linkCandidate) {
            const urlText = linkCandidate.startsWith("http") ? linkCandidate : ("https://" + linkCandidate);
            let parsed;
            try {
                parsed = new URL(urlText);
            } catch (error) {
                parsed = null;
            }
            if (parsed) {
                let host = String(parsed.hostname || "").toLowerCase();
                if (host.startsWith("www.")) {
                    host = host.slice(4);
                }
                if (host === "t.me" || host === "telegram.me") {
                    const parts = String(parsed.pathname || "").split("/").filter(Boolean);
                    if (!parts.length) {
                        throw new Error("群组链接格式不完整");
                    }
                    if (parts[0] === "joinchat" || parts[0].startsWith("+")) {
                        throw new Error("邀请链接无法自动解析，请使用 @userinfobot 获取 Chat ID 或填写 @username / -100... ID");
                    }
                    if (parts[0] === "c" && parts[1] && /^\d+$/.test(parts[1])) {
                        return "-100" + parts[1];
                    }
                    if (parts[0] === "s" && parts[1]) {
                        parts.shift();
                    }
                    const username = parts[0];
                    if (!/^[a-zA-Z0-9_]{5,32}$/.test(username)) {
                        throw new Error("群组链接中的 username 不合法");
                    }
                    return username;
                }
            }
        }

        throw new Error("群组标识格式不支持，请填写 @username、-100... 或粘贴 t.me 链接");
    }

    function parseTemplateConfigText(rawText) {
        const text = String(rawText || "").trim();
        if (!text) {
            return {};
        }
        let payload;
        try {
            payload = JSON.parse(text);
        } catch (error) {
            throw new Error("模板规则 JSON 解析失败，请检查逗号和引号格式");
        }
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
            throw new Error("模板规则 JSON 必须是对象");
        }
        return payload;
    }

    function stringifyTemplateConfig(config) {
        return JSON.stringify(config || {}, null, 2);
    }

    function normalizeTemplateConfig(config) {
        return config && typeof config === "object" && !Array.isArray(config) ? cloneJson(config) : {};
    }

    function nextTemplateCustomRuleUid() {
        const current = templateCustomRuleSeq;
        templateCustomRuleSeq += 1;
        return "custom-rule-" + String(current);
    }

    function createTemplateCustomRuleState(betType, ruleConfig) {
        const config = ruleConfig && typeof ruleConfig === "object" ? ruleConfig : {};
        const rawValueMap = config.value_map && typeof config.value_map === "object" ? config.value_map : {};
        const mappings = Object.keys(rawValueMap).map(function (sourceValue) {
            return {
                source: sourceValue,
                target: String(rawValueMap[sourceValue] || ""),
            };
        });
        if (!mappings.length) {
            mappings.push({source: "", target: ""});
        }
        return {
            uid: nextTemplateCustomRuleUid(),
            betType: String(betType || "").trim(),
            format: String(config.format || "").trim(),
            mappings: mappings,
        };
    }

    function splitTemplateConfig(config) {
        const normalized = normalizeTemplateConfig(config);
        const knownBetTypes = {};
        const extraConfig = {};
        const betRules = normalized.bet_rules && typeof normalized.bet_rules === "object" && !Array.isArray(normalized.bet_rules)
            ? normalized.bet_rules
            : {};
        const customRules = [];

        Object.keys(normalized).forEach(function (key) {
            if (key !== "bet_rules") {
                extraConfig[key] = normalized[key];
            }
        });

        Object.keys(betRules).forEach(function (betType) {
            if (templateRuleCatalog.some(function (entry) { return entry.betType === betType; })) {
                knownBetTypes[betType] = betRules[betType];
                return;
            }
            customRules.push(createTemplateCustomRuleState(betType, betRules[betType]));
        });

        return {
            extraConfig: extraConfig,
            knownBetTypes: knownBetTypes,
            customRules: customRules,
        };
    }

    function buildTemplateConfigFromBuilder() {
        const payload = cloneJson(templateBuilderExtraConfig);
        const betRules = {};
        templateRuleCatalog.forEach(function (entry) {
            const formatInput = templateRuleBuilder.querySelector('[data-template-rule-format="' + entry.betType + '"]');
            const formatValue = String(formatInput && formatInput.value || "").trim();
            const rule = {};
            const valueMap = {};
            entry.values.forEach(function (rawValue) {
                const mapInput = templateRuleBuilder.querySelector('[data-template-rule-map="' + entry.betType + '::' + rawValue + '"]');
                const mappedValue = String(mapInput && mapInput.value || "").trim();
                if (mappedValue) {
                    valueMap[rawValue] = mappedValue;
                }
            });

            if (formatValue) {
                rule.format = formatValue;
            } else if (Object.keys(valueMap).length) {
                rule.format = entry.defaultFormat;
            }
            if (Object.keys(valueMap).length) {
                rule.value_map = valueMap;
            }
            if (Object.keys(rule).length) {
                betRules[entry.betType] = rule;
            }
        });

        templateRuleBuilder.querySelectorAll(".template-custom-rule-card").forEach(function (card) {
            const betTypeInput = card.querySelector("[data-template-custom-bet-type]");
            const formatInput = card.querySelector("[data-template-custom-format]");
            const betType = String(betTypeInput && betTypeInput.value || "").trim();
            const formatValue = String(formatInput && formatInput.value || "").trim();
            const valueMap = {};
            card.querySelectorAll(".template-custom-mapping-row").forEach(function (row) {
                const sourceInput = row.querySelector("[data-template-custom-map-source]");
                const targetInput = row.querySelector("[data-template-custom-map-target]");
                const sourceValue = String(sourceInput && sourceInput.value || "").trim();
                const targetValue = String(targetInput && targetInput.value || "").trim();
                if (sourceValue && targetValue) {
                    valueMap[sourceValue] = targetValue;
                }
            });
            if (!betType) {
                return;
            }
            const rule = {};
            if (formatValue) {
                rule.format = formatValue;
            }
            if (Object.keys(valueMap).length) {
                rule.value_map = valueMap;
            }
            if (Object.keys(rule).length) {
                betRules[betType] = rule;
            }
        });

        if (Object.keys(betRules).length) {
            payload.bet_rules = betRules;
        }
        return payload;
    }

    function syncTemplateConfigFromBuilder() {
        if (!(messageTemplateForm instanceof HTMLFormElement) || !(templateRuleBuilder instanceof HTMLElement)) {
            return;
        }
        messageTemplateForm.elements.config.value = stringifyTemplateConfig(buildTemplateConfigFromBuilder());
        syncTemplatePreview();
    }

    function captureTemplateBuilderCustomRulesFromDom() {
        if (!(templateRuleBuilder instanceof HTMLElement)) {
            return;
        }
        templateBuilderCustomRules = Array.from(templateRuleBuilder.querySelectorAll("[data-template-custom-rule]")).map(function (card) {
            const uid = String(card.getAttribute("data-template-custom-rule") || nextTemplateCustomRuleUid());
            const betTypeInput = card.querySelector("[data-template-custom-bet-type]");
            const formatInput = card.querySelector("[data-template-custom-format]");
            const mappings = Array.from(card.querySelectorAll(".template-custom-mapping-row")).map(function (row) {
                const sourceInput = row.querySelector("[data-template-custom-map-source]");
                const targetInput = row.querySelector("[data-template-custom-map-target]");
                return {
                    source: String(sourceInput && sourceInput.value || ""),
                    target: String(targetInput && targetInput.value || ""),
                };
            });
            return {
                uid: uid,
                betType: String(betTypeInput && betTypeInput.value || ""),
                format: String(formatInput && formatInput.value || ""),
                mappings: mappings.length ? mappings : [{source: "", target: ""}],
            };
        });
    }

    function renderTemplateRuleBuilder(config, options) {
        if (!(templateRuleBuilder instanceof HTMLElement)) {
            return;
        }
        const preserveCustomRules = Boolean(options && options.preserveCustomRules);
        const parts = splitTemplateConfig(config);
        templateBuilderExtraConfig = parts.extraConfig;
        templateBuilderCustomRules = preserveCustomRules ? templateBuilderCustomRules : parts.customRules;

        const builtInMarkup = templateRuleCatalog.map(function (entry) {
            const rule = parts.knownBetTypes[entry.betType] && typeof parts.knownBetTypes[entry.betType] === "object"
                ? parts.knownBetTypes[entry.betType]
                : {};
            const formatValue = String(rule.format || "").trim();
            const valueMap = rule.value_map && typeof rule.value_map === "object" ? rule.value_map : {};
            const mappingMarkup = entry.values.length
                ? ('<div class="template-rule-mapping-grid">' + entry.values.map(function (rawValue) {
                    return '<label class="template-rule-map-item"><strong>' + escapeHtml(rawValue) + '</strong><input class="text-input" type="text" data-template-rule-map="' + entry.betType + "::" + rawValue + '" value="' + escapeHtml(String(valueMap[rawValue] || "")) + '" placeholder="例如：' + escapeHtml(rawValue) + '"></label>';
                }).join("") + "</div>")
                : '<p class="field-hint">这个玩法通常只需要改格式，不需要逐项映射。</p>';
            return [
                '<article class="template-rule-card">',
                '<div class="template-rule-card-head"><div><strong>' + escapeHtml(entry.label) + '</strong><span>' + escapeHtml(entry.description) + "</span></div></div>",
                '<label><span>该玩法格式</span><input class="text-input" type="text" data-template-rule-format="' + entry.betType + '" value="' + escapeHtml(formatValue) + '" placeholder="' + escapeHtml(entry.defaultFormat) + '"></label>',
                mappingMarkup,
                "</article>",
            ].join("");
        }).join("");

        const customMarkup = templateBuilderCustomRules.length
            ? templateBuilderCustomRules.map(function (rule) {
                const mappingMarkup = rule.mappings.map(function (mapping, index) {
                    return [
                        '<div class="template-custom-mapping-row" data-template-custom-mapping-row="' + rule.uid + "::" + String(index) + '">',
                        '<label><span>原始值</span><input class="text-input" type="text" data-template-custom-map-source value="' + escapeHtml(mapping.source) + '" placeholder="例如：大双"></label>',
                        '<label><span>映射值</span><input class="text-input" type="text" data-template-custom-map-target value="' + escapeHtml(mapping.target) + '" placeholder="例如：ds"></label>',
                        '<button class="ghost-btn" type="button" data-template-custom-remove-mapping="' + rule.uid + '" data-template-custom-remove-index="' + String(index) + '">删除</button>',
                        "</div>",
                    ].join("");
                }).join("");
                return [
                    '<article class="template-rule-card template-custom-rule-card" data-template-custom-rule="' + rule.uid + '">',
                    '<div class="template-rule-card-head"><div><strong>自定义玩法</strong><span>手动输入 bet_type、格式和映射值</span></div><button class="ghost-btn danger-btn" type="button" data-template-custom-remove-rule="' + rule.uid + '">删除玩法</button></div>',
                    '<label><span>玩法标识</span><input class="text-input" type="text" data-template-custom-bet-type value="' + escapeHtml(rule.betType) + '" placeholder="例如：dragon_tiger"></label>',
                    '<label><span>该玩法格式</span><input class="text-input" type="text" data-template-custom-format value="' + escapeHtml(rule.format) + '" placeholder="{{bet_value}}{{amount}}"></label>',
                    '<div class="template-custom-mapping-list">' + mappingMarkup + "</div>",
                    '<div class="template-custom-rule-actions"><button class="ghost-btn" type="button" data-template-custom-add-mapping="' + rule.uid + '">新增映射项</button></div>',
                    "</article>",
                ].join("");
            }).join("")
            : '<article class="template-rule-card template-custom-rule-empty"><div class="template-rule-card-head"><div><strong>自定义玩法</strong><span>当前还没有自定义玩法；需要特殊 bet_type 时再新增。</span></div></div></article>';

        templateRuleBuilder.innerHTML = builtInMarkup + customMarkup;
    }

    function applyTemplateConfigToForm(config) {
        const normalized = normalizeTemplateConfig(config);
        if (!(messageTemplateForm instanceof HTMLFormElement)) {
            return;
        }
        messageTemplateForm.elements.config.value = stringifyTemplateConfig(normalized);
        renderTemplateRuleBuilder(normalized);
        syncTemplatePreview();
    }

    function setTemplateJsonAdvancedVisible(visible) {
        templateJsonAdvancedVisible = Boolean(visible);
        if (templateRuleBuilderContent instanceof HTMLElement) {
            templateRuleBuilderContent.hidden = templateJsonAdvancedVisible;
        }
        if (templateConfigJsonField instanceof HTMLElement) {
            templateConfigJsonField.hidden = !templateJsonAdvancedVisible;
        }
        if (toggleTemplateJsonBtn instanceof HTMLButtonElement) {
            toggleTemplateJsonBtn.textContent = templateJsonAdvancedVisible ? "切换到填写区" : "切换到高级 JSON";
        }
        if (!templateJsonAdvancedVisible && messageTemplateForm instanceof HTMLFormElement) {
            try {
                renderTemplateRuleBuilder(parseTemplateConfigText(messageTemplateForm.elements.config.value));
            } catch (error) {
                // keep current builder state when JSON is invalid
            }
        }
    }

    function parsePreviewPayloadText(rawText) {
        const text = String(rawText || "").trim();
        if (!text) {
            return {};
        }
        let payload;
        try {
            payload = JSON.parse(text);
        } catch (error) {
            throw new Error("额外字段 JSON 解析失败，请检查预览区格式");
        }
        if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
            throw new Error("额外字段 JSON 必须是对象");
        }
        return payload;
    }

    function renderTemplateString(templateText, context) {
        let output = String(templateText || "");
        Object.keys(context).forEach(function (key) {
            output = output.replaceAll("{{" + key + "}}", String(context[key]));
        });
        return output.trim();
    }

    function amountText(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "10";
        }
        return Number.isInteger(numeric) ? String(numeric) : String(numeric);
    }

    function signedAmountText(value) {
        const numeric = Number(value);
        if (!Number.isFinite(numeric)) {
            return "0";
        }
        if (numeric > 0) {
            return "+" + amountText(numeric);
        }
        return amountText(numeric);
    }

    function progressionResultText(value) {
        const mapping = {
            hit: "命中",
            refund: "回本",
            miss: "未中",
            reset: "已重置",
        };
        return mapping[String(value || "").trim()] || String(value || "--");
    }

    function defaultMessageText(signal, amount) {
        const payload = signal && signal.normalized_payload && typeof signal.normalized_payload === "object"
            ? signal.normalized_payload
            : {};
        const custom = String(payload.message_text || "").trim();
        if (custom) {
            return custom;
        }
        return String(signal && signal.bet_value || "") + amountText(amount);
    }

    function templatePreviewResult(signal, amount, template) {
        const combos = signalGroupCombos(signal);
        if (combos.length) {
            const payload = Object.assign({}, signal.normalized_payload || {});
            delete payload.message_text;
            return {
                text: combos.map(function (value) {
                    return templatePreviewResult(Object.assign({}, signal, {bet_type: "combo", bet_value: value, normalized_payload: payload}), amount, template).text;
                }).join(" "),
                meta: "每组合 " + amountText(amount) + "，共 " + combos.length + " 注，整组 " + amountText(amount * combos.length) + "；逐注使用组合模板。",
            };
        }
        const fallbackText = defaultMessageText(signal, amount);
        if (!template) {
            return {
                text: fallbackText,
                meta: "当前群组未绑定模板，将退回默认文本。",
            };
        }
        if (String(template.status || "active") !== "active") {
            return {
                text: fallbackText,
                meta: "当前模板未启用，将退回默认文本。",
            };
        }
        const config = template.config && typeof template.config === "object" ? template.config : {};
        const betRules = config.bet_rules && typeof config.bet_rules === "object" ? config.bet_rules : {};
        const betType = String(signal && signal.bet_type || "").trim();
        const hasDedicatedRule = Boolean(betType && betRules[betType] && typeof betRules[betType] === "object");
        const rule = hasDedicatedRule ? betRules[betType] : (betRules["*"] || {});
        const templateText = String(rule.format || template.template_text || "").trim();
        if (!templateText) {
            return {
                text: fallbackText,
                meta: "当前模板没有可用格式，将退回默认文本。",
            };
        }
        const rawValue = String(signal && signal.bet_value || "");
        const valueMap = rule.value_map && typeof rule.value_map === "object" ? rule.value_map : {};
        const renderedValue = Object.prototype.hasOwnProperty.call(valueMap, rawValue) ? String(valueMap[rawValue]) : rawValue;
        const payload = signal && signal.normalized_payload && typeof signal.normalized_payload === "object"
            ? signal.normalized_payload
            : {};
        const context = {
            bet_value: renderedValue,
            raw_bet_value: rawValue,
            bet_type: betType,
            lottery_type: String(signal && signal.lottery_type || ""),
            issue_no: String(signal && signal.issue_no || ""),
            amount: amountText(amount),
        };
        Object.keys(payload).forEach(function (key) {
            if (key in context || payload[key] == null || payload[key] === "") {
                return;
            }
            context[String(key)] = payload[key];
        });
        const rendered = renderTemplateString(templateText, context);
        return {
            text: rendered || fallbackText,
            meta: rendered
                ? ("命中规则：" + (hasDedicatedRule ? betType : (betRules["*"] ? "*" : "默认格式")) + "，当前会发送这条下注文本。")
                : "模板渲染结果为空，已退回默认文本。",
        };
    }

    function syncTemplatePreview() {
        if (!(messageTemplateForm instanceof HTMLFormElement) || !(templatePreviewOutput instanceof HTMLElement) || !(templatePreviewMeta instanceof HTMLElement)) {
            return;
        }
        try {
            const betType = String(templatePreviewBetType && templatePreviewBetType.value || "").trim() || "big_small";
            const betValue = String(templatePreviewBetValue && templatePreviewBetValue.value || "").trim() || "大";
            const amountValue = Number(templatePreviewAmount && templatePreviewAmount.value || 10);
            const amountNumber = Number.isFinite(amountValue) && amountValue > 0 ? amountValue : 10;
            const issueNo = String(templatePreviewIssueNo && templatePreviewIssueNo.value || "").trim() || "20260410001";
            const previewPayload = parsePreviewPayloadText(templatePreviewPayload && templatePreviewPayload.value || "");
            const template = {
                status: "active",
                lottery_type: String(messageTemplateForm.elements.lottery_type.value || "pc28").trim() || "pc28",
                template_text: String(messageTemplateForm.elements.template_text.value || "").trim(),
                config: parseTemplateConfigText(messageTemplateForm.elements.config.value),
            };
            const signal = {
                lottery_type: template.lottery_type,
                issue_no: issueNo,
                bet_type: betType,
                bet_value: betValue,
                normalized_payload: previewPayload,
            };
            const rendered = templatePreviewResult(signal, amountNumber, template);
            templatePreviewOutput.textContent = rendered.text || "--";
            templatePreviewMeta.textContent = rendered.meta || "模板预览生成失败";
        } catch (error) {
            templatePreviewOutput.textContent = "--";
            templatePreviewMeta.textContent = error && error.message ? error.message : "模板预览生成失败";
        }
    }

    function pc28TemplateExampleConfig() {
        return {
            bet_rules: {
                big_small: {
                    format: "{{bet_value}}{{amount}}",
                    value_map: {"大": "大", "小": "小", "单": "单", "双": "双"},
                },
                combo: {
                    format: "{{bet_value}}{{amount}}",
                    value_map: {"大单": "大单", "大双": "大双", "小单": "小单", "小双": "小双"},
                },
                extreme: {
                    format: "{{bet_value}}{{amount}}",
                    value_map: {"极大": "极大", "极小": "极小"},
                },
                special: {
                    format: "{{bet_value}}{{amount}}",
                    value_map: {"对子": "对子", "顺子": "顺子", "豹子": "豹子"},
                },
                number_sum: {
                    format: "{{bet_value}}/{{amount}}",
                },
                abc: {
                    format: "{{position}}{{bet_value}}{{amount}}",
                },
                edge: {
                    format: "{{bet_value}}{{amount}}",
                    value_map: {"小边": "小边", "大边": "大边", "边": "边", "中": "中"},
                },
            },
        };
    }

    function cloneJson(value) {
        return JSON.parse(JSON.stringify(value || {}));
    }

    function templateStatusLabel(status) {
        const normalized = String(status || "inactive").trim() || "inactive";
        if (normalized === "active") {
            return "启用";
        }
        if (normalized === "archived") {
            return "归档";
        }
        return "草稿";
    }

    function templatePresetCatalog() {
        return {
            pc28_high_roll: {
                id: "pc28_high_roll",
                name: "加拿大28高倍模板",
                summary: "适合大小单双、组合、极值、特殊词直接拼接金额。",
                lottery_type: "pc28",
                status: "active",
                template_text: "{{bet_value}}{{amount}}",
                config: pc28TemplateExampleConfig(),
                preview: {sampleId: "big_small"},
            },
            number_sum_slash: {
                id: "number_sum_slash",
                name: "和值斜杠模板",
                summary: "适合和值类机器人，格式如 0/100。",
                lottery_type: "pc28",
                status: "inactive",
                template_text: "{{bet_value}}/{{amount}}",
                config: {
                    bet_rules: {
                        number_sum: {format: "{{bet_value}}/{{amount}}"},
                        "*": {format: "{{bet_value}}{{amount}}"},
                    },
                },
                preview: {sampleId: "number_sum"},
            },
            abc_position: {
                id: "abc_position",
                name: "ABC 定位模板",
                summary: "适合需要附带位置位的格式，如 A大10。",
                lottery_type: "pc28",
                status: "inactive",
                template_text: "{{position}}{{bet_value}}{{amount}}",
                config: {
                    bet_rules: {
                        abc: {format: "{{position}}{{bet_value}}{{amount}}"},
                        "*": {format: "{{bet_value}}{{amount}}"},
                    },
                },
                preview: {sampleId: "abc"},
            },
            combo_matrix: {
                id: "combo_matrix",
                name: "组合矩阵模板",
                summary: "适合大单/大双/小单/小双等组合玩法集中维护。",
                lottery_type: "pc28",
                status: "inactive",
                template_text: "{{bet_value}}{{amount}}",
                config: {
                    bet_rules: {
                        combo: {
                            format: "{{bet_value}}{{amount}}",
                            value_map: {"大单": "大单", "大双": "大双", "小单": "小单", "小双": "小双"},
                        },
                        extreme: {
                            format: "{{bet_value}}{{amount}}",
                            value_map: {"极大": "极大", "极小": "极小"},
                        },
                        special: {
                            format: "{{bet_value}}{{amount}}",
                            value_map: {"对子": "对子", "顺子": "顺子", "豹子": "豹子"},
                        },
                    },
                },
                preview: {sampleId: "combo"},
            },
        };
    }

    function templateSampleCatalog() {
        return {
            big_small: {betType: "big_small", betValue: "大", amount: "10", issueNo: "20260410001", payload: ""},
            combo: {betType: "combo", betValue: "大单", amount: "20", issueNo: "20260410001", payload: ""},
            double_group: {betType: "double_group", betValue: "大单+小双", amount: "10", issueNo: "20260410001", payload: ""},
            kill_group: {betType: "kill_group", betValue: "小双", amount: "10", issueNo: "20260410001", payload: ""},
            number_sum: {betType: "number_sum", betValue: "0", amount: "100", issueNo: "20260410001", payload: ""},
            abc: {betType: "abc", betValue: "大", amount: "10", issueNo: "20260410001", payload: '{"position":"A"}'},
            special: {betType: "special", betValue: "豹子", amount: "10", issueNo: "20260410001", payload: ""},
        };
    }

    function applyTemplateSample(sampleId) {
        const sample = templateSampleCatalog()[String(sampleId || "").trim()];
        if (!sample) {
            return;
        }
        if (templatePreviewBetType instanceof HTMLInputElement) {
            templatePreviewBetType.value = sample.betType;
        }
        if (templatePreviewBetValue instanceof HTMLInputElement) {
            templatePreviewBetValue.value = sample.betValue;
        }
        if (templatePreviewAmount instanceof HTMLInputElement) {
            templatePreviewAmount.value = sample.amount;
        }
        if (templatePreviewIssueNo instanceof HTMLInputElement) {
            templatePreviewIssueNo.value = sample.issueNo;
        }
        if (templatePreviewPayload instanceof HTMLTextAreaElement) {
            templatePreviewPayload.value = sample.payload;
        }
        syncTemplatePreview();
    }

    function applyTemplatePreset(presetId) {
        if (!(messageTemplateForm instanceof HTMLFormElement)) {
            return;
        }
        if (window.PlatformNavigation && !window.PlatformNavigation.confirmLeaveForm(messageTemplateForm)) return;
        const preset = templatePresetCatalog()[String(presetId || "").trim()];
        if (!preset) {
            return;
        }
        messageTemplateForm.elements.edit_id.value = "";
        messageTemplateForm.elements.name.value = preset.name;
        messageTemplateForm.elements.lottery_type.value = preset.lottery_type;
        messageTemplateForm.elements.status.value = preset.status;
        messageTemplateForm.elements.template_text.value = preset.template_text;
        applyTemplateConfigToForm(cloneJson(preset.config));
        applyTemplateSample(preset.preview && preset.preview.sampleId || "big_small");
        setFormEditingState(messageTemplateForm, document.getElementById("createMessageTemplateBtn"), cancelMessageTemplateEditBtn, false, "新增模板");
        if (window.PlatformNavigation) window.PlatformNavigation.markDirty(messageTemplateForm);
        setStatus("已套用模板预设：「" + preset.name + "」。", false);
    }

    function syncTargetKeyPreview() {
        if (!(targetKeyPreview instanceof HTMLElement)) {
            return;
        }
        const rawValue = String(targetForm.elements.target_key.value || "").trim();
        if (!rawValue) {
            targetKeyPreview.hidden = true;
            targetKeyPreview.textContent = "";
            targetKeyPreview.classList.remove("is-error");
            return;
        }
        try {
            const normalized = normalizeTelegramTargetKey(rawValue);
            targetKeyPreview.hidden = false;
            targetKeyPreview.textContent = normalized === rawValue ? ("识别成功：" + normalized) : ("将保存为：" + normalized);
            targetKeyPreview.classList.remove("is-error");
        } catch (error) {
            targetKeyPreview.hidden = false;
            targetKeyPreview.textContent = error && error.message ? error.message : "群组标识解析失败";
            targetKeyPreview.classList.add("is-error");
        }
    }

    function resetAccountVerifyForm() {
        if (!(accountVerifyForm instanceof HTMLFormElement)) {
            return;
        }
        accountVerifyForm.reset();
        accountVerifyForm.hidden = true;
        accountVerifyForm.elements.telegram_account_id.value = "";
        const verifyCodeBtn = accountVerifyForm.querySelector("#verifyAccountCodeBtn");
        const verifyPasswordBtn = accountVerifyForm.querySelector("#verifyAccountPasswordBtn");
        const verifySummary = document.getElementById("accountVerifySummary");
        const passwordHint = document.getElementById("accountPasswordHint");
        if (verifyCodeBtn instanceof HTMLElement) {
            verifyCodeBtn.hidden = false;
        }
        if (verifyPasswordBtn instanceof HTMLElement) {
            verifyPasswordBtn.hidden = true;
        }
        if (verifySummary instanceof HTMLElement) {
            verifySummary.textContent = "";
        }
        if (passwordHint instanceof HTMLElement) {
            passwordHint.hidden = true;
        }
        setAccountVerifyFeedback("");
    }

    function setAccountVerifyFeedback(message, isError) {
        const feedback = document.getElementById("accountVerifyFeedback");
        if (!(feedback instanceof HTMLElement)) {
            return;
        }
        const text = String(message || "").trim();
        feedback.textContent = text;
        feedback.hidden = !text;
        feedback.classList.toggle("is-error", Boolean(isError) && Boolean(text));
    }

    function renderAccountModeGuide(mode) {
        const normalized = String(mode || "phone_login").trim() || "phone_login";
        const config = accountModeConfig[normalized] || accountModeConfig.phone_login;
        if (accountModeBadge instanceof HTMLElement) {
            accountModeBadge.textContent = config.badge;
            accountModeBadge.classList.toggle("is-session-import", normalized === "session_import");
            accountModeBadge.classList.toggle("is-phone-login", normalized !== "session_import");
        }
        if (accountModeHeadline instanceof HTMLElement) {
            accountModeHeadline.textContent = config.headline;
        }
        if (accountModeGuideTitle instanceof HTMLElement) {
            accountModeGuideTitle.textContent = config.title;
        }
        if (accountModeGuideSummary instanceof HTMLElement) {
            accountModeGuideSummary.textContent = config.summary;
        }
        if (accountModeGuideSteps instanceof HTMLElement) {
            accountModeGuideSteps.innerHTML = config.steps.map(function (step, index) {
                return '<div class="account-mode-step"><span class="account-mode-step-index">' + String(index + 1) + "</span><span>" + escapeHtml(step) + "</span></div>";
            }).join("");
        }
        if (accountPostActionTitle instanceof HTMLElement) {
            accountPostActionTitle.textContent = config.postActionTitle;
        }
        if (accountPostActionBody instanceof HTMLElement) {
            accountPostActionBody.textContent = config.postActionBody;
        }
        if (accountPostActionHint instanceof HTMLElement) {
            accountPostActionHint.hidden = false;
        }
    }

    function syncAccountModeUI(mode) {
        if (!(accountForm instanceof HTMLFormElement)) {
            return;
        }
        const normalized = String(mode || "phone_login").trim() || "phone_login";
        const config = accountModeConfig[normalized] || accountModeConfig.phone_login;
        accountModeButtons.forEach(function (button) {
            button.classList.toggle("is-active", button.getAttribute("data-account-auth-mode") === normalized);
        });
        accountForm.elements.auth_mode.value = normalized;
        const phoneField = accountForm.querySelector(".account-phone-field");
        const sessionField = accountForm.querySelector(".account-session-file-field");
        if (phoneField instanceof HTMLElement) {
            phoneField.hidden = normalized !== "phone_login";
        }
        if (sessionField instanceof HTMLElement) {
            sessionField.hidden = normalized !== "session_import";
        }
        if (accountForm.elements.phone instanceof HTMLInputElement) {
            accountForm.elements.phone.required = normalized === "phone_login";
        }
        if (accountForm.elements.session_file instanceof HTMLInputElement) {
            accountForm.elements.session_file.required = normalized === "session_import" && !String(accountForm.elements.edit_id.value || "").trim();
        }
        const accountModeDescription = document.getElementById("accountModeDescription");
        if (accountModeDescription instanceof HTMLElement) {
            accountModeDescription.textContent = config.description;
        }
        renderAccountModeGuide(normalized);
        const createAccountBtn = document.getElementById("createAccountBtn");
        if (createAccountBtn instanceof HTMLElement) {
            createAccountBtn.textContent = String(accountForm.elements.edit_id.value || "").trim()
                ? config.editText
                : config.createText;
        }
        if (normalized !== "phone_login") {
            resetAccountVerifyForm();
        }
    }

    function setSubscriptionAdvancedVisible(visible) {
        const expanded = Boolean(visible);
        if (subscriptionAdvancedFields instanceof HTMLElement) {
            subscriptionAdvancedFields.hidden = !expanded;
        }
        if (toggleSubscriptionAdvancedBtn instanceof HTMLButtonElement) {
            toggleSubscriptionAdvancedBtn.setAttribute("aria-expanded", expanded ? "true" : "false");
            toggleSubscriptionAdvancedBtn.textContent = expanded ? "收起更多设置" : "展开更多设置";
        }
    }

    function normalizeSubscriptionStrategyMode(value) {
        const normalized = String(value || "fixed").trim() || "fixed";
        if (normalized === "follow_source" || normalized === "martingale" || normalized === "win_streak_roll") {
            return normalized;
        }
        return "fixed";
    }

    function normalizeSubscriptionBetFilterMode(value) {
        return String(value || "all").trim() === "selected" ? "selected" : "all";
    }

    function normalizeSubscriptionSettlementRuleSource(value) {
        return String(value || "follow_signal").trim() === "subscription_fixed" ? "subscription_fixed" : "follow_signal";
    }

    function currentSubscriptionSettlementRuleSource() {
        if (!(subscriptionSettlementRuleSourceSelect instanceof HTMLSelectElement)) {
            return "follow_signal";
        }
        return normalizeSubscriptionSettlementRuleSource(subscriptionSettlementRuleSourceSelect.value);
    }

    function currentSubscriptionBetFilterMode() {
        if (!(subscriptionBetFilterModeSelect instanceof HTMLSelectElement)) {
            return "all";
        }
        return normalizeSubscriptionBetFilterMode(subscriptionBetFilterModeSelect.value);
    }

    function selectedSubscriptionBetFilterKeys() {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return [];
        }
        const selected = [];
        Array.from(subscriptionForm.querySelectorAll("input[name='bet_filter_key']")).forEach(function (input) {
            if (!(input instanceof HTMLInputElement) || !input.checked) {
                return;
            }
            const key = String(input.value || "").trim();
            if (SUBSCRIPTION_PLAY_FILTER_LABELS[key] && !selected.includes(key)) {
                selected.push(key);
            }
        });
        return selected;
    }

    function setSelectedSubscriptionBetFilterKeys(keys) {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        const allowedKeys = Array.isArray(keys) ? keys.filter(function (key) {
            return Boolean(SUBSCRIPTION_PLAY_FILTER_LABELS[key]);
        }) : [];
        Array.from(subscriptionForm.querySelectorAll("input[name='bet_filter_key']")).forEach(function (input) {
            if (!(input instanceof HTMLInputElement)) {
                return;
            }
            input.checked = allowedKeys.includes(String(input.value || "").trim());
        });
    }

    function applySubscriptionPlayFilterPreset(presetKey) {
        const normalizedPreset = String(presetKey || "").trim();
        if (normalizedPreset === "custom") {
            if (subscriptionBetFilterModeSelect instanceof HTMLSelectElement) {
                subscriptionBetFilterModeSelect.value = "selected";
            }
            setSelectedSubscriptionBetFilterKeys([]);
            syncSubscriptionBetFilterUI();
            return;
        }
        const keys = SUBSCRIPTION_PLAY_FILTER_PRESETS[normalizedPreset] || [];
        if (!keys.length) {
            return;
        }
        if (subscriptionBetFilterModeSelect instanceof HTMLSelectElement) {
            subscriptionBetFilterModeSelect.value = "selected";
        }
        setSelectedSubscriptionBetFilterKeys(keys);
        syncSubscriptionBetFilterUI();
    }

    function samePlayFilterKeys(leftKeys, rightKeys) {
        const left = Array.isArray(leftKeys) ? leftKeys.slice().sort() : [];
        const right = Array.isArray(rightKeys) ? rightKeys.slice().sort() : [];
        return left.length === right.length && left.every(function (item, index) {
            return item === right[index];
        });
    }

    function syncSubscriptionBetFilterUI() {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        const mode = currentSubscriptionBetFilterMode();
        const isSelectedMode = mode === "selected";
        const selectedKeys = selectedSubscriptionBetFilterKeys();
        if (subscriptionBetFilterFields instanceof HTMLElement) {
            subscriptionBetFilterFields.hidden = !isSelectedMode;
        }
        if (subscriptionBetFilterHint instanceof HTMLElement) {
            if (currentSubscriptionStrategyMode() === "win_streak_roll") {
                subscriptionBetFilterHint.textContent = "过关滚仓：这里决定每期用来源的哪条信号触发。「押哪种组合」选了跟随信号时，建议只勾 4 个组合玩法，触发最稳定。";
            } else {
                subscriptionBetFilterHint.textContent = "建议先用自选玩法，只勾你真正想下的内容。";
            }
        }
        Array.from(subscriptionForm.querySelectorAll("input[name='bet_filter_key']")).forEach(function (input) {
            if (!(input instanceof HTMLInputElement)) {
                return;
            }
            input.disabled = !isSelectedMode;
        });
        Array.from(document.querySelectorAll("[data-play-filter-preset]")).forEach(function (button) {
            if (!(button instanceof HTMLButtonElement)) {
                return;
            }
            const presetKey = String(button.getAttribute("data-play-filter-preset") || "").trim();
            let isActive = false;
            if (isSelectedMode) {
                if (presetKey === "custom") {
                    isActive = selectedKeys.length === 0 || !Object.keys(SUBSCRIPTION_PLAY_FILTER_PRESETS).some(function (key) {
                        return samePlayFilterKeys(selectedKeys, SUBSCRIPTION_PLAY_FILTER_PRESETS[key]);
                    });
                } else {
                    isActive = samePlayFilterKeys(selectedKeys, SUBSCRIPTION_PLAY_FILTER_PRESETS[presetKey]);
                }
            }
            button.classList.toggle("is-active", isActive);
        });
        if (subscriptionBetFilterHint instanceof HTMLElement) {
            if (!isSelectedMode) {
                subscriptionBetFilterHint.textContent = "保留原有玩法范围。双组、杀组需要切换到自选并明确勾选。";
            } else if (selectedKeys.length) {
                subscriptionBetFilterHint.textContent = "只会跟你勾选的玩法，其他信号会自动跳过。当前已选 " + selectedKeys.length + " 项。";
            } else {
                subscriptionBetFilterHint.textContent = "请至少勾选一个玩法。建议先从“大小”或“组合”里选你真正想下的内容。";
            }
        }
        if (subscriptionBetFilterSelectionSummary instanceof HTMLElement) {
            if (!isSelectedMode) {
                subscriptionBetFilterSelectionSummary.textContent = "当前设置：原有全部玩法，不包含双组、杀组。";
            } else if (!selectedKeys.length) {
                subscriptionBetFilterSelectionSummary.textContent = "当前还没选玩法。可以直接点上面的快捷按钮。";
            } else {
                subscriptionBetFilterSelectionSummary.textContent = "当前已选：" + selectedKeys.map(function (key) {
                    return SUBSCRIPTION_PLAY_FILTER_LABELS[key];
                }).join(" / ");
            }
        }
        syncGroupStakeHint();
    }

    function signalGroupCombos(signal) {
        const all = ["大单", "大双", "小单", "小双"];
        const metric = String(signal && signal.bet_type || "");
        let value = String(signal && signal.bet_value || "");
        if (metric === "kill_group") {
            value = value.replace(/^杀/, "");
            return all.includes(value) ? all.filter(function (combo) { return combo !== value; }) : [];
        }
        if (metric === "double_group") {
            value = ({"单组": "大单+小双", "双组": "大双+小单"})[value] || value;
            const values = value.split(/[+＋/、,，]/).map(function (part) { return part.trim(); });
            return values.length === 2 && new Set(values).size === 2 && values.every(function (combo) { return all.includes(combo); })
                ? all.filter(function (combo) { return values.includes(combo); }) : [];
        }
        return [];
    }

    function syncGroupStakeHint() {
        if (!subscriptionForm) return;
        const keys = currentSubscriptionBetFilterMode() === "selected" ? selectedSubscriptionBetFilterKeys() : [];
        const double = keys.some(function (key) { return key.startsWith("double_group:"); });
        const kill = keys.some(function (key) { return key.startsWith("kill_group:"); });
        const hasGroup = double || kill;
        const fixedLabel = document.getElementById("subscriptionFixedAmountLabel");
        const baseLabel = document.getElementById("subscriptionBaseAmountLabel");
        const hint = document.getElementById("subscriptionGroupStakeHint");
        if (fixedLabel) fixedLabel.textContent = hasGroup ? "每组合固定金额" : "每期固定金额";
        if (baseLabel) baseLabel.textContent = hasGroup ? "每组合起始金额" : "起始金额";
        if (!hint) return;
        hint.hidden = !hasGroup;
        const mode = currentSubscriptionStrategyMode();
        if (mode === "win_streak_roll" && subscriptionForm.elements.roll_structure.value === "single_play") { hint.hidden = true; return; }
        const amount = Number((mode === "martingale" ? subscriptionForm.elements.base_stake : subscriptionForm.elements.stake_amount).value || 0);
        const totals = [];
        if (double) totals.push("双组共 " + amountText(amount * 2));
        if (kill) totals.push("杀组共 " + amountText(amount * 3));
        hint.textContent = mode === "win_streak_roll" ? "双组、杀组请使用均注、来源金额或倍投，不能与过关结构同时使用。"
            : mode === "follow_source" ? "来源金额按每个组合使用，双组总额为 2 倍，杀组总额为 3 倍。整组盈亏结算后再推进策略。"
            : "每组合 " + amountText(amount) + "，" + totals.join("，") + (mode === "martingale" ? "（第 1 手）" : "（本期）") + "。预测命中不等于盈利，按整组实际盈亏推进策略。";
    }

    function currentSubscriptionStrategyMode() {
        if (!(subscriptionStrategyModeSelect instanceof HTMLSelectElement)) {
            return "fixed";
        }
        return normalizeSubscriptionStrategyMode(subscriptionStrategyModeSelect.value);
    }

    function syncGenericRollPlayUI() {
        if (!subscriptionForm || !subscriptionForm.elements.roll_play_type) return;
        const form = subscriptionForm.elements;
        const active = currentSubscriptionStrategyMode() === "win_streak_roll" && form.roll_structure.value === "single_play";
        const values = {big_small: ["大", "小"], odd_even: ["单", "双"], combo: ["大单", "大双", "小单", "小双"],
            kill_group: ["大单", "大双", "小单", "小双"], number: Array.from({length: 28}, (_, i) => String(i)),
            double_group: ["大单+大双", "大单+小单", "大单+小双", "大双+小单", "大双+小双", "小单+小双"]};
        const previous = form.roll_fixed_value.value;
        form.roll_fixed_value.innerHTML = (values[form.roll_play_type.value] || []).map(value =>
            '<option value="' + escapeHtml(value) + '">' + escapeHtml((form.roll_play_type.value === 'kill_group' ? '杀' : '') + value) + '</option>').join('');
        if ((values[form.roll_play_type.value] || []).includes(previous)) form.roll_fixed_value.value = previous;
        const showValue = active && form.roll_selection_mode.value !== "follow";
        subscriptionForm.querySelectorAll('.subscription-roll-play-field').forEach(el => { el.hidden = !active; });
        subscriptionForm.querySelectorAll('.subscription-roll-value-field').forEach(el => { el.hidden = !showValue; });
        form.roll_play_type.disabled = !active;
        form.roll_selection_mode.disabled = !active;
        form.roll_fixed_value.disabled = !showValue;
        subscriptionForm.querySelectorAll('.subscription-play-filter-mode-field').forEach(el => { el.hidden = active; });
        if (subscriptionBetFilterFields) subscriptionBetFilterFields.hidden = active || currentSubscriptionBetFilterMode() !== "selected";
    }

    function syncSubscriptionPresetUI() {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        const strategyMode = currentSubscriptionStrategyMode();
        const showFixed = strategyMode === "fixed";
        const showFollowSource = strategyMode === "follow_source";
        const showMartingale = strategyMode === "martingale";
        const showRoll = strategyMode === "win_streak_roll";
        const amountInput = subscriptionForm.elements.stake_amount;
        const baseStakeInput = subscriptionForm.elements.base_stake;
        const multiplierInput = subscriptionForm.elements.multiplier;
        const maxStepsInput = subscriptionForm.elements.max_steps;
        const refundActionInput = subscriptionForm.elements.refund_action;
        const capActionInput = subscriptionForm.elements.cap_action;
        const targetStreakInput = subscriptionForm.elements.target_streak;
        const baseAmountInput = subscriptionForm.elements.base_amount;
        const rollStructureInput = subscriptionForm.elements.roll_structure;
        const rollModeInput = subscriptionForm.elements.roll_mode;

        Array.from(subscriptionForm.querySelectorAll(".subscription-fixed-field")).forEach(function (field) {
            if (field instanceof HTMLElement) {
                field.hidden = !showFixed;
            }
        });
        Array.from(subscriptionForm.querySelectorAll(".subscription-follow-tip-field")).forEach(function (field) {
            if (field instanceof HTMLElement) {
                field.hidden = !showFollowSource;
            }
        });
        Array.from(subscriptionForm.querySelectorAll(".subscription-roll-field")).forEach(function (field) {
            if (field instanceof HTMLElement) {
                field.hidden = !showRoll;
            }
        });
        Array.from(subscriptionForm.querySelectorAll(".subscription-martingale-field")).forEach(function (field) {
            if (field instanceof HTMLElement) {
                field.hidden = !showMartingale;
            }
        });
        Array.from(subscriptionForm.querySelectorAll(".subscription-martingale-advanced-field")).forEach(function (field) {
            if (field instanceof HTMLElement) {
                field.hidden = !showMartingale;
            }
        });

        if (amountInput instanceof HTMLInputElement) {
            amountInput.disabled = !showFixed;
            if (showFixed && !String(amountInput.value || "").trim()) {
                amountInput.value = "10";
            }
        }
        if (baseStakeInput instanceof HTMLInputElement) {
            baseStakeInput.disabled = !showMartingale;
            if (showMartingale && !String(baseStakeInput.value || "").trim()) {
                baseStakeInput.value = "10";
            }
        }
        if (multiplierInput instanceof HTMLInputElement) {
            multiplierInput.disabled = !showMartingale;
            if (showMartingale && !String(multiplierInput.value || "").trim()) {
                multiplierInput.value = "2";
            }
        }
        if (maxStepsInput instanceof HTMLInputElement) {
            maxStepsInput.disabled = !showMartingale;
            if (showMartingale && !String(maxStepsInput.value || "").trim()) {
                maxStepsInput.value = "3";
            }
        }
        if (refundActionInput instanceof HTMLSelectElement) {
            refundActionInput.disabled = !showMartingale;
            if (!String(refundActionInput.value || "").trim()) {
                refundActionInput.value = "hold";
            }
        }
        if (capActionInput instanceof HTMLSelectElement) {
            capActionInput.disabled = !showMartingale;
            if (!String(capActionInput.value || "").trim()) {
                capActionInput.value = "reset";
            }
        }
        if (targetStreakInput instanceof HTMLInputElement) {
            targetStreakInput.disabled = !showRoll;
            if (showRoll && (!String(targetStreakInput.value || "").trim() || Number(targetStreakInput.value) < 1)) {
                targetStreakInput.value = "3";
            }
        }
        if (baseAmountInput instanceof HTMLInputElement) {
            baseAmountInput.disabled = !showRoll;
            if (showRoll && !String(baseAmountInput.value || "").trim()) {
                baseAmountInput.value = "100";
            }
        }
        if (rollStructureInput instanceof HTMLSelectElement) {
            rollStructureInput.disabled = !showRoll;
            if (showRoll && !String(rollStructureInput.value || "").trim()) {
                rollStructureInput.value = "A";
            }
        }
        if (rollModeInput instanceof HTMLSelectElement) {
            rollModeInput.disabled = !showRoll;
            if (showRoll && !String(rollModeInput.value || "").trim()) {
                rollModeInput.value = "chain";
            }
        }
        if (subscriptionStrategyModeHint instanceof HTMLElement) {
            if (showFixed) {
                subscriptionStrategyModeHint.textContent = "均注最简单，适合先跑通整条链路。";
            } else if (showFollowSource) {
                subscriptionStrategyModeHint.textContent = "系统会直接使用来源信号里的金额，适合想完全照抄来源金额时使用。";
            } else if (showRoll) {
                subscriptionStrategyModeHint.textContent = "过关滚仓：连续赢够设定的关数就收工，盈利自动滚进下一注；当天亏完起始注就止损，第二天重新开始。";
            } else {
                subscriptionStrategyModeHint.textContent = "倍投会按当前手数自动放大金额；更多设置里还能调整回本后和追顶后的处理方式。";
            }
        }
        syncGroupStakeHint();
        syncGenericRollPlayUI();
    }

    function settlementRulePreset(ruleId) {
        return SETTLEMENT_RULE_UI_PRESETS[String(ruleId || "")] || SETTLEMENT_RULE_UI_PRESETS.pc28_netdisk_regular;
    }

    function sameOddsValue(left, right) {
        return Math.round(Number(left) * 10000) === Math.round(Number(right) * 10000);
    }

    function parseOddsInputValue(raw) {
        const text = String(raw == null ? "" : raw).trim();
        if (!text) {
            return null;
        }
        const odds = Number(text);
        if (!Number.isFinite(odds) || odds <= 1 || odds > 1000) {
            throw new Error("自定义赔率必须大于 1 且不超过 1000");
        }
        return Math.round(odds * 10000) / 10000;
    }

    function oddsInputValue(value) {
        if (value == null || value === "") {
            return "";
        }
        return String(value);
    }

    function comboOverrideValue(overrides, names, fallback) {
        const payload = overrides && typeof overrides === "object" ? overrides : {};
        const combo = payload.combo && typeof payload.combo === "object" ? payload.combo : {};
        for (let index = 0; index < names.length; index += 1) {
            if (combo[names[index]] != null && String(combo[names[index]]).trim() !== "") {
                return combo[names[index]];
            }
        }
        return fallback;
    }

    function fillSubscriptionOddsInputs(ruleId, overrides) {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        const preset = settlementRulePreset(ruleId);
        const payload = overrides && typeof overrides === "object" ? overrides : {};
        const bigSmall = payload.big_small != null ? payload.big_small : (payload.odd_even != null ? payload.odd_even : preset.bigSmall);
        if (subscriptionForm.elements.odds_big_small instanceof HTMLInputElement) {
            subscriptionForm.elements.odds_big_small.value = oddsInputValue(bigSmall);
        }
        if (subscriptionForm.elements.odds_combo_small_odd_big_even instanceof HTMLInputElement) {
            subscriptionForm.elements.odds_combo_small_odd_big_even.value = oddsInputValue(
                comboOverrideValue(payload, ["小单", "大双"], payload.combo_small_odd_big_even != null ? payload.combo_small_odd_big_even : preset.comboSmallOddBigEven)
            );
        }
        if (subscriptionForm.elements.odds_combo_big_odd_small_even instanceof HTMLInputElement) {
            subscriptionForm.elements.odds_combo_big_odd_small_even.value = oddsInputValue(
                comboOverrideValue(payload, ["大单", "小双"], payload.combo_big_odd_small_even != null ? payload.combo_big_odd_small_even : preset.comboBigOddSmallEven)
            );
        }
        const hint = document.getElementById("subscriptionSettlementRefundHint");
        if (hint instanceof HTMLElement) {
            hint.textContent = preset.refundHint + " 与预设相同的赔率不会写入自定义覆盖。";
        }
    }

    function collectSubscriptionOddsOverrides(ruleId) {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return {};
        }
        const preset = settlementRulePreset(ruleId);
        const overrides = {};
        const bigSmall = parseOddsInputValue(subscriptionForm.elements.odds_big_small && subscriptionForm.elements.odds_big_small.value);
        if (bigSmall != null && !sameOddsValue(bigSmall, preset.bigSmall)) {
            overrides.big_small = bigSmall;
            overrides.odd_even = bigSmall;
        }
        const comboA = parseOddsInputValue(subscriptionForm.elements.odds_combo_small_odd_big_even && subscriptionForm.elements.odds_combo_small_odd_big_even.value);
        if (comboA != null && !sameOddsValue(comboA, preset.comboSmallOddBigEven)) {
            overrides.combo = overrides.combo || {};
            overrides.combo["小单"] = comboA;
            overrides.combo["大双"] = comboA;
        }
        const comboB = parseOddsInputValue(subscriptionForm.elements.odds_combo_big_odd_small_even && subscriptionForm.elements.odds_combo_big_odd_small_even.value);
        if (comboB != null && !sameOddsValue(comboB, preset.comboBigOddSmallEven)) {
            overrides.combo = overrides.combo || {};
            overrides.combo["大单"] = comboB;
            overrides.combo["小双"] = comboB;
        }
        return overrides;
    }

    function hasCustomOddsOverrides(overrides) {
        const payload = overrides && typeof overrides === "object" ? overrides : {};
        if (payload.big_small != null || payload.odd_even != null) {
            return true;
        }
        if (payload.combo_small_odd_big_even != null || payload.combo_big_odd_small_even != null) {
            return true;
        }
        const combo = payload.combo && typeof payload.combo === "object" ? payload.combo : {};
        return Object.keys(combo).length > 0;
    }

    function syncSubscriptionSettlementUI() {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        const ruleSource = currentSubscriptionSettlementRuleSource();
        const isFixedRule = ruleSource === "subscription_fixed";
        Array.from(subscriptionForm.querySelectorAll(".subscription-settlement-fixed-field")).forEach(function (field) {
            if (field instanceof HTMLElement) {
                field.hidden = !isFixedRule;
            }
        });
        if (subscriptionForm.elements.settlement_rule_id instanceof HTMLSelectElement) {
            subscriptionForm.elements.settlement_rule_id.disabled = !isFixedRule;
            if (!String(subscriptionForm.elements.settlement_rule_id.value || "").trim()) {
                subscriptionForm.elements.settlement_rule_id.value = "pc28_netdisk_regular";
            }
        }
        if (subscriptionForm.elements.fallback_profit_ratio instanceof HTMLInputElement) {
            if (!String(subscriptionForm.elements.fallback_profit_ratio.value || "").trim()) {
                subscriptionForm.elements.fallback_profit_ratio.value = "1";
            }
        }
        if (subscriptionSettlementRuleHint instanceof HTMLElement) {
            subscriptionSettlementRuleHint.textContent = isFixedRule
                ? "当前会强制按你指定的规则记收益盈亏，不再跟随来源信号里的结算口径。"
                : "默认跟随来源信号里的结算规则。只有当你明确知道自己要统一按哪套口径记账时，才建议固定指定。";
        }
        if (isFixedRule && subscriptionForm.elements.settlement_rule_id instanceof HTMLSelectElement) {
            const currentBigSmall = String((subscriptionForm.elements.odds_big_small && subscriptionForm.elements.odds_big_small.value) || "").trim();
            if (!currentBigSmall) {
                fillSubscriptionOddsInputs(subscriptionForm.elements.settlement_rule_id.value, {});
            } else {
                const hint = document.getElementById("subscriptionSettlementRefundHint");
                const preset = settlementRulePreset(subscriptionForm.elements.settlement_rule_id.value);
                if (hint instanceof HTMLElement) {
                    hint.textContent = preset.refundHint + " 与预设相同的赔率不会写入自定义覆盖。";
                }
            }
        }
    }

    function syncSubscriptionRiskControlUI() {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        const enabled = subscriptionRiskControlEnabledCheckbox instanceof HTMLInputElement
            && subscriptionRiskControlEnabledCheckbox.checked;
        Array.from(subscriptionForm.querySelectorAll(".subscription-risk-field")).forEach(function (field) {
            if (!(field instanceof HTMLElement)) {
                return;
            }
            field.hidden = !enabled;
        });
        ["profit_target", "loss_limit", "risk_hit_rate_threshold", "risk_hit_rate_min_settled"].forEach(function (name) {
            const input = subscriptionForm.elements[name];
            if (!(input instanceof HTMLInputElement)) {
                return;
            }
            input.disabled = !enabled;
        });
        const gateEnabledInput = subscriptionForm.elements.risk_hit_rate_gate_enabled;
        if (gateEnabledInput instanceof HTMLInputElement) {
            gateEnabledInput.disabled = !enabled;
        }
        Object.keys(SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS).forEach(function (playKey) {
            const fields = SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS[playKey];
            [fields.profit, fields.loss].forEach(function (name) {
                const input = subscriptionForm.elements[name];
                if (input instanceof HTMLInputElement) {
                    input.disabled = !enabled;
                }
            });
        });
    }

    function emptySubscriptionRiskPlayLimits() {
        const result = {};
        Object.keys(SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS).forEach(function (playKey) {
            result[playKey] = {
                profit_target: 0,
                loss_limit: 0,
            };
        });
        return result;
    }

    function subscriptionRiskPlayLimitsFromForm(form) {
        const result = emptySubscriptionRiskPlayLimits();
        Object.keys(SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS).forEach(function (playKey) {
            const fields = SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS[playKey];
            const profitValue = String(form.elements[fields.profit].value || "").trim();
            const lossValue = String(form.elements[fields.loss].value || "").trim();
            result[playKey] = {
                profit_target: profitValue ? Number(profitValue) : 0,
                loss_limit: lossValue ? Number(lossValue) : 0,
            };
        });
        return result;
    }

    function applySubscriptionRiskPlayLimits(playLimits) {
        const payload = playLimits && typeof playLimits === "object" ? playLimits : {};
        Object.keys(SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS).forEach(function (playKey) {
            const fields = SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS[playKey];
            const item = payload[playKey] && typeof payload[playKey] === "object" ? payload[playKey] : {};
            const profitInput = subscriptionForm.elements[fields.profit];
            const lossInput = subscriptionForm.elements[fields.loss];
            if (profitInput instanceof HTMLInputElement) {
                profitInput.value = Number(item.profit_target || 0) > 0 ? String(item.profit_target) : "";
            }
            if (lossInput instanceof HTMLInputElement) {
                lossInput.value = Number(item.loss_limit || 0) > 0 ? String(item.loss_limit) : "";
            }
        });
    }

    function subscriptionStrategyModeFromStrategy(strategy) {
        const payload = strategy && typeof strategy === "object" ? strategy : {};
        if (payload.win_streak_roll && typeof payload.win_streak_roll === "object") {
            return "win_streak_roll";
        }
        if (String(payload.mode || "follow").trim() === "martingale") {
            return "martingale";
        }
        if (payload.stake_amount != null && String(payload.stake_amount).trim() !== "") {
            return "fixed";
        }
        return "follow_source";
    }

    function resetSubscriptionStrategyFormState() {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        if (subscriptionBetFilterModeSelect instanceof HTMLSelectElement) {
            subscriptionBetFilterModeSelect.value = "selected";
        }
        Array.from(subscriptionForm.querySelectorAll("input[name='bet_filter_key']")).forEach(function (input) {
            if (input instanceof HTMLInputElement) {
                input.checked = false;
            }
        });
        if (subscriptionStrategyModeSelect instanceof HTMLSelectElement) {
            subscriptionStrategyModeSelect.value = "fixed";
        }
        if (subscriptionForm.elements.subscription_name instanceof HTMLInputElement) {
            subscriptionForm.elements.subscription_name.value = "";
        }
        subscriptionForm.elements.stake_amount.value = "10";
        subscriptionForm.elements.base_stake.value = "10";
        subscriptionForm.elements.multiplier.value = "2";
        subscriptionForm.elements.max_steps.value = "3";
        subscriptionForm.elements.refund_action.value = "hold";
        subscriptionForm.elements.cap_action.value = "reset";
        if (subscriptionSettlementRuleSourceSelect instanceof HTMLSelectElement) {
            subscriptionSettlementRuleSourceSelect.value = "follow_signal";
        }
        if (subscriptionForm.elements.settlement_rule_id instanceof HTMLSelectElement) {
            subscriptionForm.elements.settlement_rule_id.value = "pc28_netdisk_regular";
        }
        fillSubscriptionOddsInputs("pc28_netdisk_regular", {});
        subscriptionForm.elements.fallback_profit_ratio.value = "1";
        if (subscriptionRiskControlEnabledCheckbox instanceof HTMLInputElement) {
            subscriptionRiskControlEnabledCheckbox.checked = false;
        }
        subscriptionForm.elements.profit_target.value = "";
        subscriptionForm.elements.loss_limit.value = "";
        applySubscriptionRiskPlayLimits({});
        if (subscriptionForm.elements.risk_hit_rate_gate_enabled instanceof HTMLInputElement) {
            subscriptionForm.elements.risk_hit_rate_gate_enabled.checked = false;
        }
        subscriptionForm.elements.risk_hit_rate_threshold.value = "";
        subscriptionForm.elements.risk_hit_rate_min_settled.value = "";
        refreshSubscriptionTargetSelects();
        setSubscriptionAdvancedVisible(false);
        syncSubscriptionBetFilterUI();
        syncSubscriptionPresetUI();
        syncSubscriptionSettlementUI();
        syncSubscriptionRiskControlUI();
    }

    function loadAccountIntoForm(account, options) {
        const config = options || {};
        const mode = accountAuthMode(account);
        resetAccountVerifyForm();
        accountForm.elements.edit_id.value = String(account.id);
        accountForm.elements.label.value = account.label || "";
        accountForm.elements.phone.value = account.phone || "";
        accountForm.elements.session_file.value = "";
        syncAccountModeUI(mode);
        setFormEditingState(accountForm, document.getElementById("createAccountBtn"), cancelAccountEditBtn, true, "");
        syncAccountModeUI(mode);
        if (config.scroll !== false) {
            accountForm.scrollIntoView({behavior: "smooth", block: "start"});
        }
    }

    function showAccountVerification(account) {
        if (!(accountVerifyForm instanceof HTMLFormElement) || !account) {
            return;
        }
        if (accountAuthMode(account) !== "phone_login") {
            resetAccountVerifyForm();
            return;
        }
        const authState = accountAuthState(account);
        if (authState === "login_expired") {
            resetAccountVerifyForm();
            return;
        }
        const passwordRequired = authState === "password_required";
        accountVerifyForm.hidden = false;
        accountVerifyForm.elements.telegram_account_id.value = String(account.id || "");
        accountVerifyForm.elements.code.value = "";
        accountVerifyForm.elements.password.value = "";
        const verifyCodeBtn = accountVerifyForm.querySelector("#verifyAccountCodeBtn");
        const verifyPasswordBtn = accountVerifyForm.querySelector("#verifyAccountPasswordBtn");
        const verifySummary = document.getElementById("accountVerifySummary");
        const passwordHint = document.getElementById("accountPasswordHint");
        if (verifyCodeBtn instanceof HTMLElement) {
            verifyCodeBtn.hidden = passwordRequired;
        }
        if (verifyPasswordBtn instanceof HTMLElement) {
            verifyPasswordBtn.hidden = !passwordRequired;
        }
        if (passwordHint instanceof HTMLElement) {
            passwordHint.hidden = !passwordRequired;
        }
        if (verifySummary instanceof HTMLElement) {
            verifySummary.textContent = passwordRequired
                ? "账号「" + (account.label || "--") + "」需要输入 Telegram 两步验证密码（云密码），不是手机号登录密码。"
                : "账号「" + (account.label || "--") + "」已发送验证码，请继续填写。";
        }
        const lastError = String((account.meta && account.meta.last_auth_error) || "").trim();
        setAccountVerifyFeedback(lastError, Boolean(lastError));
        accountVerifyForm.scrollIntoView({behavior: "smooth", block: "start"});
        const focusTarget = passwordRequired ? accountVerifyForm.elements.password : accountVerifyForm.elements.code;
        focusElement(focusTarget);
    }

    function visibleAccounts() {
        return state.accounts.filter(function (item) {
            return !isArchivedItem(item);
        });
    }

    function accountLinkedTargets(item) {
        return visibleTargets().filter(function (targetItem) {
            return Number(targetItem.telegram_account_id) === Number(item && item.id);
        });
    }

    function visibleTargets() {
        return state.targets.filter(function (item) {
            return !isArchivedItem(item);
        });
    }

    function archivedTargets() {
        return state.targets.filter(isArchivedItem);
    }

    function visibleSubscriptions() {
        return state.subscriptions.filter(function (item) {
            return !isArchivedItem(item);
        });
    }

    function aiSources() {
        return state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
    }

    function primarySubscription() {
        const items = visibleSubscriptions();
        return items.find(function (item) {
            return item.status === "active";
        }) || items[0] || null;
    }

    function sourceSubscription(sourceId) {
        return visibleSubscriptions().find(function (item) {
            return Number(item.source_id) === Number(sourceId);
        }) || null;
    }

    function activeSourceSubscription(sourceId) {
        return activeSubscriptions().find(function (item) {
            return Number(item.source_id) === Number(sourceId);
        }) || null;
    }

    function sourceRawItems(sourceId) {
        return state.rawItems.filter(function (item) {
            return Number(item.source_id) === Number(sourceId);
        });
    }

    function sourceSignals(sourceId) {
        return state.signals.filter(function (item) {
            return Number(item.source_id) === Number(sourceId);
        });
    }

    function sourceJobs(sourceId) {
        const jobsWithSourceId = state.jobs.filter(function (item) {
            return Number(item.source_id) === Number(sourceId);
        });
        if (jobsWithSourceId.length) {
            return jobsWithSourceId;
        }
        const signalIds = {};
        sourceSignals(sourceId).forEach(function (item) {
            signalIds[String(item.id)] = true;
        });
        return state.jobs.filter(function (item) {
            return Boolean(signalIds[String(item.signal_id)]);
        });
    }

    function latestByTime(items, fieldNames) {
        const fields = Array.isArray(fieldNames) ? fieldNames : [fieldNames];
        return (items || []).slice().sort(function (left, right) {
            const leftMs = fields.reduce(function (result, field) {
                return result || parseTimeMs(left && left[field]);
            }, 0);
            const rightMs = fields.reduce(function (result, field) {
                return result || parseTimeMs(right && right[field]);
            }, 0);
            return rightMs - leftMs;
        })[0] || null;
    }

    function sourceSignalHasJobs(signalId) {
        return state.jobs.some(function (item) {
            return Number(item.signal_id) === Number(signalId);
        });
    }

    function sourcePipelineState(source) {
        const sourceId = source && source.id;
        const summary = source && source.pipeline_summary && typeof source.pipeline_summary === "object"
            ? source.pipeline_summary
            : null;
        const subscription = sourceSubscription(sourceId);
        const activeSubscription = activeSourceSubscription(sourceId);
        const rawItems = sourceRawItems(sourceId);
        const signals = sourceSignals(sourceId);
        const jobs = sourceJobs(sourceId);
        const latestRawItem = source && source.latest_raw_item
            ? source.latest_raw_item
            : latestByTime(rawItems, ["created_at", "published_at"]);
        const latestSignal = source && source.latest_signal
            ? source.latest_signal
            : latestByTime(signals, ["published_at", "created_at"]);
        const latestJob = latestByTime(jobs, ["updated_at", "created_at"]);
        const rawItemCount = summary ? Number(summary.raw_item_count || 0) : rawItems.length;
        const signalCount = summary ? Number(summary.signal_count || 0) : signals.length;
        const pendingRawCount = summary
            ? Number(summary.pending_raw_count || 0)
            : rawItems.filter(function (item) {
                return String(item.parse_status || "") === "pending";
            }).length;
        const failedRawCount = summary
            ? Number(summary.failed_raw_count || 0)
            : rawItems.filter(function (item) {
                return String(item.parse_status || "") === "failed";
            }).length;
        const readySignalCount = summary
            ? Number(summary.ready_signal_count || 0)
            : signals.filter(function (item) {
                return String(item.status || "") === "ready";
            }).length;
        const undispatchedSignalCount = signals.filter(function (item) {
            return !sourceSignalHasJobs(item.id);
        }).length;
        let headline = "还没有开始跑来源链路。";
        if (activeSubscription && !rawItemCount) {
            headline = "当前来源已经被跟单使用，但还没有抓取到任何原始内容。";
        } else if (pendingRawCount > 0) {
            headline = "最近已经抓到原始内容，但还没完成标准化。";
        } else if (!summary && signals.length && undispatchedSignalCount > 0) {
            headline = "已经生成标准信号，但还没有继续展开成执行任务。";
        } else if (signalCount > 0 && !jobs.length) {
            headline = "已经生成标准信号，最近执行任务可去记录页查看。";
        } else if (jobs.length) {
            headline = "这个来源最近已经跑到执行任务阶段。";
        } else if (!activeSubscription) {
            headline = "来源已经导入，但当前没有激活跟单策略会使用它。";
        }
        return {
            subscription: subscription,
            activeSubscription: activeSubscription,
            rawItems: rawItems,
            signals: signals,
            jobs: jobs,
            rawItemCount: rawItemCount,
            signalCount: signalCount,
            jobCount: jobs.length,
            latestRawItem: latestRawItem,
            latestSignal: latestSignal,
            latestJob: latestJob,
            pendingRawCount: pendingRawCount,
            failedRawCount: failedRawCount,
            readySignalCount: readySignalCount,
            undispatchedSignalCount: undispatchedSignalCount,
            headline: headline,
        };
    }

    function hasTargetRecentActivity(item) {
        if (!item) {
            return false;
        }
        return state.jobs.some(function (job) {
            return Number(job.delivery_target_id) === Number(item.id);
        });
    }

    function isTargetWizardVerified(item) {
        if (!item) {
            return false;
        }
        const testStatus = targetLastTestStatus(item);
        return testStatus === "success" || testStatus === "ok" || hasTargetRecentActivity(item) || readStoredValue("wizard-tested-target") === targetProgressToken(item);
    }

    function activeTargets() {
        return state.targets.filter(function (item) {
            return item.status === "active";
        });
    }

    function isSubscriptionRiskBlocked(item) {
        const financial = item && item.financial && typeof item.financial === "object" ? item.financial : null;
        const thresholdStatus = String(financial && financial.threshold_status || "").trim();
        return thresholdStatus === "profit_target_hit" || thresholdStatus === "loss_limit_hit";
    }

    function subscriptionRuntimeLabel(item) {
        if (!item) {
            return "策略未启用";
        }
        if (String(item.status || "inactive") === "standby") {
            return "待命触发";
        }
        if (String(item.status || "inactive") !== "active") {
            return "策略未启用";
        }
        const financial = item && item.financial && typeof item.financial === "object" ? item.financial : null;
        if (String(financial && financial.threshold_status || "") === "profit_target_hit") {
            return "风控止盈阻塞";
        }
        if (String(financial && financial.threshold_status || "") === "loss_limit_hit") {
            return "风控止损阻塞";
        }
        return "已接入跟单";
    }

    function activeSubscriptions() {
        return state.subscriptions.filter(function (item) {
            return item.status === "active" && !isSubscriptionRiskBlocked(item);
        });
    }

    function summarizeStrategyMode(payload) {
        const mode = String(payload && payload.mode || "").trim();
        const hasStakeAmount = payload && payload.stake_amount != null && String(payload.stake_amount).trim() !== "";
        if (mode === "martingale") {
            return "倍投";
        }
        if (payload && payload.win_streak_roll && typeof payload.win_streak_roll === "object") {
            const roll = payload.win_streak_roll;
            return "过关滚仓（过 " + (roll.target_streak || 3) + " 关）";
        }
        if (hasStakeAmount) {
            return "均注";
        }
        if (mode === "follow" || !mode) {
            return "跟随来源金额";
        }
        return mode;
    }

    function hasGroupSelection(strategy) {
        const filter = strategy && (strategy.play_filter || strategy.bet_filter) || {};
        return filter.mode === "selected" && (filter.selected_keys || []).some(function (key) { return key.startsWith("double_group:") || key.startsWith("kill_group:"); });
    }

    function summarizeBetFilter(strategy) {
        const payload = strategy && typeof strategy === "object" ? strategy : {};
        if (payload.win_streak_roll?.structure === 'single_play') {
            const roll = payload.win_streak_roll;
            return ({big_small:'大小',odd_even:'单双',combo:'组合',number:'号码',double_group:'双组',kill_group:'杀组'}[roll.play_type] || roll.play_type) +
                (roll.selection_mode === 'fixed' ? ' · 固定买 ' + roll.fixed_value : roll.selection_mode === 'filter' ? ' · 只跟 ' + roll.fixed_value : ' · 跟随预测');
        }
        const betFilter = payload.bet_filter && typeof payload.bet_filter === "object" ? payload.bet_filter : {};
        const mode = normalizeSubscriptionBetFilterMode(betFilter.mode);
        const selectedKeys = Array.isArray(betFilter.selected_keys)
            ? betFilter.selected_keys.filter(function (key) { return Boolean(SUBSCRIPTION_PLAY_FILTER_LABELS[key]); })
            : [];
        if (mode !== "selected" || !selectedKeys.length) {
            return "原有全部玩法（不含双组、杀组）";
        }
        return selectedKeys.map(function (key) {
            return SUBSCRIPTION_PLAY_FILTER_LABELS[key];
        }).join(" / ");
    }

    function summarizeSettlementPolicy(strategyV2, strategy) {
        const v2Payload = strategyV2 && typeof strategyV2 === "object" ? strategyV2 : {};
        const legacyPayload = strategy && typeof strategy === "object" ? strategy : {};
        const settlementPolicy = v2Payload.settlement_policy && typeof v2Payload.settlement_policy === "object"
            ? v2Payload.settlement_policy
            : {};
        const ruleSource = String(settlementPolicy.rule_source || "follow_signal").trim() || "follow_signal";
        const settlementRuleId = String(settlementPolicy.settlement_rule_id || "").trim();
        const fallbackProfitRatio = settlementPolicy.fallback_profit_ratio != null
            ? settlementPolicy.fallback_profit_ratio
            : (((legacyPayload.risk_control && legacyPayload.risk_control.win_profit_ratio) != null)
                ? legacyPayload.risk_control.win_profit_ratio
                : 1);
        if (ruleSource === "subscription_fixed" && settlementRuleId) {
            const custom = hasCustomOddsOverrides(settlementPolicy.odds_overrides) ? " · 自定义赔率" : "";
            return "结算 " + (SUBSCRIPTION_SETTLEMENT_RULE_LABELS[settlementRuleId] || settlementRuleId) + custom + " · 兜底净利 " + amountText(fallbackProfitRatio) + " 倍";
        }
        return "结算 跟随来源规则 · 兜底净利 " + amountText(fallbackProfitRatio) + " 倍";
    }

    function summarizeRiskPlayLimits(playLimits) {
        const payload = playLimits && typeof playLimits === "object" ? playLimits : {};
        const parts = [];
        Object.keys(SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS).forEach(function (playKey) {
            const fields = SUBSCRIPTION_RISK_PLAY_LIMIT_FIELDS[playKey];
            const item = payload[playKey] && typeof payload[playKey] === "object" ? payload[playKey] : {};
            const limits = [];
            if (Number(item.profit_target || 0) > 0) {
                limits.push("赚 " + amountText(item.profit_target));
            }
            if (Number(item.loss_limit || 0) > 0) {
                limits.push("亏 " + amountText(item.loss_limit));
            }
            if (limits.length) {
                parts.push(fields.label + " " + limits.join("/"));
            }
        });
        return parts;
    }

    function summarizeStrategy(strategy, strategyV2) {
        const payload = strategy && typeof strategy === "object" ? strategy : {};
        const parts = [];
        const riskControl = payload.risk_control && typeof payload.risk_control === "object"
            ? payload.risk_control
            : {};
        const modeLabel = summarizeStrategyMode(payload);
        parts.push("玩法 " + summarizeBetFilter(payload));
        if (modeLabel) {
            parts.push("方式 " + modeLabel);
        }
        if (payload.win_streak_roll && typeof payload.win_streak_roll === "object") {
            const roll = payload.win_streak_roll;
            parts.push("过 " + (roll.target_streak || 3) + " 关收工");
            if (roll.base_amount != null) {
                parts.push("每轮起始注 " + roll.base_amount);
            }
            const structureLabels = { A: "单+小双", B: "单+大双", C: "双+小单", D: "双+大单", follow_signal: "跟随信号" };
            if (roll.structure === "single_play") {
                const labels = {big_small: '大小', odd_even: '单双', combo: '组合', number: '号码', double_group: '双组', kill_group: '杀组'};
                parts.push((labels[roll.play_type] || roll.play_type) + ' · ' + ({follow: '跟随预测', fixed: '固定买', filter: '只跟'}[roll.selection_mode || 'follow']) + (roll.fixed_value == null ? '' : ' ' + roll.fixed_value));
            } else if (roll.structure) {
                parts.push("组合 " + (structureLabels[roll.structure] || roll.structure));
            }
            if (roll.roll_mode) {
                parts.push(roll.roll_mode === "chain" ? "链条滚仓" : "全仓滚仓");
            }
            const rollStopPolicy = roll.stop_policy && typeof roll.stop_policy === "object" ? roll.stop_policy : {};
            const onTargetLabels = { close_day: "手动达标当天收工", new_cycle: "手动达标重开一轮", continue: "手动达标继续滚" };
            const onBustLabels = { close_day: "手动资金耗尽当天收工", new_cycle: "手动资金耗尽重开一轮" };
            if (rollStopPolicy.on_target) {
                parts.push(onTargetLabels[rollStopPolicy.on_target] || ("达标后 " + rollStopPolicy.on_target));
            }
            if (rollStopPolicy.on_bust) {
                parts.push(onBustLabels[rollStopPolicy.on_bust] || ("爆仓后 " + rollStopPolicy.on_bust));
            }
            if (Number(rollStopPolicy.max_cycles_per_day || 0) > 1) {
                parts.push("手动一天最多 " + rollStopPolicy.max_cycles_per_day + " 轮");
            }
        }
        if (payload.base_stake != null && String(payload.base_stake).trim() !== "") {
            parts.push((hasGroupSelection(payload) ? "每组合起始 " : "起始金额 ") + payload.base_stake);
        }
        if (payload.stake_amount != null && String(payload.stake_amount).trim() !== "") {
            parts.push((hasGroupSelection(payload) ? "每组合 " : "每期 ") + payload.stake_amount);
        } else if (modeLabel !== "倍投" && !(payload.win_streak_roll && typeof payload.win_streak_roll === "object")) {
            parts.push("金额 跟随来源");
        }
        if (payload.multiplier != null && String(payload.multiplier).trim() !== "") {
            parts.push("每次乘 " + payload.multiplier);
        }
        if (payload.max_steps != null && String(payload.max_steps).trim() !== "") {
            parts.push("最多追 " + payload.max_steps + " 手");
        }
        if (payload.refund_action) {
            parts.push(payload.refund_action === "hold" ? "回本后保持当前手数" : (payload.refund_action === "reset" ? "回本后回到第 1 手" : ("回本后 " + payload.refund_action)));
        }
        if (payload.cap_action) {
            parts.push(payload.cap_action === "reset" ? "到顶后回到第 1 手" : (payload.cap_action === "hold" ? "到顶后停在最高一手" : ("到顶后 " + payload.cap_action)));
        }
        if (payload.expire_after_seconds) {
            parts.push("信号 " + payload.expire_after_seconds + " 秒后过期");
        }
        parts.push(summarizeSettlementPolicy(strategyV2, payload));
        if (riskControl.enabled) {
            const riskParts = [];
            if (Number(riskControl.profit_target || 0) > 0) {
                riskParts.push("净赚到 " + amountText(riskControl.profit_target) + " 停");
            }
            if (Number(riskControl.loss_limit || 0) > 0) {
                riskParts.push("净亏到 " + amountText(riskControl.loss_limit) + " 停");
            }
            summarizeRiskPlayLimits(riskControl.play_limits).forEach(function (item) {
                riskParts.push(item);
            });
            const summaryGate = riskControl.hit_rate_gate && typeof riskControl.hit_rate_gate === "object"
                ? riskControl.hit_rate_gate
                : {};
            if (summaryGate.enabled) {
                riskParts.push("当日命中率 ≥" + String(summaryGate.threshold) + "%（满" + String(summaryGate.min_settled_count) + "单）");
            }
            parts.push("自动停单 " + riskParts.join(" / "));
        }
        return parts.length ? parts.join(" · ") : "已保存跟单策略";
    }

    function summarizeProgression(subscription) {
        const progression = subscription && subscription.progression && typeof subscription.progression === "object"
            ? subscription.progression
            : null;
        const financial = subscription && subscription.financial && typeof subscription.financial === "object"
            ? subscription.financial
            : null;
        if (!progression) {
            return financial
                ? ("当前手数 1 · 本轮净盈亏 " + signedAmountText(financial.net_profit || 0))
                : "当前手数 1";
        }
        const parts = ["当前手数 " + String(progression.current_step || 1)];
        if (progression.last_result_type) {
            parts.push("最近资金结果 " + (hasGroupSelection(subscription.strategy_v2 || subscription.strategy) ? ({hit: "盈利", miss: "亏损", refund: "持平"})[progression.last_result_type] || progressionResultText(progression.last_result_type) : progressionResultText(progression.last_result_type)));
        }
        if (progression.pending_event_id) {
            parts.push("待结算 " + (progression.pending_issue_no || "--"));
            parts.push("状态 " + (progression.pending_status || "pending"));
        }
        if (financial) {
            parts.push("本轮净盈亏 " + signedAmountText(financial.net_profit || 0));
        }
        return parts.join(" · ");
    }

    function summarizeFinancial(subscription) {
        const financial = subscription && subscription.financial && typeof subscription.financial === "object"
            ? subscription.financial
            : null;
        if (!financial) {
            return "本轮净盈亏 0";
        }
        const parts = [
            "本轮净盈亏 " + signedAmountText(financial.net_profit || 0),
            "累计盈利 " + amountText(financial.realized_profit || 0),
            "累计亏损 " + amountText(financial.realized_loss || 0),
        ];
        if (financial.threshold_status === "profit_target_hit") {
            parts.push("已触发止盈");
        } else if (financial.threshold_status === "loss_limit_hit") {
            parts.push("已触发止损");
        }
        if (financial.baseline_reset_at) {
            parts.push("最近重置 " + String(financial.baseline_reset_at));
        }
        return parts.join(" · ");
    }

    function subscriptionDailyStat(subscription) {
        return subscription && subscription.daily_stat && typeof subscription.daily_stat === "object"
            ? subscription.daily_stat
            : null;
    }

    function summarizeDailyFinancial(subscription) {
        const stat = subscriptionDailyStat(subscription);
        if (!stat) {
            return "当日净盈亏 0";
        }
        return [
            String(stat.stat_date || state.subscriptionStatDate || "--") + " 净盈亏 " + signedAmountText(stat.net_profit || 0),
            "盈利 " + amountText(stat.profit_amount || 0),
            "亏损 " + amountText(stat.loss_amount || 0),
            "已结算 " + String(stat.settled_event_count || 0) + " 单",
            "命中 " + String(stat.hit_count || 0) + " / 未中 " + String(stat.miss_count || 0) + " / 回本 " + String(stat.refund_count || 0),
        ].join(" · ");
    }

    function subscriptionDailyNet(subscription) {
        const stat = subscriptionDailyStat(subscription);
        return stat ? Number(stat.net_profit || 0) : 0;
    }

    function subscriptionRuntimeNet(subscription) {
        const financial = subscription && subscription.financial && typeof subscription.financial === "object"
            ? subscription.financial
            : null;
        return financial ? Number(financial.net_profit || 0) : 0;
    }

    function subscriptionDailyHistory(subscription) {
        return subscription && Array.isArray(subscription.daily_history)
            ? subscription.daily_history
            : [];
    }

    function renderSubscriptionDailyHistoryItems(items, options) {
        const normalized = options || {};
        const title = normalized.title || "最近 7 天盈亏";
        const copy = normalized.copy || "按自然日查看这条跟单方案最近 7 天的已实现盈亏。";
        const emptyCopy = normalized.emptyCopy || "当前还没有历史日统计，等有已结算记录后，这里会自动累计。";
        const emptyText = normalized.emptyText || "暂无历史盈亏数据";
        if (!items.length) {
            return [
                '<section class="subscription-history-panel">',
                '<strong class="subscription-section-title">' + escapeHtml(title) + '</strong>',
                '<p class="subscription-section-copy">' + escapeHtml(emptyCopy) + '</p>',
                '<div class="subscription-history-empty">' + escapeHtml(emptyText) + '</div>',
                '</section>',
            ].join("");
        }
        return [
            '<section class="subscription-history-panel">',
            '<strong class="subscription-section-title">' + escapeHtml(title) + '</strong>',
            '<p class="subscription-section-copy">' + escapeHtml(copy) + '</p>',
            '<div class="subscription-history-list">',
            items.map(function (item) {
                return [
                    '<article class="subscription-history-item">',
                    '<div class="subscription-history-head"><strong>' + escapeHtml(String(item.stat_date || "--")) + '</strong><span>' + escapeHtml("净盈亏 " + signedAmountText(item.net_profit || 0)) + '</span></div>',
                    '<p>' + escapeHtml("盈利 " + amountText(item.profit_amount || 0) + " · 亏损 " + amountText(item.loss_amount || 0) + " · 已结算 " + String(item.settled_event_count || 0) + " 单") + '</p>',
                    '<p>' + escapeHtml("命中 " + String(item.hit_count || 0) + " / 未中 " + String(item.miss_count || 0) + " / 回本 " + String(item.refund_count || 0)) + '</p>',
                    '</article>',
                ].join("");
            }).join(""),
            '</div>',
            '</section>',
        ].join("");
    }

    function renderSubscriptionProfitDetail(subscription) {
        const stat = subscriptionDailyStat(subscription) || {};
        const statDate = String(stat.stat_date || state.subscriptionStatDate || "--");
        const allHistory = state.subscriptionAllDailyHistories[String(subscription.id)] || null;
        const historyItems = allHistory && Array.isArray(allHistory.items)
            ? allHistory.items
            : subscriptionDailyHistory(subscription);
        const historyTitle = allHistory ? "全部天数盈亏" : "最近 7 天盈亏";
        const historyCopy = allHistory
            ? "按自然日倒序展示这条跟单方案已有的全部日统计。"
            : "先展示最近 7 天，需要复盘完整周期时再展开全部天数。";
        const loadAllText = allHistory ? "已显示全部天数" : "查看全部天数";
        return [
            '<section class="subscription-detail-section">',
            '<div class="subscription-section-head">',
            '<div><strong class="subscription-section-title">盈亏日历</strong><p class="subscription-section-copy">上方日期选择器控制当前查看日期，这里保留单方案盈亏详情和最近趋势。</p></div>',
            '<span class="subscription-detail-date">' + escapeHtml(statDate) + '</span>',
            '</div>',
            renderConfigSummaryGrid([
                renderConfigSummaryItem("查看日净盈亏", signedAmountText(stat.net_profit || 0)),
                renderConfigSummaryItem("盈利", amountText(stat.profit_amount || 0)),
                renderConfigSummaryItem("亏损", amountText(stat.loss_amount || 0)),
                renderConfigSummaryItem("已结算", String(stat.settled_event_count || 0) + " 单"),
            ]),
            '<p class="subscription-detail-copy">' + escapeHtml("命中 " + String(stat.hit_count || 0) + " / 未中 " + String(stat.miss_count || 0) + " / 回本 " + String(stat.refund_count || 0)) + '</p>',
            '<div class="subscription-detail-actions"><button class="ghost-btn load-subscription-all-days-btn" type="button" data-subscription-id="' + subscription.id + '"' + (allHistory ? " disabled" : "") + '>' + escapeHtml(loadAllText) + '</button></div>',
            renderSubscriptionDailyHistoryItems(historyItems, {
                title: historyTitle,
                copy: historyCopy,
                emptyCopy: "当前还没有历史日统计，等有已结算记录后，这里会自动累计。",
                emptyText: "暂无历史盈亏数据",
            }),
            '</section>',
        ].join("");
    }

    function subscriptionRuntimeHistory(subscription) {
        const cached = state.subscriptionRuntimeHistories[String(subscription && subscription.id || "")] || null;
        if (cached && Array.isArray(cached.items)) {
            return cached.items;
        }
        return subscription && Array.isArray(subscription.runtime_history)
            ? subscription.runtime_history
            : [];
    }

    function subscriptionRuntimeResetKind(item) {
        const endReason = String(item && item.end_reason || "");
        if (endReason !== "manual_reset") {
            return "";
        }
        const resetNote = String(item && item.baseline_reset_note || "").trim();
        if (!resetNote) {
            return "manual_reset";
        }
        if (resetNote.indexOf("自动触发规则：") === 0) {
            return "auto_restart";
        }
        if (resetNote === "前端开始新一轮") {
            return "manual_restart";
        }
        return "manual_reset";
    }

    function subscriptionRuntimeStatusText(item) {
        const status = String(item && item.status || "");
        if (status === "active") {
            return "当前轮次";
        }
        if (status === "closed") {
            const endReason = String(item && item.end_reason || "");
            if (endReason === "profit_target_hit") {
                return "止盈结束";
            }
            if (endReason === "loss_limit_hit") {
                return "止损结束";
            }
            if (endReason === "manual_reset") {
                const resetKind = subscriptionRuntimeResetKind(item);
                if (resetKind === "auto_restart" || resetKind === "manual_restart") {
                    return "开始新一轮";
                }
                return "手动重置";
            }
            return "已结束";
        }
        return status || "--";
    }

    function subscriptionRuntimeEndReasonText(item) {
        const status = String(item && item.status || "");
        if (status === "active") {
            return "进行中";
        }
        const endReason = String(item && item.end_reason || "");
        if (!endReason) {
            return "正常结束";
        }
        if (endReason === "profit_target_hit") {
            return "达到止盈阈值";
        }
        if (endReason === "loss_limit_hit") {
            return "达到止损阈值";
        }
        if (endReason === "manual_reset") {
            const resetKind = subscriptionRuntimeResetKind(item);
            if (resetKind === "auto_restart") {
                return "自动触发开始新一轮";
            }
            if (resetKind === "manual_restart") {
                return "手动开始新一轮";
            }
            return "手动重置";
        }
        return endReason;
    }

    function summarizeSubscriptionRuntimePlayFilter(item) {
        const playFilter = item && item.play_filter && typeof item.play_filter === "object" ? item.play_filter : {};
        const mode = normalizeSubscriptionBetFilterMode(playFilter.mode);
        const selectedKeys = Array.isArray(playFilter.selected_keys)
            ? playFilter.selected_keys.filter(function (key) { return Boolean(SUBSCRIPTION_PLAY_FILTER_LABELS[key]); })
            : [];
        if (mode !== "selected" || !selectedKeys.length) {
            return Object.keys(playFilter).length ? "原有全部玩法（不含双组、杀组）" : "未记录";
        }
        return selectedKeys.map(function (key) {
            return SUBSCRIPTION_PLAY_FILTER_LABELS[key];
        }).join(" / ");
    }

    function subscriptionRuntimeScopeLabel(item) {
        if (!item || typeof item !== "object") {
            return "";
        }
        if (String(item.scope || "") === "route") {
            const routeName = String(item.route_name || "").trim();
            return routeName ? (" · 路由 " + routeName) : " · 路由";
        }
        if (String(item.scope || "") === "subscription") {
            return item.auto_trigger_rule_run_id ? " · 规则跟单" : " · 订阅直派";
        }
        return "";
    }

    function renderSubscriptionRuntimeHistory(subscription) {
        const items = subscriptionRuntimeHistory(subscription);
        if (!items.length) {
            return [
                '<section class="subscription-history-panel">',
                '<strong class="subscription-section-title">轮次历史</strong>',
                '<p class="subscription-section-copy">当前还没有形成轮次记录。等开始结算或手动重置后，这里会显示每轮结果。</p>',
                '<div class="subscription-history-empty">暂无轮次历史</div>',
                '</section>',
            ].join("");
        }
        const copyText = "当前显示 " + items.length + " 轮";
        return [
            '<section class="subscription-history-panel">',
            '<strong class="subscription-section-title">轮次历史</strong>',
            '<p class="subscription-section-copy">' + escapeHtml(copyText) + '</p>',
            '<div class="subscription-history-list">',
            items.map(function (item) {
                return [
                    '<article class="subscription-history-item">',
                    '<div class="subscription-history-head"><strong>' + escapeHtml(subscriptionRuntimeStatusText(item)) + '</strong><span>' + escapeHtml("净盈亏 " + signedAmountText(item.net_profit || 0)) + '</span></div>',
                    '<p>' + escapeHtml("玩法 " + summarizeSubscriptionRuntimePlayFilter(item) + subscriptionRuntimeScopeLabel(item)) + '</p>',
                    '<p>' + escapeHtml("开始期号 " + String(item.started_issue_no || "--") + " · 最近期号 " + String(item.last_issue_no || "--") + " · 已结算 " + String(item.settled_event_count || 0) + " 单") + '</p>',
                    '<p>' + escapeHtml("盈利 " + amountText(item.realized_profit || 0) + " · 亏损 " + amountText(item.realized_loss || 0) + " · 命中 " + String(item.hit_count || 0) + " / 未中 " + String(item.miss_count || 0) + " / 回本 " + String(item.refund_count || 0)) + '</p>',
                    '<p>' + escapeHtml("结束原因 " + subscriptionRuntimeEndReasonText(item)) + '</p>',
                    (item && item.baseline_reset_note
                        ? ('<p>' + escapeHtml("备注 " + String(item.baseline_reset_note || "")) + '</p>')
                        : ""),
                    '<p>' + escapeHtml("开始 " + String(item.started_at || "--") + (item.ended_at ? (" · 结束 " + String(item.ended_at)) : "")) + '</p>',
                    '</article>',
                ].join("");
            }).join(""),
            '</div>',
            '</section>',
        ].join("");
    }

    function normalizedSubscriptionDetailTab(subscriptionId) {
        const key = String(subscriptionId || "");
        const value = String(state.subscriptionDetailTabs[key] || "overview");
        return ["overview", "profit", "runtime", "delivery"].includes(value) ? value : "overview";
    }

    function renderSubscriptionDetailTabs(subscription, options) {
        const normalized = options || {};
        const activeTab = normalized.activeTab || normalizedSubscriptionDetailTab(subscription.id);
        const tabs = [
            {key: "overview", label: "概览"},
            {key: "profit", label: "盈亏日历"},
            {key: "runtime", label: "轮次历史"},
            {key: "delivery", label: "投递链路"},
        ];
        const tabMarkup = tabs.map(function (tab) {
            const selected = tab.key === activeTab;
            return '<button class="subscription-detail-tab' + (selected ? " is-active" : "") + '" type="button" data-subscription-id="' + subscription.id + '" data-subscription-detail-tab="' + escapeHtml(tab.key) + '" aria-selected="' + (selected ? "true" : "false") + '">' + escapeHtml(tab.label) + '</button>';
        }).join("");
        let panelMarkup = "";
        if (activeTab === "profit") {
            panelMarkup = renderSubscriptionProfitDetail(subscription);
        } else if (activeTab === "runtime") {
            panelMarkup = renderSubscriptionRuntimeHistory(subscription);
        } else if (activeTab === "delivery") {
            panelMarkup = [
                normalized.chainMetaMarkup || "",
                normalized.deliveryMarkup || "",
            ].join("");
        } else {
            panelMarkup = [
                '<section class="subscription-detail-section">',
                '<strong class="subscription-section-title">方案概览</strong>',
                '<p class="subscription-section-copy">默认只展示关键状态；需要复盘时再切到盈亏、轮次或投递链路。</p>',
                renderConfigDetailRows([
                    renderConfigDetailRow("策略摘要", summarizeStrategy(subscription.strategy || {}, subscription.strategy_v2 || {})),
                    renderConfigDetailRow("跟单进度", summarizeProgression(subscription)),
                    renderConfigDetailRow("盈亏摘要", summarizeFinancial(subscription)),
                    renderConfigDetailRow("当日盈亏", summarizeDailyFinancial(subscription)),
                ]),
                normalized.chainMetaMarkup || "",
                '</section>',
            ].join("");
        }
        return [
            '<section class="subscription-detail-panel">',
            '<div class="subscription-detail-tabs" role="tablist" aria-label="跟单详情切换">',
            tabMarkup,
            '</div>',
            '<div class="subscription-detail-content">',
            panelMarkup,
            '</div>',
            '</section>',
        ].join("");
    }

    function entityStatusAction(status, pauseText, resumeText) {
        const normalized = String(status || "inactive");
        if (normalized === "active") {
            return {nextStatus: "inactive", actionText: pauseText};
        }
        if (normalized === "archived") {
            return {nextStatus: "inactive", actionText: "取消归档"};
        }
        return {nextStatus: "active", actionText: resumeText};
    }

    function confirmDangerousAction(message) {
        return window.confirm(message);
    }

    function parseTimeMs(value) {
        const text = String(value || "").trim();
        if (!text) {
            return 0;
        }
        const ms = Date.parse(text);
        return Number.isFinite(ms) ? ms : 0;
    }

    function targetLastTestAt(item) {
        if (!item || typeof item !== "object") {
            return "";
        }
        return String(item.last_tested_at || item.last_test_sent_at || item.last_test_at || item.test_sent_at || "").trim();
    }

    function targetLastTestStatus(item) {
        if (!item || typeof item !== "object") {
            return "";
        }
        return String(item.last_test_status || item.test_status || "").trim().toLowerCase();
    }

    function targetTestSummary(item) {
        const testStatus = targetLastTestStatus(item);
        const testAt = targetLastTestAt(item);
        const testMessage = String(item && item.last_test_message || "").trim();
        if (testStatus === "success" || testStatus === "ok") {
            return testAt
                ? ("最近测试：" + formatDateTime(testAt) + "，群组可正常接收消息。")
                : "最近一次测试发送成功，群组可正常接收消息。";
        }
        if (testStatus === "failed") {
            return testMessage
                ? ("最近测试失败：" + testMessage)
                : "最近测试发送失败，请先修复群组可达性后再启用。";
        }
        return "还没有成功的测试发送记录，当前不能启用。";
    }

    function targetErrorCodeLabel(code) {
        const normalized = String(code || "").trim().toLowerCase();
        const labels = {
            target_not_joined: "账号未入群",
            target_no_write_permission: "无发言权限",
            target_unreachable: "目标不可达",
            target_test_failed: "测试发送失败",
            account_unauthorized: "账号未授权",
            account_archived: "账号已归档",
            target_account_missing: "未绑定账号",
            target_account_invalid: "账号无效",
            target_test_required: "需先测试",
        };
        return labels[normalized] || "";
    }

    function targetRepairState(item) {
        const account = accountById(item && item.telegram_account_id);
        const template = templateById(item && item.template_id);
        if (!account) {
            return {
                title: "还没有绑定托管账号",
                detail: "没有托管账号时，群组无法测试发送，也不能进入自动执行。",
                href: "/autobet/accounts#accountsSection",
                text: "去绑定账号",
            };
        }
        if (!isAccountAuthorized(account)) {
            return {
                title: "绑定账号尚未授权",
                detail: "当前群组绑定的账号不能正常发送消息，需要先完成验证码或二次密码授权。",
                href: "/autobet/accounts#accountsSection",
                text: "去完成授权",
            };
        }
        if (!template) {
            return {
                title: "还没有绑定下注模板",
                detail: "群组可以继续测试发送，但正式下注前最好绑定专用模板，避免不同机器人协议不一致。",
                href: "/autobet/templates#templatesSection",
                text: "去绑定模板",
            };
        }
        const testStatus = targetLastTestStatus(item);
        if (testStatus === "success" || testStatus === "ok") {
            return {
                title: "当前群组已通过校验",
                detail: "模板、账号和群组可达性都已具备，可以继续启用并等待跟单任务进入执行链路。",
                href: "/autobet/subscriptions#subscriptionsSection",
                text: "去设置跟单",
            };
        }
        const reasonLabel = targetErrorCodeLabel(item && item.last_test_error_code);
        return {
            title: reasonLabel ? ("待修复：" + reasonLabel) : "待修复：先完成测试发送",
            detail: String(item && item.last_test_message || "").trim() || "当前还没有成功的测试发送记录，建议先修复群组可达性后再启用。",
            href: "/autobet/targets#targetsSection",
            text: "去检查群组",
        };
    }

    function sampleSignalForTemplate(template) {
        const config = template && template.config && typeof template.config === "object" ? template.config : {};
        const betRules = config.bet_rules && typeof config.bet_rules === "object" ? config.bet_rules : {};
        const ruleKeys = Object.keys(betRules).filter(function (key) {
            return key !== "*";
        });
        const preferredBetType = ruleKeys[0] || "big_small";
        const samples = {
            big_small: {bet_value: "大"},
            combo: {bet_value: "大单"},
            extreme: {bet_value: "极大"},
            special: {bet_value: "豹子"},
            number_sum: {bet_value: "0"},
            abc: {bet_value: "大", normalized_payload: {position: "A"}},
            edge: {bet_value: "边"},
        };
        const fallback = samples[preferredBetType] || {bet_value: "大"};
        const payload = Object.assign({stake_amount: 10}, fallback.normalized_payload || {});
        return {
            lottery_type: String(template && template.lottery_type || "pc28").trim() || "pc28",
            issue_no: "20260410001",
            bet_type: preferredBetType,
            bet_value: String(fallback.bet_value || "大"),
            normalized_payload: payload,
        };
    }

    function subscriptionTargetDiagnostics(item) {
        const account = accountById(item && item.telegram_account_id);
        const template = templateById(item && item.template_id);
        const diagnostics = [];
        if (!account) {
            diagnostics.push({
                key: "account_missing",
                label: "未绑定账号",
                severity: "blocked",
                href: "/autobet/targets#targetsSection",
            });
        } else if (!isAccountAuthorized(account)) {
            diagnostics.push({
                key: "account_unauthorized",
                label: "账号未授权",
                severity: "blocked",
                href: "/autobet/accounts#accountsSection",
            });
        }
        if (!template) {
            diagnostics.push({
                key: "template_missing",
                label: "未绑定模板",
                severity: "warning",
                href: "/autobet/templates#templatesSection",
            });
        } else if (String(template.status || "active") !== "active") {
            diagnostics.push({
                key: "template_inactive",
                label: "模板未启用",
                severity: "warning",
                href: "/autobet/templates#templatesSection",
            });
        }
        if (!isTargetWizardVerified(item)) {
            diagnostics.push({
                key: "target_not_tested",
                label: "群组未测试",
                severity: "blocked",
                href: "/autobet/targets#targetsSection",
            });
        }
        if (String(item && item.status || "inactive") !== "active") {
            diagnostics.push({
                key: "target_inactive",
                label: "群组未启用",
                severity: "warning",
                href: "/autobet/targets#targetsSection",
            });
        }
        return diagnostics;
    }

    function summarizeDiagnostics(entries) {
        const order = [
            "subscription_inactive",
            "target_missing",
            "target_inactive",
            "account_missing",
            "account_unauthorized",
            "target_not_tested",
            "template_missing",
            "template_inactive",
        ];
        const counter = {};
        entries.forEach(function (entry) {
            if (!entry || !entry.key) {
                return;
            }
            const current = counter[entry.key] || {
                key: entry.key,
                label: entry.label || entry.key,
                href: entry.href || "/autobet/subscriptions#subscriptionsSection",
                severity: entry.severity || "warning",
                count: 0,
            };
            current.count += 1;
            counter[entry.key] = current;
        });
        return Object.keys(counter).sort(function (left, right) {
            const leftIndex = order.indexOf(left);
            const rightIndex = order.indexOf(right);
            const normalizedLeft = leftIndex >= 0 ? leftIndex : 99;
            const normalizedRight = rightIndex >= 0 ? rightIndex : 99;
            if (normalizedLeft !== normalizedRight) {
                return normalizedLeft - normalizedRight;
            }
            return String(counter[left].label).localeCompare(String(counter[right].label), "zh-CN");
        }).map(function (key) {
            return counter[key];
        });
    }

    function subscriptionTargetsState(item) {
        const selectedTargetIds = subscriptionDispatchTargetIds(item);
        const selectedTargetSet = selectedTargetIds.reduce(function (result, targetId) {
            result[String(targetId)] = true;
            return result;
        }, {});
        const configuredTargets = visibleTargets().filter(function (targetItem) {
            return !selectedTargetIds.length || selectedTargetSet[String(targetItem.id)];
        });
        const activeTargetItems = activeTargets().filter(function (targetItem) {
            return !selectedTargetIds.length || selectedTargetSet[String(targetItem.id)];
        });
        const effectiveTargets = String(item && item.status || "inactive") === "active" ? activeTargetItems : [];
        const previewTargets = effectiveTargets.length ? effectiveTargets : (activeTargetItems.length ? activeTargetItems : configuredTargets);
        const diagnostics = [];
        const subscriptionStatus = String(item && item.status || "inactive");
        const invalidSelectedCount = selectedTargetIds.filter(function (targetId) {
            const targetItem = targetById(targetId);
            return !targetItem || isArchivedItem(targetItem);
        }).length;
        if (subscriptionStatus === "standby") {
            diagnostics.push({
                key: "subscription_standby",
                label: "待自动触发",
                severity: "warning",
                href: "/auto-triggers",
            });
        } else if (item && subscriptionStatus !== "active") {
            diagnostics.push({
                key: "subscription_inactive",
                label: "策略未启用",
                severity: "blocked",
                href: "/autobet/subscriptions#subscriptionsSection",
            });
        }
        if (!configuredTargets.length) {
            diagnostics.push({
                key: "target_missing",
                label: selectedTargetIds.length ? "指定群组不可用" : "未指定群组",
                severity: "blocked",
                href: "/autobet/targets#targetsSection",
            });
        } else if (!activeTargetItems.length) {
            diagnostics.push({
                key: "target_inactive",
                label: "没有激活群组",
                severity: "blocked",
                href: "/autobet/targets#targetsSection",
            });
        }
        if (invalidSelectedCount > 0) {
            diagnostics.push({
                key: "target_selection_invalid",
                label: "指定群组已失效",
                severity: "blocked",
                href: "/autobet/subscriptions#subscriptionsSection",
            });
        }
        configuredTargets.forEach(function (targetItem) {
            subscriptionTargetDiagnostics(targetItem).forEach(function (entry) {
                diagnostics.push(entry);
            });
        });
        return {
            configuredTargets: configuredTargets,
            activeTargets: activeTargetItems,
            effectiveTargets: effectiveTargets,
            previewTargets: previewTargets,
            summaries: summarizeDiagnostics(diagnostics),
        };
    }

    function subscriptionPreviewAmount(subscription, signal) {
        const strategy = subscription && subscription.strategy && typeof subscription.strategy === "object"
            ? subscription.strategy
            : {};
        const payload = signal && signal.normalized_payload && typeof signal.normalized_payload === "object"
            ? signal.normalized_payload
            : {};
        const mode = String(strategy.mode || "follow").trim() || "follow";
        const currentStep = Math.max(1, Number(subscription && subscription.progression && subscription.progression.current_step || 1));
        const baseStake = Number(
            strategy.base_stake != null && String(strategy.base_stake).trim() !== ""
                ? strategy.base_stake
                : (strategy.stake_amount != null && String(strategy.stake_amount).trim() !== ""
                    ? strategy.stake_amount
                    : ((payload.base_stake != null && String(payload.base_stake).trim() !== ""
                        ? payload.base_stake
                        : payload.stake_amount) || 10))
        );
        if (mode === "martingale") {
            const multiplier = Number(
                strategy.multiplier != null && String(strategy.multiplier).trim() !== ""
                    ? strategy.multiplier
                    : (payload.multiplier || 2)
            );
            return Math.round(baseStake * (multiplier ** Math.max(0, currentStep - 1)) * 100) / 100;
        }
        if (strategy.stake_amount != null && String(strategy.stake_amount).trim() !== "") {
            return Number(strategy.stake_amount);
        }
        if (payload.stake_amount != null && String(payload.stake_amount).trim() !== "") {
            return Number(payload.stake_amount);
        }
        return baseStake;
    }

    function subscriptionPreviewForTarget(subscription, targetItem) {
        const template = templateById(targetItem && targetItem.template_id);
        const account = accountById(targetItem && targetItem.telegram_account_id);
        let signal = sampleSignalForTemplate(template);
        const filter = (subscription.strategy_v2 || {}).play_filter || (subscription.strategy || {}).bet_filter || {};
        const selected = filter.mode === "selected" && (filter.selected_keys || []).find(function (key) { return key.startsWith("double_group:") || key.startsWith("kill_group:"); });
        if (selected) {
            signal = Object.assign({}, signal, {bet_type: selected.split(":")[0], bet_value: selected.split(":")[1], normalized_payload: {}});
        }
        const sampleAmount = subscriptionPreviewAmount(subscription, signal);
        return {
            signal: signal,
            amount: sampleAmount * (signalGroupCombos(signal).length || 1),
            currentStep: Math.max(1, Number(subscription && subscription.progression && subscription.progression.current_step || 1)),
            strategyMode: summarizeStrategyMode(subscription && subscription.strategy),
            account: account,
            template: template,
            rendered: templatePreviewResult(signal, sampleAmount, template),
            diagnostics: subscriptionTargetDiagnostics(targetItem),
        };
    }

    function renderSubscriptionWorkspaceSummary() {
        if (!(subscriptionWorkspaceSummary instanceof HTMLElement)) {
            return;
        }
        const subscriptions = visibleSubscriptions();
        const chainState = subscriptionTargetsState(primarySubscription());
        const configuredTargets = chainState.configuredTargets;
        const activeTargetItems = chainState.activeTargets;
        const templateBoundCount = configuredTargets.filter(function (item) {
            return Boolean(templateById(item.template_id));
        }).length;
        const blockerCount = chainState.summaries.reduce(function (total, item) {
            return total + Number(item.count || 0);
        }, 0);
        const dailySummary = state.subscriptionDailySummary && typeof state.subscriptionDailySummary === "object"
            ? state.subscriptionDailySummary
            : null;
        const statDate = state.subscriptionStatDate || (dailySummary && dailySummary.stat_date) || "--";

        subscriptionWorkspaceSummary.innerHTML = [
            '<article class="health-card"><span class="setup-label">跟单策略数</span><strong>' + escapeHtml(String(subscriptions.length)) + '</strong><p>当前账号下未归档的跟单数量。</p></article>',
            '<article class="health-card"><span class="setup-label">会收到信号的群组</span><strong>' + escapeHtml(String(activeTargetItems.length)) + '</strong><p>只有启用且可用的群组才会真正发单。</p></article>',
            '<article class="health-card"><span class="setup-label">已绑定模板的群组</span><strong>' + escapeHtml(String(templateBoundCount)) + '</strong><p>这些群组已经选好自己的发单格式。</p></article>',
            '<article class="health-card"><span class="setup-label">当前问题数</span><strong>' + escapeHtml(String(blockerCount)) + '</strong><p>这里统计会挡住发单的授权、模板或群组问题。</p></article>',
            '<article class="health-card"><span class="setup-label">当日净盈亏</span><strong>' + escapeHtml(signedAmountText(dailySummary && dailySummary.net_profit || 0)) + '</strong><p>当前查看 ' + escapeHtml(String(statDate)) + ' 的已结算净盈亏。</p></article>',
            '<article class="health-card"><span class="setup-label">当日已结算</span><strong>' + escapeHtml(String(dailySummary && dailySummary.settled_event_count || 0)) + '</strong><p>命中 ' + escapeHtml(String(dailySummary && dailySummary.hit_count || 0)) + ' / 未中 ' + escapeHtml(String(dailySummary && dailySummary.miss_count || 0)) + ' / 回本 ' + escapeHtml(String(dailySummary && dailySummary.refund_count || 0)) + '。</p></article>',
        ].join("");

        if (!(subscriptionWorkspaceSummaryText instanceof HTMLElement)) {
            return;
        }
        if (!subscriptions.length) {
            subscriptionWorkspaceSummaryText.textContent = "先建一条跟单策略。建好后，这里会显示这条来源最终会发到哪些群。";
            return;
        }
        if (!configuredTargets.length) {
            subscriptionWorkspaceSummaryText.textContent = "已建跟单，但还没配置投递群组，所以当前不会发单。";
            return;
        }
        if (!activeTargetItems.length) {
            subscriptionWorkspaceSummaryText.textContent = "群组已配置，但都没启用或测试未通过，所以当前不会发单。";
            return;
        }
        if (chainState.summaries.length) {
            subscriptionWorkspaceSummaryText.textContent = "当前主要问题：" + chainState.summaries.map(function (item) {
                return item.label + " " + item.count + " 个";
            }).join("；") + "。";
            return;
        }
        subscriptionWorkspaceSummaryText.textContent = "当前链路正常：来源信号会发到启用中的群组，并套用各群组自己的模板。当前查看 " + String(statDate) + " 的日盈亏。";
    }

    function boundTargetsForTemplate(template) {
        if (!template) {
            return [];
        }
        if (Array.isArray(template.bound_targets)) {
            return template.bound_targets.slice();
        }
        return state.targets.filter(function (item) {
            return Number(item.template_id) === Number(template.id);
        }).map(function (item) {
            return {
                id: item.id,
                target_name: item.target_name,
                target_key: item.target_key,
                status: item.status,
            };
        });
    }

    function templateRuleCount(template) {
        const config = template && template.config && typeof template.config === "object" ? template.config : {};
        const betRules = config.bet_rules && typeof config.bet_rules === "object" ? config.bet_rules : {};
        return Object.keys(betRules).length;
    }

    function targetSubscriptionRefs(item) {
        if (item && Array.isArray(item.subscription_refs)) {
            return item.subscription_refs.slice();
        }
        return visibleSubscriptions().map(function (subscription) {
            const source = sourceById(subscription.source_id);
            return {
                id: subscription.id,
                source_id: subscription.source_id,
                source_name: source ? source.name : ("#" + subscription.source_id),
                status: subscription.status,
            };
        });
    }

    function targetFailureDigest(item) {
        if (item && item.recent_failure_summary && typeof item.recent_failure_summary === "object") {
            return item.recent_failure_summary;
        }
        const targetFailures = state.failures.filter(function (failure) {
            return Number(failure.delivery_target_id) === Number(item && item.id);
        });
        const summary = {
            count: targetFailures.length,
            top_reason: "",
            details: [],
            last_failure_at: targetFailures[0] ? targetFailures[0].executed_at : "",
        };
        if (!targetFailures.length) {
            return summary;
        }
        const counter = {};
        targetFailures.forEach(function (failure) {
            const reason = normalizeFailureReason(failure.error_message);
            counter[reason] = (counter[reason] || 0) + 1;
        });
        const ordered = Object.keys(counter).sort(function (left, right) {
            return counter[right] - counter[left];
        });
        summary.top_reason = ordered[0] || "";
        summary.details = ordered.slice(0, 3).map(function (reason) {
            return reason + " " + counter[reason] + " 次";
        });
        return summary;
    }

    function targetRecentActivities(item) {
        return state.jobs.filter(function (job) {
            return Number(job.delivery_target_id) === Number(item && item.id);
        }).slice(0, 3);
    }

    function activeTemplateCount() {
        return state.templates.filter(function (item) {
            return !isArchivedItem(item) && String(item.status || "") === "active";
        }).length;
    }

    function renderTemplateWorkspaceSummary() {
        if (!(templateWorkspaceSummary instanceof HTMLElement)) {
            return;
        }
        const items = state.templates.filter(function (item) {
            return !isArchivedItem(item);
        });
        const draftCount = items.filter(function (item) {
            return String(item.status || "inactive") === "inactive";
        }).length;
        const activeCount = items.filter(function (item) {
            return String(item.status || "") === "active";
        }).length;
        const usedCount = items.filter(function (item) {
            return boundTargetsForTemplate(item).length > 0;
        }).length;
        const boundTargetCount = visibleTargets().filter(function (item) {
            return Boolean(item.template_id);
        }).length;

        templateWorkspaceSummary.innerHTML = [
            '<article class="health-card"><span class="setup-label">模板总数</span><strong>' + escapeHtml(String(items.length)) + '</strong><p>当前未归档模板数量。</p></article>',
            '<article class="health-card"><span class="setup-label">启用中</span><strong>' + escapeHtml(String(activeCount)) + '</strong><p>处于启用状态、可直接参与渲染的模板数量。</p></article>',
            '<article class="health-card"><span class="setup-label">草稿中</span><strong>' + escapeHtml(String(draftCount)) + '</strong><p>尚未正式启用、适合继续调试的模板数量。</p></article>',
            '<article class="health-card"><span class="setup-label">已绑定群组</span><strong>' + escapeHtml(String(boundTargetCount)) + '</strong><p>当前有模板绑定关系的投递群组数量。</p></article>',
        ].join("");

        if (!(templateWorkspaceSummaryText instanceof HTMLElement)) {
            return;
        }
        if (!items.length) {
            templateWorkspaceSummaryText.textContent = "先从预设开始创建模板，避免每次都从空白 JSON 起步。";
            return;
        }
        if (!usedCount) {
            templateWorkspaceSummaryText.textContent = "模板已经存在，但还没有群组在使用它；下一步去群组工作区绑定模板。";
            return;
        }
        templateWorkspaceSummaryText.textContent = "当前有 " + usedCount + " 套模板已进入实际群组链路，其余模板可继续作为草稿调试。";
    }

    function renderAccountWorkspaceSummary() {
        if (!(accountWorkspaceSummary instanceof HTMLElement)) {
            return;
        }
        const items = visibleAccounts();
        const authorizedCount = items.filter(isAccountAuthorized).length;
        const pendingCount = items.filter(function (item) {
            return !isAccountAuthorized(item);
        }).length;
        const linkedTargetCount = items.reduce(function (total, item) {
            return total + accountLinkedTargets(item).length;
        }, 0);

        accountWorkspaceSummary.innerHTML = [
            '<article class="health-card"><span class="setup-label">账号总数</span><strong>' + escapeHtml(String(items.length)) + '</strong><p>当前未归档托管账号数量。</p></article>',
            '<article class="health-card"><span class="setup-label">已授权</span><strong>' + escapeHtml(String(authorizedCount)) + '</strong><p>已可直接发送消息的账号数量。</p></article>',
            '<article class="health-card"><span class="setup-label">待处理</span><strong>' + escapeHtml(String(pendingCount)) + '</strong><p>仍需验证码、二次密码或 Session 导入的账号数量。</p></article>',
            '<article class="health-card"><span class="setup-label">已绑群组</span><strong>' + escapeHtml(String(linkedTargetCount)) + '</strong><p>当前账号链路已经承接的投递群组数量。</p></article>',
        ].join("");

        if (!(accountWorkspaceSummaryText instanceof HTMLElement)) {
            return;
        }
        if (!items.length) {
            accountWorkspaceSummaryText.textContent = "先创建第一个托管账号，后续群组测试和自动发送才有执行主体。";
            return;
        }
        if (!authorizedCount) {
            accountWorkspaceSummaryText.textContent = "当前账号已录入，但还没有真正可执行的账号，优先完成授权。";
            return;
        }
        if (pendingCount) {
            accountWorkspaceSummaryText.textContent = "当前已有可执行账号，但仍有 " + pendingCount + " 个账号待继续授权或导入。";
            return;
        }
        accountWorkspaceSummaryText.textContent = "当前账号接入链路已稳定，可继续去群组工作区完成绑定与测试。";
    }

    function renderTargetWorkspaceSummary() {
        const root = document.getElementById("targetWorkspaceSummary");
        const summaryText = document.getElementById("targetWorkspaceSummaryText");
        if (!(root instanceof HTMLElement)) {
            return;
        }
        const items = visibleTargets();
        const archivedCount = archivedTargets().length;
        const verifiedCount = items.filter(isTargetWizardVerified).length;
        const templateBoundCount = items.filter(function (item) {
            return Boolean(templateById(item.template_id));
        }).length;
        const repairCount = items.filter(function (item) {
            return targetRepairState(item).title.indexOf("待修复") === 0 || targetRepairState(item).title.indexOf("还没有") === 0 || targetRepairState(item).title.indexOf("绑定账号尚未授权") === 0;
        }).length;
        const activeSubscriptionCount = items.reduce(function (total, item) {
            return total + Number(item.active_matched_subscription_count || 0);
        }, 0);
        const failureCount = items.filter(function (item) {
            const failure = targetFailureDigest(item);
            return Number(failure.count || 0) > 0;
        }).length;
        if (!items.length) {
            root.innerHTML = '<article class="health-card"><span class="setup-label">当前状态</span><strong>还没有可用群组</strong><p>' + (archivedCount ? ('已归档 ' + escapeHtml(String(archivedCount)) + ' 个旧群组，不会参与后续发送。') : '先新增一个目标群组，再继续测试发送和模板绑定。') + '</p></article>';
            if (summaryText instanceof HTMLElement) {
                summaryText.textContent = archivedCount
                    ? "当前没有可用投递群组；已归档群组默认隐藏，需要时可展开查看。"
                    : "当前还没有投递群组，建议先创建第一个群组。";
            }
            return;
        }
        root.innerHTML = [
            '<article class="health-card"><span class="setup-label">群组总数</span><strong>' + escapeHtml(String(items.length)) + '</strong><p>当前用户已配置的投递群组数量。</p></article>',
            '<article class="health-card"><span class="setup-label">已验证可达</span><strong>' + escapeHtml(String(verifiedCount)) + '</strong><p>最近测试发送成功，或已有执行活动的群组数量。</p></article>',
            '<article class="health-card"><span class="setup-label">已绑定模板</span><strong>' + escapeHtml(String(templateBoundCount)) + '</strong><p>已明确绑定下注格式模板的群组数量。</p></article>',
            '<article class="health-card"><span class="setup-label">订阅命中</span><strong>' + escapeHtml(String(activeSubscriptionCount)) + '</strong><p>当前激活中的跟单策略会同时命中的群组引用总数。</p></article>',
            '<article class="health-card"><span class="setup-label">有失败历史</span><strong>' + escapeHtml(String(failureCount)) + '</strong><p>最近执行反馈里出现失败的群组数量。</p></article>',
            '<article class="health-card"><span class="setup-label">待修复</span><strong>' + escapeHtml(String(repairCount)) + '</strong><p>还需要处理账号、模板或测试发送问题的群组数量。</p></article>',
        ].join("");
        if (summaryText instanceof HTMLElement) {
            summaryText.textContent = "这里既看群组可达性，也能反查有哪些跟单策略会命中这个群组。";
        }
    }

    function normalizeFailureReason(errorMessage) {
        const text = String(errorMessage || "").trim().toLowerCase();
        if (!text) {
            return "执行返回异常";
        }
        if (text.indexOf("timeout") >= 0 || text.indexOf("timed out") >= 0) {
            return "网络超时";
        }
        if (text.indexOf("session expired") >= 0 || text.indexOf("session invalid") >= 0) {
            return "会话失效";
        }
        if (text.indexOf("retry exhausted") >= 0) {
            return "自动重试耗尽";
        }
        if (text.indexOf("offline") >= 0) {
            return "执行器离线";
        }
        if (text.indexOf("network") >= 0) {
            return "网络异常";
        }
        return String(errorMessage || "").trim();
    }

    function renderLoadedScope(scope) {
        const normalized = String(scope || "workbench").trim() || "workbench";
        if (normalized === "sources") {
            safeRender("source-cards", renderSourceCards);
            safeRender("source-list", renderSourceList);
            return;
        }
        if (normalized === "accounts") {
            safeRender("account-cards", renderAccountCards);
            return;
        }
        if (normalized === "templates") {
            safeRender("template-cards", renderTemplateCards);
            return;
        }
        if (normalized === "targets") {
            safeRender("target-summary", renderTargetWorkspaceSummary);
            safeRender("target-cards", renderTargetCards);
            return;
        }
        if (normalized === "subscriptions") {
            safeRender("subscription-cards", renderSubscriptionCards);
            return;
        }
    }

    function renderSelectOptions(selectEl, items, config) {
        if (!(selectEl instanceof HTMLSelectElement)) {
            return;
        }
        const previousValue = String(selectEl.value || "");
        const placeholder = config && config.placeholder ? config.placeholder : "请选择";
        const options = ['<option value="">' + escapeHtml(placeholder) + "</option>"].concat(items.map(function (item) {
            return '<option value="' + escapeHtml(config.value(item)) + '">' + escapeHtml(config.label(item)) + "</option>";
        }));
        selectEl.innerHTML = options.join("");
        const shouldRestore = items.some(function (item) {
            return String(config.value(item)) === previousValue;
        });
        if (shouldRestore) {
            selectEl.value = previousValue;
        } else if (items.length === 1) {
            selectEl.value = String(config.value(items[0]));
        }
    }

    function refreshSourceSelects() {
        if (!(subscriptionForm instanceof HTMLFormElement)) return;
        const items = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        renderSelectOptions(subscriptionForm.elements.source_id, items, {
            placeholder: items.length ? "请选择要跟单的来源" : "请先导入一个来源",
            value: function (item) { return String(item.id); },
            label: function (item) { return "#" + item.id + " · " + item.name; },
        });
    }

    function setSubscriptionTargetSelection(targetIds) {
        if (!(subscriptionTargetSelect instanceof HTMLSelectElement)) {
            return;
        }
        const selected = (Array.isArray(targetIds) ? targetIds : []).reduce(function (result, targetId) {
            result[String(targetId)] = true;
            return result;
        }, {});
        Array.from(subscriptionTargetSelect.options).forEach(function (option) {
            option.selected = Boolean(selected[String(option.value)]);
        });
    }

    function selectedSubscriptionTargetIds() {
        if (!(subscriptionTargetSelect instanceof HTMLSelectElement)) {
            return [];
        }
        return Array.from(subscriptionTargetSelect.selectedOptions).map(function (option) {
            return Number(option.value);
        }).filter(function (value) {
            return Number.isInteger(value) && value > 0;
        });
    }

    function syncSubscriptionTargetHint() {
        if (!(subscriptionTargetHint instanceof HTMLElement)) {
            return;
        }
        const selectedIds = selectedSubscriptionTargetIds();
        const activeSelectedCount = selectedIds.filter(function (targetId) {
            const item = targetById(targetId);
            return item && String(item.status || "") === "active";
        }).length;
        if (!visibleTargets().length) {
            subscriptionTargetHint.textContent = "还没有可选群组，请先到群组页新增投递群组。";
            return;
        }
        if (!selectedIds.length) {
            subscriptionTargetHint.textContent = "必须至少选择一个群组；只会向这里选中的群组发单。";
            return;
        }
        subscriptionTargetHint.textContent = "已选择 " + selectedIds.length + " 个群组，其中 " + activeSelectedCount + " 个已启用；停用群组不会收到新任务。";
    }

    function refreshSubscriptionTargetSelects() {
        if (!(subscriptionTargetSelect instanceof HTMLSelectElement)) {
            return;
        }
        const selectedValues = selectedSubscriptionTargetIds().map(function (value) {
            return String(value);
        });
        const selectedSet = selectedValues.reduce(function (result, value) {
            result[value] = true;
            return result;
        }, {});
        const items = visibleTargets().slice();
        selectedValues.forEach(function (value) {
            const item = targetById(value);
            if (item && items.some(function (entry) { return Number(entry.id) === Number(item.id); }) === false) {
                items.push(item);
            }
        });
        subscriptionTargetSelect.innerHTML = items.map(function (item) {
            const account = accountById(item.telegram_account_id);
            const label = "#" + item.id + " · " + (item.target_name || item.target_key || "未命名群组") + " · " + statusText(item.status) + (account ? (" · " + account.label) : "");
            return '<option value="' + escapeHtml(item.id) + '"' + (selectedSet[String(item.id)] ? " selected" : "") + ">" + escapeHtml(label) + "</option>";
        }).join("");
        if (!selectedValues.length && items.length === 1) {
            subscriptionTargetSelect.options[0].selected = true;
        }
        syncSubscriptionTargetHint();
    }

    function refreshAccountSelects() {
        if (!(targetForm instanceof HTMLFormElement)) return;
        const items = availableAccountsForTargets().slice();
        const currentValue = String(targetForm.elements.telegram_account_id.value || "").trim();
        const currentAccount = accountById(currentValue);
        if (currentAccount && items.some(function (item) { return Number(item.id) === Number(currentAccount.id); }) === false) {
            items.push(currentAccount);
        }
        renderSelectOptions(targetForm.elements.telegram_account_id, items, {
            placeholder: items.length ? "请选择已授权账号" : "请先完成账号授权",
            value: function (item) { return String(item.id); },
            label: function (item) {
                return "#" + item.id + " · " + item.label + " · " + accountAuthLabel(item) + (isArchivedItem(item) ? " · 已归档" : "");
            },
        });
    }

    function refreshTemplateSelects() {
        if (!(targetForm instanceof HTMLFormElement)) {
            return;
        }
        const currentValue = String(targetForm.elements.template_id.value || "").trim();
        const items = state.templates.filter(function (item) {
            return !isArchivedItem(item);
        }).slice();
        const currentTemplate = templateById(currentValue);
        if (currentTemplate && items.some(function (item) { return Number(item.id) === Number(currentTemplate.id); }) === false) {
            items.push(currentTemplate);
        }
        renderSelectOptions(targetForm.elements.template_id, items, {
            placeholder: items.length ? "默认格式（未绑定模板）" : "默认格式（暂未创建模板）",
            value: function (item) { return String(item.id); },
            label: function (item) {
                return "#" + item.id + " · " + item.name + " · " + templateStatusLabel(item.status);
            },
        });
    }

    function summarizeTemplate(item) {
        if (!item) {
            return "未绑定模板";
        }
        const ruleCount = templateRuleCount(item);
        return (item.lottery_type || "--") + " · 默认 " + (item.template_text || "--") + (ruleCount ? (" · " + ruleCount + " 条玩法规则") : "");
    }

    function renderConfigSummaryItem(label, value, extraClassName) {
        const normalizedValue = String(value == null ? "--" : value).trim() || "--";
        const className = extraClassName ? (' class="' + extraClassName + '"') : "";
        return '<div class="config-summary-item"><span class="config-summary-label">' + escapeHtml(label || "--") + '</span><strong' + className + '>' + escapeHtml(normalizedValue) + "</strong></div>";
    }

    function renderConfigSummaryGrid(items) {
        return '<div class="config-summary-grid">' + (items || []).join("") + "</div>";
    }

    function renderConfigDetailRows(items) {
        return '<div class="config-detail-list">' + (items || []).join("") + "</div>";
    }

    function renderConfigDetailRow(label, value, valueClassName) {
        const normalizedValue = String(value == null ? "--" : value).trim() || "--";
        const className = valueClassName ? (' class="' + valueClassName + '"') : "";
        return '<div class="config-detail-row"><span class="config-detail-label">' + escapeHtml(label || "--") + '</span><span' + className + '>' + escapeHtml(normalizedValue) + "</span></div>";
    }

    function renderConfigCardShell(options) {
        const headerBadges = Array.isArray(options.headerBadges) ? options.headerBadges.join("") : "";
        const summaryMarkup = options.summaryMarkup ? ('<div class="config-card-summary">' + options.summaryMarkup + '</div>') : "";
        const detailMarkup = options.detailMarkup ? ('<div class="config-card-details">' + options.detailMarkup + '</div>') : "";
        const footerMarkup = options.footerMarkup ? ('<div class="config-card-footer">' + options.footerMarkup + '</div>') : "";
        return [
            '<article class="config-list-item' + (options.cardClassName ? (' ' + options.cardClassName) : '') + '">',
            '<div class="config-list-head">',
            '<div class="config-card-title-group">',
            '<strong>' + escapeHtml(options.title || "--") + '</strong>',
            (options.subtitle ? ('<p class="config-card-subtitle">' + escapeHtml(options.subtitle) + '</p>') : ""),
            '</div>',
            (headerBadges ? ('<div class="config-card-badges">' + headerBadges + '</div>') : ""),
            '</div>',
            summaryMarkup,
            detailMarkup,
            footerMarkup,
            '</article>',
        ].join("");
    }

    function renderTemplateCards() {
        if (!(messageTemplateCards instanceof HTMLElement)) {
            return;
        }
        if (!state.templates.length) {
            messageTemplateCards.innerHTML = '<article class="config-list-item config-list-empty-card"><strong>当前没有下注模板</strong><p>先创建一个模板，再把模板绑定到对应群组，让不同玩法按群协议生成下注文本。</p></article>';
            renderTemplateWorkspaceSummary();
            return;
        }
        messageTemplateCards.innerHTML = state.templates.map(function (item) {
            const statusAction = entityStatusAction(item.status, "停用模板", "恢复模板");
            const archiveAction = isArchivedItem(item)
                ? '<button class="ghost-btn toggle-template-btn" type="button" data-template-id="' + item.id + '" data-next-status="inactive">取消归档</button>'
                : '<button class="ghost-btn archive-template-btn" type="button" data-template-id="' + item.id + '">归档模板</button>';
            const boundTargets = boundTargetsForTemplate(item);
            const preview = templatePreviewResult(sampleSignalForTemplate(item), 10, Object.assign({}, item, {status: "active"}));
            const usageText = boundTargets.length
                ? boundTargets.map(function (target) {
                    return (target.target_name || target.target_key || "--") + "（" + templateStatusLabel(target.status) + "）";
                }).join("、")
                : "当前还没有群组绑定这套模板。";
            const activeTargetCount = item.active_target_count != null
                ? item.active_target_count
                : boundTargets.filter(function (target) { return String(target.status || "") === "active"; }).length;
            return renderConfigCardShell({
                title: item.name || "--",
                cardClassName: "template-card-item",
                headerBadges: [
                    renderStatusPillWithLabel(item.status, templateStatusLabel(item.status)),
                ],
                summaryMarkup: renderConfigSummaryGrid([
                    renderConfigSummaryItem("模板状态", templateStatusLabel(item.status)),
                    renderConfigSummaryItem("使用中群组", String(activeTargetCount)),
                    renderConfigSummaryItem("更新时间", formatDateTime(item.updated_at || item.created_at)),
                ]),
                detailMarkup: renderConfigDetailRows([
                    renderConfigDetailRow("模板摘要", summarizeTemplate(item)),
                    renderConfigDetailRow("绑定群组", usageText),
                    renderConfigDetailRow("默认口令", item.template_text || "--", "mono-text"),
                    renderConfigDetailRow("样例预览", preview.text || "--", "mono-text"),
                ]),
                footerMarkup: '<div class="config-list-actions"><button class="ghost-btn edit-template-btn" type="button" data-template-id="' + item.id + '">编辑</button><button class="ghost-btn duplicate-template-btn" type="button" data-template-id="' + item.id + '">复制模板</button><button class="ghost-btn toggle-template-btn" type="button" data-template-id="' + item.id + '" data-next-status="' + statusAction.nextStatus + '">' + statusAction.actionText + "</button>" + archiveAction + "</div>",
            });
        }).join("");
        renderTemplateWorkspaceSummary();
    }

    function focusScopeSectionOnLoad() {
        const params = new URLSearchParams(window.location.search);
        const subscriptionId = params.get("subscription_id");
        if (currentAutobetScope() === "subscriptions" && subscriptionId) {
            const item = state.subscriptions.find(function (entry) { return String(entry.id) === subscriptionId; });
            if (item) {
                state.expandedSubscriptionId = subscriptionId;
                renderSubscriptionCards();
                const button = subscriptionCards.querySelector('[data-subscription-id="' + Number(item.id) + '"]');
                if (button) (button.closest("article") || button).scrollIntoView({block: "start"});
            }
        }
        const sourceId = params.get("source_id");
        if (currentAutobetScope() === "subscriptions" && !subscriptionId && sourceId && sourceById(sourceId)) {
            subscriptionForm.elements.source_id.value = sourceId;
            syncSubscriptionBetFilterUI();
            if (window.PlatformNavigation) window.PlatformNavigation.markClean(subscriptionForm);
        }
        if (window.location.hash) scrollToHashTarget(window.location.hash);
    }

    function renderSourceWorkspaceSummary() {
        if (!(sourceWorkspaceSummary instanceof HTMLElement)) {
            return;
        }
        const items = aiSources();
        const activeLinkedCount = items.filter(function (item) {
            return Boolean(activeSourceSubscription(item.id));
        }).length;
        const pendingRawCount = items.reduce(function (total, item) {
            return total + sourcePipelineState(item).pendingRawCount;
        }, 0);
        const undispatchedSignalCount = items.reduce(function (total, item) {
            return total + sourcePipelineState(item).undispatchedSignalCount;
        }, 0);
        const recentJobCount = items.reduce(function (total, item) {
            return total + sourcePipelineState(item).jobs.length;
        }, 0);

        sourceWorkspaceSummary.innerHTML = [
            '<article class="health-card"><span class="setup-label">来源总数</span><strong>' + escapeHtml(String(items.length)) + '</strong><p>当前已导入的 AI 方案来源数量。</p></article>',
            '<article class="health-card"><span class="setup-label">已接入跟单</span><strong>' + escapeHtml(String(activeLinkedCount)) + '</strong><p>当前有激活跟单策略实际在使用的来源数量。</p></article>',
            '<article class="health-card"><span class="setup-label">待标准化</span><strong>' + escapeHtml(String(pendingRawCount)) + '</strong><p>已经抓到 raw item 但还没进入标准信号的数量。</p></article>',
            '<article class="health-card"><span class="setup-label">待派发信号</span><strong>' + escapeHtml(String(undispatchedSignalCount)) + '</strong><p>已生成信号但还没展开成执行任务的数量。</p></article>',
            '<article class="health-card"><span class="setup-label">历史任务</span><strong>' + escapeHtml(String(recentJobCount)) + '</strong><p>当前来源们累计已经进入执行任务的记录数。</p></article>',
        ].join("");

        if (!(sourceWorkspaceSummaryText instanceof HTMLElement)) {
            return;
        }
        if (!items.length) {
            sourceWorkspaceSummaryText.textContent = "先导入一个来源，后续的抓取、标准化和派发入口才有意义。";
            return;
        }
        if (!activeLinkedCount) {
            sourceWorkspaceSummaryText.textContent = "来源已经导入，但还没有任何激活跟单策略在使用它们。先把某个来源接到跟单策略。";
            return;
        }
        if (pendingRawCount > 0 || undispatchedSignalCount > 0) {
            sourceWorkspaceSummaryText.textContent = "当前至少有一个来源卡在抓取后的中间阶段。可以直接在下方来源卡片点“一键推进来源链路”。";
            return;
        }
        sourceWorkspaceSummaryText.textContent = "当前来源链路已经基本闭合。后续主要看最近抓取是否持续产生 signal 和 execution job。";
    }

    function renderSourceCards() {
        if (!(sourceCards instanceof HTMLElement)) {
            return;
        }
        const items = aiSources();
        if (!items.length) {
            sourceCards.innerHTML = '<article class="config-list-item config-list-empty-card"><strong>还没有导入 AI 方案</strong><p>在上方导入来源，再配置跟单方案。</p></article>';
            return;
        }
        sourceCards.innerHTML = items.map(function (item) {
            const fetchConfig = item.config && item.config.fetch ? item.config.fetch : {};
            const pipeline = sourcePipelineState(item);
            return renderConfigCardShell({
                title: item.name || "--",
                cardClassName: "source-card-item source-card-item-compact",
                summaryMarkup: renderConfigSummaryGrid([
                    renderConfigSummaryItem("来源类型", item.source_type || "--"),
                    renderConfigSummaryItem("可见性", item.visibility || "--"),
                ]),
                detailMarkup: renderConfigDetailRows([
                    renderConfigDetailRow("当前状态", pipeline.headline),
                    renderConfigDetailRow("抓取地址", fetchConfig.url || "--", "mono-text"),
                ]),
            });
        }).join("");
    }

    function renderSourceList() {
        if (!(sourceList instanceof HTMLElement)) {
            return;
        }
        const items = aiSources();
        if (!items.length) {
            sourceList.innerHTML = '<article class="config-list-item config-list-empty-card"><strong>当前还没有来源链路</strong><p>在上方导入来源，再选择用于跟单的方案。</p></article>';
            renderSourceWorkspaceSummary();
            return;
        }
        sourceList.innerHTML = items.map(function (item) {
            const fetchConfig = item.config && item.config.fetch ? item.config.fetch : {};
            const pipeline = sourcePipelineState(item);
            const activeSubscription = pipeline.activeSubscription;
            const subscription = pipeline.subscription;
            const statusAction = entityStatusAction(item.status, "停用来源", "启用来源");
            const archiveAction = isArchivedItem(item)
                ? '<button class="ghost-btn danger-btn delete-source-btn" type="button" data-source-id="' + item.id + '">删除来源</button>'
                : '<button class="ghost-btn archive-source-btn" type="button" data-source-id="' + item.id + '">归档来源</button>';
            const latestRawText = pipeline.latestRawItem
                ? ("#" + pipeline.latestRawItem.id + " · " + formatDateTime(pipeline.latestRawItem.created_at || pipeline.latestRawItem.published_at))
                : "还没有抓取记录";
            const latestSignalText = pipeline.latestSignal
                ? ("signal #" + pipeline.latestSignal.id + " · " + (pipeline.latestSignal.issue_no || "--") + " · " + formatDateTime(pipeline.latestSignal.published_at || pipeline.latestSignal.created_at))
                : "还没有标准信号";
            const latestJobText = pipeline.latestJob
                ? ("job #" + pipeline.latestJob.id + " · " + statusText(pipeline.latestJob.status || "pending") + " · " + formatDateTime(pipeline.latestJob.updated_at || pipeline.latestJob.created_at))
                : "还没有执行任务";
            const blockedSubscription = Boolean(subscription) && isSubscriptionRiskBlocked(subscription);
            const subscriptionText = activeSubscription
                ? ("当前激活策略 #" + activeSubscription.id + " 正在使用这个来源。")
                : (subscription
                    ? (blockedSubscription
                        ? ("这个来源已有策略 #" + subscription.id + "，但当前处于 " + subscriptionRuntimeLabel(subscription) + "。")
                        : ("这个来源已有策略 #" + subscription.id + "，但当前状态是 " + statusText(subscription.status || "inactive") + "。"))
                    : "当前还没有任何跟单策略会使用这个来源。");
            const pipelineButtonText = activeSubscription ? "一键推进来源链路" : "先接入跟单后再推进";
            return renderConfigCardShell({
                title: item.name || "--",
                cardClassName: "source-card-item",
                headerBadges: [
                    renderStatusPill(item.status || "--"),
                    activeSubscription
                        ? renderStatusPillWithLabel("active", "已接入跟单")
                        : renderStatusPillWithLabel("inactive", subscription ? subscriptionRuntimeLabel(subscription) : "未接入跟单"),
                ],
                summaryMarkup: renderConfigSummaryGrid([
                    renderConfigSummaryItem("跟单状态", activeSubscription ? "已接入跟单" : (subscription ? subscriptionRuntimeLabel(subscription) : "未接入跟单")),
                    renderConfigSummaryItem("raw", String(pipeline.rawItemCount)),
                    renderConfigSummaryItem("待标准化", String(pipeline.pendingRawCount)),
                    renderConfigSummaryItem("signals", String(pipeline.signalCount)),
                    renderConfigSummaryItem("jobs", String(pipeline.jobCount)),
                ]),
                detailMarkup: [
                    renderConfigDetailRows([
                        renderConfigDetailRow("来源说明", subscriptionText),
                        renderConfigDetailRow("抓取地址", fetchConfig.url || "--", "mono-text"),
                        renderConfigDetailRow("最近 raw", latestRawText),
                        renderConfigDetailRow("最近 signal", latestSignalText),
                        renderConfigDetailRow("最近任务", latestJobText),
                        renderConfigDetailRow("链路判断", pipeline.headline),
                    ]),
                ].join(""),
                footerMarkup: '<div class="config-list-actions source-card-actions">' +
                    '<button class="ghost-btn edit-source-btn" type="button" data-source-id="' + escapeHtml(String(item.id)) + '">编辑来源</button>' +
                    '<a class="ghost-btn link-btn source-subscription-link" href="/autobet/subscriptions?' + (subscription ? 'subscription_id=' + encodeURIComponent(subscription.id) : 'source_id=' + encodeURIComponent(item.id) + '#subscriptionForm') + '">' + escapeHtml(subscription ? "查看这个来源的跟单" : "用这个来源建跟单") + '</a>' +
                    '<button class="' + (activeSubscription ? 'primary-btn' : 'ghost-btn') + ' source-pipeline-btn" type="button" data-source-id="' + escapeHtml(String(item.id)) + '"' + (activeSubscription ? '' : ' disabled') + '>' + escapeHtml(pipelineButtonText) + '</button>' +
                    '<button class="ghost-btn toggle-source-btn" type="button" data-source-id="' + escapeHtml(String(item.id)) + '" data-next-status="' + escapeHtml(statusAction.nextStatus) + '">' + escapeHtml(statusAction.actionText) + '</button>' +
                    archiveAction +
                    '<a class="ghost-btn link-btn" href="/records' + (pipeline.latestSignal ? ('?signal_id=' + encodeURIComponent(String(pipeline.latestSignal.id))) : '') + '">' + escapeHtml(pipeline.latestSignal ? "看这个来源最近记录" : "去执行记录") + '</a>' +
                    '</div>',
            });
        }).join("");
        renderSourceWorkspaceSummary();
    }

    function renderAccountCards() {
        if (!(accountCards instanceof HTMLElement)) {
            return;
        }
        if (!state.accounts.length) {
            accountCards.innerHTML = '<article class="config-list-item config-list-empty-card"><strong>当前没有托管账号</strong><p>先选择接入方式，再完成登录或导入 Session，后续才能继续配置投递群组。</p></article>';
            renderAccountWorkspaceSummary();
            return;
        }
        accountCards.innerHTML = state.accounts.map(function (item) {
            const statusAction = entityStatusAction(item.status, "暂停账号", "恢复账号");
            const archiveAction = isArchivedItem(item)
                ? '<button class="ghost-btn danger-btn delete-account-btn" type="button" data-account-id="' + item.id + '">删除账号</button>'
                : '<button class="ghost-btn archive-account-btn" type="button" data-account-id="' + item.id + '">归档账号</button>';
            const authState = accountAuthState(item);
            const authMode = accountAuthMode(item);
            const deletion = accountDeletionSummary(item);
            const allLinkedTargets = state.targets.filter(function (targetItem) {
                return Number(targetItem.telegram_account_id) === Number(item && item.id);
            });
            const authAction = authMode === "session_import"
                ? '<button class="ghost-btn continue-account-auth-btn" type="button" data-account-id="' + item.id + '">' + (authState === "authorized" ? "重新导入" : "继续导入") + "</button>"
                : '<button class="ghost-btn continue-account-auth-btn" type="button" data-account-id="' + item.id + '">' + (authState === "authorized" ? "重新登录" : (authState === "login_expired" ? "重新发送验证码" : "继续授权")) + "</button>";
            return renderConfigCardShell({
                title: item.label || "--",
                cardClassName: "account-card-item",
                headerBadges: [
                    renderStatusPill(item.status),
                    '<span class="config-status-pill is-auth-' + escapeHtml(authState) + '">' + escapeHtml(accountAuthLabel(item)) + "</span>",
                ],
                summaryMarkup: renderConfigSummaryGrid([
                    renderConfigSummaryItem("授权状态", accountAuthLabel(item)),
                    renderConfigSummaryItem("接入方式", authMode === "session_import" ? "导入 Session" : "手机号登录"),
                    renderConfigSummaryItem("承接群组", String(allLinkedTargets.length)),
                    renderConfigSummaryItem("历史执行", String(deletion.executionJobCount)),
                ]),
                detailMarkup: renderConfigDetailRows([
                    renderConfigDetailRow("手机号", item.phone || "--"),
                    renderConfigDetailRow("承接群组", allLinkedTargets.length ? allLinkedTargets.map(function (targetItem) { return targetItem.target_name || targetItem.target_key || "--"; }).join("、") : "当前还没有群组绑定这个账号。"),
                    renderConfigDetailRow("删除影响", accountDeletionDescription(item)),
                    renderConfigDetailRow("授权说明", accountAuthDescription(item)),
                ]),
                footerMarkup: "<div class=\"config-list-actions\"><button class=\"ghost-btn edit-account-btn\" type=\"button\" data-account-id=\"" + item.id + "\">编辑</button>" + authAction + "<button class=\"ghost-btn toggle-account-btn\" type=\"button\" data-account-id=\"" + item.id + "\" data-next-status=\"" + statusAction.nextStatus + "\">" + statusAction.actionText + "</button>" + archiveAction + "</div>",
            });
        }).join("");
        renderAccountWorkspaceSummary();
    }

    function renderTargetCards() {
        if (!(targetCards instanceof HTMLElement)) {
            return;
        }
        const archivedCount = archivedTargets().length;
        if (toggleArchivedTargetsBtn instanceof HTMLButtonElement) {
            toggleArchivedTargetsBtn.hidden = archivedCount <= 0;
            toggleArchivedTargetsBtn.textContent = state.showArchivedTargets ? "隐藏已归档" : ("查看已归档（" + String(archivedCount) + "）");
            toggleArchivedTargetsBtn.setAttribute("aria-pressed", state.showArchivedTargets ? "true" : "false");
        }
        const items = (state.showArchivedTargets ? state.targets : visibleTargets()).slice().sort(function (left, right) {
            if (isArchivedItem(left) !== isArchivedItem(right)) {
                return isArchivedItem(left) ? 1 : -1;
            }
            return Number(left.id || 0) - Number(right.id || 0);
        });
        if (!items.length) {
            targetCards.innerHTML = '<article class="config-list-item config-list-empty-card"><strong>' + (archivedCount ? '当前没有可用投递群组' : '当前没有投递群组') + '</strong><p>' + (archivedCount ? '旧群组已归档并从日常列表隐藏，不会继续占用配置位置。' : '新增至少一个投递群组后，后续信号才有实际执行落点。') + '</p></article>';
            renderTargetWorkspaceSummary();
            return;
        }
        targetCards.innerHTML = items.map(function (item) {
            const account = accountById(item.telegram_account_id);
            const template = templateById(item.template_id);
            const repair = targetRepairState(item);
            const subscriptionRefs = targetSubscriptionRefs(item);
            const failureDigest = targetFailureDigest(item);
            const recentActivities = targetRecentActivities(item);
            const statusAction = entityStatusAction(item.status, "暂停群组", "恢复群组");
            const archiveAction = isArchivedItem(item)
                ? '<button class="ghost-btn danger-btn delete-target-btn" type="button" data-target-id="' + item.id + '">删除群组</button>'
                : '<button class="ghost-btn archive-target-btn" type="button" data-target-id="' + item.id + '">归档群组</button>';
            const canTest = Boolean(account && isAccountAuthorized(account) && !isArchivedItem(item));
            const testPassed = isTargetWizardVerified(item);
            const testActionClass = testPassed ? "ghost-btn" : "primary-btn";
            const testAction = '<button class="' + testActionClass + ' test-target-btn" type="button" data-target-id="' + item.id + '"' + (canTest ? "" : " disabled") + ">" + (canTest ? (testPassed ? "一键重测" : "先测试发送") : "无法测试") + "</button>";
            const activationLocked = !isArchivedItem(item) && statusAction.nextStatus === "active" && !testPassed;
            const toggleAction = activationLocked
                ? '<button class="ghost-btn toggle-target-btn" type="button" data-target-id="' + item.id + '" data-next-status="active" disabled>测试成功后启用</button>'
                : '<button class="ghost-btn toggle-target-btn" type="button" data-target-id="' + item.id + '" data-next-status="' + statusAction.nextStatus + '">' + statusAction.actionText + "</button>";
            const subscriptionText = subscriptionRefs.length
                ? subscriptionRefs.map(function (entry) {
                    return entry.source_name + "（" + statusText(entry.status || "inactive") + "）";
                }).join("、")
                : "当前还没有跟单策略会命中这个群。";
            const recentExecutionText = item.recent_execution_at
                ? ("最近执行：" + formatDateTime(item.recent_execution_at) + " · " + statusText(item.recent_execution_status || "pending") + " · 尝试 " + String(item.recent_execution_attempt_count || 0) + " 次")
                : "最近没有执行记录。";
            const recentActivityMarkup = recentActivities.length
                ? recentActivities.map(function (job) {
                    return '<li>' + escapeHtml(formatDateTime(job.last_executed_at || job.updated_at || job.created_at) + " · " + statusText(job.last_delivery_status || job.status || "pending") + " · " + (job.planned_message_text || job.bet_value || "--")) + '</li>';
                }).join("")
                : "<li>当前没有最近执行轨迹。</li>";
            const failureText = Number(failureDigest.count || 0)
                ? ((failureDigest.details || []).join("；") + (failureDigest.last_failure_at ? (" · 最近一次 " + formatDateTime(failureDigest.last_failure_at)) : ""))
                : "最近没有失败原因聚合。";
            return renderConfigCardShell({
                title: item.target_name || item.target_key || "--",
                cardClassName: "target-card-item",
                headerBadges: [
                    renderStatusPill(item.status),
                ],
                summaryMarkup: renderConfigSummaryGrid([
                    renderConfigSummaryItem("测试状态", targetLastTestStatus(item) ? statusText(targetLastTestStatus(item)) : "待测试"),
                    renderConfigSummaryItem("绑定账号", account ? account.label : "--"),
                    renderConfigSummaryItem("绑定模板", template ? template.name : "默认格式"),
                    renderConfigSummaryItem("命中策略", String(subscriptionRefs.length)),
                ]),
                detailMarkup: [
                    renderConfigDetailRows([
                        renderConfigDetailRow("群组标识", item.target_key || "--", "mono-text"),
                        renderConfigDetailRow("派单模式", ({shared: "互斥并存", rule_only: "仅自动规则", direct_only: "仅手动跟单"})[item.dispatch_mode || "shared"] || "--"),
                        renderConfigDetailRow("命中策略", subscriptionText),
                        renderConfigDetailRow("测试说明", targetTestSummary(item)),
                        renderConfigDetailRow("最近执行", recentExecutionText),
                        renderConfigDetailRow("失败摘要", failureText),
                    ]),
                    '<div class="config-card-section"><span class="config-card-section-title">最近轨迹</span><ul class="target-activity-list">' + recentActivityMarkup + '</ul></div>',
                    '<div class="config-card-section config-card-section-muted"><span class="config-card-section-title">处理建议</span><p class="target-repair-note"><strong>' + escapeHtml(repair.title || "当前状态") + '</strong> · ' + escapeHtml(repair.detail || "--") + '</p><div class="target-repair-actions"><a class="panel-link" href="' + escapeHtml(repair.href || "/autobet/targets#targetsSection") + '">' + escapeHtml(repair.text || "去处理") + '</a></div></div>',
                ].join(""),
                footerMarkup: "<div class=\"config-list-actions\"><button class=\"ghost-btn edit-target-btn\" type=\"button\" data-target-id=\"" + item.id + "\">编辑</button>" + testAction + toggleAction + archiveAction + "</div>",
            });
        }).join("");
        renderTargetWorkspaceSummary();
    }

    function isWinStreakRollSubscription(subscription) {
        return Boolean(subscription && ((subscription.strategy && subscription.strategy.win_streak_roll) ||
            ((subscription.strategy_v2 || {}).staking_policy || {}).mode === "win_streak_roll"));
    }

    function renderManualCycleState(subscription) {
        if (!isWinStreakRollSubscription(subscription)) return "";
        const cycle = subscription.manual_cycle_state;
        let content;
        if (cycle === null) {
            content = '<p>尚未手动开始。</p>';
        } else if (!cycle || typeof cycle !== "object") {
            content = '<p>手动过关状态暂不可用，请刷新后重试。</p>';
        } else {
            const number = function (value, money) {
                return value == null || value === "" || !Number.isFinite(Number(value)) ? "—" : (money ? Number(value).toFixed(2) : String(value));
            };
            const reason = {
                balance_depleted: "本轮资金耗尽", win_streak_target: "达到过关目标",
                cycle_balance_depleted: "本轮资金耗尽", cycle_target_hit: "达到过关目标", manual_stop: "手动停止",
            }[String(cycle.stop_reason || "")] || (cycle.stop_reason ? "已停止，详见运行记录" : "");
            const status = !cycle.periods && !cycle.pending_issue && subscription.status !== "active" ? "尚未直接启动" : cycle.day_closed ? "手动当天收工" : ({running: "手动进行中", stopped: "手动已停止", waiting: "手动待命", blocked: "手动状态需核对"}[cycle.status] || "手动状态未知");
            content = '<p>' + escapeHtml(status) + (reason ? ' · ' + escapeHtml(reason) : '') + '</p>' +
                '<p>余额 ' + escapeHtml(number(cycle.balance, true)) + ' · 当前关数 ' + escapeHtml(number(cycle.win_streak)) + ' / ' + escapeHtml(number(cycle.target_streak)) + '</p>' +
                '<p>' + escapeHtml(cycle.stat_date || "—") + ' · 当天已重开 ' + escapeHtml(number(cycle.cycles_today)) + ' 次</p>' +
                (cycle.pending_issue ? '<p>第 <span class="mono-text">' + escapeHtml(cycle.pending_issue) + '</span> 期待结算</p>' : '');
        }
        return '<section class="manual-cycle-state" aria-label="手动过关状态"><strong>手动过关状态</strong>' + content +
            '<a href="/auto-triggers?subscription_id=' + encodeURIComponent(subscription.id) + '">查看自动规则</a></section>';
    }

    function showSavedSubscriptionActions(item) {
        const box = document.getElementById('subscriptionSavedActions');
        if (!box || !item) return;
        box.hidden = false;
        box.dataset.subscriptionId = item.id;
        box.innerHTML = '<strong>' + escapeHtml(item.name || '方案') + '已保存，尚未启动</strong><p>配置自动规则后等待条件，或从下一条新信号开始直接跟单。</p>' +
            '<div class="config-list-actions"><a class="primary-btn" href="/auto-triggers?subscription_id=' + encodeURIComponent(item.id) + '">配置自动规则</a>' +
            '<button class="ghost-btn saved-subscription-start" type="button" data-id="' + item.id + '">开始直接跟单</button></div>';
        box.querySelector('button').addEventListener('click', async function () {
            this.disabled = true;
            try {
                await updateEntityStatus('/api/platform/subscriptions/', item.id, 'active', '已开始直接跟单，从下一条新信号开始。');
                box.hidden = true;
            } catch (error) { setStatus(error.message, true); } finally { this.disabled = false; }
        });
    }

    function subscriptionLaunchLabel(item) {
        return item.launch_view?.status_label || (item.status === 'standby' ? '等待自动规则' : item.status === 'active' ? '直接跟单中' : item.status === 'archived' ? '已归档' : '未启动／已暂停');
    }

    function renderSubscriptionCards() {
        if (!(subscriptionCards instanceof HTMLElement)) {
            return;
        }
        if (!state.subscriptions.length) {
            subscriptionCards.innerHTML = '<article class="config-list-item config-list-empty-card"><strong>当前还没有跟单策略</strong><p>建好后，来源信号才会按你的规则生成实际发单任务。</p></article>';
            renderSubscriptionWorkspaceSummary();
            return;
        }
        const savedActions = document.getElementById('subscriptionSavedActions');
        const savedItem = savedActions && subscriptionById(savedActions.dataset.subscriptionId);
        if (savedActions && savedItem && savedItem.launch_mode !== 'unstarted') savedActions.hidden = true;
        subscriptionCards.innerHTML = state.subscriptions.map(function (item) {
            const source = sourceById(item.source_id);
            const copyAction = '<button class="ghost-btn copy-subscription-btn" type="button" data-subscription-id="' + item.id + '">复制为未启动方案</button>';
            const statusAction = entityStatusAction(item.status, "暂停直接跟单", "开始直接跟单");
            if (item.launch_mode === "automatic" && item.status !== "archived") { statusAction.nextStatus = item.status === "standby" ? "inactive" : "standby"; statusAction.actionText = item.status === "standby" ? "暂停自动待命" : "恢复自动待命"; }
            const normalizedStatus = String(item.status || "inactive");
            const standbyAction = "";
            const archiveAction = isArchivedItem(item)
                ? '<button class="ghost-btn danger-btn delete-subscription-btn" type="button" data-subscription-id="' + item.id + '">删除策略</button>'
                : '<button class="ghost-btn archive-subscription-btn" type="button" data-subscription-id="' + item.id + '">归档策略</button>';
            const chainState = subscriptionTargetsState(item);
            const previewTargets = chainState.previewTargets.slice(0, 6);
            const hiddenPreviewCount = Math.max(0, chainState.previewTargets.length - previewTargets.length);
            const routeTitle = chainState.effectiveTargets.length
                ? ("当前会收到信号的群组（" + chainState.effectiveTargets.length + "）")
                : (normalizedStatus === "standby"
                    ? "待命触发后会收到信号的群组"
                    : (chainState.previewTargets.length ? "启用后会收到信号的群组" : "还没有可用群组"));
            const routeCopy = chainState.effectiveTargets.length
                ? "这些群组会在信号到来时生成实际发单任务。"
                : (normalizedStatus === "standby"
                    ? "当前只等待自动触发规则，普通信号不会直接生成发单任务。"
                    : (chainState.previewTargets.length
                    ? "现在先展示未来会走到的链路，等策略和群组都启用后才会真的发出。"
                    : "先去群组页新增并启用至少一个投递群组。"));
            const routeMarkup = previewTargets.length
                ? previewTargets.map(function (targetItem) {
                    const preview = subscriptionPreviewForTarget(item, targetItem);
                    const targetTemplate = preview.template;
                    const targetAccount = preview.account;
                    const targetStatus = String(targetItem.status || "inactive");
                    const testStatus = targetLastTestStatus(targetItem);
                    const testStatusText = testStatus ? statusText(testStatus) : "待测试";
                    const diagnosticsMarkup = preview.diagnostics.length
                        ? preview.diagnostics.map(function (entry) {
                            return '<span class="subscription-diagnostic-pill is-' + escapeHtml(entry.severity || "warning") + '">' + escapeHtml(entry.label) + "</span>";
                        }).join("")
                        : '<span class="subscription-diagnostic-pill is-ok">链路可用</span>';
                    return [
                        '<article class="subscription-route-card">',
                        '<div class="subscription-route-head"><strong>' + escapeHtml(targetItem.target_name || targetItem.target_key || "--") + "</strong>" + renderStatusPill(targetStatus) + "</div>",
                        '<p class="subscription-route-meta">模板：' + escapeHtml(targetTemplate ? targetTemplate.name : "默认格式") + ' · 账号：' + escapeHtml(targetAccount ? targetAccount.label : "--") + ' · 测试：' + escapeHtml(testStatusText) + "</p>",
                        '<div class="subscription-message-preview"><span class="setup-label">最终发单示例</span><strong>' + escapeHtml(preview.rendered.text || "--") + "</strong><p>" + escapeHtml("玩法：" + preview.signal.bet_type + " / " + preview.signal.bet_value + " · 金额 " + amountText(preview.amount) + (preview.strategyMode === "倍投" ? (" · 当前第 " + preview.currentStep + " 手") : "") + " · 期号 " + preview.signal.issue_no) + "</p></div>",
                        '<p class="subscription-route-hint">' + escapeHtml(preview.rendered.meta || "--") + "</p>",
                        '<div class="subscription-diagnostic-row">' + diagnosticsMarkup + "</div>",
                        "</article>",
                    ].join("");
                }).join("")
                : '<article class="subscription-route-card"><strong>还没有可用群组</strong><p class="subscription-route-empty">先去群组页新增并启用至少一个投递群组，这条跟单才会真正发出去。</p></article>';
            const blockerMarkup = chainState.summaries.length
                ? chainState.summaries.map(function (entry) {
                    return [
                        '<a class="subscription-blocker-card" href="' + escapeHtml(entry.href) + '">',
                        '<strong>' + escapeHtml(entry.label + " · " + entry.count + " 个") + "</strong>",
                        '<p>这类问题会挡住发单，建议优先处理。</p>',
                        "</a>",
                    ].join("");
                }).join("")
                : '<article class="subscription-blocker-card"><strong>当前没有阻塞项</strong><p>账号、群组测试、模板和启用状态都正常，后续重点看执行记录。</p></article>';
            const progression = item.progression && typeof item.progression === "object" ? item.progression : null;
            const financial = item.financial && typeof item.financial === "object" ? item.financial : null;
            const settlementRuleId = String((item.strategy_v2 && item.strategy_v2.settlement_policy && item.strategy_v2.settlement_policy.settlement_rule_id) || "").trim();
            const settlementCopy = settlementRuleId && SETTLEMENT_RULE_UI_PRESETS[settlementRuleId]
                ? SETTLEMENT_RULE_UI_PRESETS[settlementRuleId].refundHint
                : "系统会按当前结算规则识别回本（如中奖遇 0/27、13/14、对子、顺子、豹子、ABC 含 0/9）。";
            const settlementPanelMarkup = progression && progression.pending_event_id && String(progression.pending_status || "") === "placed"
                ? [
                    '<div class="subscription-settlement-panel">',
                    '<div class="subscription-settlement-copy"><strong>自动结算</strong><p>输入和值或三球表达式，例如 14、4+4+6、1,2,3。' + escapeHtml(settlementCopy) + '</p></div>',
                    '<div class="subscription-settlement-form">',
                    '<input class="text-input subscription-settlement-input" type="text" name="draw_input" placeholder="例如：4+4+6 或 14" inputmode="numeric" autocomplete="off">',
                    '<button class="ghost-btn resolve-progression-btn" type="button" data-subscription-id="' + item.id + '" data-progression-event-id="' + progression.pending_event_id + '">自动结算</button>',
                    '</div>',
                    '</div>',
                ].join("")
                : "";
            const progressionActions = progression && !progression.pending_group && progression.pending_event_id && String(progression.pending_status || "") === "placed"
                ? [
                    '<button class="ghost-btn settle-progression-btn" type="button" data-subscription-id="' + item.id + '" data-progression-event-id="' + progression.pending_event_id + '" data-result-type="hit">命中</button>',
                    '<button class="ghost-btn settle-progression-btn" type="button" data-subscription-id="' + item.id + '" data-progression-event-id="' + progression.pending_event_id + '" data-result-type="refund">回本</button>',
                    '<button class="ghost-btn settle-progression-btn" type="button" data-subscription-id="' + item.id + '" data-progression-event-id="' + progression.pending_event_id + '" data-result-type="miss">未中</button>'
                ].join("")
                : "";
            const isRollPlanRow = isWinStreakRollSubscription(item);
            const resetActionText = (isSubscriptionRiskBlocked(item) || isRollPlanRow) ? "手动开始新一轮" : "重置手动盈亏";
            const resetAction = item.launch_mode === "automatic" || item.launch_mode === "unstarted" ? "" : '<button class="ghost-btn reset-subscription-runtime-btn" type="button" data-subscription-id="' + item.id + '">' + resetActionText + '</button>';
            const stoppedReasonMarkup = financial && financial.stopped_reason
                ? ('<div class="config-card-note subscription-runtime-note">' + escapeHtml(financial.stopped_reason) + '</div>')
                : "";
            const blockerCount = chainState.summaries.reduce(function (total, entry) { return total + entry.count; }, 0);
            const chainMetaMarkup = '<div class="subscription-chain-meta"><span class="subscription-chain-stat">当前命中 ' + escapeHtml(String(chainState.effectiveTargets.length)) + ' 个群组</span><span class="subscription-chain-stat">已配置 ' + escapeHtml(String(chainState.configuredTargets.length)) + ' 个群组</span><span class="subscription-chain-stat">阻塞 ' + escapeHtml(String(blockerCount)) + ' 项</span></div>';
            const deliveryMarkup = '<div class="subscription-detail-grid"><section class="subscription-routes"><strong class="subscription-section-title">' + escapeHtml(routeTitle) + '</strong><p class="subscription-section-copy">' + escapeHtml(routeCopy) + '</p><div class="subscription-route-grid">' + routeMarkup + '</div>' + (hiddenPreviewCount ? ('<p class="subscription-more-note">另有 ' + escapeHtml(String(hiddenPreviewCount)) + ' 个群组未展开，去群组页可查看全部详情。</p>') : "") + '</section><aside class="subscription-blockers"><strong class="subscription-section-title">当前阻塞项</strong><p class="subscription-section-copy">这里会直接说明信号现在发不出去的原因，以及应该去哪一页处理。</p><div class="subscription-blocker-list">' + blockerMarkup + '</div></aside></div>';
            const isDetailOpen = String(state.expandedSubscriptionId || "") === String(item.id);
            const detailToggleText = isDetailOpen ? "收起详情" : "查看详情";
            const expandedDetailMarkup = isDetailOpen
                ? renderSubscriptionDetailTabs(item, {
                    chainMetaMarkup: chainMetaMarkup,
                    deliveryMarkup: deliveryMarkup,
                })
                : "";
            return renderConfigCardShell({
                title: (item.name && String(item.name).trim()) || (source ? source.name : ("#" + item.source_id)),
                cardClassName: "subscription-card-item",
                headerBadges: [
                    '<span class="config-status-pill">' + escapeHtml(subscriptionLaunchLabel(item)) + '</span>',
                ],
                summaryMarkup: renderConfigSummaryGrid([
                    renderConfigSummaryItem("启动方式", item.launch_mode === "automatic" || item.status === "standby" ? "自动规则" : item.launch_mode === "unstarted" ? "尚未选择" : "直接跟单"),
                    renderConfigSummaryItem("策略方式", summarizeStrategyMode(item.strategy || {})),
                    renderConfigSummaryItem("当日净盈亏", signedAmountText(subscriptionDailyNet(item)), subscriptionDailyNet(item) >= 0 ? "amount-positive" : "amount-negative"),
                    renderConfigSummaryItem("当前轮次", signedAmountText(subscriptionRuntimeNet(item)), subscriptionRuntimeNet(item) >= 0 ? "amount-positive" : "amount-negative"),
                    renderConfigSummaryItem("阻塞项", String(blockerCount)),
                ]),
                detailMarkup: [
                    renderConfigDetailRows([
                        renderConfigDetailRow("方案默认结算", summarizeSettlementPolicy(item.strategy_v2 || {}, item.strategy || {})),
                        renderConfigDetailRow("自动规则结算配置", (item.launch_view?.automatic_settlements || []).map(entry => entry.rule_name + (entry.rule_status === 'active' ? '：' : '（未启用）：') + summarizeSettlementPolicy({settlement_policy:entry.policy}, {}) + (entry.source === 'route_override' ? '（路由覆盖）' : '（继承方案）')).join('；') || '未关联自动规则'),
                        renderConfigDetailRow("关联规则", (item.launch_view?.rules || []).map(rule => rule.name + (rule.status === 'active' ? '（已启用）' : '（未启用）')).join('；') || '未关联'),
                        renderConfigDetailRow("自动运行", (item.launch_view?.running || []).map(run => run.rule_name + (run.target_streak ? ' · ' + run.win_streak + '/' + run.target_streak + ' 关' : '') + (run.settlement_policy ? ' · 本轮' + summarizeSettlementPolicy({settlement_policy:run.settlement_policy}, {}) : '')).join('；') || '暂无进行中的自动轮次'),
                        renderConfigDetailRow("跟单进度", summarizeProgression(item)),
                        renderConfigDetailRow("当日盈亏", summarizeDailyFinancial(item)),
                        renderConfigDetailRow("投递概况", "命中 " + String(chainState.effectiveTargets.length) + " 个群组 · 已配置 " + String(chainState.configuredTargets.length) + " 个群组"),
                    ]),
                    stoppedReasonMarkup,
                    renderManualCycleState(item),
                    settlementPanelMarkup,
                    expandedDetailMarkup,
                ].join(""),
                footerMarkup: "<div class=\"config-list-actions\"><button class=\"ghost-btn subscription-detail-toggle-btn\" type=\"button\" data-subscription-id=\"" + item.id + "\">" + detailToggleText + "</button><button class=\"ghost-btn edit-subscription-btn\" type=\"button\" data-subscription-id=\"" + item.id + "\">编辑</button><button class=\"ghost-btn toggle-subscription-btn\" type=\"button\" data-subscription-id=\"" + item.id + "\" data-next-status=\"" + statusAction.nextStatus + "\">" + statusAction.actionText + "</button>" + standbyAction + resetAction + archiveAction + copyAction + progressionActions + '<a class="ghost-btn link-btn" href="/auto-triggers?subscription_id=' + encodeURIComponent(item.id) + '">配置自动规则</a></div>',
            });
        }).join("");
        renderSubscriptionWorkspaceSummary();
        syncBatchResolveButtonState();
    }

    function resetCollections() {
        pipelineDataLoadToken += 1;
        state.sources = [];
        state.rawItems = [];
        state.signals = [];
        state.accounts = [];
        state.targets = [];
        state.templates = [];
        state.subscriptions = [];
        state.jobs = [];
        state.failures = [];
        state.subscriptionDailySummary = null;
        state.expandedSubscriptionId = null;
        state.subscriptionDetailTabs = {};
        state.subscriptionAllDailyHistories = {};
        state.subscriptionRuntimeHistories = {};
        state.scopeFocused = false;
        if (sourceForm instanceof HTMLFormElement) {
            sourceForm.reset();
            sourceForm.elements.visibility.value = "private";
        }
        if (accountForm) accountForm.reset();
        resetAccountVerifyForm();
        if (targetForm) targetForm.reset();
        if (messageTemplateForm instanceof HTMLFormElement) {
            messageTemplateForm.reset();
        }
        if (subscriptionForm) subscriptionForm.reset();
        setFormEditingState(sourceForm, createSourceBtn, cancelSourceEditBtn, false, "导入来源");
        setFormEditingState(accountForm, document.getElementById("createAccountBtn"), cancelAccountEditBtn, false, "新增托管账号");
        syncAccountModeUI("phone_login");
        setFormEditingState(targetForm, document.getElementById("createTargetBtn"), cancelTargetEditBtn, false, "新增投递群组");
        if (messageTemplateForm instanceof HTMLFormElement) {
            setFormEditingState(messageTemplateForm, document.getElementById("createMessageTemplateBtn"), cancelMessageTemplateEditBtn, false, "新增模板");
            messageTemplateForm.elements.status.value = "active";
            messageTemplateForm.elements.template_text.value = "{{bet_value}}{{amount}}";
            applyTemplateConfigToForm(pc28TemplateExampleConfig());
        }
        setFormEditingState(subscriptionForm, document.getElementById("createSubscriptionBtn"), cancelSubscriptionEditBtn, false, "保存方案");
        resetSubscriptionStrategyFormState();
        refreshSourceSelects();
        refreshAccountSelects();
        refreshTemplateSelects();
        refreshSubscriptionTargetSelects();
        renderLoadedScope(currentAutobetScope());
        syncBatchResolveButtonState();
    }

    function autobetScopeDataKeys(scope) {
        const normalized = String(scope || "workbench").trim() || "workbench";
        if (normalized === "sources") {
            return ["sources", "subscriptions", "jobs"];
        }
        if (normalized === "accounts") {
            return ["accounts", "targets"];
        }
        if (normalized === "templates") {
            return ["templates", "targets"];
        }
        if (normalized === "targets") {
            return ["accounts", "targets", "templates", "jobs"];
        }
        if (normalized === "subscriptions") {
            return ["sources", "accounts", "targets", "templates", "subscriptions"];
        }
        return [];
    }

    function shouldLoadRecentPipelineData(scope) {
        return false;
    }

    function resetLoadedDataState() {
        state.sources = [];
        state.rawItems = [];
        state.signals = [];
        state.accounts = [];
        state.targets = [];
        state.templates = [];
        state.subscriptions = [];
        state.jobs = [];
        state.failures = [];
        state.subscriptionDailySummary = null;
    }

    async function loadPageData() {
        pipelineDataLoadToken += 1;
        resetLoadedDataState();
        const scope = currentAutobetScope();
        const dataKeys = autobetScopeDataKeys(scope);
        const subscriptionStatDateQuery = state.subscriptionStatDate
            ? ("?stat_date=" + encodeURIComponent(state.subscriptionStatDate))
            : "";
        const loaders = {
            sources: function () {
                return request("/api/platform/sources/summary").then(function (payload) {
                    state.sources = payload.items || [];
                });
            },
            accounts: function () {
                return request("/api/platform/telegram-accounts").then(function (payload) {
                    state.accounts = payload.items || [];
                });
            },
            targets: function () {
                return request("/api/platform/delivery-targets/summary").then(function (payload) {
                    state.targets = payload.items || [];
                });
            },
            templates: function () {
                return request("/api/platform/message-templates").then(function (payload) {
                    state.templates = payload.items || [];
                });
            },
            subscriptions: function () {
                // 跟单工作区需要直接展示最近 7 天历史；摘要接口为其他工作区优化过，按约定不返回 daily_history。
                const endpoint = scope === "subscriptions"
                    ? "/api/platform/subscriptions"
                    : "/api/platform/subscriptions/summary";
                return request(endpoint + subscriptionStatDateQuery).then(function (payload) {
                    state.subscriptions = payload.items || [];
                    state.supportsExplicitStart = payload.launch_workflow === "explicit_start_v1";
                    state.subscriptionStatDate = payload.stat_date || state.subscriptionStatDate || todayDateString();
                    state.subscriptionDailySummary = payload.daily_summary || null;
                });
            },
            jobs: function () {
                return request("/api/platform/execution-jobs?limit=100").then(function (payload) {
                    state.jobs = payload.items || [];
                });
            },
            failures: function () {
                return request("/api/platform/execution-failures?limit=20").then(function (payload) {
                    state.failures = payload.items || [];
                });
            },
        };
        await Promise.all(dataKeys.map(function (key) {
            return loaders[key]();
        }));
    }

    async function loadRecentPipelineData() {
        const token = ++pipelineDataLoadToken;
        const results = await Promise.allSettled([
            request("/api/platform/raw-items?limit=" + encodeURIComponent(String(RECENT_PIPELINE_LIMIT))),
            request("/api/platform/signals?limit=" + encodeURIComponent(String(RECENT_PIPELINE_LIMIT))),
        ]);
        if (token !== pipelineDataLoadToken) {
            return false;
        }
        if (results[0].status === "fulfilled") {
            state.rawItems = results[0].value.items || [];
        } else {
            state.rawItems = [];
        }
        if (results[1].status === "fulfilled") {
            state.signals = results[1].value.items || [];
        } else {
            state.signals = [];
        }
        if (results.some(function (result) { return result.status === "rejected"; })) {
            throw new Error("最近来源链路数据加载失败，基础配置已刷新。");
        }
        return true;
    }

    function syncSubscriptionStatDateControls() {
        if (subscriptionStatDateInput instanceof HTMLInputElement) {
            subscriptionStatDateInput.value = state.subscriptionStatDate || "";
        }
    }

    async function refreshAll(options) {
        const normalized = options || {};
        const scope = currentAutobetScope();
        setButtonBusy(refreshAutobetBtn, true, "刷新中...");
        try {
            const user = normalized.skipCurrentUserReload ? state.currentUser : await loadCurrentUser();
            if (!user) {
                resetCollections();
                setStatus("登录后可集中配置自动投注条件。", false);
                return;
            }

            await loadPageData();
            syncSubscriptionStatDateControls();
            refreshSourceSelects();
            refreshAccountSelects();
            refreshTemplateSelects();
            refreshSubscriptionTargetSelects();
            renderLoadedScope(scope);
            if (!state.scopeFocused) {
                focusScopeSectionOnLoad();
                state.scopeFocused = true;
            }
            if (!normalized.keepStatus) {
                setStatus("自动投注配置已刷新。", false);
            }
            if (shouldLoadRecentPipelineData(scope)) {
                loadRecentPipelineData().then(function (updated) {
                    if (!updated) {
                        return;
                    }
                    safeRender("source-cards", renderSourceCards);
                    safeRender("source-list", renderSourceList);
                }).catch(function (error) {
                    safeRender("source-cards", renderSourceCards);
                    safeRender("source-list", renderSourceList);
                    setStatus(error.message || "最近来源链路数据加载失败，基础配置已刷新。", true);
                });
            }
        } catch (error) {
            setStatus(error.message || "配置页面加载失败，请刷新重试", true);
        } finally {
            setButtonBusy(refreshAutobetBtn, false, "刷新配置");
        }
    }

    async function updateEntityStatus(pathPrefix, entityId, nextStatus, successMessage) {
        await request(pathPrefix + entityId + "/status", {
            method: "POST",
            body: {status: nextStatus},
        });
        await refreshAll();
        setStatus(successMessage, false);
    }

    async function deleteEntity(pathPrefix, entityId, successMessage) {
        await request(pathPrefix + entityId + "/delete", {
            method: "POST",
            body: {},
        });
        await refreshAll();
        setStatus(successMessage, false);
    }

    async function performTargetTestSend(item, button, options) {
        const config = options || {};
        if (!item) {
            throw new Error("请选择要测试的投递群组");
        }
        const account = accountById(item.telegram_account_id);
        if (!account || !isAccountAuthorized(account)) {
            throw new Error("该投递群组绑定的托管账号尚未完成授权，无法测试发送。");
        }
        if (isArchivedItem(item)) {
            throw new Error("已归档的投递群组不能测试发送。");
        }
        const confirmed = confirmDangerousAction(
            config.confirmMessage || ("将使用账号「" + (account.label || "--") + "」向群组「" + (item.target_name || item.target_key || "--") + "」发送一条测试消息。确认继续吗？")
        );
        if (!confirmed) {
            return {cancelled: true};
        }
        setButtonBusy(button, true, config.busyText || "发送中...");
        try {
            const payload = await request("/api/platform/delivery-targets/" + item.id + "/test-send", {
                method: "POST",
                body: {},
            });
            writeStoredValue("wizard-tested-target", targetProgressToken(item));
            writeStoredValue("wizard-last-target-test-at", String((payload.item || {}).last_tested_at || new Date().toISOString()));
            await refreshAll();
            setStatus(config.successMessage || "测试发送成功，群组已通过校验。", false);
            return {success: true};
        } finally {
            setButtonBusy(button, false, config.idleText || "测试发送");
        }
    }

    function loadSourceIntoForm(source) {
        if (!source || !(sourceForm instanceof HTMLFormElement)) {
            return;
        }
        setSourceUrlError("");
        const fetchConfig = source.config && source.config.fetch ? source.config.fetch : {};
        sourceForm.elements.edit_id.value = String(source.id || "");
        sourceForm.elements.name.value = source.name || "";
        sourceForm.elements.visibility.value = String(source.visibility || "private") || "private";
        sourceForm.elements.source_url.value = String(fetchConfig.url || "");
        setFormEditingState(sourceForm, createSourceBtn, cancelSourceEditBtn, true, "保存来源");
        sourceForm.scrollIntoView({behavior: "smooth", block: "start"});
        focusElement(sourceNameInput);
    }

    async function runSourcePipeline(source, button) {
        if (!source || source.id == null) {
            throw new Error("请选择要推进的来源");
        }
        const activeSubscription = activeSourceSubscription(source.id);
        if (!activeSubscription) {
            throw new Error("这个来源当前还没有激活跟单策略，请先把它接入并启用跟单。");
        }
        const confirmed = confirmDangerousAction(
            "将为来源「" + (source.name || "--") + "」执行一次完整推进：抓取最新内容、标准化成信号，并把新信号展开为执行任务。确认继续吗？"
        );
        if (!confirmed) {
            return {cancelled: true};
        }
        setButtonBusy(button, true, "推进中...");
        try {
            const fetchPayload = await request("/api/platform/sources/" + source.id + "/fetch", {
                method: "POST",
                body: {},
            });
            const rawItem = fetchPayload.raw_item || null;
            if (!rawItem || rawItem.id == null) {
                throw new Error("来源抓取成功，但没有返回可继续标准化的原始内容。");
            }
            const normalizePayload = await request("/api/platform/raw-items/" + rawItem.id + "/normalize", {
                method: "POST",
                body: {},
            });
            const createdSignals = Array.isArray(normalizePayload.items) ? normalizePayload.items : [];
            let createdJobCount = 0;
            let candidateCount = 0;
            for (const signal of createdSignals) {
                const dispatchPayload = await request("/api/platform/signals/" + signal.id + "/dispatch", {
                    method: "POST",
                    body: {},
                });
                createdJobCount += Number(dispatchPayload.created_count || 0);
                candidateCount += Number(dispatchPayload.candidate_count || 0);
            }
            await refreshAll();
            if (!createdSignals.length) {
                setStatus("来源已抓取 raw item，但这次没有产出可用信号。请检查上游返回内容。", true);
                return {rawItem: rawItem, createdSignals: [], createdJobCount: 0, candidateCount: 0};
            }
            if (createdJobCount <= 0) {
                setStatus(
                    "来源链路已推进：raw #" + rawItem.id + "，新增 " + createdSignals.length + " 条信号，但没有生成执行任务。请检查跟单玩法过滤、群组启用状态或信号内容。",
                    true
                );
                return {rawItem: rawItem, createdSignals: createdSignals, createdJobCount: 0, candidateCount: candidateCount};
            }
            setStatus(
                "来源链路已推进：raw #" + rawItem.id + "，新增 " + createdSignals.length + " 条信号，生成 " + createdJobCount + " 条执行任务。执行器会继续拉取并发送。",
                false
            );
            return {
                rawItem: rawItem,
                createdSignals: createdSignals,
                createdJobCount: createdJobCount,
                candidateCount: candidateCount,
            };
        } finally {
            setButtonBusy(button, false, "一键推进来源链路");
        }
    }

    if (accountForm) accountForm.addEventListener("submit", async function (event) {
        event.preventDefault();
        const form = event.currentTarget;
        let accountId = "";
        try {
            if (!state.currentUser) {
                throw new Error("请先登录");
            }
            const editId = String(form.elements.edit_id.value || "").trim();
            const authMode = String(form.elements.auth_mode.value || "phone_login").trim() || "phone_login";
            const phone = String(form.phone.value || "").trim();
            const sessionFile = form.elements.session_file.files && form.elements.session_file.files[0];
            if (authMode === "phone_login" && !phone) {
                throw new Error("手机号不能为空");
            }
            if (authMode === "session_import" && !sessionFile && !editId) {
                throw new Error("请先选择 Session 文件");
            }
            const submitButton = document.getElementById("createAccountBtn");
            setButtonBusy(submitButton, true, editId ? "正在重新发送验证码..." : (authMode === "phone_login" ? "创建并发送验证码..." : "创建并导入中..."));
            accountId = editId;
            if (editId) {
                const updated = await request("/api/platform/telegram-accounts/" + editId, {
                    method: "POST",
                    body: {
                        label: form.label.value,
                        phone: phone,
                        auth_mode: authMode,
                        meta: {},
                    },
                });
                accountId = String((updated.item || {}).id || editId);
            } else {
                const created = await request("/api/platform/telegram-accounts", {
                    method: "POST",
                    body: {
                        label: form.label.value,
                        phone: phone,
                        auth_mode: authMode,
                        meta: {},
                    },
                });
                accountId = String((created.item || {}).id || "");
            }

            if (!accountId) {
                throw new Error("创建账号失败");
            }

            if (authMode === "session_import" && !sessionFile) {
                form.reset();
                setFormEditingState(form, submitButton, cancelAccountEditBtn, false, "");
                syncAccountModeUI("session_import");
                resetAccountVerifyForm();
                await refreshAll();
                setStatus("账号资料已更新。如需替换 Session，请重新选择文件后再次保存。", false);
                return;
            }

            if (authMode === "session_import" && sessionFile) {
                const fileBase64 = await readFileAsBase64(sessionFile);
                const imported = await request("/api/platform/telegram-accounts/" + accountId + "/import-session", {
                    method: "POST",
                    body: {
                        file_name: sessionFile.name,
                        session_file_base64: fileBase64,
                    },
                });
                form.reset();
                setFormEditingState(form, submitButton, cancelAccountEditBtn, false, "");
                syncAccountModeUI("phone_login");
                resetAccountVerifyForm();
                await refreshAll();
                setStatus("托管账号已导入，当前状态：" + accountAuthLabel(imported.item || {} ) + "。", false);
                return;
            }

            const loginResult = await request("/api/platform/telegram-accounts/" + accountId + "/auth/send-code", {
                method: "POST",
                body: {phone: phone},
            });
            await refreshAll({skipCurrentUserReload: true, keepStatus: true});
            const latestAccount = loginResult.item || accountById(accountId);
            loadAccountIntoForm(latestAccount, {scroll: false});
            showAccountVerification(latestAccount);
            setStatus(editId ? "验证码已重新发送。请填写新的验证码，不要继续用旧的二次密码。" : "账号已创建，验证码已发送。", false);
        } catch (error) {
            try {
                await refreshAll({skipCurrentUserReload: true, keepStatus: true});
                const latestAccount = accountById(accountId);
                if (latestAccount) {
                    loadAccountIntoForm(latestAccount, {scroll: false});
                    showAccountVerification(latestAccount);
                }
            } catch (_refreshError) {
                // keep the original send-code error
            }
            setStatus(error.message, true);
        } finally {
            const latestMode = String(accountForm.elements.auth_mode.value || "phone_login");
            const stillEditing = Boolean(String(accountForm.elements.edit_id.value || "").trim());
            setButtonBusy(document.getElementById("createAccountBtn"), false, stillEditing ? "重新发送验证码" : "新增托管账号");
            syncAccountModeUI(latestMode);
        }
    });

    accountModeButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            syncAccountModeUI(button.getAttribute("data-account-auth-mode"));
        });
    });

    if (accountVerifyForm) accountVerifyForm.addEventListener("submit", async function (event) {
        event.preventDefault();
        const form = event.currentTarget;
        let accountId = "";
        try {
            accountId = String(form.elements.telegram_account_id.value || "").trim();
            const account = accountById(accountId);
            if (!accountId || !account) {
                throw new Error("请选择要继续验证的托管账号");
            }
            const authState = accountAuthState(account);
            if (authState === "password_required") {
                const password = String(form.elements.password.value || "").trim();
                if (!password) {
                    throw new Error("二次密码不能为空。请输入 Telegram 两步验证密码（云密码）。");
                }
                setButtonBusy(document.getElementById("verifyAccountPasswordBtn"), true, "提交中...");
                setAccountVerifyFeedback("正在校验二次密码...", false);
                await request("/api/platform/telegram-accounts/" + accountId + "/auth/verify-password", {
                    method: "POST",
                    body: {password: password},
                });
                resetAccountVerifyForm();
                await refreshAll({skipCurrentUserReload: true, keepStatus: true});
                setStatus("二次密码验证成功，账号已可用于发送。", false);
                return;
            }
            setButtonBusy(document.getElementById("verifyAccountCodeBtn"), true, "验证中...");
            setAccountVerifyFeedback("正在校验验证码...", false);
            const result = await request("/api/platform/telegram-accounts/" + accountId + "/auth/verify-code", {
                method: "POST",
                body: {code: form.elements.code.value},
            });
            await refreshAll({skipCurrentUserReload: true, keepStatus: true});
            const latestAccount = result.item || accountById(accountId);
            if (accountAuthState(latestAccount) === "password_required") {
                showAccountVerification(latestAccount);
                setStatus("该账号需要继续输入二次密码。", false);
                return;
            }
            resetAccountVerifyForm();
            setStatus("验证码验证成功，账号已可用于发送。", false);
        } catch (error) {
            const payload = error && error.payload ? error.payload : {};
            const reasonCode = String(payload.reason_code || "").trim();
            const localMessage = String(payload.error || error.message || "提交失败").trim();
            try {
                await refreshAll({skipCurrentUserReload: true, keepStatus: true});
                const latestAccount = accountById(accountId);
                if (latestAccount) {
                    showAccountVerification(latestAccount);
                }
            } catch (_refreshError) {
                // keep the original verify error
            }
            if (reasonCode === "password_invalid") {
                setAccountVerifyFeedback("二次密码不正确。请输入 Telegram 设置里的两步验证密码（云密码），不是登录密码或验证码。", true);
            } else if (reasonCode === "password_empty") {
                setAccountVerifyFeedback("二次密码不能为空。", true);
            } else if (reasonCode === "password_flood") {
                setAccountVerifyFeedback(localMessage || "二次密码尝试过于频繁，请稍后再试。等待期间不要反复提交。", true);
            } else if (reasonCode === "login_expired") {
                setAccountVerifyFeedback("登录已过期，请重新发送验证码，不要继续提交二次密码。", true);
            } else {
                setAccountVerifyFeedback(localMessage, true);
            }
            setStatus(error.message, true);
            focusElement(form.elements.password || form.elements.code);
        } finally {
            setButtonBusy(document.getElementById("verifyAccountCodeBtn"), false, "提交验证码");
            setButtonBusy(document.getElementById("verifyAccountPasswordBtn"), false, "提交二次密码");
        }
    });

    if (accountCards instanceof HTMLElement) {
        accountCards.addEventListener("click", async function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.classList.contains("edit-account-btn")) {
                const accountId = target.getAttribute("data-account-id");
                const account = accountById(accountId);
                if (!account) {
                    return;
                }
                loadAccountIntoForm(account);
                return;
            }
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (target.classList.contains("continue-account-auth-btn")) {
                const accountId = target.getAttribute("data-account-id");
                const account = accountById(accountId);
                if (!account) {
                    return;
                }
                loadAccountIntoForm(account, {scroll: false});
                if (accountAuthMode(account) === "session_import") {
                    resetAccountVerifyForm();
                    setStatus("请选择新的 Session 文件，然后点击“保存并重新导入 Session”。", false);
                    accountForm.scrollIntoView({behavior: "smooth", block: "start"});
                    focusElement(accountForm.querySelector('input[name="session_file"]'));
                } else {
                    if (accountAuthState(account) === "code_sent" || accountAuthState(account) === "password_required") {
                        showAccountVerification(account);
                    } else if (accountAuthState(account) === "login_expired") {
                        resetAccountVerifyForm();
                        setStatus("登录已过期，正在重新发送验证码。", false);
                        accountForm.scrollIntoView({behavior: "smooth", block: "start"});
                        if (typeof accountForm.requestSubmit === "function") {
                            accountForm.requestSubmit();
                        } else {
                            accountForm.dispatchEvent(new Event("submit", {cancelable: true, bubbles: true}));
                        }
                    } else {
                        setStatus("请先点击“重新发送验证码”。", false);
                        accountForm.scrollIntoView({behavior: "smooth", block: "start"});
                    }
                }
                return;
            }
            if (target.classList.contains("archive-account-btn")) {
                const accountId = target.getAttribute("data-account-id");
                if (!accountId || !confirmDangerousAction("归档后该托管账号不会再参与自动投注；如需彻底删除，需在归档后再手动删除。确认继续吗？")) {
                    return;
                }
                try {
                    await updateEntityStatus("/api/platform/telegram-accounts/", accountId, "archived", "托管账号已归档。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target.classList.contains("delete-account-btn")) {
                const accountId = target.getAttribute("data-account-id");
                const account = accountById(accountId);
                const deletion = accountDeletionSummary(account);
                const confirmation = "删除后将清除手机号和 Session，历史执行记录保留为“已删除账号”。" +
                    (deletion.targetCount ? (" 将解除 " + String(deletion.targetCount) + " 个关联群组的账号绑定。") : "") +
                    "确认继续吗？";
                if (!accountId || !confirmDangerousAction(confirmation)) {
                    return;
                }
                try {
                    await deleteEntity(
                        "/api/platform/telegram-accounts/",
                        accountId,
                        deletion.executionJobCount
                            ? "托管账号已删除，历史执行记录已保留。"
                            : "托管账号已删除。"
                    );
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (!target.classList.contains("toggle-account-btn")) {
                return;
            }
            const accountId = target.getAttribute("data-account-id");
            const nextStatus = target.getAttribute("data-next-status");
            if (!accountId || !nextStatus) {
                return;
            }
            try {
                const wasArchived = isArchivedItem(accountById(accountId));
                await updateEntityStatus(
                    "/api/platform/telegram-accounts/",
                    accountId,
                    nextStatus,
                    wasArchived && nextStatus === "inactive" ? "托管账号已取消归档，当前状态为已停用。" : ("托管账号状态已更新为 " + statusText(nextStatus) + "。")
                );
            } catch (error) {
                setStatus(error.message, true);
            }
        });
    }

    if (cancelAccountEditBtn) cancelAccountEditBtn.addEventListener("click", function () {
        accountForm.reset();
        setFormEditingState(accountForm, document.getElementById("createAccountBtn"), cancelAccountEditBtn, false, "新增托管账号");
        resetAccountVerifyForm();
        syncAccountModeUI("phone_login");
    });

    if (fillSourceExampleBtn instanceof HTMLButtonElement && sourceUrlInput instanceof HTMLInputElement) {
        fillSourceExampleBtn.addEventListener("click", function () {
            sourceUrlInput.value = "https://your-ai-platform.example.com/public/predictors/12";
            if (sourceNameInput instanceof HTMLInputElement && !String(sourceNameInput.value || "").trim()) {
                sourceNameInput.value = "AITradingSimulator 方案 #12";
            }
            setStatus("已填入 AITradingSimulator 示例公开方案链接。", false);
        });
    }

    if (sourceForm instanceof HTMLFormElement) {
        sourceForm.addEventListener("submit", async function (event) {
            event.preventDefault();
            try {
                setSourceUrlError("");
                if (!state.currentUser) {
                    throw new Error("请先登录，再导入来源");
                }
                const editId = String(sourceForm.elements.edit_id.value || "").trim();
                const normalized = normalizeAiSourceUrl(sourceForm.elements.source_url.value);
                const existingSource = findImportedAiSourceByUrl(normalized.url, editId);
                if (existingSource) {
                    throw new Error(editId
                        ? "保存失败：这个链接已经是来源「" + (existingSource.name || "--") + "」的抓取地址。"
                        : "该方案已存在于你的来源列表中，无需重复导入");
                }
                setButtonBusy(createSourceBtn, true, editId ? "保存中..." : "导入中...");
                const payload = {
                    source_type: "ai_trading_simulator_export",
                    name: String(sourceForm.elements.name.value || "").trim() || normalized.suggestedName,
                    visibility: String(sourceForm.elements.visibility.value || "private").trim() || "private",
                    status: editId ? undefined : "active",
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
                if (editId) {
                    await request("/api/platform/sources/" + editId, {
                        method: "POST",
                        body: payload,
                    });
                } else {
                    await request("/api/platform/sources", {
                        method: "POST",
                        body: payload,
                    });
                }
                sourceForm.reset();
                sourceForm.elements.visibility.value = "private";
                setFormEditingState(sourceForm, createSourceBtn, cancelSourceEditBtn, false, "导入来源");
                await refreshAll();
                setStatus(editId ? "来源已更新。" : "来源已导入，现在可以直接把它接到跟单策略。", false);
            } catch (error) {
                setStatus(error.message, true);
                setSourceUrlError(error.message);
            } finally {
                setButtonBusy(createSourceBtn, false, sourceForm && String(sourceForm.elements.edit_id.value || "").trim() ? "保存来源" : "导入来源");
            }
        });

        sourceForm.addEventListener("invalid", function (event) {
            if (event.target !== sourceForm.elements.source_url) {
                return;
            }
            setSourceUrlError("链接格式未通过浏览器校验：请检查粘贴内容是否夹带了多余文字（例如“抓取地址”标签）、中文标点，或缺少 http:// 前缀。");
        }, true);
    }

    if (cancelSourceEditBtn instanceof HTMLButtonElement && sourceForm instanceof HTMLFormElement) {
        cancelSourceEditBtn.addEventListener("click", function () {
            sourceForm.reset();
            sourceForm.elements.visibility.value = "private";
            setSourceUrlError("");
            setFormEditingState(sourceForm, createSourceBtn, cancelSourceEditBtn, false, "导入来源");
        });
    }

    if (sourceList instanceof HTMLElement) {
        sourceList.addEventListener("click", async function (event) {
            const target = event.target;
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (target.classList.contains("edit-source-btn")) {
                const sourceId = target.getAttribute("data-source-id");
                const source = sourceById(sourceId);
                if (!source) {
                    return;
                }
                loadSourceIntoForm(source);
                return;
            }
            if (target.classList.contains("archive-source-btn")) {
                const sourceId = target.getAttribute("data-source-id");
                if (!sourceId || !confirmDangerousAction("归档后该来源不会继续作为活跃来源参与新链路。确认继续吗？")) {
                    return;
                }
                try {
                    await updateEntityStatus("/api/platform/sources/", sourceId, "archived", "来源已归档。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target.classList.contains("delete-source-btn")) {
                const sourceId = target.getAttribute("data-source-id");
                if (!sourceId || !confirmDangerousAction("删除后将无法恢复。只有无跟单、无抓取记录、无标准信号的已归档来源才能删除。确认继续吗？")) {
                    return;
                }
                try {
                    await deleteEntity("/api/platform/sources/", sourceId, "来源已删除。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target.classList.contains("toggle-source-btn")) {
                const sourceId = target.getAttribute("data-source-id");
                const nextStatus = target.getAttribute("data-next-status");
                if (!sourceId || !nextStatus) {
                    return;
                }
                try {
                    const wasArchived = isArchivedItem(sourceById(sourceId));
                    await updateEntityStatus(
                        "/api/platform/sources/",
                        sourceId,
                        nextStatus,
                        wasArchived && nextStatus === "inactive" ? "来源已取消归档，当前状态为已停用。" : ("来源状态已更新为 " + statusText(nextStatus) + "。")
                    );
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (!target.classList.contains("source-pipeline-btn")) {
                return;
            }
            const sourceId = target.getAttribute("data-source-id");
            if (!sourceId) {
                return;
            }
            try {
                await runSourcePipeline(sourceById(sourceId), target);
            } catch (error) {
                setStatus(error.message, true);
            }
        });
    }

    if (targetForm) targetForm.elements.telegram_account_id.addEventListener("change", function (event) {
        const select = event.currentTarget;
        if (!(select instanceof HTMLSelectElement)) {
            return;
        }
        const account = accountById(select.value);
        if (!account) {
            return;
        }
        if (!String(targetForm.elements.target_name.value || "").trim()) {
            targetForm.elements.target_name.value = (account.label || "托管账号") + " 投递群";
        }
    });

    if (targetForm) targetForm.elements.target_key.addEventListener("input", function () {
        syncTargetKeyPreview();
    });

    if (targetForm) targetForm.elements.target_key.addEventListener("blur", function (event) {
        const input = event.currentTarget;
        if (!(input instanceof HTMLInputElement)) {
            return;
        }
        const rawValue = String(input.value || "").trim();
        if (!rawValue) {
            return;
        }
        try {
            const normalized = normalizeTelegramTargetKey(rawValue);
            input.value = normalized;
        } catch (error) {
            // keep user's raw input; errors will be shown on submit
        }
        syncTargetKeyPreview();
    });

    if (targetForm) targetForm.addEventListener("submit", async function (event) {
        event.preventDefault();
        const form = event.currentTarget;
        try {
            if (!state.currentUser) {
                throw new Error("请先登录");
            }
            const telegramAccountId = String(form.telegram_account_id.value || "").trim();
            if (!telegramAccountId) {
                throw new Error("请先选择托管账号");
            }
            const targetKey = normalizeTelegramTargetKey(form.target_key.value);
            form.target_key.value = targetKey;
            syncTargetKeyPreview();
            const editId = String(form.elements.edit_id.value || "").trim();
            const duplicated = state.targets.find(function (item) {
                return Number(item.telegram_account_id) === Number(telegramAccountId) && String(item.target_key || "") === targetKey && String(item.id) !== editId;
            });
            if (duplicated) {
                throw new Error("该账号下已存在相同目标 Key 的投递群组");
            }
            const submitButton = document.getElementById("createTargetBtn");
            const templateId = String(form.template_id.value || "").trim();
            setButtonBusy(submitButton, true, editId ? "保存中..." : "创建中...");
            if (editId) {
                await request("/api/platform/delivery-targets/" + editId, {
                    method: "POST",
                    body: {
                        telegram_account_id: Number(telegramAccountId),
                        executor_type: "telegram_group",
                        target_key: targetKey,
                        target_name: form.target_name.value,
                        template_id: templateId ? Number(templateId) : null,
                        dispatch_mode: form.dispatch_mode.value,
                    },
                });
            } else {
                await request("/api/platform/delivery-targets", {
                    method: "POST",
                    body: {
                        telegram_account_id: Number(telegramAccountId),
                        executor_type: "telegram_group",
                        target_key: targetKey,
                        target_name: form.target_name.value,
                        template_id: templateId ? Number(templateId) : null,
                        dispatch_mode: form.dispatch_mode.value,
                    },
                });
            }
            form.reset();
            setFormEditingState(form, submitButton, cancelTargetEditBtn, false, "新增投递群组");
            refreshTemplateSelects();
            await refreshAll();
            setStatus(editId ? "投递群组已更新。" : "投递群组已创建，当前为已停用，请先测试发送再启用。", false);
        } catch (error) {
            setStatus(error.message, true);
        } finally {
            setButtonBusy(document.getElementById("createTargetBtn"), false, "新增投递群组");
        }
    });

    if (targetCards instanceof HTMLElement) {
        targetCards.addEventListener("click", async function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.classList.contains("edit-target-btn")) {
                const targetId = target.getAttribute("data-target-id");
                const item = targetById(targetId);
                if (!item) {
                    return;
                }
                targetForm.elements.edit_id.value = String(item.id);
                targetForm.elements.telegram_account_id.value = item.telegram_account_id == null ? "" : String(item.telegram_account_id);
                refreshAccountSelects();
                targetForm.elements.telegram_account_id.value = item.telegram_account_id == null ? "" : String(item.telegram_account_id);
                refreshTemplateSelects();
                targetForm.elements.template_id.value = item.template_id == null ? "" : String(item.template_id);
                targetForm.elements.target_name.value = item.target_name || "";
                targetForm.elements.dispatch_mode.value = item.dispatch_mode || "shared";
                targetForm.elements.target_key.value = item.target_key || "";
                syncTargetKeyPreview();
                setFormEditingState(targetForm, document.getElementById("createTargetBtn"), cancelTargetEditBtn, true, "保存群组");
                targetForm.scrollIntoView({behavior: "smooth", block: "start"});
                return;
            }
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (target.classList.contains("test-target-btn")) {
                const targetId = target.getAttribute("data-target-id");
                const item = targetById(targetId);
                if (!targetId || !item) {
                    return;
                }
                try {
                    await performTargetTestSend(item, target, {
                        idleText: "测试发送",
                    });
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target.classList.contains("archive-target-btn")) {
                const targetId = target.getAttribute("data-target-id");
                if (!targetId || !confirmDangerousAction("归档后该投递群组不会再接收自动投注任务；如需彻底删除，需在归档后再手动删除。确认继续吗？")) {
                    return;
                }
                try {
                    await updateEntityStatus("/api/platform/delivery-targets/", targetId, "archived", "投递群组已归档。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target.classList.contains("delete-target-btn")) {
                const targetId = target.getAttribute("data-target-id");
                if (!targetId || !confirmDangerousAction("删除后将无法恢复。只有无执行记录的已归档投递群组才能被删除。确认继续吗？")) {
                    return;
                }
                try {
                    await deleteEntity("/api/platform/delivery-targets/", targetId, "投递群组已删除。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (!target.classList.contains("toggle-target-btn")) {
                return;
            }
            const targetId = target.getAttribute("data-target-id");
            const nextStatus = target.getAttribute("data-next-status");
            if (!targetId || !nextStatus) {
                return;
            }
            try {
                const wasArchived = isArchivedItem(targetById(targetId));
                await updateEntityStatus(
                    "/api/platform/delivery-targets/",
                    targetId,
                    nextStatus,
                    wasArchived && nextStatus === "inactive" ? "投递群组已取消归档，当前状态为已停用。" : ("投递群组状态已更新为 " + statusText(nextStatus) + "。")
                );
            } catch (error) {
                setStatus(error.message, true);
            }
        });
    }

    if (cancelTargetEditBtn) cancelTargetEditBtn.addEventListener("click", function () {
        targetForm.reset();
        refreshAccountSelects();
        refreshTemplateSelects();
        setFormEditingState(targetForm, document.getElementById("createTargetBtn"), cancelTargetEditBtn, false, "新增投递群组");
        syncTargetKeyPreview();
    });

    if (messageTemplateForm instanceof HTMLFormElement) {
        messageTemplateForm.addEventListener("submit", async function (event) {
            event.preventDefault();
            const form = event.currentTarget;
            try {
                if (!state.currentUser) {
                    throw new Error("请先登录");
                }
                const editId = String(form.elements.edit_id.value || "").trim();
                const payload = {
                    name: form.elements.name.value,
                    lottery_type: form.elements.lottery_type.value,
                    bet_type: "*",
                    template_text: form.elements.template_text.value,
                    config: parseTemplateConfigText(form.elements.config.value),
                    status: String(form.elements.status.value || "active").trim() || "active",
                };
                const submitButton = document.getElementById("createMessageTemplateBtn");
                setButtonBusy(submitButton, true, editId ? "保存中..." : "创建中...");
                if (editId) {
                    await request("/api/platform/message-templates/" + editId, {
                        method: "POST",
                        body: payload,
                    });
                } else {
                    await request("/api/platform/message-templates", {
                        method: "POST",
                        body: payload,
                    });
                }
                form.reset();
                form.elements.status.value = "active";
                form.elements.template_text.value = "{{bet_value}}{{amount}}";
                applyTemplateConfigToForm(pc28TemplateExampleConfig());
                setFormEditingState(form, submitButton, cancelMessageTemplateEditBtn, false, "新增模板");
                await refreshAll();
                setStatus(editId ? "下注模板已更新。" : "下注模板已创建。", false);
            } catch (error) {
                setStatus(error.message, true);
            } finally {
                setButtonBusy(document.getElementById("createMessageTemplateBtn"), false, "新增模板");
            }
        });
    }

    if (messageTemplateCards instanceof HTMLElement) {
        messageTemplateCards.addEventListener("click", async function (event) {
            const target = event.target;
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (target.classList.contains("edit-template-btn")) {
                const templateId = target.getAttribute("data-template-id");
                const item = templateById(templateId);
                if (!item || !(messageTemplateForm instanceof HTMLFormElement)) {
                    return;
                }
                messageTemplateForm.elements.edit_id.value = String(item.id);
                messageTemplateForm.elements.name.value = item.name || "";
                messageTemplateForm.elements.lottery_type.value = item.lottery_type || "pc28";
                messageTemplateForm.elements.status.value = String(item.status || "active").trim() || "active";
                messageTemplateForm.elements.template_text.value = item.template_text || "{{bet_value}}{{amount}}";
                applyTemplateConfigToForm(item.config || {});
                setFormEditingState(messageTemplateForm, document.getElementById("createMessageTemplateBtn"), cancelMessageTemplateEditBtn, true, "保存模板");
                messageTemplateForm.scrollIntoView({behavior: "smooth", block: "start"});
                return;
            }
            if (target.classList.contains("duplicate-template-btn")) {
                const templateId = target.getAttribute("data-template-id");
                const item = templateById(templateId);
                if (!item || !(messageTemplateForm instanceof HTMLFormElement)) {
                    return;
                }
                messageTemplateForm.elements.edit_id.value = "";
                messageTemplateForm.elements.name.value = (item.name || "模板") + " 副本";
                messageTemplateForm.elements.lottery_type.value = item.lottery_type || "pc28";
                messageTemplateForm.elements.status.value = "inactive";
                messageTemplateForm.elements.template_text.value = item.template_text || "{{bet_value}}{{amount}}";
                applyTemplateConfigToForm(cloneJson(item.config || {}));
                setFormEditingState(messageTemplateForm, document.getElementById("createMessageTemplateBtn"), cancelMessageTemplateEditBtn, false, "新增模板");
                messageTemplateForm.scrollIntoView({behavior: "smooth", block: "start"});
                if (window.PlatformNavigation) window.PlatformNavigation.markDirty(messageTemplateForm);
                setStatus("已复制模板到表单，建议先以草稿保存。", false);
                return;
            }
            if (target.classList.contains("archive-template-btn")) {
                const templateId = target.getAttribute("data-template-id");
                if (!templateId || !confirmDangerousAction("归档后该下注模板不会继续用于新配置。确认继续吗？")) {
                    return;
                }
                try {
                    await updateEntityStatus("/api/platform/message-templates/", templateId, "archived", "下注模板已归档。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (!target.classList.contains("toggle-template-btn")) {
                return;
            }
            const templateId = target.getAttribute("data-template-id");
            const nextStatus = target.getAttribute("data-next-status");
            if (!templateId || !nextStatus) {
                return;
            }
            try {
                await updateEntityStatus(
                    "/api/platform/message-templates/",
                    templateId,
                    nextStatus,
                    "下注模板状态已更新为 " + statusText(nextStatus) + "。"
                );
            } catch (error) {
                setStatus(error.message, true);
            }
        });
    }

    if (cancelMessageTemplateEditBtn instanceof HTMLButtonElement) {
        cancelMessageTemplateEditBtn.addEventListener("click", function () {
            if (!(messageTemplateForm instanceof HTMLFormElement)) {
                return;
            }
            messageTemplateForm.reset();
            messageTemplateForm.elements.status.value = "active";
            messageTemplateForm.elements.template_text.value = "{{bet_value}}{{amount}}";
            applyTemplateConfigToForm(pc28TemplateExampleConfig());
            setFormEditingState(messageTemplateForm, document.getElementById("createMessageTemplateBtn"), cancelMessageTemplateEditBtn, false, "新增模板");
        });
    }

    const fillPc28TemplateBtn = document.getElementById("fillPc28TemplateBtn");
    if (fillPc28TemplateBtn instanceof HTMLButtonElement) {
        fillPc28TemplateBtn.addEventListener("click", function () {
            if (!(messageTemplateForm instanceof HTMLFormElement)) {
                return;
            }
            if (!String(messageTemplateForm.elements.name.value || "").trim()) {
                messageTemplateForm.elements.name.value = "加拿大28高倍模板";
            }
            messageTemplateForm.elements.lottery_type.value = "pc28";
            messageTemplateForm.elements.status.value = "active";
            messageTemplateForm.elements.template_text.value = "{{bet_value}}{{amount}}";
            applyTemplateConfigToForm(pc28TemplateExampleConfig());
            if (templatePreviewBetType instanceof HTMLInputElement) {
                templatePreviewBetType.value = "big_small";
            }
            if (templatePreviewBetValue instanceof HTMLInputElement) {
                templatePreviewBetValue.value = "大";
            }
            if (templatePreviewAmount instanceof HTMLInputElement) {
                templatePreviewAmount.value = "10";
            }
            if (templatePreviewIssueNo instanceof HTMLInputElement) {
                templatePreviewIssueNo.value = "20260410001";
            }
            if (templatePreviewPayload instanceof HTMLTextAreaElement) {
                templatePreviewPayload.value = "";
            }
            syncTemplatePreview();
            setStatus("已填充加拿大28高倍示例模板。", false);
        });
    }

    templatePresetButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            applyTemplatePreset(button.getAttribute("data-template-preset"));
        });
    });

    templateSampleButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            applyTemplateSample(button.getAttribute("data-template-sample"));
        });
    });

    if (toggleTemplateJsonBtn instanceof HTMLButtonElement) {
        toggleTemplateJsonBtn.addEventListener("click", function () {
            setTemplateJsonAdvancedVisible(!templateJsonAdvancedVisible);
        });
    }

    if (addCustomTemplateRuleBtn instanceof HTMLButtonElement) {
        addCustomTemplateRuleBtn.addEventListener("click", function () {
            captureTemplateBuilderCustomRulesFromDom();
            templateBuilderCustomRules.push(createTemplateCustomRuleState("", {}));
            renderTemplateRuleBuilder(buildTemplateConfigFromBuilder(), {preserveCustomRules: true});
            syncTemplateConfigFromBuilder();
        });
    }

    if (templateRuleBuilder instanceof HTMLElement) {
        templateRuleBuilder.addEventListener("click", function (event) {
            const target = event.target;
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (target.hasAttribute("data-template-custom-remove-rule")) {
                captureTemplateBuilderCustomRulesFromDom();
                const uid = target.getAttribute("data-template-custom-remove-rule");
                templateBuilderCustomRules = templateBuilderCustomRules.filter(function (rule) {
                    return rule.uid !== uid;
                });
                renderTemplateRuleBuilder(buildTemplateConfigFromBuilder(), {preserveCustomRules: true});
                syncTemplateConfigFromBuilder();
                return;
            }
            if (target.hasAttribute("data-template-custom-add-mapping")) {
                captureTemplateBuilderCustomRulesFromDom();
                const uid = target.getAttribute("data-template-custom-add-mapping");
                templateBuilderCustomRules = templateBuilderCustomRules.map(function (rule) {
                    if (rule.uid !== uid) {
                        return rule;
                    }
                    return {
                        uid: rule.uid,
                        betType: rule.betType,
                        format: rule.format,
                        mappings: rule.mappings.concat([{source: "", target: ""}]),
                    };
                });
                renderTemplateRuleBuilder(buildTemplateConfigFromBuilder(), {preserveCustomRules: true});
                syncTemplateConfigFromBuilder();
                return;
            }
            if (target.hasAttribute("data-template-custom-remove-mapping")) {
                captureTemplateBuilderCustomRulesFromDom();
                const uid = target.getAttribute("data-template-custom-remove-mapping");
                const removeIndex = Number(target.getAttribute("data-template-custom-remove-index"));
                templateBuilderCustomRules = templateBuilderCustomRules.map(function (rule) {
                    if (rule.uid !== uid) {
                        return rule;
                    }
                    const nextMappings = rule.mappings.filter(function (_item, index) {
                        return index !== removeIndex;
                    });
                    return {
                        uid: rule.uid,
                        betType: rule.betType,
                        format: rule.format,
                        mappings: nextMappings.length ? nextMappings : [{source: "", target: ""}],
                    };
                });
                renderTemplateRuleBuilder(buildTemplateConfigFromBuilder(), {preserveCustomRules: true});
                syncTemplateConfigFromBuilder();
            }
        });
        templateRuleBuilder.addEventListener("input", function () {
            syncTemplateConfigFromBuilder();
        });
    }

    [
        messageTemplateForm && messageTemplateForm.elements ? messageTemplateForm.elements.template_text : null,
        templatePreviewBetType,
        templatePreviewBetValue,
        templatePreviewAmount,
        templatePreviewIssueNo,
        templatePreviewPayload,
    ].forEach(function (field) {
        if (field instanceof HTMLElement) {
            field.addEventListener("input", function () {
                syncTemplatePreview();
            });
        }
    });

    if (messageTemplateForm instanceof HTMLFormElement) {
        messageTemplateForm.elements.config.addEventListener("input", function () {
            syncTemplatePreview();
        });
        messageTemplateForm.elements.config.addEventListener("blur", function () {
            try {
                renderTemplateRuleBuilder(parseTemplateConfigText(messageTemplateForm.elements.config.value));
            } catch (error) {
                // keep current builder state until JSON becomes valid
            }
        });
    }

    if (subscriptionForm) subscriptionForm.addEventListener("submit", async function (event) {
        event.preventDefault();
        const form = event.currentTarget;
        try {
            if (!state.currentUser) {
                throw new Error("请先登录");
            }
            // 必填预检：浏览器原生气泡不显眼，漏填时明确提示并滚动到对应字段
            const firstInvalidField = form.querySelector(":invalid");
            if (firstInvalidField instanceof HTMLElement) {
                const labelElement = firstInvalidField.closest("label");
                const labelText = labelElement
                    ? ((labelElement.querySelector("span") || {}).textContent || "").trim()
                    : "";
                firstInvalidField.scrollIntoView({ behavior: "smooth", block: "center" });
                try {
                    firstInvalidField.focus({ preventScroll: true });
                } catch (focusError) {
                    // 部分控件不可聚焦，忽略
                }
                throw new Error(
                    labelText
                        ? "还有必填项未完成：「" + labelText + "」，请在表单中填写后再点新建。"
                        : "还有必填项未完成，请在表单中检查后重试。"
                );
            }
            const sourceId = String(form.source_id.value || "").trim();
            if (!sourceId) {
                throw new Error("请先选择方案来源");
            }
            const editId = String(form.elements.edit_id.value || "").trim();
            // 同一来源允许建多个不同玩法的方案（如普通跟单 + 过关滚仓），不再拦截；
            // 同期同一群的双注由派单层的占位保护兜底。

            const betFilterMode = currentSubscriptionBetFilterMode();
            const selectedBetFilterKeys = selectedSubscriptionBetFilterKeys();
            const genericRoll = currentSubscriptionStrategyMode() === "win_streak_roll" && form.roll_structure.value === "single_play";
            if (!genericRoll && betFilterMode === "selected" && !selectedBetFilterKeys.length) {
                throw new Error("自选玩法模式下，请至少勾选一个玩法");
            }
            const strategyMode = currentSubscriptionStrategyMode();
            if (strategyMode === "win_streak_roll" && !genericRoll && selectedBetFilterKeys.some(function (key) { return key.startsWith("double_group:") || key.startsWith("kill_group:"); })) {
                throw new Error("双组、杀组请使用均注、来源金额或倍投，不能与过关结构同时使用");
            }
            const subscriptionName = String(form.subscription_name.value || "").trim();
            const stakeAmount = strategyMode === "fixed" ? Number(form.stake_amount.value || 0) : null;
            const baseStake = strategyMode === "martingale" ? Number(form.base_stake.value || 0) : null;
            const multiplier = strategyMode === "martingale" ? Number(form.multiplier.value || 0) : null;
            const maxSteps = strategyMode === "martingale" ? Number(form.max_steps.value || 0) : null;
            const targetStreak = strategyMode === "win_streak_roll" ? Number(form.target_streak.value || 0) : null;
            const baseAmount = strategyMode === "win_streak_roll" ? Number(form.base_amount.value || 0) : null;
            const rollStructure = strategyMode === "win_streak_roll"
                ? String(form.roll_structure.value || "A").trim()
                : "A";
            const rollMode = strategyMode === "win_streak_roll"
                ? String(form.roll_mode.value || "chain").trim()
                : "chain";
            const rollOnTarget = strategyMode === "win_streak_roll"
                ? String(form.roll_on_target.value || "close_day").trim()
                : "close_day";
            const rollOnBust = strategyMode === "win_streak_roll"
                ? String(form.roll_on_bust.value || "close_day").trim()
                : "close_day";
            const rollCloseScope = strategyMode === "win_streak_roll"
                ? String(form.roll_close_scope.value || "rule").trim()
                : "rule";
            const rollMaxCycles = strategyMode === "win_streak_roll"
                ? Number(form.roll_max_cycles.value || 1)
                : 1;
            const refundAction = strategyMode === "martingale"
                ? String(form.refund_action.value || "hold").trim() || "hold"
                : "hold";
            const capAction = strategyMode === "martingale"
                ? String(form.cap_action.value || "reset").trim() || "reset"
                : "reset";
            const expireAfterSeconds = Number(form.expire_after_seconds.value || 120);
            const settlementRuleSource = currentSubscriptionSettlementRuleSource();
            const settlementRuleId = settlementRuleSource === "subscription_fixed"
                ? String(form.settlement_rule_id.value || "").trim()
                : "";
            const oddsOverrides = settlementRuleSource === "subscription_fixed"
                ? collectSubscriptionOddsOverrides(settlementRuleId)
                : {};
            const fallbackProfitRatio = Number(form.fallback_profit_ratio.value || 0);
            const riskControlEnabled = subscriptionRiskControlEnabledCheckbox instanceof HTMLInputElement
                && subscriptionRiskControlEnabledCheckbox.checked;
            const profitTarget = String(form.profit_target.value || "").trim();
            const lossLimit = String(form.loss_limit.value || "").trim();
            const hitRateGateEnabled = form.risk_hit_rate_gate_enabled instanceof HTMLInputElement
                && form.risk_hit_rate_gate_enabled.checked;
            const hitRateThreshold = Number(form.risk_hit_rate_threshold.value || 0);
            const hitRateMinSettled = Number(form.risk_hit_rate_min_settled.value || 20);
            if (hitRateGateEnabled && !(hitRateThreshold > 0 && hitRateThreshold < 100)) {
                throw new Error("启用当日命中率熔断时，阈值必须为 0~100 之间的百分比");
            }
            if (!subscriptionName) {
                throw new Error("请给跟单策略起个名称，方便在列表里区分不同方案");
            }
            if (strategyMode === "fixed" && (!Number.isFinite(stakeAmount) || stakeAmount <= 0)) {
                throw new Error("均注模式下，请填写大于 0 的固定金额");
            }
            if (strategyMode === "win_streak_roll") {
                if (!Number.isInteger(targetStreak) || targetStreak < 1) {
                    throw new Error("过关滚仓模式下，过几关收工必须是至少 1 的整数");
                }
                if (!Number.isFinite(baseAmount) || baseAmount <= 0) {
                    throw new Error("过关滚仓模式下，请填写大于 0 的每轮起始注");
                }
                if (!["A", "B", "C", "D", "follow_signal", "single_play"].includes(rollStructure)) {
                    throw new Error("过关滚仓模式下，请选择押哪种组合");
                }
                if (!["chain", "all_in"].includes(rollMode)) {
                    throw new Error("过关滚仓模式下，滚仓方式不正确");
                }
                if (!["close_day", "new_cycle", "continue"].includes(rollOnTarget)) {
                    throw new Error("过关滚仓模式下，达标后的处理方式不正确");
                }
                if (!["close_day", "new_cycle"].includes(rollOnBust)) {
                    throw new Error("过关滚仓模式下，爆仓后的处理方式不正确");
                }
                if (!["rule", "route"].includes(rollCloseScope)) {
                    throw new Error("过关滚仓模式下，收工停哪一层不正确");
                }
                if (!Number.isInteger(rollMaxCycles) || rollMaxCycles < 1) {
                    throw new Error("过关滚仓模式下，一天最多几轮必须是至少 1 的整数");
                }
            }
            if (strategyMode === "martingale") {
                if (!Number.isFinite(baseStake) || baseStake <= 0) {
                    throw new Error("倍投模式下，请填写大于 0 的起始金额");
                }
                if (!Number.isFinite(multiplier) || multiplier <= 1) {
                    throw new Error("倍投模式下，倍数必须大于 1");
                }
                if (!Number.isInteger(maxSteps) || maxSteps < 2) {
                    throw new Error("倍投模式下，最多追几手至少为 2");
                }
                if (!["hold", "reset"].includes(refundAction)) {
                    throw new Error("回本后的处理方式不正确");
                }
                if (!["hold", "reset"].includes(capAction)) {
                    throw new Error("追顶后的处理方式不正确");
                }
            }
            if (!["follow_signal", "subscription_fixed"].includes(settlementRuleSource)) {
                throw new Error("收益结算规则来源不正确");
            }
            if (settlementRuleSource === "subscription_fixed" && !settlementRuleId) {
                throw new Error("固定结算规则时，请选择一套规则");
            }
            if (!Number.isFinite(fallbackProfitRatio) || fallbackProfitRatio <= 0) {
                throw new Error("兜底净利倍数必须大于 0");
            }
            const submitButton = document.getElementById("createSubscriptionBtn");
            setButtonBusy(submitButton, true, editId ? "保存中..." : "创建中...");
            const strategyPayload = {
                name: subscriptionName,
                play_filter: {
                    mode: genericRoll ? "all" : betFilterMode,
                    selected_keys: genericRoll ? [] : selectedBetFilterKeys,
                },
                staking_policy: {
                    mode: strategyMode,
                    fixed_amount: stakeAmount,
                    base_stake: baseStake,
                    multiplier: multiplier,
                    max_steps: maxSteps,
                    refund_action: refundAction,
                    cap_action: capAction,
                    ...(strategyMode === "win_streak_roll" ? {
                        base_amount: baseAmount,
                        main_share: 0.70,
                        aux_share: 0.30,
                        target_streak: targetStreak,
                        structure: rollStructure,
                        ...(genericRoll ? {play_type: form.roll_play_type.value, selection_mode: form.roll_selection_mode.value, fixed_value: form.roll_selection_mode.value === "follow" ? null : form.roll_fixed_value.value} : {}),
                        roll_mode: rollMode,
                        reset_scope: "daily",
                        stop_policy: {
                            on_target: rollOnTarget,
                            on_bust: rollOnBust,
                            close_scope: rollCloseScope,
                            max_cycles_per_day: rollMaxCycles,
                        },
                    } : {}),
                },
                settlement_policy: {
                    rule_source: settlementRuleSource,
                    settlement_rule_id: settlementRuleSource === "subscription_fixed" ? settlementRuleId : null,
                    fallback_profit_ratio: fallbackProfitRatio,
                    ...(settlementRuleSource === "subscription_fixed" ? {odds_overrides: oddsOverrides} : {}),
                },
                risk_control: {
                    enabled: riskControlEnabled,
                    profit_target: profitTarget ? Number(profitTarget) : 0,
                    loss_limit: lossLimit ? Number(lossLimit) : 0,
                    play_limits: subscriptionRiskPlayLimitsFromForm(form),
                    hit_rate_gate: {
                        enabled: hitRateGateEnabled,
                        threshold: hitRateGateEnabled ? hitRateThreshold : 0,
                        min_settled_count: hitRateMinSettled > 0 ? hitRateMinSettled : 20,
                    },
                },
                dispatch: {
                    expire_after_seconds: expireAfterSeconds,
                    delivery_target_ids: selectedSubscriptionTargetIds(),
                },
            };
            if (!strategyPayload.dispatch.delivery_target_ids.length) {
                throw new Error("请至少选择一个发送群组");
            }
            let savedSubscription;
            if (editId) {
                savedSubscription = await request("/api/platform/subscriptions/" + editId, {
                    method: "POST",
                    body: {
                        source_id: Number(sourceId),
                        strategy_v2: strategyPayload,
                        require_delivery_targets: true,
                    },
                });
            } else {
                savedSubscription = await request("/api/platform/subscriptions", {
                    method: "POST",
                    body: {
                        source_id: Number(sourceId),
                        strategy_v2: strategyPayload,
                        require_delivery_targets: true,
                    },
                });
            }
            form.reset();
            setFormEditingState(form, submitButton, cancelSubscriptionEditBtn, false, "保存方案");
            resetSubscriptionStrategyFormState();
            await refreshAll();
            if (!editId) showSavedSubscriptionActions(savedSubscription.item);
            setStatus(editId ? "方案配置已保存，启动状态和在途投注保持原样。" : "方案已保存，尚未启动。请选择自动规则或直接跟单。", false);
        } catch (error) {
            setStatus(error.message, true);
        } finally {
            setButtonBusy(document.getElementById("createSubscriptionBtn"), false, "保存方案");
        }
    });

    if (subscriptionCards instanceof HTMLElement) {
        subscriptionCards.addEventListener("click", async function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.classList.contains("subscription-detail-toggle-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                if (!subscriptionId) {
                    return;
                }
                state.expandedSubscriptionId = String(state.expandedSubscriptionId || "") === String(subscriptionId)
                    ? null
                    : String(subscriptionId);
                renderSubscriptionCards();
                return;
            }
            if (target instanceof HTMLElement && target.classList.contains("subscription-detail-tab")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                const tabKey = target.getAttribute("data-subscription-detail-tab");
                if (!subscriptionId || !tabKey) {
                    return;
                }
                state.expandedSubscriptionId = String(subscriptionId);
                if (window.PlatformNavigation) window.PlatformNavigation.setQuery({subscription_id: subscriptionId});
                state.subscriptionDetailTabs[String(subscriptionId)] = String(tabKey);
                if (String(tabKey) === "runtime" && !state.subscriptionRuntimeHistories[String(subscriptionId)]) {
                    try {
                        setButtonBusy(target, true, "加载中...");
                        const payload = await request("/api/platform/subscriptions/" + subscriptionId + "/runtime-runs?limit=20");
                        state.subscriptionRuntimeHistories[String(subscriptionId)] = {
                            items: payload.items || [],
                        };
                    } catch (error) {
                        setStatus(error.message, true);
                    } finally {
                        setButtonBusy(target, false, "轮次历史");
                    }
                }
                renderSubscriptionCards();
                return;
            }
            if (target instanceof HTMLElement && target.classList.contains("load-subscription-all-days-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                if (!subscriptionId) {
                    return;
                }
                try {
                    setButtonBusy(target, true, "加载中...");
                    const payload = await request("/api/platform/subscriptions/" + subscriptionId + "/daily-stats?limit=365");
                    state.subscriptionAllDailyHistories[String(subscriptionId)] = {
                        items: payload.items || [],
                    };
                    state.expandedSubscriptionId = String(subscriptionId);
                if (window.PlatformNavigation) window.PlatformNavigation.setQuery({subscription_id: subscriptionId});
                    state.subscriptionDetailTabs[String(subscriptionId)] = "profit";
                    renderSubscriptionCards();
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    setButtonBusy(target, false, "查看全部天数");
                }
                return;
            }
            if (target instanceof HTMLElement && target.classList.contains("edit-subscription-btn")) {
                const savedActions = document.getElementById('subscriptionSavedActions');
                if (savedActions) savedActions.hidden = true;
                const subscriptionId = target.getAttribute("data-subscription-id");
                const item = subscriptionById(subscriptionId);
                if (!item) {
                    return;
                }
                subscriptionForm.elements.edit_id.value = String(item.id);
                subscriptionForm.elements.source_id.value = String(item.source_id);
                const betFilter = item.strategy && item.strategy.bet_filter && typeof item.strategy.bet_filter === "object"
                    ? item.strategy.bet_filter
                    : {};
                const betFilterMode = normalizeSubscriptionBetFilterMode(betFilter.mode);
                if (subscriptionBetFilterModeSelect instanceof HTMLSelectElement) {
                    subscriptionBetFilterModeSelect.value = betFilterMode;
                }
                Array.from(subscriptionForm.querySelectorAll("input[name='bet_filter_key']")).forEach(function (input) {
                    if (!(input instanceof HTMLInputElement)) {
                        return;
                    }
                    input.checked = Array.isArray(betFilter.selected_keys) && betFilter.selected_keys.includes(input.value);
                });
                const strategyMode = subscriptionStrategyModeFromStrategy(item.strategy);
                const strategyV2 = item.strategy_v2 && typeof item.strategy_v2 === "object"
                    ? item.strategy_v2
                    : {};
                const settlementPolicy = strategyV2.settlement_policy && typeof strategyV2.settlement_policy === "object"
                    ? strategyV2.settlement_policy
                    : {};
                const hasStakeAmount = item.strategy && item.strategy.stake_amount != null && String(item.strategy.stake_amount).trim() !== "";
                if (subscriptionStrategyModeSelect instanceof HTMLSelectElement) {
                    subscriptionStrategyModeSelect.value = strategyMode;
                }
                subscriptionForm.elements.subscription_name.value = String(item.name || "");
                subscriptionForm.elements.stake_amount.value = hasStakeAmount ? String(item.strategy.stake_amount) : "";
                subscriptionForm.elements.base_stake.value = item.strategy && item.strategy.base_stake != null && String(item.strategy.base_stake).trim() !== ""
                    ? String(item.strategy.base_stake)
                    : "10";
                const rollSettings = item.strategy && item.strategy.win_streak_roll && typeof item.strategy.win_streak_roll === "object"
                    ? item.strategy.win_streak_roll
                    : null;
                subscriptionForm.elements.target_streak.value = rollSettings && rollSettings.target_streak != null
                    ? String(rollSettings.target_streak)
                    : "3";
                subscriptionForm.elements.base_amount.value = rollSettings && rollSettings.base_amount != null
                    ? String(rollSettings.base_amount)
                    : "100";
                subscriptionForm.elements.roll_structure.value = rollSettings && rollSettings.structure
                    ? String(rollSettings.structure)
                    : "A";
                if (subscriptionForm.elements.roll_play_type) {
                    subscriptionForm.elements.roll_play_type.value = rollSettings?.play_type || 'big_small';
                    subscriptionForm.elements.roll_selection_mode.value = rollSettings?.selection_mode || 'follow';
                    syncGenericRollPlayUI();
                    if (rollSettings?.fixed_value != null) subscriptionForm.elements.roll_fixed_value.value = String(rollSettings.fixed_value);
                }
                subscriptionForm.elements.roll_mode.value = rollSettings && rollSettings.roll_mode
                    ? String(rollSettings.roll_mode)
                    : "chain";
                const rollStopPolicy = rollSettings && rollSettings.stop_policy && typeof rollSettings.stop_policy === "object"
                    ? rollSettings.stop_policy
                    : {};
                subscriptionForm.elements.roll_on_target.value = ["close_day", "new_cycle", "continue"].includes(rollStopPolicy.on_target)
                    ? String(rollStopPolicy.on_target)
                    : "close_day";
                subscriptionForm.elements.roll_on_bust.value = ["close_day", "new_cycle"].includes(rollStopPolicy.on_bust)
                    ? String(rollStopPolicy.on_bust)
                    : "close_day";
                subscriptionForm.elements.roll_close_scope.value = ["rule", "route"].includes(rollStopPolicy.close_scope)
                    ? String(rollStopPolicy.close_scope)
                    : "rule";
                subscriptionForm.elements.roll_max_cycles.value = Number(rollStopPolicy.max_cycles_per_day || 0) >= 1
                    ? String(rollStopPolicy.max_cycles_per_day)
                    : "1";
                subscriptionForm.elements.multiplier.value = item.strategy && item.strategy.multiplier != null && String(item.strategy.multiplier).trim() !== ""
                    ? String(item.strategy.multiplier)
                    : "2";
                subscriptionForm.elements.max_steps.value = item.strategy && item.strategy.max_steps != null && String(item.strategy.max_steps).trim() !== ""
                    ? String(item.strategy.max_steps)
                    : "3";
                subscriptionForm.elements.refund_action.value = item.strategy && String(item.strategy.refund_action || "hold").trim() === "reset"
                    ? "reset"
                    : "hold";
                subscriptionForm.elements.cap_action.value = item.strategy && String(item.strategy.cap_action || "reset").trim() === "hold"
                    ? "hold"
                    : "reset";
                subscriptionForm.elements.expire_after_seconds.value = String(item.strategy && item.strategy.expire_after_seconds ? item.strategy.expire_after_seconds : 120);
                const riskControl = item.strategy && item.strategy.risk_control && typeof item.strategy.risk_control === "object"
                    ? item.strategy.risk_control
                    : {};
                subscriptionForm.elements.profit_target.value = riskControl.profit_target != null && Number(riskControl.profit_target) > 0
                    ? String(riskControl.profit_target)
                    : "";
                subscriptionForm.elements.loss_limit.value = riskControl.loss_limit != null && Number(riskControl.loss_limit) > 0
                    ? String(riskControl.loss_limit)
                    : "";
                applySubscriptionRiskPlayLimits(riskControl.play_limits);
                const editGate = riskControl.hit_rate_gate && typeof riskControl.hit_rate_gate === "object"
                    ? riskControl.hit_rate_gate
                    : {};
                if (subscriptionForm.elements.risk_hit_rate_gate_enabled instanceof HTMLInputElement) {
                    subscriptionForm.elements.risk_hit_rate_gate_enabled.checked = Boolean(editGate.enabled);
                }
                subscriptionForm.elements.risk_hit_rate_threshold.value = Number(editGate.threshold || 0) > 0
                    ? String(editGate.threshold)
                    : "";
                subscriptionForm.elements.risk_hit_rate_min_settled.value = Number(editGate.min_settled_count || 0) > 0
                    ? String(editGate.min_settled_count)
                    : "";
                if (subscriptionSettlementRuleSourceSelect instanceof HTMLSelectElement) {
                    subscriptionSettlementRuleSourceSelect.value = normalizeSubscriptionSettlementRuleSource(settlementPolicy.rule_source);
                }
                if (subscriptionForm.elements.settlement_rule_id instanceof HTMLSelectElement) {
                    subscriptionForm.elements.settlement_rule_id.value = String(settlementPolicy.settlement_rule_id || "pc28_netdisk_regular");
                }
                fillSubscriptionOddsInputs(
                    String(settlementPolicy.settlement_rule_id || "pc28_netdisk_regular"),
                    settlementPolicy.odds_overrides
                );
                refreshSubscriptionTargetSelects();
                setSubscriptionTargetSelection(subscriptionDispatchTargetIds(item));
                syncSubscriptionTargetHint();
                subscriptionForm.elements.fallback_profit_ratio.value = String(
                    settlementPolicy.fallback_profit_ratio != null
                        ? settlementPolicy.fallback_profit_ratio
                        : (riskControl.win_profit_ratio != null ? riskControl.win_profit_ratio : 1)
                );
                if (subscriptionRiskControlEnabledCheckbox instanceof HTMLInputElement) {
                    subscriptionRiskControlEnabledCheckbox.checked = Boolean(riskControl.enabled);
                }
                syncSubscriptionBetFilterUI();
                syncSubscriptionPresetUI();
                syncSubscriptionSettlementUI();
                syncSubscriptionRiskControlUI();
                const shouldExpandAdvanced = Number(item.strategy && item.strategy.expire_after_seconds || 120) !== 120
                    || Boolean(riskControl.enabled)
                    || normalizeSubscriptionSettlementRuleSource(settlementPolicy.rule_source) === "subscription_fixed"
                    || Number(settlementPolicy.fallback_profit_ratio != null ? settlementPolicy.fallback_profit_ratio : (riskControl.win_profit_ratio != null ? riskControl.win_profit_ratio : 1)) !== 1
                    || (strategyMode === "martingale"
                        && (
                            String(item.strategy && item.strategy.refund_action || "hold") !== "hold"
                            || String(item.strategy && item.strategy.cap_action || "reset") !== "reset"
                        ));
                setSubscriptionAdvancedVisible(shouldExpandAdvanced);
                setFormEditingState(subscriptionForm, document.getElementById("createSubscriptionBtn"), cancelSubscriptionEditBtn, true, "保存策略");
                subscriptionForm.scrollIntoView({behavior: "smooth", block: "start"});
                return;
            }
            if (target instanceof HTMLElement && target.classList.contains("settle-progression-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                const progressionEventId = target.getAttribute("data-progression-event-id");
                const resultType = target.getAttribute("data-result-type");
                if (!subscriptionId || !resultType) {
                    return;
                }
                try {
                    await request("/api/platform/subscriptions/" + subscriptionId + "/progression/settle", {
                        method: "POST",
                        body: {
                            progression_event_id: progressionEventId ? Number(progressionEventId) : null,
                            result_type: resultType,
                        },
                    });
                    const refreshed = subscriptionById(subscriptionId);
                    await refreshAll();
                    const nextItem = subscriptionById(subscriptionId) || refreshed;
                    setStatus(settlementStatusMessage(nextItem, "当前待结算记录已处理，"), false);
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target instanceof HTMLElement && target.classList.contains("resolve-progression-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                const progressionEventId = target.getAttribute("data-progression-event-id");
                if (!subscriptionId) {
                    return;
                }
                const settlementPanel = target.closest(".subscription-settlement-panel");
                const drawInputField = settlementPanel ? settlementPanel.querySelector("input[name='draw_input']") : null;
                const normalizedDrawInput = String(drawInputField && "value" in drawInputField ? drawInputField.value : "").trim();
                if (!normalizedDrawInput) {
                    setStatus("请输入开奖结果后再自动结算。", true);
                    return;
                }
                try {
                    setButtonBusy(target, true, "结算中...");
                    const payload = await request("/api/platform/subscriptions/" + subscriptionId + "/progression/resolve", {
                        method: "POST",
                        body: {
                            progression_event_id: progressionEventId ? Number(progressionEventId) : null,
                            draw_context: {
                                open_code: normalizedDrawInput,
                            },
                        },
                    });
                    const refreshed = subscriptionById(subscriptionId);
                    await refreshAll();
                    const nextItem = subscriptionById(subscriptionId) || refreshed;
                    const resolved = payload && payload.resolved && typeof payload.resolved === "object" ? payload.resolved : {};
                    const refundReason = String(resolved.refund_reason || "").trim();
                    const resultLabel = typeof resolved.prediction_hit === "boolean"
                        ? "预测" + (resolved.prediction_hit ? "命中" : "未中") + "，整组" + ({hit: "盈利", miss: "亏损", refund: "持平"})[resolved.result_type]
                        : progressionResultText(resolved.result_type);
                    const prefix = refundReason
                        ? ("自动结算完成，结果为" + resultLabel + "（" + refundReason + "），")
                        : ("自动结算完成，结果为" + resultLabel + "，");
                    setStatus(settlementStatusMessage(nextItem, prefix), false);
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    setButtonBusy(target, false, "自动结算");
                }
                return;
            }
            if (!(target instanceof HTMLElement)) {
                return;
            }
            if (target.classList.contains('copy-subscription-btn')) {
                const original = subscriptionById(target.dataset.subscriptionId);
                if (!original) return;
                target.disabled = true;
                try {
                    const saved = await request('/api/platform/subscriptions', {method:'POST', body:{source_id:original.source_id, name:(original.name || '方案') + ' 副本', strategy_v2:original.strategy_v2, require_delivery_targets:true}});
                    await refreshAll(); showSavedSubscriptionActions(saved.item);
                    setStatus('副本已保存为未启动，原方案保持原运行状态。', false);
                } catch (error) { setStatus(error.message, true); } finally { target.disabled = false; }
                return;
            }
            if (target.classList.contains("archive-subscription-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                if (!subscriptionId || !confirmDangerousAction("归档后该跟单策略不会再参与自动投注；如需彻底删除，需在归档后再手动删除。确认继续吗？")) {
                    return;
                }
                try {
                    await updateEntityStatus("/api/platform/subscriptions/", subscriptionId, "archived", "跟单策略已归档。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target.classList.contains("delete-subscription-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                if (!subscriptionId || !confirmDangerousAction("策略配置删除后无法恢复，已结算的历史盈亏会保留。请先归档并处理完待执行、待结算投注。确认删除吗？")) {
                    return;
                }
                try {
                    await deleteEntity("/api/platform/subscriptions/", subscriptionId, "跟单策略已删除，历史盈亏已保留。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target.classList.contains("reset-subscription-runtime-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                const currentSubscription = subscriptionById(subscriptionId);
                const isRollPlan = isWinStreakRollSubscription(currentSubscription);
                const isRestartingCycle = isSubscriptionRiskBlocked(currentSubscription) || isRollPlan;
                const confirmMessage = isRollPlan
                    ? "手动开始新一轮会重置手动过关周期（回到起始注、连赢次数清零），解除手动当天收工。自动规则的过关状态、日次数和历史盈亏保持不变。确认继续吗？"
                    : (isRestartingCycle
                        ? "手动开始新一轮会重置手动轮次盈亏基线、回到第 1 手。自动规则的状态和次数保持不变。确认继续吗？"
                        : "重置手动盈亏后会从新的手动基线重新计算，并跳过未执行的手动旧任务。自动账本和日次数保持不变。确认继续吗？");
                if (!subscriptionId || !confirmDangerousAction(confirmMessage)) {
                    return;
                }
                try {
                    setButtonBusy(target, true, isRestartingCycle ? "处理中..." : "重置中...");
                    await request("/api/platform/subscriptions/" + subscriptionId + (isRollPlan ? "/restart" : "/reset"), {
                        method: "POST",
                        body: {
                            note: isRestartingCycle ? "前端开始新一轮" : "前端手动重置",
                        },
                    });
                    await refreshAll();
                    setStatus(
                        isRollPlan
                            ? "手动新一轮已开始：手动过关周期已重置，手动当天收工已解除。自动规则的过关状态、日次数和历史盈亏保持不变。"
                            : (isRestartingCycle
                                ? "手动新一轮已开始，手动盈亏和手数已回到新基线；自动规则不受影响。"
                                : "手动盈亏和手数已重置到新基线；自动账本和日次数保持不变。"),
                        false
                    );
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    setButtonBusy(target, false, isRestartingCycle ? "手动开始新一轮" : "重置手动盈亏");
                }
                return;
            }
            if (target.classList.contains("standby-subscription-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                if (!subscriptionId) {
                    return;
                }
                try {
                    await updateEntityStatus(
                        "/api/platform/subscriptions/",
                        subscriptionId,
                        "standby",
                        "跟单策略已进入待命触发状态，普通信号不会立即跟单。"
                    );
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (!target.classList.contains("toggle-subscription-btn")) {
                return;
            }
            const subscriptionId = target.getAttribute("data-subscription-id");
            const nextStatus = target.getAttribute("data-next-status");
            if (!subscriptionId || !nextStatus) {
                return;
            }
            try {
                const wasArchived = isArchivedItem(subscriptionById(subscriptionId));
                await updateEntityStatus(
                    "/api/platform/subscriptions/",
                    subscriptionId,
                    nextStatus,
                    wasArchived && nextStatus === "inactive" ? "跟单策略已取消归档，当前状态为已停用。" : ("跟单策略状态已更新为 " + statusText(nextStatus) + "。")
                );
            } catch (error) {
                setStatus(error.message, true);
            }
        });
    }

    if (cancelSubscriptionEditBtn) cancelSubscriptionEditBtn.addEventListener("click", function () {
        subscriptionForm.reset();
        refreshSourceSelects();
        setFormEditingState(subscriptionForm, document.getElementById("createSubscriptionBtn"), cancelSubscriptionEditBtn, false, "保存方案");
        resetSubscriptionStrategyFormState();
    });

    ['roll_structure', 'roll_play_type', 'roll_selection_mode'].forEach(function (name) {
        if (subscriptionForm?.elements[name]) subscriptionForm.elements[name].addEventListener('change', syncGenericRollPlayUI);
    });

    if (subscriptionStrategyModeSelect instanceof HTMLSelectElement) {
        subscriptionStrategyModeSelect.addEventListener("change", function () {
            syncSubscriptionPresetUI();
        });
    }

    if (subscriptionBetFilterModeSelect instanceof HTMLSelectElement) {
        subscriptionBetFilterModeSelect.addEventListener("change", function () {
            syncSubscriptionBetFilterUI();
        });
    }

    if (subscriptionSettlementRuleSourceSelect instanceof HTMLSelectElement) {
        subscriptionSettlementRuleSourceSelect.addEventListener("change", function () {
            syncSubscriptionSettlementUI();
            if (currentSubscriptionSettlementRuleSource() === "subscription_fixed" && subscriptionForm instanceof HTMLFormElement && subscriptionForm.elements.settlement_rule_id instanceof HTMLSelectElement) {
                fillSubscriptionOddsInputs(subscriptionForm.elements.settlement_rule_id.value, {});
            }
        });
    }
    if (subscriptionForm instanceof HTMLFormElement && subscriptionForm.elements.settlement_rule_id instanceof HTMLSelectElement) {
        subscriptionForm.elements.settlement_rule_id.addEventListener("change", function () {
            fillSubscriptionOddsInputs(subscriptionForm.elements.settlement_rule_id.value, {});
        });
    }
    const resetSubscriptionOddsBtn = document.getElementById("resetSubscriptionOddsBtn");
    if (resetSubscriptionOddsBtn instanceof HTMLButtonElement) {
        resetSubscriptionOddsBtn.addEventListener("click", function () {
            if (!(subscriptionForm instanceof HTMLFormElement) || !(subscriptionForm.elements.settlement_rule_id instanceof HTMLSelectElement)) {
                return;
            }
            fillSubscriptionOddsInputs(subscriptionForm.elements.settlement_rule_id.value, {});
        });
    }

    if (subscriptionForm instanceof HTMLFormElement && subscriptionForm.elements.source_id instanceof HTMLSelectElement) {
        subscriptionForm.elements.source_id.addEventListener("change", function () {
            syncSubscriptionBetFilterUI();
        });
    }

    if (subscriptionTargetSelect instanceof HTMLSelectElement) {
        subscriptionTargetSelect.addEventListener("change", function () {
            syncSubscriptionTargetHint();
        });
    }

    Array.from(document.querySelectorAll("input[name='bet_filter_key']")).forEach(function (input) {
        if (!(input instanceof HTMLInputElement)) {
            return;
        }
        input.addEventListener("change", function () {
            syncSubscriptionBetFilterUI();
        });
    });

    Array.from(document.querySelectorAll("[data-play-filter-preset]")).forEach(function (button) {
        if (!(button instanceof HTMLButtonElement)) {
            return;
        }
        button.addEventListener("click", function () {
            applySubscriptionPlayFilterPreset(button.getAttribute("data-play-filter-preset"));
        });
    });

    if (subscriptionForm) {
        ["stake_amount", "base_stake"].forEach(function (name) {
            const input = subscriptionForm.elements[name];
            if (input) input.addEventListener("input", syncGroupStakeHint);
        });
    }

    if (subscriptionRiskControlEnabledCheckbox instanceof HTMLInputElement) {
        subscriptionRiskControlEnabledCheckbox.addEventListener("change", function () {
            syncSubscriptionRiskControlUI();
        });
    }

    if (toggleSubscriptionAdvancedBtn instanceof HTMLButtonElement) {
        toggleSubscriptionAdvancedBtn.addEventListener("click", function () {
            const expanded = toggleSubscriptionAdvancedBtn.getAttribute("aria-expanded") === "true";
            setSubscriptionAdvancedVisible(!expanded);
        });
    }

    if (toggleArchivedTargetsBtn instanceof HTMLButtonElement) {
        toggleArchivedTargetsBtn.addEventListener("click", function () {
            state.showArchivedTargets = !state.showArchivedTargets;
            renderTargetCards();
            setStatus(state.showArchivedTargets ? "已展开归档群组。" : "已隐藏归档群组。", false);
        });
    }

    document.addEventListener("platform-auth:changed", async function (event) {
        const detail = event.detail || {};
        setCurrentUser(detail.user || null);
        if (detail.action === "logout") {
            resetCollections();
            setStatus(detail.message || "已退出登录。", false);
            return;
        }
        await refreshAll({skipCurrentUserReload: true});
        if (detail.message) {
            setStatus(detail.message, false);
        }
    });

    refreshAutobetBtn.addEventListener("click", function () {
        refreshAll();
    });

    if (subscriptionStatDateInput instanceof HTMLInputElement) {
        subscriptionStatDateInput.addEventListener("change", async function (event) {
            state.subscriptionStatDate = event.target.value || todayDateString();
            if (window.PlatformNavigation) window.PlatformNavigation.setQuery({stat_date: state.subscriptionStatDate});
            syncSubscriptionStatDateControls();
            await refreshAll();
        });
    }

    if (subscriptionStatTodayBtn instanceof HTMLButtonElement) {
        subscriptionStatTodayBtn.addEventListener("click", async function () {
            state.subscriptionStatDate = todayDateString();
            if (window.PlatformNavigation) window.PlatformNavigation.setQuery({stat_date: state.subscriptionStatDate});
            syncSubscriptionStatDateControls();
            await refreshAll();
        });
    }

    if (batchResolveSubscriptionsBtn instanceof HTMLButtonElement) {
        batchResolveSubscriptionsBtn.addEventListener("click", async function () {
            if (batchResolveSubscriptionsBtn.hasAttribute("disabled")) {
                return;
            }
            try {
                setButtonBusy(batchResolveSubscriptionsBtn, true, "结算中...");
                const payload = await request("/api/platform/subscriptions/progression/resolve-batch", {
                    method: "POST",
                    body: {},
                });
                await refreshAll();
                const summary = payload && payload.summary && typeof payload.summary === "object" ? payload.summary : {};
                const resolvedCount = Number(summary.resolved_count || 0);
                const unmatchedCount = Number(summary.unmatched_count || 0);
                if (resolvedCount <= 0) {
                    setStatus("当前没有可自动结算的待结算记录，或最近开奖中还没有对应期号。", false);
                    return;
                }
                const parts = [
                    "本次批量自动结算完成",
                    "命中 " + String(Number(summary.hit_count || 0)) + " 条",
                    "回本 " + String(Number(summary.refund_count || 0)) + " 条",
                    "未中 " + String(Number(summary.miss_count || 0)) + " 条",
                ];
                if (unmatchedCount > 0) {
                    parts.push("仍有 " + String(unmatchedCount) + " 条未匹配到开奖");
                }
                setStatus(parts.join("，") + "。", false);
            } catch (error) {
                setStatus(error.message, true);
            } finally {
                syncBatchResolveButtonState();
            }
        });
    }

    applyAutobetScope();
    syncAccountModeUI("phone_login");
    if (messageTemplateForm instanceof HTMLFormElement) {
        messageTemplateForm.elements.status.value = "active";
        messageTemplateForm.elements.template_text.value = "{{bet_value}}{{amount}}";
        applyTemplateConfigToForm(pc28TemplateExampleConfig());
    }
    setTemplateJsonAdvancedVisible(false);
    syncTemplatePreview();
    resetAccountVerifyForm();
    resetSubscriptionStrategyFormState();
    state.subscriptionStatDate = new URLSearchParams(location.search).get("stat_date") || todayDateString();
    syncSubscriptionStatDateControls();
    refreshAll();
}());
