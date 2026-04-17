(function () {
    const state = {
        currentUser: null,
        sources: [],
        accounts: [],
        targets: [],
        templates: [],
        subscriptions: [],
        jobs: [],
        alerts: [],
        failures: [],
        lastRecommendedActionKey: null,
        showIssueJobsOnly: false,
        scopeFocused: false,
    };

    const statusMessage = document.getElementById("statusMessage");
    const refreshAutobetBtn = document.getElementById("refreshAutobetBtn");
    const sourceCards = document.getElementById("sourceCards");
    const accountCards = document.getElementById("accountList");
    const targetCards = document.getElementById("targetList");
    const subscriptionCards = document.getElementById("subscriptionList");
    const accountForm = document.getElementById("accountForm");
    const targetForm = document.getElementById("targetForm");
    const messageTemplateForm = document.getElementById("messageTemplateForm");
    const subscriptionForm = document.getElementById("subscriptionForm");
    const toggleAutobetBtn = document.getElementById("toggleAutobetBtn");
    const workspaceLinks = Array.from(document.querySelectorAll(".workspace-link"));
    const cancelAccountEditBtn = document.getElementById("cancelAccountEditBtn");
    const cancelTargetEditBtn = document.getElementById("cancelTargetEditBtn");
    const cancelMessageTemplateEditBtn = document.getElementById("cancelMessageTemplateEditBtn");
    const cancelSubscriptionEditBtn = document.getElementById("cancelSubscriptionEditBtn");
    const priorityActionCard = document.getElementById("autobetPriorityActionCard");
    const toggleRecentIssueJobsBtn = document.getElementById("toggleRecentIssueJobsBtn");
    const accountVerifyForm = document.getElementById("accountVerifyForm");
    const accountModeButtons = Array.from(document.querySelectorAll("[data-account-auth-mode]"));
    const targetKeyPreview = document.getElementById("targetKeyPreview");
    const onboardingGuideSteps = document.getElementById("onboardingGuideSteps");
    const onboardingProgressMeta = document.getElementById("onboardingProgressMeta");
    const onboardingPrerequisite = document.getElementById("onboardingPrerequisite");
    const continueOnboardingBtn = document.getElementById("continueOnboardingBtn");
    const subscriptionBetFilterModeSelect = document.getElementById("subscriptionBetFilterModeSelect");
    const subscriptionBetFilterHint = document.getElementById("subscriptionBetFilterHint");
    const subscriptionBetFilterFields = document.getElementById("subscriptionBetFilterFields");
    const subscriptionBetFilterSelectionSummary = document.getElementById("subscriptionBetFilterSelectionSummary");
    const subscriptionStrategyModeSelect = document.getElementById("subscriptionStrategyModeSelect");
    const subscriptionStrategyModeHint = document.getElementById("subscriptionStrategyModeHint");
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
        big_small: ["big_small:大", "big_small:小"],
        odd_even: ["odd_even:单", "odd_even:双"],
        combo: ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
    };
    const templatePreviewPayload = document.getElementById("templatePreviewPayload");
    const templatePreviewOutput = document.getElementById("templatePreviewOutput");
    const templatePreviewMeta = document.getElementById("templatePreviewMeta");
    const subscriptionWorkspaceSummary = document.getElementById("subscriptionWorkspaceSummary");
    const subscriptionWorkspaceSummaryText = document.getElementById("subscriptionWorkspaceSummaryText");
    const accountWorkspaceSummary = document.getElementById("accountWorkspaceSummary");
    const accountWorkspaceSummaryText = document.getElementById("accountWorkspaceSummaryText");
    const templateWorkspaceSummary = document.getElementById("templateWorkspaceSummary");
    const templateWorkspaceSummaryText = document.getElementById("templateWorkspaceSummaryText");
    const executionFlowSummary = document.getElementById("autobetExecutionFlowSummary");
    const executionFlowSummaryText = document.getElementById("autobetExecutionFlowSummaryText");
    const executionFlowChains = document.getElementById("autobetExecutionFlowChains");
    const workspaceCards = document.getElementById("autobetWorkspaceCards");
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
            editText: "保存并重新发送验证码",
            postActionTitle: "提交后继续完成账号授权",
            postActionBody: "发送验证码后，下方会出现“继续完成账号授权”，继续输入验证码或二次密码即可。",
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
        const pathname = String(window.location.pathname || "").trim();
        if (pathname === "/autobet/accounts") {
            return "accounts";
        }
        if (pathname === "/autobet/templates") {
            return "templates";
        }
        if (pathname === "/autobet/targets") {
            return "targets";
        }
        if (pathname === "/autobet/subscriptions") {
            return "subscriptions";
        }
        return "workbench";
    }

    function autobetScopeConfig(scope) {
        const normalized = String(scope || "workbench").trim() || "workbench";
        if (normalized === "accounts") {
            return {
                eyebrow: "托管账号",
                title: "这里专门处理托管账号接入、授权和可执行状态。",
                detail: "先让账号真正可用，再去群组、模板和跟单工作区完成后续链路配置。",
                primaryHref: "#accountsSection",
                primaryText: "去处理账号",
                secondaryHref: "/autobet/targets#targetsSection",
                secondaryText: "去群组页",
                activeHref: "/autobet/accounts#accountsSection",
            };
        }
        if (normalized === "templates") {
            return {
                eyebrow: "下注模板",
                title: "这里专门管理下注格式模板和玩法映射。",
                detail: "不同平台、群组和机器人协议不同。当前页面只聚焦模板维护，保存后再把模板绑定到对应投注群。",
                primaryHref: "#templatesSection",
                primaryText: "去维护模板",
                secondaryHref: "/autobet/targets#targetsSection",
                secondaryText: "去绑定群组",
                activeHref: "/autobet/templates#templatesSection",
            };
        }
        if (normalized === "targets") {
            return {
                eyebrow: "投递群组",
                title: "这里专门处理投递群组、可达性测试和模板绑定。",
                detail: "群组创建默认停用，先测试发送成功，再启用并绑定对应下注模板。",
                primaryHref: "#targetsSection",
                primaryText: "去配置群组",
                secondaryHref: "/autobet/templates#templatesSection",
                secondaryText: "去管理模板",
                activeHref: "/autobet/targets#targetsSection",
            };
        }
        if (normalized === "subscriptions") {
            return {
                eyebrow: "跟单策略",
                title: "在这里设置一条来源收到后怎么下单、发到哪些群。",
                detail: "选来源、定金额，再检查群组和模板，信号才会真正发出去。",
                primaryHref: "#subscriptionsSection",
                primaryText: "去设置跟单",
                secondaryHref: "/autobet/targets#targetsSection",
                secondaryText: "去看群组",
                activeHref: "/autobet/subscriptions#subscriptionsSection",
            };
        }
        return {
            eyebrow: "总控台",
            title: "这里是自动投注总控台，先看业务链路，再进入对应工作区处理。",
            detail: "主总控台只保留总览、健康检查、执行链路和入口卡片；群组、模板、跟单这些具体配置动作，优先进入对应工作区完成。",
            primaryHref: "#autobetQuickstartSection",
            primaryText: "开始 4 步向导",
            secondaryHref: "/",
            secondaryText: "返回总览",
            activeHref: "/autobet#autobetQuickstartSection",
        };
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

    function renderPill(text, className) {
        return '<span class="' + className + '">' + escapeHtml(statusText(text)) + "</span>";
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
    }

    function setActiveWorkspaceLink(targetHash) {
        const hasMatch = workspaceLinks.some(function (link) {
            return link.getAttribute("href") === targetHash;
        });
        if (!hasMatch) {
            return;
        }
        workspaceLinks.forEach(function (link) {
            link.classList.toggle("is-active", link.getAttribute("href") === targetHash);
        });
    }

    function applyAutobetScope() {
        const scope = currentAutobetScope();
        const config = autobetScopeConfig(scope);
        document.body.setAttribute("data-autobet-scope", scope);
        const eyebrow = document.getElementById("autobetHeroEyebrow");
        const title = document.getElementById("autobetHeroTitle");
        const detail = document.getElementById("autobetHeroText");
        const primaryAction = document.getElementById("autobetHeroPrimaryAction");
        const secondaryAction = document.getElementById("autobetHeroSecondaryAction");
        if (eyebrow instanceof HTMLElement) {
            eyebrow.textContent = config.eyebrow;
        }
        if (title instanceof HTMLElement) {
            title.textContent = config.title;
        }
        if (detail instanceof HTMLElement) {
            detail.textContent = config.detail;
        }
        if (primaryAction instanceof HTMLAnchorElement) {
            primaryAction.setAttribute("href", config.primaryHref);
            primaryAction.textContent = config.primaryText;
        }
        if (secondaryAction instanceof HTMLAnchorElement) {
            secondaryAction.setAttribute("href", config.secondaryHref);
            secondaryAction.textContent = config.secondaryText;
        }
        workspaceLinks.forEach(function (link) {
            link.classList.toggle("is-active", link.getAttribute("href") === config.activeHref);
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

    function scrollToHashTarget(hash) {
        const normalized = String(hash || "").trim();
        if (!normalized || normalized.charAt(0) !== "#") {
            return false;
        }
        const target = document.querySelector(normalized);
        if (!(target instanceof HTMLElement)) {
            return false;
        }
        setActiveWorkspaceLink(normalized);
        target.scrollIntoView({behavior: "smooth", block: "start"});
        if (window.history && typeof window.history.replaceState === "function") {
            window.history.replaceState({}, "", window.location.pathname + window.location.search + normalized);
        }
        return true;
    }

    function highlightPriorityActionCard() {
        if (!(priorityActionCard instanceof HTMLElement)) {
            return;
        }
        priorityActionCard.classList.remove("is-updated");
        void priorityActionCard.offsetWidth;
        priorityActionCard.classList.add("is-updated");
        window.setTimeout(function () {
            priorityActionCard.classList.remove("is-updated");
        }, 1300);
    }

    function focusRecentJobCard(jobId) {
        const normalizedJobId = String(jobId || "").trim();
        if (!normalizedJobId) {
            return;
        }
        const card = document.querySelector('[data-job-card-id="' + normalizedJobId.replace(/"/g, '\\"') + '"]');
        if (!(card instanceof HTMLElement)) {
            return;
        }
        card.scrollIntoView({behavior: "smooth", block: "start"});
        card.classList.remove("is-highlighted");
        void card.offsetWidth;
        card.classList.add("is-highlighted");
        window.setTimeout(function () {
            card.classList.remove("is-highlighted");
        }, 1450);
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
            heroUsernameId: "autobetHeroUsername",
            heroUserMetaId: "autobetHeroUserMeta",
            heroLoggedInText: "当前会话已就绪，可集中在总控台和工作区完成自动投注配置。",
            heroLoggedOutText: "登录后查看当前配置和缺失项。",
            panelTitleLoggedIn: "当前账户",
            panelTitleLoggedOut: "登录与账户",
        });
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
        if (authMode === "session_import") {
            return "请上传商家附带的 Telethon Session 文件。";
        }
        return "请先发送验证码并完成登录验证。";
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
        accountVerifyForm.querySelector(".account-code-field").hidden = false;
        accountVerifyForm.querySelector(".account-password-field").hidden = true;
        accountVerifyForm.querySelector("#verifyAccountCodeBtn").hidden = false;
        accountVerifyForm.querySelector("#verifyAccountPasswordBtn").hidden = true;
        document.getElementById("accountVerifySummary").textContent = "";
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
        const normalized = String(mode || "phone_login").trim() || "phone_login";
        const config = accountModeConfig[normalized] || accountModeConfig.phone_login;
        accountModeButtons.forEach(function (button) {
            button.classList.toggle("is-active", button.getAttribute("data-account-auth-mode") === normalized);
        });
        accountForm.elements.auth_mode.value = normalized;
        accountForm.querySelector(".account-phone-field").hidden = normalized !== "phone_login";
        accountForm.querySelector(".account-session-file-field").hidden = normalized !== "session_import";
        accountForm.elements.phone.required = normalized === "phone_login";
        accountForm.elements.session_file.required = normalized === "session_import" && !String(accountForm.elements.edit_id.value || "").trim();
        document.getElementById("accountModeDescription").textContent = config.description;
        renderAccountModeGuide(normalized);
        document.getElementById("createAccountBtn").textContent = String(accountForm.elements.edit_id.value || "").trim()
            ? config.editText
            : config.createText;
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
        if (normalized === "follow_source" || normalized === "martingale") {
            return normalized;
        }
        return "fixed";
    }

    function normalizeSubscriptionBetFilterMode(value) {
        return String(value || "all").trim() === "selected" ? "selected" : "all";
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

    function resolveSignalPlayFilterKey(signal) {
        const item = signal && typeof signal === "object" ? signal : {};
        const betType = String(item.bet_type || "").trim();
        const betValue = String(item.bet_value || "").trim();
        if (betType === "combo" && ["大单", "大双", "小单", "小双"].includes(betValue)) {
            return "combo:" + betValue;
        }
        if (betType === "big_small" && ["大", "小"].includes(betValue)) {
            return "big_small:" + betValue;
        }
        if ((betType === "big_small" || betType === "odd_even") && ["单", "双"].includes(betValue)) {
            return "odd_even:" + betValue;
        }
        return "";
    }

    function syncSubscriptionBetFilterUI() {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        const mode = currentSubscriptionBetFilterMode();
        const isSelectedMode = mode === "selected";
        const selectedSource = sourceById(subscriptionForm.elements.source_id.value);
        const isAiTradingSimulatorSource = Boolean(selectedSource) && selectedSource.source_type === "ai_trading_simulator_export";
        const selectedKeys = selectedSubscriptionBetFilterKeys();
        if (subscriptionBetFilterFields instanceof HTMLElement) {
            subscriptionBetFilterFields.hidden = !isSelectedMode;
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
            if (!isSelectedMode && isAiTradingSimulatorSource) {
                subscriptionBetFilterHint.textContent = "这个来源通常会同时导出大小、单双、组合等多条信号。全部都跟风险很高，不建议普通用户这样用。";
            } else if (!isSelectedMode) {
                subscriptionBetFilterHint.textContent = "不过滤玩法，来源这次导出什么就会跟什么。";
            } else if (selectedKeys.length) {
                subscriptionBetFilterHint.textContent = "只会跟你勾选的玩法，其他信号会自动跳过。当前已选 " + selectedKeys.length + " 项。";
            } else {
                subscriptionBetFilterHint.textContent = "请至少勾选一个玩法。建议先从“大小”或“组合”里选你真正想下的内容。";
            }
        }
        if (subscriptionBetFilterSelectionSummary instanceof HTMLElement) {
            if (!isSelectedMode) {
                subscriptionBetFilterSelectionSummary.textContent = "当前设置：不过滤玩法，来源这次导出什么就跟什么。";
            } else if (!selectedKeys.length) {
                subscriptionBetFilterSelectionSummary.textContent = "当前还没选玩法。可以直接点上面的快捷按钮。";
            } else {
                subscriptionBetFilterSelectionSummary.textContent = "当前已选：" + selectedKeys.map(function (key) {
                    return SUBSCRIPTION_PLAY_FILTER_LABELS[key];
                }).join(" / ");
            }
        }
    }

    function currentSubscriptionStrategyMode() {
        if (!(subscriptionStrategyModeSelect instanceof HTMLSelectElement)) {
            return "fixed";
        }
        return normalizeSubscriptionStrategyMode(subscriptionStrategyModeSelect.value);
    }

    function syncSubscriptionPresetUI() {
        if (!(subscriptionForm instanceof HTMLFormElement)) {
            return;
        }
        const strategyMode = currentSubscriptionStrategyMode();
        const showFixed = strategyMode === "fixed";
        const showFollowSource = strategyMode === "follow_source";
        const showMartingale = strategyMode === "martingale";
        const amountInput = subscriptionForm.elements.stake_amount;
        const baseStakeInput = subscriptionForm.elements.base_stake;
        const multiplierInput = subscriptionForm.elements.multiplier;
        const maxStepsInput = subscriptionForm.elements.max_steps;
        const refundActionInput = subscriptionForm.elements.refund_action;
        const capActionInput = subscriptionForm.elements.cap_action;

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
        if (subscriptionStrategyModeHint instanceof HTMLElement) {
            if (showFixed) {
                subscriptionStrategyModeHint.textContent = "均注最简单，适合先跑通整条链路。";
            } else if (showFollowSource) {
                subscriptionStrategyModeHint.textContent = "系统会直接使用来源信号里的金额，适合想完全照抄来源金额时使用。";
            } else {
                subscriptionStrategyModeHint.textContent = "倍投会按当前手数自动放大金额；更多设置里还能调整回本后和追顶后的处理方式。";
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
        ["profit_target", "loss_limit", "win_profit_ratio"].forEach(function (name) {
            const input = subscriptionForm.elements[name];
            if (!(input instanceof HTMLInputElement)) {
                return;
            }
            input.disabled = !enabled;
            if (enabled && name === "win_profit_ratio" && !String(input.value || "").trim()) {
                input.value = "1";
            }
        });
    }

    function subscriptionStrategyModeFromStrategy(strategy) {
        const payload = strategy && typeof strategy === "object" ? strategy : {};
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
        subscriptionForm.elements.stake_amount.value = "10";
        subscriptionForm.elements.base_stake.value = "10";
        subscriptionForm.elements.multiplier.value = "2";
        subscriptionForm.elements.max_steps.value = "3";
        subscriptionForm.elements.refund_action.value = "hold";
        subscriptionForm.elements.cap_action.value = "reset";
        if (subscriptionRiskControlEnabledCheckbox instanceof HTMLInputElement) {
            subscriptionRiskControlEnabledCheckbox.checked = false;
        }
        subscriptionForm.elements.profit_target.value = "";
        subscriptionForm.elements.loss_limit.value = "";
        subscriptionForm.elements.win_profit_ratio.value = "1";
        setSubscriptionAdvancedVisible(false);
        syncSubscriptionBetFilterUI();
        syncSubscriptionPresetUI();
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
        const passwordRequired = authState === "password_required";
        accountVerifyForm.hidden = false;
        accountVerifyForm.elements.telegram_account_id.value = String(account.id || "");
        accountVerifyForm.elements.code.value = "";
        accountVerifyForm.elements.password.value = "";
        accountVerifyForm.querySelector(".account-code-field").hidden = passwordRequired;
        accountVerifyForm.querySelector(".account-password-field").hidden = !passwordRequired;
        accountVerifyForm.querySelector("#verifyAccountCodeBtn").hidden = passwordRequired;
        accountVerifyForm.querySelector("#verifyAccountPasswordBtn").hidden = !passwordRequired;
        document.getElementById("accountVerifySummary").textContent = passwordRequired
            ? "账号「" + (account.label || "--") + "」需要输入二次密码。"
            : "账号「" + (account.label || "--") + "」已发送验证码，请继续填写。";
        accountVerifyForm.scrollIntoView({behavior: "smooth", block: "start"});
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

    function visibleSubscriptions() {
        return state.subscriptions.filter(function (item) {
            return !isArchivedItem(item);
        });
    }

    function primarySource() {
        const aiSource = state.sources.find(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        return aiSource || state.sources[0] || null;
    }

    function primaryAccount() {
        const items = availableAccountsForTargets();
        return items.find(function (item) {
            return item.status === "active";
        }) || items[0] || null;
    }

    function primaryTarget() {
        const items = visibleTargets();
        return items.find(function (item) {
            return item.status === "active";
        }) || items[0] || null;
    }

    function primarySubscription() {
        const items = visibleSubscriptions();
        return items.find(function (item) {
            return item.status === "active";
        }) || items[0] || null;
    }

    function pendingAccountForWizard() {
        const priorities = {
            password_required: 0,
            code_sent: 1,
            pending_import: 2,
            new: 3,
            pending: 4,
        };
        return visibleAccounts().filter(function (item) {
            return !isAccountAuthorized(item);
        }).sort(function (left, right) {
            const leftPriority = priorities[accountAuthState(left)] == null ? 99 : priorities[accountAuthState(left)];
            const rightPriority = priorities[accountAuthState(right)] == null ? 99 : priorities[accountAuthState(right)];
            return leftPriority - rightPriority;
        })[0] || null;
    }

    function wizardTargetCandidate() {
        const items = visibleTargets();
        return items.find(function (item) {
            return item.status === "active";
        }) || items[0] || null;
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

    function activeAccounts() {
        return state.accounts.filter(function (item) {
            return item.status === "active" && isAccountAuthorized(item);
        });
    }

    function activeTargets() {
        return state.targets.filter(function (item) {
            return item.status === "active";
        });
    }

    function activeSubscriptions() {
        return state.subscriptions.filter(function (item) {
            return item.status === "active";
        });
    }

    function readinessState() {
        const aiSourceCount = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        }).length;
        const activeTargetCount = activeTargets().length;
        const activeSubscriptionCount = activeSubscriptions().length;
        const missing = [];
        if (!state.currentUser) {
            missing.push("登录");
        }
        if (!aiSourceCount) {
            missing.push("AI 方案");
        }
        if (!activeAccounts().length) {
            missing.push("激活托管账号");
        }
        if (!activeTargetCount) {
            missing.push("激活投递群组");
        }
        if (!activeSubscriptionCount) {
            missing.push("激活跟单策略");
        }
        return {
            ready: missing.length === 0,
            missing: missing,
        };
    }

    function summarizeStrategyMode(payload) {
        const mode = String(payload && payload.mode || "").trim();
        const hasStakeAmount = payload && payload.stake_amount != null && String(payload.stake_amount).trim() !== "";
        if (mode === "martingale") {
            return "倍投";
        }
        if (hasStakeAmount) {
            return "均注";
        }
        if (mode === "follow" || !mode) {
            return "跟随来源金额";
        }
        return mode;
    }

    function summarizeBetFilter(strategy) {
        const payload = strategy && typeof strategy === "object" ? strategy : {};
        const betFilter = payload.bet_filter && typeof payload.bet_filter === "object" ? payload.bet_filter : {};
        const mode = normalizeSubscriptionBetFilterMode(betFilter.mode);
        const selectedKeys = Array.isArray(betFilter.selected_keys)
            ? betFilter.selected_keys.filter(function (key) { return Boolean(SUBSCRIPTION_PLAY_FILTER_LABELS[key]); })
            : [];
        if (mode !== "selected" || !selectedKeys.length) {
            return "全部玩法";
        }
        return selectedKeys.map(function (key) {
            return SUBSCRIPTION_PLAY_FILTER_LABELS[key];
        }).join(" / ");
    }

    function summarizeStrategy(strategy) {
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
        if (payload.base_stake != null && String(payload.base_stake).trim() !== "") {
            parts.push("起始金额 " + payload.base_stake);
        }
        if (payload.stake_amount != null && String(payload.stake_amount).trim() !== "") {
            parts.push("每期 " + payload.stake_amount);
        } else if (modeLabel !== "倍投") {
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
        if (riskControl.enabled) {
            const riskParts = [];
            if (Number(riskControl.profit_target || 0) > 0) {
                riskParts.push("净赚到 " + amountText(riskControl.profit_target) + " 停");
            }
            if (Number(riskControl.loss_limit || 0) > 0) {
                riskParts.push("净亏到 " + amountText(riskControl.loss_limit) + " 停");
            }
            riskParts.push("命中按 " + amountText(riskControl.win_profit_ratio || 1) + " 倍净利计算");
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
            parts.push("最近结果 " + progressionResultText(progression.last_result_type));
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

    function archivedCounts() {
        return {
            accounts: state.accounts.filter(isArchivedItem).length,
            targets: state.targets.filter(isArchivedItem).length,
            subscriptions: state.subscriptions.filter(isArchivedItem).length,
        };
    }

    function globalAutobetState() {
        const accountItems = visibleAccounts();
        const targetItems = visibleTargets();
        const subscriptionItems = visibleSubscriptions();
        const activeAccountCount = activeAccounts().length;
        const activeTargetCount = activeTargets().length;
        const activeSubscriptionCount = activeSubscriptions().length;
        const configuredCount = accountItems.length + targetItems.length + subscriptionItems.length;

        if (!configuredCount) {
            return {
                status: "inactive",
                title: "自动投注尚未配置",
                detail: "当前没有可控制的托管账号、投递群组或跟单策略；已归档项不会参与全局启停。",
                actionText: "恢复自动投注",
                nextStatus: "active",
                disabled: true,
            };
        }

        if (activeAccountCount > 0 && activeTargetCount > 0 && activeSubscriptionCount > 0) {
            return {
                status: "active",
                title: "自动投注运行条件已开启",
                detail: "当前至少有一条激活中的托管账号、投递群组和跟单策略，后续派发会继续命中这些激活配置。",
                actionText: "暂停自动投注",
                nextStatus: "inactive",
                disabled: false,
            };
        }

        return {
            status: "inactive",
            title: "自动投注已部分或全部暂停",
            detail: "当前激活中的账号、投递群组或跟单策略不完整。恢复自动投注只会批量恢复未归档配置项的启用状态。",
            actionText: "恢复自动投注",
            nextStatus: "active",
            disabled: false,
        };
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

    function runtimeBoardState() {
        const aiSources = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        const activeAccountItems = activeAccounts();
        const activeTargetItems = activeTargets();
        const activeSubscriptionItems = activeSubscriptions();
        const visibleAccountItems = visibleAccounts();
        const visibleTargetItems = visibleTargets();
        const visibleSubscriptionItems = visibleSubscriptions();
        const archives = archivedCounts();
        const totalArchived = archives.accounts + archives.targets + archives.subscriptions;
        const blockers = [];
        const suggestions = [];

        if (!state.currentUser) {
            blockers.push("当前未登录，无法判断当前账号的自动投注配置。");
            suggestions.push("先登录当前用户账号。");
        }
        if (!aiSources.length) {
            blockers.push("尚未导入 AI 方案来源，自动投注没有可跟随的信号入口。");
            suggestions.push("先回首页导入 AITradingSimulator 方案链接。");
        }
        if (!activeAccountItems.length) {
            blockers.push(
                visibleAccountItems.length
                    ? "托管账号存在但都未激活，当前没有可执行任务的账号。"
                    : "没有可用的托管账号，投递链路无法启动。"
            );
            suggestions.push("至少保留一个激活中的托管账号。");
        }
        if (!activeTargetItems.length) {
            blockers.push(
                visibleTargetItems.length
                    ? "投递群组存在但都未激活，任务没有实际落点。"
                    : "没有可用的投递群组，任务无法派发。"
            );
            suggestions.push("至少保留一个激活中的投递群组。");
        }
        if (!activeSubscriptionItems.length) {
            blockers.push(
                visibleSubscriptionItems.length
                    ? "跟单策略存在但都未激活，来源信号不会展开为执行任务。"
                    : "没有可用的跟单策略，来源信号不会进入你的执行链路。"
            );
            suggestions.push("至少保留一个激活中的跟单策略。");
        }

        let headline = "待配置";
        let detail = "当前还没有形成完整的自动投注运行链路。";
        if (!state.currentUser) {
            headline = "等待登录";
            detail = "登录后才能基于当前用户的配置判断自动投注是否处于可运行状态。";
        } else if (!blockers.length) {
            headline = "可运行";
            detail = "当前来源、托管账号、投递群组和跟单策略都已具备，自动投注满足最小运行条件。";
        } else if (!visibleAccountItems.length && !visibleTargetItems.length && !visibleSubscriptionItems.length && totalArchived > 0) {
            headline = "仅有归档配置";
            detail = "当前只剩已归档配置；这些项目会保留历史展示，但不会参与自动投注。";
        } else if (visibleAccountItems.length || visibleTargetItems.length || visibleSubscriptionItems.length || aiSources.length) {
            headline = "存在阻塞";
            detail = "系统已识别到部分配置，但运行链路仍不完整，自动投注不会稳定命中。";
        }

        const checklistItems = [
            {
                title: "AI 方案来源",
                status: aiSources.length ? "active" : "inactive",
                body: aiSources.length
                    ? ("已导入 " + aiSources.length + " 个 AI 来源，当前主来源为「" + (primarySource() ? primarySource().name : "--") + "」。")
                    : "当前还没有导入 AI 方案来源。",
            },
            {
                title: "托管账号",
                status: activeAccountItems.length ? "active" : (archives.accounts ? "archived" : "inactive"),
                body: activeAccountItems.length
                    ? ("当前有 " + activeAccountItems.length + " 个激活账号，主执行账号为「" + (primaryAccount() ? primaryAccount().label : "--") + "」。")
                    : (visibleAccountItems.length ? "已配置账号但没有激活账号。" : "当前没有可用于执行的托管账号。"),
            },
            {
                title: "投递群组",
                status: activeTargetItems.length ? "active" : (archives.targets ? "archived" : "inactive"),
                body: activeTargetItems.length
                    ? ("当前有 " + activeTargetItems.length + " 个激活群组，主投递群组为「" + (primaryTarget() ? (primaryTarget().target_name || primaryTarget().target_key || "--") : "--") + "」。")
                    : (visibleTargetItems.length ? "已配置投递群组但没有激活群组。" : "当前没有可用的投递群组。"),
            },
            {
                title: "跟单策略",
                status: activeSubscriptionItems.length ? "active" : (archives.subscriptions ? "archived" : "inactive"),
                body: activeSubscriptionItems.length
                    ? ("当前有 " + activeSubscriptionItems.length + " 条激活策略，主策略为「" + summarizeStrategy(primarySubscription() ? primarySubscription().strategy : {}) + "」。")
                    : (visibleSubscriptionItems.length ? "已配置跟单策略但没有激活策略。" : "当前没有可用的跟单策略。"),
            },
            {
                title: "建议动作",
                status: blockers.length ? "inactive" : "active",
                body: blockers.length ? suggestions.join("；") : "当前配置已具备运行条件，下一步更适合去执行记录和异常提醒页观察实际派发结果。",
            },
        ];

        if (totalArchived > 0) {
            checklistItems.push({
                title: "归档统计",
                status: "archived",
                body: "已归档 " + totalArchived + " 项，其中托管账号 " + archives.accounts + "、投递群组 " + archives.targets + "、跟单策略 " + archives.subscriptions + "。",
            });
        }

        return {
            headline: headline,
            detail: detail,
            blockerCount: blockers.length,
            blockerSummary: blockers.length ? blockers.join("；") : "当前没有检测到阻塞项，自动投注可进入运行观察阶段。",
            archiveCount: totalArchived,
            archiveSummary: totalArchived
                ? ("托管账号 " + archives.accounts + "、投递群组 " + archives.targets + "、跟单策略 " + archives.subscriptions + "。")
                : "当前没有已归档配置。",
            suggestionSummary: blockers.length ? suggestions.join("；") : "关键链路已完整，可继续关注执行记录与异常提醒。",
            checklistItems: checklistItems,
        };
    }

    function renderRunChecklist(runtime) {
        const root = document.getElementById("autobetRunChecklist");
        if (!(root instanceof HTMLElement)) {
            return;
        }
        root.innerHTML = runtime.checklistItems.map(function (item) {
            return [
                '<article class="checklist-item">',
                '<div class="checklist-head"><strong class="checklist-title">' + escapeHtml(item.title || "--") + "</strong>" + renderStatusPill(item.status) + "</div>",
                '<p class="checklist-body">' + escapeHtml(item.body || "--") + "</p>",
                "</article>",
            ].join("");
        }).join("");
    }

    function renderCurrentConfig() {
        const source = primarySource();
        const account = primaryAccount();
        const target = primaryTarget();
        const subscription = primarySubscription();

        document.getElementById("currentSourceName").textContent = source ? source.name : "未导入方案";
        document.getElementById("currentSourceMeta").textContent = source
            ? ((source.source_type || "--") + " · " + (source.visibility || "private"))
            : "先在首页导入一个来源，再回来设置跟单。";

        document.getElementById("currentAccountName").textContent = account ? account.label : "未绑定";
        document.getElementById("currentAccountMeta").textContent = account
            ? ((account.phone || "--") + " · " + accountAuthLabel(account))
            : "当前没有可用于执行的 Telegram 托管账号。";

        document.getElementById("currentTargetName").textContent = target ? (target.target_name || target.target_key || "未命名群组") : "未配置";
        document.getElementById("currentTargetMeta").textContent = target
            ? ("目标 Key：" + (target.target_key || "--") + " · 状态：" + statusText(target.status || "inactive"))
            : "没有投递群组时，任务没有实际执行落点。";

        document.getElementById("currentStrategyName").textContent = subscription ? ("来源 #" + subscription.source_id) : "未建立";
        document.getElementById("currentStrategyMeta").textContent = subscription
            ? summarizeStrategy(subscription.strategy || {})
            : "建立订阅后，来源信号才会进入你的执行任务视图。";
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
        const configuredTargets = visibleTargets();
        const activeTargetItems = activeTargets();
        const effectiveTargets = String(item && item.status || "inactive") === "active" ? activeTargetItems : [];
        const previewTargets = effectiveTargets.length ? effectiveTargets : (activeTargetItems.length ? activeTargetItems : configuredTargets);
        const diagnostics = [];
        if (item && String(item.status || "inactive") !== "active") {
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
                label: "未配置群组",
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
        const signal = sampleSignalForTemplate(template);
        const sampleAmount = subscriptionPreviewAmount(subscription, signal);
        return {
            signal: signal,
            amount: sampleAmount,
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

        subscriptionWorkspaceSummary.innerHTML = [
            '<article class="health-card"><span class="setup-label">跟单策略数</span><strong>' + escapeHtml(String(subscriptions.length)) + '</strong><p>当前账号下未归档的跟单数量。</p></article>',
            '<article class="health-card"><span class="setup-label">会收到信号的群组</span><strong>' + escapeHtml(String(activeTargetItems.length)) + '</strong><p>只有启用且可用的群组才会真正发单。</p></article>',
            '<article class="health-card"><span class="setup-label">已绑定模板的群组</span><strong>' + escapeHtml(String(templateBoundCount)) + '</strong><p>这些群组已经选好自己的发单格式。</p></article>',
            '<article class="health-card"><span class="setup-label">当前问题数</span><strong>' + escapeHtml(String(blockerCount)) + '</strong><p>这里统计会挡住发单的授权、模板或群组问题。</p></article>',
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
        subscriptionWorkspaceSummaryText.textContent = "当前链路正常：来源信号会发到启用中的群组，并套用各群组自己的模板。";
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

    function renderExecutionFlowSection() {
        if (!(executionFlowSummary instanceof HTMLElement) || !(executionFlowChains instanceof HTMLElement)) {
            return;
        }
        const subscriptions = visibleSubscriptions();
        const activeSubs = activeSubscriptions();
        const activeTargetsList = activeTargets();
        executionFlowSummary.innerHTML = [
            '<article class="health-card"><span class="setup-label">来源</span><strong>' + escapeHtml(String(state.sources.filter(function (item) { return item.source_type === "ai_trading_simulator_export"; }).length)) + '</strong><p>可进入自动投注的 AI 方案来源数量。</p></article>',
            '<article class="health-card"><span class="setup-label">跟单</span><strong>' + escapeHtml(String(activeSubs.length)) + '</strong><p>当前处于启用状态、会实际接收信号的策略数量。</p></article>',
            '<article class="health-card"><span class="setup-label">群组</span><strong>' + escapeHtml(String(activeTargetsList.length)) + '</strong><p>实际会接收展开任务的启用群组数量。</p></article>',
            '<article class="health-card"><span class="setup-label">模板</span><strong>' + escapeHtml(String(activeTemplateCount())) + '</strong><p>处于启用状态的模板数量。</p></article>',
        ].join("");

        if (!(executionFlowSummaryText instanceof HTMLElement)) {
            return;
        }
        if (!subscriptions.length) {
            executionFlowSummaryText.textContent = "还没有跟单策略，当前只有配置对象，还没有形成业务链路。";
            executionFlowChains.innerHTML = '<article class="checklist-item"><div class="checklist-head"><strong class="checklist-title">还没有执行链路</strong>' + renderStatusPill("inactive") + '</div><p class="checklist-body">先去跟单工作区建立策略，页面才会展示来源如何落到群组和模板。</p></article>';
            return;
        }
        const chainItems = subscriptions.slice(0, 3).map(function (subscription) {
            const source = sourceById(subscription.source_id);
            const chainState = subscriptionTargetsState(subscription);
            const previewTarget = chainState.previewTargets[0] || null;
            const preview = previewTarget ? subscriptionPreviewForTarget(subscription, previewTarget) : null;
            const blockers = chainState.summaries.length
                ? chainState.summaries.map(function (entry) { return entry.label + " " + entry.count + " 个"; }).join("；")
                : "当前没有阻塞项。";
            return [
                '<article class="checklist-item">',
                '<div class="checklist-head"><strong class="checklist-title">' + escapeHtml(source ? source.name : ("#" + subscription.source_id)) + '</strong>' + renderStatusPill(subscription.status) + '</div>',
                '<p class="checklist-body">' + escapeHtml("来源 -> 跟单策略 -> " + chainState.previewTargets.length + " 个群组 -> " + chainState.previewTargets.filter(function (item) { return item.template_id != null; }).length + " 个模板绑定。") + '</p>',
                '<div class="checklist-foot"><span class="cell-muted">' + escapeHtml(summarizeStrategy(subscription.strategy || {})) + '</span><span class="cell-muted">' + escapeHtml(blockers) + '</span>' + (preview ? ('<span class="cell-muted">样例：' + escapeHtml(preview.rendered.text || "--") + '</span>') : "") + '</div>',
                '</article>',
            ].join("");
        });
        executionFlowSummaryText.textContent = "从主总控台可以直接定位链路卡点，再决定去模板、群组还是跟单工作区处理。";
        executionFlowChains.innerHTML = chainItems.join("");
    }

    function renderWorkspaceCards() {
        if (!(workspaceCards instanceof HTMLElement)) {
            return;
        }
        const targetIssues = visibleTargets().filter(function (item) {
            return subscriptionTargetDiagnostics(item).some(function (entry) {
                return entry.severity === "blocked";
            });
        }).length;
        const templateDrafts = state.templates.filter(function (item) {
            return !isArchivedItem(item) && String(item.status || "inactive") === "inactive";
        }).length;
        const subscriptionIssues = visibleSubscriptions().filter(function (item) {
            return subscriptionTargetsState(item).summaries.length > 0;
        }).length;
        const cards = [
            {
                kicker: "Accounts",
                title: "托管账号与授权",
                body: activeAccounts().length
                    ? ("当前有 " + activeAccounts().length + " 个可执行账号。")
                    : "当前没有可执行账号，优先完成授权。",
                meta: activeAccounts().length ? "账号链路已具备。" : "会直接阻塞所有发送。",
                href: "/autobet/accounts#accountsSection",
                text: "去账号区",
            },
            {
                kicker: "Targets",
                title: "群组工作区",
                body: "已配置 " + visibleTargets().length + " 个群组，其中 " + targetIssues + " 个存在关键阻塞。",
                meta: "可反查哪些跟单会发到这个群。",
                href: "/autobet/targets#targetsSection",
                text: "去群组页",
            },
            {
                kicker: "Templates",
                title: "模板工作区",
                body: "当前 " + templateDrafts + " 套草稿模板，" + activeTemplateCount() + " 套启用模板。",
                meta: "可查看哪些群组正在使用模板。",
                href: "/autobet/templates#templatesSection",
                text: "去模板页",
            },
            {
                kicker: "Follow",
                title: "跟单工作区",
                body: "当前 " + visibleSubscriptions().length + " 条策略，其中 " + subscriptionIssues + " 条存在链路阻塞。",
                meta: "可直接看最终发单示例。",
                href: "/autobet/subscriptions#subscriptionsSection",
                text: "去跟单页",
            },
        ];
        workspaceCards.innerHTML = cards.map(function (card) {
            return [
                '<article class="action-card workspace-action-card">',
                '<p class="panel-kicker">' + escapeHtml(card.kicker) + '</p>',
                '<h2>' + escapeHtml(card.title) + '</h2>',
                '<p>' + escapeHtml(card.body) + '</p>',
                '<p class="workspace-action-meta">' + escapeHtml(card.meta) + '</p>',
                '<a class="panel-link" href="' + escapeHtml(card.href) + '">' + escapeHtml(card.text) + '</a>',
                '</article>',
            ].join("");
        }).join("");
    }

    function renderTargetWorkspaceSummary() {
        const root = document.getElementById("targetWorkspaceSummary");
        const summaryText = document.getElementById("targetWorkspaceSummaryText");
        if (!(root instanceof HTMLElement)) {
            return;
        }
        const items = visibleTargets();
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
            root.innerHTML = '<article class="health-card"><span class="setup-label">当前状态</span><strong>还没有投递群组</strong><p>先新增一个目标群组，再继续测试发送和模板绑定。</p></article>';
            if (summaryText instanceof HTMLElement) {
                summaryText.textContent = "当前还没有投递群组，建议先创建第一个群组。";
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

    function renderHealthCheck() {
        const accountHeadline = document.getElementById("autobetHealthAccountAuthHeadline");
        const accountDetail = document.getElementById("autobetHealthAccountAuthDetail");
        const targetHeadline = document.getElementById("autobetHealthTargetReachabilityHeadline");
        const targetDetail = document.getElementById("autobetHealthTargetReachabilityDetail");
        const lastTestHeadline = document.getElementById("autobetHealthLastTestHeadline");
        const lastTestDetail = document.getElementById("autobetHealthLastTestDetail");
        const healthSummary = document.getElementById("autobetHealthSummary");
        if (!(accountHeadline instanceof HTMLElement) || !(targetHeadline instanceof HTMLElement) || !(lastTestHeadline instanceof HTMLElement)) {
            return;
        }

        if (!state.currentUser) {
            accountHeadline.textContent = "待检查";
            accountDetail.textContent = "登录后查看托管账号授权状态。";
            targetHeadline.textContent = "待检查";
            targetDetail.textContent = "登录后查看投递群组测试结果和最近活动。";
            lastTestHeadline.textContent = "--";
            lastTestDetail.textContent = "完成测试发送后，这里会显示最近一次测试时间。";
            if (healthSummary instanceof HTMLElement) {
                healthSummary.textContent = "登录后可生成配置健康报告。";
            }
            return;
        }

        const allAccounts = visibleAccounts();
        const authorizedCount = allAccounts.filter(isAccountAuthorized).length;
        if (!allAccounts.length) {
            accountHeadline.textContent = "不可用";
            accountDetail.textContent = "当前没有托管账号，请先创建并完成授权。";
        } else if (authorizedCount === allAccounts.length) {
            accountHeadline.textContent = "健康";
            accountDetail.textContent = "共 " + allAccounts.length + " 个托管账号，全部已授权。";
        } else {
            accountHeadline.textContent = "待修复";
            accountDetail.textContent = "共 " + allAccounts.length + " 个托管账号，仅 " + authorizedCount + " 个已授权。";
        }

        const allTargets = visibleTargets();
        const reachableCount = allTargets.filter(function (item) {
            const testState = targetLastTestStatus(item);
            if (testState === "success" || testState === "ok") {
                return true;
            }
            return isTargetWizardVerified(item);
        }).length;
        if (!allTargets.length) {
            targetHeadline.textContent = "不可用";
            targetDetail.textContent = "当前没有投递群组，请先绑定群组并完成测试发送。";
        } else if (reachableCount === allTargets.length) {
            targetHeadline.textContent = "健康";
            targetDetail.textContent = "共 " + allTargets.length + " 个投递群组，均已验证可达。";
        } else {
            targetHeadline.textContent = "待修复";
            targetDetail.textContent = "共 " + allTargets.length + " 个投递群组，已验证 " + reachableCount + " 个。";
        }

        const targetLastTimes = allTargets.map(targetLastTestAt).filter(Boolean);
        const backendLatest = targetLastTimes.sort(function (left, right) {
            return parseTimeMs(right) - parseTimeMs(left);
        })[0] || "";
        const localLatest = readStoredValue("wizard-last-target-test-at");
        const latestValue = parseTimeMs(localLatest) > parseTimeMs(backendLatest) ? localLatest : backendLatest;
        if (!latestValue) {
            lastTestHeadline.textContent = "--";
            lastTestDetail.textContent = "还没有测试发送记录，建议先在投递群组里执行一次测试发送。";
        } else {
            lastTestHeadline.textContent = formatDateTime(latestValue);
            lastTestDetail.textContent = "最近测试发送时间已记录，可继续关注群组可达性。";
        }

        if (healthSummary instanceof HTMLElement) {
            const accountPart = allAccounts.length ? (authorizedCount + "/" + allAccounts.length) : "未配置";
            const targetPart = allTargets.length ? (reachableCount + "/" + allTargets.length) : "未配置";
            healthSummary.textContent = "账号授权 " + accountPart + "，群组可达 " + targetPart + "。";
        }
    }

    function recentJobSummary() {
        const deliveredCount = state.jobs.filter(function (item) { return item.status === "delivered"; }).length;
        const pendingCount = state.jobs.filter(function (item) { return item.status === "pending"; }).length;
        const issueCount = state.jobs.filter(function (item) {
            return item.status === "failed" || item.status === "expired" || item.status === "skipped";
        }).length;
        return {
            deliveredCount: deliveredCount,
            pendingCount: pendingCount,
            issueCount: issueCount,
        };
    }

    function issueJobs() {
        return state.jobs.filter(function (item) {
            return item.status === "failed" || item.status === "expired" || item.status === "skipped";
        });
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

    function failureReasonSummary() {
        const reasonCounter = {};
        state.failures.forEach(function (item) {
            const reason = normalizeFailureReason(item.error_message);
            reasonCounter[reason] = (reasonCounter[reason] || 0) + 1;
        });
        const orderedReasons = Object.keys(reasonCounter).sort(function (left, right) {
            return reasonCounter[right] - reasonCounter[left];
        });
        return {
            topReason: orderedReasons[0] || "",
            parts: orderedReasons.slice(0, 3).map(function (reason) {
                return reason + " " + reasonCounter[reason] + " 次";
            }),
        };
    }

    function recommendedActionState(runtime) {
        const aiSources = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        const summary = recentJobSummary();
        const failureSummary = failureReasonSummary();
        const hasCriticalAlerts = state.alerts.some(function (item) {
            return String(item.severity || "") === "critical";
        });

        if (!state.currentUser) {
            return {
                actionKey: "login",
                badgeText: "登录",
                badgeClass: "step-pill is-pending",
                title: "先登录当前账号",
                description: "当前还没有用户会话。先登录，系统才能判断你的自动投注配置和最近运行反馈。",
                primaryHref: "#autobetAccountSection",
                primaryText: "去登录与账户",
                secondaryHref: "/",
                secondaryText: "返回首页",
            };
        }
        if (!aiSources.length) {
            return {
                actionKey: "import_source",
                badgeText: "来源",
                badgeClass: "step-pill is-pending",
                title: "先导入 AI 方案来源",
                description: "没有来源时，自动投注没有可跟随的信号入口，后续账号和群组都无法真正命中任务。",
                primaryHref: "/#importSection",
                primaryText: "去导入方案",
                secondaryHref: "/autobet/subscriptions#subscriptionsSection",
                secondaryText: "查看跟单策略",
            };
        }
        if (!activeAccounts().length) {
            return {
                actionKey: "fix_accounts",
                badgeText: "账号",
                badgeClass: "step-pill is-pending",
                title: "优先处理托管账号",
                description: "当前没有激活中的托管账号，执行链路无法真正把任务发出去。",
                primaryHref: "/autobet/accounts#accountsSection",
                primaryText: "去检查托管账号",
                secondaryHref: "/records?status=failed",
                secondaryText: "查看失败记录",
            };
        }
        if (!activeTargets().length) {
            return {
                actionKey: "fix_targets",
                badgeText: "群组",
                badgeClass: "step-pill is-pending",
                title: "优先恢复投递群组",
                description: "当前没有激活中的投递群组，任务即使生成也没有实际落点。",
                primaryHref: "/autobet/targets#targetsSection",
                primaryText: "去检查投递群组",
                secondaryHref: "/records?status=failed",
                secondaryText: "查看失败记录",
            };
        }
        if (!activeSubscriptions().length) {
            return {
                actionKey: "fix_subscriptions",
                badgeText: "跟单",
                badgeClass: "step-pill is-pending",
                title: "优先恢复跟单策略",
                description: "当前没有激活中的跟单策略，来源信号不会进入你的执行任务链路。",
                primaryHref: "/autobet/subscriptions#subscriptionsSection",
                primaryText: "去检查跟单策略",
                secondaryHref: "#currentConfigSection",
                secondaryText: "查看当前配置",
            };
        }
        if (failureSummary.topReason === "会话失效") {
            return {
                actionKey: "repair_session",
                badgeText: "会话",
                badgeClass: "step-pill is-pending",
                title: "优先检查托管账号会话",
                description: "最近失败主要集中在会话失效。应先检查 Telegram 托管账号的 session 路径、登录状态和可用性。",
                primaryHref: "/autobet/accounts#accountsSection",
                primaryText: "去检查托管账号",
                secondaryHref: "/records?status=failed",
                secondaryText: "查看失败记录",
            };
        }
        if (summary.issueCount > 0) {
            return {
                actionKey: "handle_failures",
                badgeText: "重试",
                badgeClass: "step-pill is-pending",
                title: "优先处理最近失败任务",
                description: failureSummary.parts.length
                    ? ("最近失败主要集中在：" + failureSummary.parts.join("、") + "。先处理失败任务，再观察是否持续复发。")
                    : "最近存在失败、过期或跳过任务，建议优先检查失败记录。",
                primaryHref: "/records?status=failed",
                primaryText: "去处理失败任务",
                secondaryHref: "/alerts",
                secondaryText: "查看异常提醒",
            };
        }
        if (hasCriticalAlerts) {
            return {
                actionKey: "check_alerts",
                badgeText: "告警",
                badgeClass: "step-pill is-pending",
                title: "优先处理关键告警",
                description: "当前存在 critical 级别告警。即使运行链路完整，也应先确认执行器状态和异常通知是否已经恢复。",
                primaryHref: "/alerts",
                primaryText: "去查看关键告警",
                secondaryHref: "/records",
                secondaryText: "查看执行记录",
            };
        }
        return {
            actionKey: runtime.blockerCount ? "review_blockers" : "observe_runtime",
            badgeText: runtime.blockerCount ? "检查" : "就绪",
            badgeClass: runtime.blockerCount ? "step-pill is-pending" : "step-pill is-done",
            title: runtime.blockerCount ? "按阻塞项逐项补齐配置" : "保持观察当前运行结果",
            description: runtime.blockerCount
                ? "当前运行链路还有阻塞项，建议先按上面的运行阻塞清单逐项补齐，再观察最近执行反馈。"
                : "当前关键链路已完整。接下来更适合继续观察执行记录、异常提醒和失败重试结果。",
            primaryHref: runtime.blockerCount ? "#autobetOverviewSection" : "/records",
            primaryText: runtime.blockerCount ? "查看阻塞清单" : "查看执行记录",
            secondaryHref: "/alerts",
            secondaryText: "查看异常提醒",
        };
    }

    function renderRecentJobSummary() {
        const summary = recentJobSummary();
        document.getElementById("autobetRecentDeliveredCount").textContent = String(summary.deliveredCount);
        document.getElementById("autobetRecentPendingCount").textContent = String(summary.pendingCount);
        document.getElementById("autobetRecentIssueCount").textContent = String(summary.issueCount);
        document.getElementById("autobetRecentDeliveredMeta").textContent = summary.deliveredCount
            ? ("最近 " + state.jobs.length + " 条记录中已送达 " + summary.deliveredCount + " 条。")
            : "最近没有已送达任务。";
        document.getElementById("autobetRecentPendingMeta").textContent = summary.pendingCount
            ? ("当前仍有 " + summary.pendingCount + " 条任务待执行。")
            : "当前没有待执行任务。";
        document.getElementById("autobetRecentIssueMeta").textContent = summary.issueCount
            ? ("最近有 " + summary.issueCount + " 条任务处于失败、过期或跳过状态。")
            : "最近没有失败、过期或跳过任务。";

        if (!state.failures.length) {
            document.getElementById("autobetFailureDigest").textContent = "最近没有新的失败反馈，当前执行结果相对平稳。";
            return;
        }

        document.getElementById("autobetFailureDigest").textContent = "最近失败主要集中在：" + failureReasonSummary().parts.join("、") + "。";
    }

    function syncRecentJobsFilterControl() {
        if (!(toggleRecentIssueJobsBtn instanceof HTMLButtonElement)) {
            return;
        }
        const issueCount = issueJobs().length;
        toggleRecentIssueJobsBtn.textContent = state.showIssueJobsOnly ? "查看全部反馈" : "只看失败项";
        toggleRecentIssueJobsBtn.disabled = !state.showIssueJobsOnly && issueCount <= 0;
    }

    function renderPriorityAction(runtime) {
        const action = recommendedActionState(runtime);
        const badge = document.getElementById("autobetActionBadge");
        const primary = document.getElementById("autobetPrimaryAction");
        const secondary = document.getElementById("autobetSecondaryAction");

        document.getElementById("autobetActionTitle").textContent = action.title;
        document.getElementById("autobetActionDescription").textContent = action.description;
        badge.textContent = action.badgeText;
        badge.className = action.badgeClass;
        primary.textContent = action.primaryText;
        primary.setAttribute("href", action.primaryHref);
        secondary.textContent = action.secondaryText;
        secondary.setAttribute("href", action.secondaryHref);
        secondary.hidden = !action.secondaryHref;
        return action.actionKey;
    }

    function renderRecentJobs() {
        const root = document.getElementById("autobetRecentJobs");
        if (!(root instanceof HTMLElement)) {
            return;
        }
        const items = (state.showIssueJobsOnly ? issueJobs() : state.jobs).slice(0, 4);
        syncRecentJobsFilterControl();
        if (!items.length) {
            root.innerHTML = state.showIssueJobsOnly
                ? '<article class="checklist-item"><div class="checklist-head"><strong class="checklist-title">当前没有失败项</strong>' + renderPill("INFO", "alert-pill is-info") + '</div><p class="checklist-body">最近记录里没有失败、过期或跳过任务，可以切回全部反馈继续观察运行结果。</p></article>'
                : '<article class="checklist-item"><div class="checklist-head"><strong class="checklist-title">暂无执行反馈</strong>' + renderPill("INFO", "alert-pill is-info") + '</div><p class="checklist-body">当前还没有最近执行记录。完成抓取、标准化和派发后，这里会显示最近执行反馈。</p></article>';
            return;
        }
        root.innerHTML = items.map(function (item) {
            const errorText = item.last_error_message || item.error_message || "";
            const metaParts = [
                (item.issue_no || "--") + " · " + ((item.bet_type || "--") + " / " + (item.bet_value || "--")),
                "目标 " + (item.target_name || item.target_key || "--"),
                "账号 " + (item.telegram_account_label || "--"),
                "更新时间 " + formatDateTime(item.updated_at || item.last_executed_at),
            ];
            if (errorText) {
                metaParts.push("异常 " + errorText);
            }
            const actions = [];
            if (item.can_retry) {
                actions.push('<button class="ghost-btn retry-job-btn" type="button" data-job-id="' + escapeHtml(String(item.id)) + '">立即重试</button>');
            }
            if (item.signal_id != null) {
                actions.push('<a class="panel-link" href="/records?signal_id=' + encodeURIComponent(String(item.signal_id)) + '">查看对应记录</a>');
            } else {
                actions.push('<a class="panel-link" href="/records">查看完整记录</a>');
            }
            return [
                '<article class="checklist-item" data-job-card-id="' + escapeHtml(String(item.id)) + '">',
                '<div class="checklist-head"><strong class="checklist-title">' + escapeHtml(item.planned_message_text || (item.bet_value || "--")) + '</strong>' + renderPill(item.status || "--", 'job-pill is-' + escapeHtml(String(item.status || "pending"))) + '</div>',
                '<p class="checklist-body">' + escapeHtml(metaParts.join(" · ")) + '</p>',
                '<div class="checklist-foot">' + actions.join("") + '</div>',
                '</article>',
            ].join("");
        }).join("");
    }

    function renderRecentAlerts() {
        const root = document.getElementById("autobetRecentAlerts");
        if (!(root instanceof HTMLElement)) {
            return;
        }
        const items = state.alerts.slice(0, 4);
        if (!items.length) {
            root.innerHTML = '<article class="checklist-item"><div class="checklist-head"><strong class="checklist-title">暂无异常提醒</strong>' + renderPill("INFO", "alert-pill is-info") + '</div><p class="checklist-body">执行器状态、任务重试和通知发送目前没有新的异常提示。</p></article>';
            return;
        }
        root.innerHTML = items.map(function (item) {
            const severity = String(item.severity || "warning");
            const metadata = item.metadata && typeof item.metadata === "object" ? item.metadata : {};
            const actions = ['<a class="panel-link" href="/alerts">查看全部告警</a>'];
            if (metadata.signal_id != null) {
                actions.unshift('<a class="panel-link" href="/records?signal_id=' + encodeURIComponent(String(metadata.signal_id)) + '">查看相关记录</a>');
            } else if (metadata.job_id != null) {
                actions.unshift('<a class="panel-link" href="/records">查看相关记录</a>');
            }
            return [
                '<article class="checklist-item">',
                '<div class="checklist-head"><strong class="checklist-title">' + escapeHtml(item.title || "--") + '</strong>' + renderPill(severity, "alert-pill is-" + escapeHtml(severity)) + '</div>',
                '<p class="checklist-body">' + escapeHtml(item.message || "--") + '</p>',
                '<div class="checklist-foot"><span class="cell-muted">' + escapeHtml(item.alert_type || "--") + '</span>' + actions.join("") + '</div>',
                '</article>',
            ].join("");
        }).join("");
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
        const items = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        renderSelectOptions(subscriptionForm.elements.source_id, items, {
            placeholder: items.length ? "请选择要跟单的来源" : "请先导入一个来源",
            value: function (item) { return String(item.id); },
            label: function (item) { return "#" + item.id + " · " + item.name; },
        });
    }

    function refreshAccountSelects() {
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

    function renderTemplateCards() {
        if (!(messageTemplateCards instanceof HTMLElement)) {
            return;
        }
        if (!state.templates.length) {
            messageTemplateCards.innerHTML = '<article class="mini-card"><strong>当前没有下注模板</strong><p>先创建一个模板，再把模板绑定到对应群组，让不同玩法按群协议生成下注文本。</p></article>';
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
            return [
                '<article class="mini-card">',
                '<div class="config-list-head"><strong>' + escapeHtml(item.name || "--") + "</strong>" + renderStatusPillWithLabel(item.status, templateStatusLabel(item.status)) + "</div>",
                '<p>' + escapeHtml(summarizeTemplate(item)) + "</p>",
                '<p>绑定群组：' + escapeHtml(usageText) + "</p>",
                '<p>版本时间：' + escapeHtml(formatDateTime(item.updated_at || item.created_at)) + ' · 使用中 ' + escapeHtml(String(item.active_target_count != null ? item.active_target_count : boundTargets.filter(function (target) { return String(target.status || "") === "active"; }).length)) + " 个群组</p>",
                '<p class="mono-text">' + escapeHtml(item.template_text || "--") + "</p>",
                '<p>样例预览：' + escapeHtml(preview.text || "--") + "</p>",
                '<div class="config-list-actions"><button class="ghost-btn edit-template-btn" type="button" data-template-id="' + item.id + '">编辑</button><button class="ghost-btn duplicate-template-btn" type="button" data-template-id="' + item.id + '">复制模板</button><button class="ghost-btn toggle-template-btn" type="button" data-template-id="' + item.id + '" data-next-status="' + statusAction.nextStatus + '">' + statusAction.actionText + "</button>" + archiveAction + "</div>",
                "</article>",
            ].join("");
        }).join("");
        renderTemplateWorkspaceSummary();
    }

    function renderOverview() {
        const aiSources = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        const readiness = readinessState();
        const source = primarySource();
        const account = primaryAccount();
        const runtime = runtimeBoardState();

        document.getElementById("autobetSourceCount").textContent = String(aiSources.length);
        document.getElementById("autobetAccountCount").textContent = String(activeAccounts().length);
        document.getElementById("autobetTargetCount").textContent = String(activeTargets().length);
        document.getElementById("autobetSubscriptionCount").textContent = String(activeSubscriptions().length);
        document.getElementById("autobetReadinessHeadline").textContent = readiness.ready ? "已具备条件" : "待配置";
        document.getElementById("autobetReadinessDetail").textContent = readiness.ready
            ? "方案、账号、投递群组和跟单策略都已存在，已具备自动投注基础条件。"
            : "还缺少：" + (readiness.missing.join("、") || "基础配置");
        document.getElementById("autobetPrimarySource").textContent = source ? source.name : "未导入";
        document.getElementById("autobetPrimarySourceMeta").textContent = source
            ? ((source.source_type || "--") + " · " + (source.visibility || "private"))
            : "先从首页导入公开方案页链接。";
        document.getElementById("autobetPrimaryAccount").textContent = account ? account.label : "未绑定";
        document.getElementById("autobetPrimaryAccountMeta").textContent = account
            ? ((account.phone || "--") + " · " + accountAuthLabel(account))
            : "当前还没有托管账号可用于执行。";

        document.getElementById("autobetRuntimeHeadline").textContent = runtime.headline;
        document.getElementById("autobetRuntimeDetail").textContent = runtime.detail;
        document.getElementById("autobetBlockerCount").textContent = String(runtime.blockerCount);
        document.getElementById("autobetBlockerSummary").textContent = runtime.blockerSummary;
        document.getElementById("autobetArchiveCount").textContent = String(runtime.archiveCount);
        document.getElementById("autobetArchiveSummary").textContent = runtime.archiveSummary;
        document.getElementById("autobetSuggestionSummary").textContent = runtime.suggestionSummary;
        renderRunChecklist(runtime);
        const nextActionKey = renderPriorityAction(runtime);
        if (state.lastRecommendedActionKey !== null && state.lastRecommendedActionKey !== nextActionKey) {
            highlightPriorityActionCard();
        }
        state.lastRecommendedActionKey = nextActionKey;
        renderCurrentConfig();

        const globalState = globalAutobetState();
        const statusEl = document.getElementById("autobetGlobalStatus");
        statusEl.className = "config-status-pill is-" + escapeHtml(globalState.status);
        statusEl.textContent = statusText(globalState.status);
        document.getElementById("autobetGlobalTitle").textContent = globalState.title;
        document.getElementById("autobetGlobalDetail").textContent = globalState.detail;
        toggleAutobetBtn.textContent = globalState.actionText;
        toggleAutobetBtn.dataset.nextStatus = globalState.nextStatus;
        toggleAutobetBtn.toggleAttribute("disabled", Boolean(globalState.disabled));
        renderHealthCheck();
    }

    function focusScopeSectionOnLoad() {
        const scope = currentAutobetScope();
        const hash = window.location.hash || "";
        if (hash && scrollToHashTarget(hash)) {
            return;
        }
        if (scope === "accounts") {
            scrollToHashTarget("#accountsSection");
            return;
        }
        if (scope === "templates") {
            scrollToHashTarget("#templatesSection");
            return;
        }
        if (scope === "targets") {
            scrollToHashTarget("#targetsSection");
            return;
        }
        if (scope === "subscriptions") {
            scrollToHashTarget("#subscriptionsSection");
        }
    }

    function buildOnboardingSteps() {
        const loggedIn = Boolean(state.currentUser);
        const aiSources = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        const authorizedAccount = primaryAccount();
        const pendingAccount = pendingAccountForWizard();
        const target = wizardTargetCandidate();
        const targetAccount = target ? accountById(target.telegram_account_id) : null;
        const targetVerified = isTargetWizardVerified(target);
        const subscriptions = visibleSubscriptions();
        const steps = [
            {
                key: "source",
                index: 1,
                title: "导入方案",
                summary: "先导入 AITradingSimulator 公开方案链接，后续跟单才有来源。",
                complete: aiSources.length > 0,
                detail: aiSources.length
                    ? ("已识别 " + aiSources.length + " 个方案来源，当前主来源是「" + (primarySource() ? primarySource().name : "--") + "」。")
                    : "先回首页导入公开方案链接，向导会在这里自动识别并进入下一步。",
                actionText: "去导入方案",
            },
            {
                key: "account",
                index: 2,
                title: "授权账号",
                summary: "创建一个托管 Telegram 账号，并完成验证码或二次密码授权。",
                complete: Boolean(authorizedAccount),
                detail: authorizedAccount
                    ? ("已授权账号「" + (authorizedAccount.label || "--") + "」，可以继续用于群组测试发送。")
                    : (pendingAccount
                        ? accountAuthDescription(pendingAccount)
                        : "还没有可用托管账号。先创建账号，系统才知道要用哪个 Session 发消息。"),
                actionText: pendingAccount
                    ? (accountAuthMode(pendingAccount) === "session_import"
                        ? "去导入 Session"
                        : ((accountAuthState(pendingAccount) === "code_sent" || accountAuthState(pendingAccount) === "password_required") ? "继续完成授权" : "去补充授权"))
                    : "去创建托管账号",
            },
            {
                key: "target",
                index: 3,
                title: "绑定群组并测试发送",
                summary: "绑定目标群组后，立即发一条测试消息，确认账号已入群且具备发言权限。",
                complete: Boolean(target && targetVerified),
                detail: !target
                    ? "还没有投递群组。建议先绑定一个常用投注群，再发测试消息确认可达性。"
                    : (targetVerified
                        ? ("群组「" + (target.target_name || target.target_key || "--") + "」已通过向导验证或最近已有任务活动。")
                        : (!targetAccount
                            ? "当前群组还没有绑定托管账号，请先补充绑定关系。"
                            : (!isAccountAuthorized(targetAccount)
                                ? ("群组「" + (target.target_name || target.target_key || "--") + "」绑定的账号尚未授权，暂时不能测试发送。")
                                : "建议现在发送一条测试消息，确认账号已入群、未被禁言且目标可正常触达。"))),
                actionText: !target ? "去绑定投递群组" : ((targetAccount && isAccountAuthorized(targetAccount)) ? "发送测试消息" : "检查群组配置"),
            },
            {
                key: "subscription",
                index: 4,
                title: "设置跟单",
                summary: "把来源、金额和群组串起来，信号才会真正变成发单任务。",
                complete: subscriptions.length > 0,
                detail: subscriptions.length
                    ? ("已建立 " + subscriptions.length + " 条跟单，当前主策略是「" + summarizeStrategy(primarySubscription() ? primarySubscription().strategy : {}) + "」。")
                    : "选一个来源，再设置金额方式，信号才会真正变成发单任务。",
                actionText: "去设置跟单",
            },
        ];

        const firstIncompleteIndex = loggedIn
            ? steps.findIndex(function (item) { return !item.complete; })
            : 0;
        const currentStepKey = !loggedIn
            ? "login"
            : (firstIncompleteIndex >= 0 ? steps[firstIncompleteIndex].key : "current-config");
        writeStoredValue("wizard-current-step", currentStepKey);

        return {
            loggedIn: loggedIn,
            currentStepKey: currentStepKey,
            steps: steps.map(function (item, index) {
                if (!loggedIn) {
                    return {
                        key: item.key,
                        index: item.index,
                        title: item.title,
                        summary: item.summary,
                        detail: "请先在右侧“登录与账户”区域登录当前账号，登录后再继续这一步。",
                        actionText: item.actionText,
                        status: index === 0 ? "current" : "locked",
                    };
                }
                return {
                    key: item.key,
                    index: item.index,
                    title: item.title,
                    summary: item.summary,
                    detail: item.detail,
                    actionText: item.actionText,
                    status: item.complete ? "complete" : (index === firstIncompleteIndex ? "current" : "locked"),
                };
            }),
        };
    }

    function renderOnboardingGuide() {
        if (!(onboardingGuideSteps instanceof HTMLElement) || !(continueOnboardingBtn instanceof HTMLButtonElement)) {
            return;
        }
        const wizard = buildOnboardingSteps();
        const completedCount = wizard.steps.filter(function (item) {
            return item.status === "complete";
        }).length;
        const currentStep = wizard.steps.find(function (item) {
            return item.status === "current";
        }) || null;

        if (onboardingPrerequisite instanceof HTMLElement) {
            onboardingPrerequisite.hidden = wizard.loggedIn;
        }
        if (onboardingProgressMeta instanceof HTMLElement) {
            onboardingProgressMeta.textContent = wizard.loggedIn
                ? (completedCount >= wizard.steps.length
                    ? "4 步已全部完成。现在可以回到配置概览观察运行状态，或继续微调账号、群组和策略。"
                    : ("已完成 " + completedCount + "/4 步，当前建议先做第 " + (currentStep ? currentStep.index : 4) + " 步。"))
                : "当前未登录，向导还不能判断你的实际进度。";
        }

        continueOnboardingBtn.textContent = wizard.loggedIn
            ? (currentStep ? ("继续第 " + currentStep.index + " 步") : "查看当前配置")
            : "先登录后开始";
        continueOnboardingBtn.dataset.stepKey = wizard.currentStepKey;

        onboardingGuideSteps.innerHTML = wizard.steps.map(function (item) {
            const statusText = item.status === "complete" ? "已完成" : (item.status === "current" ? "当前步骤" : "等待上一步");
            return [
                '<article class="onboarding-step is-' + escapeHtml(item.status) + '">',
                '<div class="onboarding-step-head">',
                '<span class="onboarding-step-index">0' + escapeHtml(String(item.index)) + '</span>',
                '<div class="onboarding-step-copy">',
                '<div class="onboarding-step-title-row"><strong>' + escapeHtml(item.title) + '</strong><span class="onboarding-step-status is-' + escapeHtml(item.status) + '">' + escapeHtml(statusText) + "</span></div>",
                '<p class="onboarding-step-summary">' + escapeHtml(item.summary) + "</p>",
                "</div>",
                "</div>",
                '<div class="onboarding-step-body">',
                '<p class="onboarding-step-detail">' + escapeHtml(item.detail) + "</p>",
                (item.status === "current"
                    ? '<button class="primary-btn onboarding-step-action-btn" type="button" data-onboarding-step="' + escapeHtml(item.key) + '">' + escapeHtml(item.actionText) + "</button>"
                    : ""),
                "</div>",
                "</article>",
            ].join("");
        }).join("");
    }

    function renderSourceCards() {
        if (!(sourceCards instanceof HTMLElement)) {
            return;
        }
        const items = state.sources.filter(function (item) {
            return item.source_type === "ai_trading_simulator_export";
        });
        if (!items.length) {
            sourceCards.innerHTML = '<article class="mini-card"><strong>还没有导入 AI 方案</strong><p>先回首页导入 AITradingSimulator 的公开方案页链接，再来这里配置账号、群组和跟单策略。</p></article>';
            return;
        }
        sourceCards.innerHTML = items.map(function (item) {
            const fetchConfig = item.config && item.config.fetch ? item.config.fetch : {};
            return [
                '<article class="mini-card">',
                '<strong>' + escapeHtml(item.name || "--") + "</strong>",
                '<p>来源类型：' + escapeHtml(item.source_type || "--") + "</p>",
                '<p>可见性：' + escapeHtml(item.visibility || "--") + "</p>",
                '<p class="mono-text">' + escapeHtml(fetchConfig.url || "--") + "</p>",
                "</article>",
            ].join("");
        }).join("");
    }

    function renderAccountCards() {
        if (!(accountCards instanceof HTMLElement)) {
            return;
        }
        if (!state.accounts.length) {
            accountCards.innerHTML = '<article class="mini-card"><strong>当前没有托管账号</strong><p>先选择接入方式，再完成登录或导入 Session，后续才能继续配置投递群组。</p></article>';
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
            const linkedTargets = accountLinkedTargets(item);
            const authAction = authMode === "session_import"
                ? '<button class="ghost-btn continue-account-auth-btn" type="button" data-account-id="' + item.id + '">' + (authState === "authorized" ? "重新导入" : "继续导入") + "</button>"
                : '<button class="ghost-btn continue-account-auth-btn" type="button" data-account-id="' + item.id + '">' + (authState === "authorized" ? "重新登录" : "继续授权") + "</button>";
            return [
                '<article class="mini-card account-card-item">',
                '<div class="config-list-head"><strong>' + escapeHtml(item.label || "--") + '</strong><div class="account-pill-row">' + renderStatusPill(item.status) + '<span class="config-status-pill is-auth-' + escapeHtml(authState) + '">' + escapeHtml(accountAuthLabel(item)) + "</span></div></div>",
                '<div class="config-list-meta">',
                '<p>接入方式：' + escapeHtml(authMode === "session_import" ? "导入 Session" : "手机号登录") + "</p>",
                '<p>手机号：' + escapeHtml(item.phone || "--") + "</p>",
                '<p>承接群组：' + escapeHtml(linkedTargets.length ? linkedTargets.map(function (targetItem) { return targetItem.target_name || targetItem.target_key || "--"; }).join("、") : "当前还没有群组绑定这个账号。") + "</p>",
                '<p>' + escapeHtml(accountAuthDescription(item)) + "</p>",
                "</div>",
                "<div class=\"config-list-actions\"><button class=\"ghost-btn edit-account-btn\" type=\"button\" data-account-id=\"" + item.id + "\">编辑</button>" + authAction + "<button class=\"ghost-btn toggle-account-btn\" type=\"button\" data-account-id=\"" + item.id + "\" data-next-status=\"" + statusAction.nextStatus + "\">" + statusAction.actionText + "</button>" + archiveAction + "</div>",
                "</article>",
            ].join("");
        }).join("");
        renderAccountWorkspaceSummary();
    }

    function renderTargetCards() {
        if (!(targetCards instanceof HTMLElement)) {
            return;
        }
        if (!state.targets.length) {
            targetCards.innerHTML = '<article class="mini-card"><strong>当前没有投递群组</strong><p>新增至少一个投递群组后，后续信号才有实际执行落点。</p></article>';
            renderTargetWorkspaceSummary();
            return;
        }
        targetCards.innerHTML = state.targets.map(function (item) {
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
            return [
                '<article class="mini-card">',
                '<div class="config-list-head"><strong>' + escapeHtml(item.target_name || item.target_key || "--") + "</strong>" + renderStatusPill(item.status) + "</div>",
                '<p>目标 Key：<span class="mono-text">' + escapeHtml(item.target_key || "--") + "</span></p>",
                '<p>托管账号：' + escapeHtml(account ? account.label : "--") + "</p>",
                '<p>模板：' + escapeHtml(template ? template.name : "默认格式") + (template ? (" · 模板版本 " + formatDateTime(template.updated_at || template.created_at)) : "") + "</p>",
                '<p>命中策略：' + escapeHtml(subscriptionText) + "</p>",
                '<p class="target-test-summary">' + escapeHtml(targetTestSummary(item)) + "</p>",
                '<p>' + escapeHtml(recentExecutionText) + "</p>",
                '<p>' + escapeHtml("失败摘要：" + failureText) + "</p>",
                '<ul class="target-activity-list">' + recentActivityMarkup + "</ul>",
                '<p class="target-repair-note"><strong>' + escapeHtml(repair.title || "当前状态") + '</strong> · ' + escapeHtml(repair.detail || "--") + "</p>",
                '<div class="target-repair-actions"><a class="panel-link" href="' + escapeHtml(repair.href || "/autobet/targets#targetsSection") + '">' + escapeHtml(repair.text || "去处理") + "</a></div>",
                "<div class=\"config-list-actions\"><button class=\"ghost-btn edit-target-btn\" type=\"button\" data-target-id=\"" + item.id + "\">编辑</button>" + testAction + toggleAction + archiveAction + "</div>",
                "</article>",
            ].join("");
        }).join("");
        renderTargetWorkspaceSummary();
    }

    function renderSubscriptionCards() {
        if (!(subscriptionCards instanceof HTMLElement)) {
            return;
        }
        if (!state.subscriptions.length) {
            subscriptionCards.innerHTML = '<article class="mini-card"><strong>当前还没有跟单策略</strong><p>建好后，来源信号才会按你的规则生成实际发单任务。</p></article>';
            renderSubscriptionWorkspaceSummary();
            return;
        }
        subscriptionCards.innerHTML = state.subscriptions.map(function (item) {
            const source = sourceById(item.source_id);
            const statusAction = entityStatusAction(item.status, "暂停跟单", "恢复跟单");
            const archiveAction = isArchivedItem(item)
                ? '<button class="ghost-btn danger-btn delete-subscription-btn" type="button" data-subscription-id="' + item.id + '">删除策略</button>'
                : '<button class="ghost-btn archive-subscription-btn" type="button" data-subscription-id="' + item.id + '">归档策略</button>';
            const chainState = subscriptionTargetsState(item);
            const previewTargets = chainState.previewTargets.slice(0, 6);
            const hiddenPreviewCount = Math.max(0, chainState.previewTargets.length - previewTargets.length);
            const routeTitle = chainState.effectiveTargets.length
                ? ("当前会收到信号的群组（" + chainState.effectiveTargets.length + "）")
                : (chainState.previewTargets.length ? "启用后会收到信号的群组" : "还没有可用群组");
            const routeCopy = chainState.effectiveTargets.length
                ? "这些群组会在信号到来时生成实际发单任务。"
                : (chainState.previewTargets.length
                    ? "现在先展示未来会走到的链路，等策略和群组都启用后才会真的发出。"
                    : "先去群组页新增并启用至少一个投递群组。");
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
            const progressionActions = progression && progression.pending_event_id && String(progression.pending_status || "") === "placed"
                ? [
                    '<button class="ghost-btn settle-progression-btn" type="button" data-subscription-id="' + item.id + '" data-progression-event-id="' + progression.pending_event_id + '" data-result-type="hit">命中</button>',
                    '<button class="ghost-btn settle-progression-btn" type="button" data-subscription-id="' + item.id + '" data-progression-event-id="' + progression.pending_event_id + '" data-result-type="refund">回本</button>',
                    '<button class="ghost-btn settle-progression-btn" type="button" data-subscription-id="' + item.id + '" data-progression-event-id="' + progression.pending_event_id + '" data-result-type="miss">未中</button>'
                ].join("")
                : "";
            const resetAction = '<button class="ghost-btn reset-subscription-runtime-btn" type="button" data-subscription-id="' + item.id + '">重置盈亏</button>';
            const stoppedReasonMarkup = financial && financial.stopped_reason
                ? ('<p class="cell-muted subscription-runtime-note">' + escapeHtml(financial.stopped_reason) + '</p>')
                : "";
            return [
                '<article class="mini-card">',
                '<div class="config-list-head"><strong>' + escapeHtml(source ? source.name : ("#" + item.source_id)) + "</strong>" + renderStatusPill(item.status) + "</div>",
                '<p>' + escapeHtml(summarizeStrategy(item.strategy || {})) + "</p>",
                '<p class="cell-muted">' + escapeHtml(summarizeProgression(item)) + "</p>",
                '<p class="cell-muted">' + escapeHtml(summarizeFinancial(item)) + "</p>",
                stoppedReasonMarkup,
                '<div class="subscription-chain-meta"><span class="subscription-chain-stat">当前命中 ' + escapeHtml(String(chainState.effectiveTargets.length)) + ' 个群组</span><span class="subscription-chain-stat">已配置 ' + escapeHtml(String(chainState.configuredTargets.length)) + ' 个群组</span><span class="subscription-chain-stat">阻塞 ' + escapeHtml(String(chainState.summaries.reduce(function (total, entry) { return total + entry.count; }, 0))) + ' 项</span></div>',
                '<div class="subscription-detail-grid"><section class="subscription-routes"><strong class="subscription-section-title">' + escapeHtml(routeTitle) + '</strong><p class="subscription-section-copy">' + escapeHtml(routeCopy) + '</p><div class="subscription-route-grid">' + routeMarkup + '</div>' + (hiddenPreviewCount ? ('<p class="subscription-more-note">另有 ' + escapeHtml(String(hiddenPreviewCount)) + ' 个群组未展开，去群组页可查看全部详情。</p>') : "") + '</section><aside class="subscription-blockers"><strong class="subscription-section-title">当前阻塞项</strong><p class="subscription-section-copy">这里会直接说明信号现在发不出去的原因，以及应该去哪一页处理。</p><div class="subscription-blocker-list">' + blockerMarkup + '</div></aside></div>',
                "<div class=\"config-list-actions\"><button class=\"ghost-btn edit-subscription-btn\" type=\"button\" data-subscription-id=\"" + item.id + "\">编辑</button><button class=\"ghost-btn toggle-subscription-btn\" type=\"button\" data-subscription-id=\"" + item.id + "\" data-next-status=\"" + statusAction.nextStatus + "\">" + statusAction.actionText + "</button>" + resetAction + archiveAction + progressionActions + "</div>",
                "</article>",
            ].join("");
        }).join("");
        renderSubscriptionWorkspaceSummary();
    }

    function resetCollections() {
        state.sources = [];
        state.accounts = [];
        state.targets = [];
        state.templates = [];
        state.subscriptions = [];
        state.jobs = [];
        state.alerts = [];
        state.failures = [];
        state.showIssueJobsOnly = false;
        state.scopeFocused = false;
        accountForm.reset();
        resetAccountVerifyForm();
        targetForm.reset();
        if (messageTemplateForm instanceof HTMLFormElement) {
            messageTemplateForm.reset();
        }
        subscriptionForm.reset();
        setFormEditingState(accountForm, document.getElementById("createAccountBtn"), cancelAccountEditBtn, false, "新增托管账号");
        syncAccountModeUI("phone_login");
        setFormEditingState(targetForm, document.getElementById("createTargetBtn"), cancelTargetEditBtn, false, "新增投递群组");
        if (messageTemplateForm instanceof HTMLFormElement) {
            setFormEditingState(messageTemplateForm, document.getElementById("createMessageTemplateBtn"), cancelMessageTemplateEditBtn, false, "新增模板");
            messageTemplateForm.elements.status.value = "active";
            messageTemplateForm.elements.template_text.value = "{{bet_value}}{{amount}}";
            applyTemplateConfigToForm(pc28TemplateExampleConfig());
        }
        setFormEditingState(subscriptionForm, document.getElementById("createSubscriptionBtn"), cancelSubscriptionEditBtn, false, "新建跟单策略");
        resetSubscriptionStrategyFormState();
        refreshSourceSelects();
        refreshAccountSelects();
        refreshTemplateSelects();
        renderOverview();
        renderExecutionFlowSection();
        renderWorkspaceCards();
        renderOnboardingGuide();
        renderSourceCards();
        renderAccountCards();
        renderTargetWorkspaceSummary();
        renderTargetCards();
        renderTemplateCards();
        renderSubscriptionCards();
        renderRecentJobSummary();
        renderRecentJobs();
        renderRecentAlerts();
    }

    async function loadPageData() {
        const results = await Promise.all([
            request("/api/platform/sources"),
            request("/api/platform/telegram-accounts"),
            request("/api/platform/delivery-targets"),
            request("/api/platform/message-templates"),
            request("/api/platform/subscriptions"),
            request("/api/platform/execution-jobs?limit=30"),
            request("/api/platform/alerts?limit=4"),
            request("/api/platform/execution-failures?limit=20"),
        ]);
        state.sources = results[0].items || [];
        state.accounts = results[1].items || [];
        state.targets = results[2].items || [];
        state.templates = results[3].items || [];
        state.subscriptions = results[4].items || [];
        state.jobs = results[5].items || [];
        state.alerts = results[6].items || [];
        state.failures = results[7].items || [];
    }

    async function refreshAll() {
        setButtonBusy(refreshAutobetBtn, true, "刷新中...");
        try {
            const user = await loadCurrentUser();
            if (!user) {
                resetCollections();
                setStatus("登录后可集中配置自动投注条件。", false);
                return;
            }

            await loadPageData();
            refreshSourceSelects();
            refreshAccountSelects();
            refreshTemplateSelects();
            renderOverview();
            renderExecutionFlowSection();
            renderWorkspaceCards();
            renderOnboardingGuide();
            renderSourceCards();
            renderAccountCards();
            renderTargetWorkspaceSummary();
            renderTargetCards();
            renderTemplateCards();
            renderSubscriptionCards();
            renderRecentJobSummary();
            renderRecentJobs();
            renderRecentAlerts();
            if (!state.scopeFocused) {
                focusScopeSectionOnLoad();
                state.scopeFocused = true;
            }
            setStatus("自动投注配置已刷新。", false);
        } catch (error) {
            resetCollections();
            setStatus(error.message || "自动投注配置页面加载失败", true);
        } finally {
            setButtonBusy(refreshAutobetBtn, false, "刷新配置");
        }
    }

    async function setAllEntityStatuses(nextStatus) {
        const requests = [];
        state.accounts.forEach(function (item) {
            if (!isArchivedItem(item) && item.status !== nextStatus) {
                requests.push(
                    request("/api/platform/telegram-accounts/" + item.id + "/status", {
                        method: "POST",
                        body: {status: nextStatus},
                    })
                );
            }
        });
        state.targets.forEach(function (item) {
            if (!isArchivedItem(item) && item.status !== nextStatus) {
                requests.push(
                    request("/api/platform/delivery-targets/" + item.id + "/status", {
                        method: "POST",
                        body: {status: nextStatus},
                    })
                );
            }
        });
        state.subscriptions.forEach(function (item) {
            if (!isArchivedItem(item) && item.status !== nextStatus) {
                requests.push(
                    request("/api/platform/subscriptions/" + item.id + "/status", {
                        method: "POST",
                        body: {status: nextStatus},
                    })
                );
            }
        });
        if (!requests.length) {
            return 0;
        }
        await Promise.all(requests);
        return requests.length;
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

    function openAccountWizardStep() {
        if (currentAutobetScope() === "workbench") {
            window.location.href = "/autobet/accounts#accountsSection";
            return;
        }
        if (!state.currentUser) {
            scrollToHashTarget("#autobetAccountSection");
            focusElement(document.getElementById("authUsername"));
            return;
        }
        const pendingAccount = pendingAccountForWizard();
        if (pendingAccount) {
            loadAccountIntoForm(pendingAccount, {scroll: false});
            if (accountAuthMode(pendingAccount) === "session_import") {
                scrollToHashTarget("#accountsSection");
                focusElement(accountForm.querySelector('input[name="session_file"]'));
                return;
            }
            if (accountAuthState(pendingAccount) === "code_sent" || accountAuthState(pendingAccount) === "password_required") {
                showAccountVerification(pendingAccount);
                return;
            }
            scrollToHashTarget("#accountsSection");
            focusElement(accountForm.elements.phone || accountForm.elements.label);
            setStatus("保存账号后，系统会重新发送验证码。", false);
            return;
        }
        scrollToHashTarget("#accountsSection");
        focusElement(accountForm.elements.label);
    }

    async function openTargetWizardStep(button) {
        if (currentAutobetScope() === "workbench") {
            window.location.href = "/autobet/targets#targetsSection";
            return;
        }
        const item = wizardTargetCandidate();
        if (!item) {
            scrollToHashTarget("#targetsSection");
            const recommendedAccount = primaryAccount();
            if (recommendedAccount) {
                targetForm.elements.telegram_account_id.value = String(recommendedAccount.id);
            }
            focusElement(targetForm.elements.target_key);
            return;
        }
        const account = accountById(item.telegram_account_id);
        if (!account) {
            scrollToHashTarget("#targetsSection");
            setStatus("该群组还没有绑定托管账号，请先补充绑定关系。", true);
            focusElement(targetForm.elements.telegram_account_id);
            return;
        }
        if (!isAccountAuthorized(account)) {
            scrollToHashTarget("#targetsSection");
            setStatus("该群组绑定的账号尚未授权，请先完成账号授权。", true);
            return;
        }
        await performTargetTestSend(item, button, {
            successMessage: "测试消息已发送，向导已进入下一步。",
            idleText: "发送测试消息",
        });
    }

    function openSubscriptionWizardStep() {
        if (currentAutobetScope() === "workbench") {
            window.location.href = "/autobet/subscriptions#subscriptionsSection";
            return;
        }
        scrollToHashTarget("#subscriptionsSection");
        const source = primarySource();
        if (source) {
            subscriptionForm.elements.source_id.value = String(source.id);
        }
        focusElement(subscriptionForm.elements.source_id);
    }

    async function handleOnboardingAction(stepKey, triggerButton) {
        writeStoredValue("wizard-current-step", stepKey);
        if (stepKey === "login") {
            scrollToHashTarget("#autobetAccountSection");
            focusElement(document.getElementById("authUsername"));
            return;
        }
        if (stepKey === "source") {
            window.location.href = "/#importSection";
            return;
        }
        if (stepKey === "account") {
            openAccountWizardStep();
            return;
        }
        if (stepKey === "target") {
            await openTargetWizardStep(triggerButton);
            return;
        }
        if (stepKey === "subscription") {
            openSubscriptionWizardStep();
            return;
        }
        if (stepKey === "current-config") {
            scrollToHashTarget("#currentConfigSection");
        }
    }

    accountForm.addEventListener("submit", async function (event) {
        event.preventDefault();
        const form = event.currentTarget;
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
            setButtonBusy(submitButton, true, editId ? "保存中..." : (authMode === "phone_login" ? "创建并发送验证码..." : "创建并导入中..."));
            let accountId = editId;
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
            form.reset();
            setFormEditingState(form, submitButton, cancelAccountEditBtn, false, "");
            syncAccountModeUI("phone_login");
            await refreshAll();
            showAccountVerification(loginResult.item || accountById(accountId));
            setStatus(editId ? "账号资料已更新，验证码已重新发送。" : "账号已创建，验证码已发送。", false);
        } catch (error) {
            setStatus(error.message, true);
        } finally {
            setButtonBusy(document.getElementById("createAccountBtn"), false, "新增托管账号");
            syncAccountModeUI(String(accountForm.elements.auth_mode.value || "phone_login"));
        }
    });

    accountModeButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            syncAccountModeUI(button.getAttribute("data-account-auth-mode"));
        });
    });

    accountVerifyForm.addEventListener("submit", async function (event) {
        event.preventDefault();
        const form = event.currentTarget;
        try {
            const accountId = String(form.elements.telegram_account_id.value || "").trim();
            const account = accountById(accountId);
            if (!accountId || !account) {
                throw new Error("请选择要继续验证的托管账号");
            }
            const authState = accountAuthState(account);
            if (authState === "password_required") {
                setButtonBusy(document.getElementById("verifyAccountPasswordBtn"), true, "提交中...");
                await request("/api/platform/telegram-accounts/" + accountId + "/auth/verify-password", {
                    method: "POST",
                    body: {password: form.elements.password.value},
                });
                resetAccountVerifyForm();
                await refreshAll();
                setStatus("二次密码验证成功，账号已可用于发送。", false);
                return;
            }
            setButtonBusy(document.getElementById("verifyAccountCodeBtn"), true, "验证中...");
            const result = await request("/api/platform/telegram-accounts/" + accountId + "/auth/verify-code", {
                method: "POST",
                body: {code: form.elements.code.value},
            });
            await refreshAll();
            const latestAccount = result.item || accountById(accountId);
            if (accountAuthState(latestAccount) === "password_required") {
                showAccountVerification(latestAccount);
                setStatus("该账号需要继续输入二次密码。", false);
                return;
            }
            resetAccountVerifyForm();
            setStatus("验证码验证成功，账号已可用于发送。", false);
        } catch (error) {
            setStatus(error.message, true);
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
                    } else {
                        setStatus("请先点击保存，系统会重新发送验证码。", false);
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
                if (!accountId || !confirmDangerousAction("删除后将无法恢复。只有无关联群组且无执行记录的已归档托管账号才能被删除。确认继续吗？")) {
                    return;
                }
                try {
                    await deleteEntity("/api/platform/telegram-accounts/", accountId, "托管账号已删除。");
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

    cancelAccountEditBtn.addEventListener("click", function () {
        accountForm.reset();
        setFormEditingState(accountForm, document.getElementById("createAccountBtn"), cancelAccountEditBtn, false, "新增托管账号");
        resetAccountVerifyForm();
        syncAccountModeUI("phone_login");
    });

    targetForm.elements.telegram_account_id.addEventListener("change", function (event) {
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

    targetForm.elements.target_key.addEventListener("input", function () {
        syncTargetKeyPreview();
    });

    targetForm.elements.target_key.addEventListener("blur", function (event) {
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

    targetForm.addEventListener("submit", async function (event) {
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

    cancelTargetEditBtn.addEventListener("click", function () {
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

    subscriptionForm.addEventListener("submit", async function (event) {
        event.preventDefault();
        const form = event.currentTarget;
        try {
            if (!state.currentUser) {
                throw new Error("请先登录");
            }
            const sourceId = String(form.source_id.value || "").trim();
            if (!sourceId) {
                throw new Error("请先选择方案来源");
            }
            const editId = String(form.elements.edit_id.value || "").trim();
            const duplicated = state.subscriptions.find(function (item) {
                return Number(item.source_id) === Number(sourceId) && String(item.id) !== editId;
            });
            if (duplicated) {
                throw new Error("该来源已经存在跟单策略");
            }

            const betFilterMode = currentSubscriptionBetFilterMode();
            const selectedBetFilterKeys = selectedSubscriptionBetFilterKeys();
            if (betFilterMode === "selected" && !selectedBetFilterKeys.length) {
                throw new Error("自选玩法模式下，请至少勾选一个玩法");
            }
            const strategyMode = currentSubscriptionStrategyMode();
            const stakeAmount = strategyMode === "fixed" ? Number(form.stake_amount.value || 0) : null;
            const baseStake = strategyMode === "martingale" ? Number(form.base_stake.value || 0) : null;
            const multiplier = strategyMode === "martingale" ? Number(form.multiplier.value || 0) : null;
            const maxSteps = strategyMode === "martingale" ? Number(form.max_steps.value || 0) : null;
            const refundAction = strategyMode === "martingale"
                ? String(form.refund_action.value || "hold").trim() || "hold"
                : "hold";
            const capAction = strategyMode === "martingale"
                ? String(form.cap_action.value || "reset").trim() || "reset"
                : "reset";
            const expireAfterSeconds = Number(form.expire_after_seconds.value || 120);
            const riskControlEnabled = subscriptionRiskControlEnabledCheckbox instanceof HTMLInputElement
                && subscriptionRiskControlEnabledCheckbox.checked;
            const profitTarget = String(form.profit_target.value || "").trim();
            const lossLimit = String(form.loss_limit.value || "").trim();
            const winProfitRatio = String(form.win_profit_ratio.value || "").trim();
            if (strategyMode === "fixed" && (!Number.isFinite(stakeAmount) || stakeAmount <= 0)) {
                throw new Error("均注模式下，请填写大于 0 的固定金额");
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
            const submitButton = document.getElementById("createSubscriptionBtn");
            setButtonBusy(submitButton, true, editId ? "保存中..." : "创建中...");
            const strategyPayload = {
                mode: strategyMode === "martingale" ? "martingale" : "follow",
                bet_filter: {
                    mode: betFilterMode,
                    selected_keys: selectedBetFilterKeys,
                },
                expire_after_seconds: expireAfterSeconds,
                risk_control: {
                    enabled: riskControlEnabled,
                    profit_target: profitTarget ? Number(profitTarget) : 0,
                    loss_limit: lossLimit ? Number(lossLimit) : 0,
                    win_profit_ratio: winProfitRatio ? Number(winProfitRatio) : 1,
                },
            };
            if (stakeAmount != null) {
                strategyPayload.stake_amount = stakeAmount;
            }
            if (strategyMode === "martingale") {
                strategyPayload.base_stake = baseStake;
                strategyPayload.multiplier = multiplier;
                strategyPayload.max_steps = maxSteps;
                strategyPayload.refund_action = refundAction;
                strategyPayload.cap_action = capAction;
            }
            if (editId) {
                await request("/api/platform/subscriptions/" + editId, {
                    method: "POST",
                    body: {
                        source_id: Number(sourceId),
                        strategy: strategyPayload,
                    },
                });
            } else {
                await request("/api/platform/subscriptions", {
                    method: "POST",
                    body: {
                        source_id: Number(sourceId),
                        strategy: strategyPayload,
                    },
                });
            }
            form.reset();
            setFormEditingState(form, submitButton, cancelSubscriptionEditBtn, false, "新建跟单策略");
            resetSubscriptionStrategyFormState();
            await refreshAll();
            setStatus(editId ? "跟单策略已更新。" : "跟单策略已创建。", false);
        } catch (error) {
            setStatus(error.message, true);
        } finally {
            setButtonBusy(document.getElementById("createSubscriptionBtn"), false, "新建跟单策略");
        }
    });

    if (subscriptionCards instanceof HTMLElement) {
        subscriptionCards.addEventListener("click", async function (event) {
            const target = event.target;
            if (target instanceof HTMLElement && target.classList.contains("edit-subscription-btn")) {
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
                const hasStakeAmount = item.strategy && item.strategy.stake_amount != null && String(item.strategy.stake_amount).trim() !== "";
                if (subscriptionStrategyModeSelect instanceof HTMLSelectElement) {
                    subscriptionStrategyModeSelect.value = strategyMode;
                }
                subscriptionForm.elements.stake_amount.value = hasStakeAmount ? String(item.strategy.stake_amount) : "";
                subscriptionForm.elements.base_stake.value = item.strategy && item.strategy.base_stake != null && String(item.strategy.base_stake).trim() !== ""
                    ? String(item.strategy.base_stake)
                    : "10";
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
                subscriptionForm.elements.win_profit_ratio.value = String(riskControl.win_profit_ratio != null ? riskControl.win_profit_ratio : 1);
                if (subscriptionRiskControlEnabledCheckbox instanceof HTMLInputElement) {
                    subscriptionRiskControlEnabledCheckbox.checked = Boolean(riskControl.enabled);
                }
                syncSubscriptionBetFilterUI();
                syncSubscriptionPresetUI();
                syncSubscriptionRiskControlUI();
                const shouldExpandAdvanced = Number(item.strategy && item.strategy.expire_after_seconds || 120) !== 120
                    || Boolean(riskControl.enabled)
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
                    const thresholdStatus = nextItem && nextItem.financial ? String(nextItem.financial.threshold_status || "") : "";
                    if (thresholdStatus === "profit_target_hit") {
                        setStatus("当前待结算记录已处理，策略达到止盈阈值并自动停用。", false);
                    } else if (thresholdStatus === "loss_limit_hit") {
                        setStatus("当前待结算记录已处理，策略达到止损阈值并自动停用。", false);
                    } else {
                        setStatus("当前待结算记录已处理。", false);
                    }
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (!(target instanceof HTMLElement)) {
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
                if (!subscriptionId || !confirmDangerousAction("删除后将无法恢复。只有已归档的跟单策略才能被删除。确认继续吗？")) {
                    return;
                }
                try {
                    await deleteEntity("/api/platform/subscriptions/", subscriptionId, "跟单策略已删除。");
                } catch (error) {
                    setStatus(error.message, true);
                }
                return;
            }
            if (target.classList.contains("reset-subscription-runtime-btn")) {
                const subscriptionId = target.getAttribute("data-subscription-id");
                if (!subscriptionId || !confirmDangerousAction("重置后会从新的盈亏基线重新计算，并跳过当前未执行的旧轮次任务。确认继续吗？")) {
                    return;
                }
                try {
                    setButtonBusy(target, true, "重置中...");
                    await request("/api/platform/subscriptions/" + subscriptionId + "/reset", {
                        method: "POST",
                        body: {
                            note: "前端手动重置",
                        },
                    });
                    await refreshAll();
                    setStatus("跟单策略已重置，当前盈亏和手数已回到新基线。", false);
                } catch (error) {
                    setStatus(error.message, true);
                } finally {
                    setButtonBusy(target, false, "重置盈亏");
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

    cancelSubscriptionEditBtn.addEventListener("click", function () {
        subscriptionForm.reset();
        refreshSourceSelects();
        setFormEditingState(subscriptionForm, document.getElementById("createSubscriptionBtn"), cancelSubscriptionEditBtn, false, "新建跟单策略");
        resetSubscriptionStrategyFormState();
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

    if (subscriptionForm instanceof HTMLFormElement && subscriptionForm.elements.source_id instanceof HTMLSelectElement) {
        subscriptionForm.elements.source_id.addEventListener("change", function () {
            syncSubscriptionBetFilterUI();
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

    document.getElementById("autobetRecentJobs").addEventListener("click", async function (event) {
        const target = event.target;
        if (!(target instanceof HTMLElement) || !target.classList.contains("retry-job-btn")) {
            return;
        }
        const jobId = target.getAttribute("data-job-id");
        if (!jobId) {
            return;
        }
        try {
            setButtonBusy(target, true, "重试中...");
            await request("/api/platform/execution-jobs/" + jobId + "/retry", {
                method: "POST",
                body: {},
            });
            await refreshAll();
            focusRecentJobCard(jobId);
            setStatus("执行任务 " + jobId + " 已重置为待执行。", false);
        } catch (error) {
            setStatus(error.message, true);
        } finally {
            setButtonBusy(target, false, "立即重试");
        }
    });

    if (toggleRecentIssueJobsBtn instanceof HTMLButtonElement) {
        toggleRecentIssueJobsBtn.addEventListener("click", function () {
            state.showIssueJobsOnly = !state.showIssueJobsOnly;
            renderRecentJobs();
        });
    }

    if (onboardingGuideSteps instanceof HTMLElement) {
        onboardingGuideSteps.addEventListener("click", async function (event) {
            const target = event.target;
            if (!(target instanceof HTMLElement) || !target.classList.contains("onboarding-step-action-btn")) {
                return;
            }
            const stepKey = String(target.getAttribute("data-onboarding-step") || "").trim();
            if (!stepKey) {
                return;
            }
            try {
                await handleOnboardingAction(stepKey, target);
            } catch (error) {
                setStatus(error.message || "向导操作失败", true);
            }
        });
    }

    if (continueOnboardingBtn instanceof HTMLButtonElement) {
        continueOnboardingBtn.addEventListener("click", async function () {
            const stepKey = String(continueOnboardingBtn.dataset.stepKey || "").trim();
            if (!stepKey) {
                return;
            }
            try {
                await handleOnboardingAction(stepKey, continueOnboardingBtn);
            } catch (error) {
                setStatus(error.message || "无法继续当前向导步骤", true);
            }
        });
    }

    ["autobetPrimaryAction", "autobetSecondaryAction"].forEach(function (elementId) {
        const link = document.getElementById(elementId);
        if (!(link instanceof HTMLAnchorElement)) {
            return;
        }
        link.addEventListener("click", function (event) {
            const href = link.getAttribute("href") || "";
            if (!href.startsWith("#")) {
                return;
            }
            event.preventDefault();
            scrollToHashTarget(href);
        });
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

    refreshAutobetBtn.addEventListener("click", function () {
        refreshAll();
    });

    toggleAutobetBtn.addEventListener("click", async function () {
        const nextStatus = String(toggleAutobetBtn.dataset.nextStatus || "").trim();
        if (!nextStatus || toggleAutobetBtn.hasAttribute("disabled")) {
            return;
        }
        try {
            setButtonBusy(toggleAutobetBtn, true, nextStatus === "inactive" ? "暂停中..." : "恢复中...");
            const changedCount = await setAllEntityStatuses(nextStatus);
            await refreshAll();
            if (changedCount <= 0) {
                setStatus("当前没有需要批量更新状态的自动投注配置。", false);
            } else {
                setStatus("已将 " + changedCount + " 个配置项批量更新为" + statusText(nextStatus) + "。", false);
            }
        } catch (error) {
            setStatus(error.message, true);
        } finally {
            const globalState = globalAutobetState();
            setButtonBusy(toggleAutobetBtn, false, globalState.actionText);
            toggleAutobetBtn.dataset.nextStatus = globalState.nextStatus;
            toggleAutobetBtn.toggleAttribute("disabled", Boolean(globalState.disabled));
        }
    });

    applyAutobetScope();
    initWorkspaceNav();
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
    refreshAll();
}());
