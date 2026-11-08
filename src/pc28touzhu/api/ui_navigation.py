"""Page metadata shared by HTTP routing, navigation and legacy link handling."""
from html import escape
import json


PAGE_GROUPS = (
    ("", (("/", "工作台", "home.html"),)),
    ("策略管理", (
        ("/autobet/subscriptions", "跟单方案", "autobet.html"),
        ("/auto-triggers", "自动触发", "auto_triggers.html"),
    )),
    ("资源配置", (
        ("/autobet/sources", "方案来源", "autobet.html"),
        ("/autobet/accounts", "Telegram 账号", "autobet.html"),
        ("/autobet/targets", "投递群组", "autobet.html"),
        ("/autobet/templates", "消息模板", "autobet.html"),
        ("/autobet/bot", "收益 Bot 绑定", "bot_binding.html"),
    )),
    ("运行监控", (
        ("/records", "执行记录", "records.html"),
        ("/alerts", "异常提醒", "alerts.html"),
    )),
    ("平台管理", (
        ("/admin", "值班总览", "dashboard.html"),
        ("/admin/sources", "来源接入中心", "dashboard.html"),
        ("/admin/signals", "信号中心", "dashboard.html"),
        ("/admin/execution", "执行中心", "dashboard.html"),
        ("/admin/alerts", "告警中心", "dashboard.html"),
        ("/admin/telegram", "Telegram 配置", "dashboard.html"),
        ("/admin/support", "支持查询页", "dashboard.html"),
    )),
)
PAGE_ROUTES = {
    path: {"title": title, "template": template, "group": group, "admin": group == "平台管理"}
    for group, pages in PAGE_GROUPS for path, title, template in pages
}
ADMIN_PAGE_ROUTES = {path for path, page in PAGE_ROUTES.items() if page["admin"]}

# Fragments are interpreted in the browser: they are never sent to WSGI.
LEGACY_FRAGMENTS = {
    "/": {
        "#autobetQuickstartSection": "/#setupGuide", "#checklistSection": "/#setupGuide",
        "#autobetOverviewSection": "/#overviewSection", "#autobetHealthSection": "/#attentionSection",
        "#nextStepSection": "/#attentionSection", "#autobetPriorityActionCard": "/#attentionSection",
        "#setupSection": "/#chainsSection", "#currentConfigSection": "/#chainsSection",
        "#autobetExecutionFlowSection": "/#chainsSection", "#autobetWorkspaceHubSection": "/#chainsSection",
        "#importSection": "/autobet/sources#sourceForm", "#botBindingSection": "/autobet/bot",
        **{"#%sSection" % name: "/autobet/%s" % name for name in ("sources", "accounts", "targets", "templates", "subscriptions")},
    },
    "/admin": {
        "#sourcesSection": "/admin/sources", "#rawItemsSection": "/admin/sources",
        "#signalsSection": "/admin/signals", "#executionJobsSection": "/admin/execution",
        "#executorsSection": "/admin/execution", "#failuresSection": "/admin/execution",
        "#alertsSection": "/admin/alerts", "#accountsSection": "/admin/support",
        "#subscriptionsSection": "/admin/support", "#targetsSection": "/admin/support",
    },
}


def render_navigation(path: str) -> str:
    groups = []
    for group, pages in PAGE_GROUPS:
        admin = group == "平台管理"
        links = []
        for href, title, _ in pages:
            current = ' aria-current="page"' if href == path else ""
            links.append('<a class="app-nav-link" href="%s"%s>%s</a>' % (escape(href), current, escape(title)))
        label = '<p class="app-nav-label">%s</p>' % escape(group) if group else ""
        groups.append('<div class="app-nav-group"%s>%s%s</div>' % (
            ' data-admin-only hidden' if admin else "", label, "".join(links)))
    return "".join(groups)


def navigation_data(path: str) -> str:
    page = PAGE_ROUTES.get(path, PAGE_ROUTES["/"])
    return json.dumps({"path": path, "title": page["title"], "group": page["group"],
                       "legacy": LEGACY_FRAGMENTS.get(path, {})}, ensure_ascii=False).replace("<", "\\u003c")
