(function () {
    function labelStatus(value) {
        const raw = String(value == null ? "--" : value).trim();
        if (!raw) {
            return "--";
        }
        const normalized = raw.toLowerCase();
        const statusMap = {
            active: "已启用",
            inactive: "已停用",
            archived: "已归档",
            success: "成功",
            ok: "正常",
            online: "在线",
            offline: "离线",
            stale: "延迟",
            authorized: "已授权",
            code_sent: "待验证码",
            password_required: "待二次密码",
            pending_import: "待导入",
            new: "待处理",
            pending: "待执行",
            delivered: "已送达",
            failed: "失败",
            expired: "已过期",
            skipped: "已跳过",
            warning: "警告",
            critical: "严重",
            info: "提示",
            sent: "已通知",
            retrying: "重试中",
            exhausted: "已耗尽",
            due: "待处理",
            scheduled: "已计划",
            manual_only: "仅人工",
            blocked: "已阻塞",
            complete: "已完成",
            current: "当前步骤",
            public: "公开",
            private: "私有",
        };
        return statusMap[normalized] || raw;
    }

    function labelSourceType(value) {
        const raw = String(value == null ? "" : value).trim();
        if (!raw) {
            return "--";
        }
        const normalized = raw.toLowerCase();
        const typeMap = {
            ai_trading_simulator_export: "AITradingSimulator 导出",
            http_json: "HTTP JSON",
            internal_ai: "内部方案",
            telegram_channel: "Telegram 频道",
            website_feed: "网站 Feed",
        };
        return typeMap[normalized] || raw;
    }

    window.PlatformUiText = {
        labelStatus: labelStatus,
        labelSourceType: labelSourceType,
    };
})();
