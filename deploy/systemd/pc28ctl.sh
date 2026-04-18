#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SYSTEMD_SOURCE_DIR="${PROJECT_ROOT}/deploy/systemd"
SYSTEMD_TARGET_DIR="/etc/systemd/system"
ENV_FILE="${PROJECT_ROOT}/.env"

PLATFORM_SERVICE="pc28touzhu-platform.service"
SOURCE_SYNC_SERVICE="pc28touzhu-source-sync.service"
ALERT_SERVICE="pc28touzhu-alert-notifier.service"
BOT_SERVICE="pc28touzhu-telegram-bot.service"
REPORT_SERVICE="pc28touzhu-telegram-report.service"
AUTO_SETTLEMENT_SERVICE="pc28touzhu-pc28-auto-settlement.service"
EXECUTOR_TEMPLATE="pc28touzhu-telegram-executor@.service"

usage() {
    cat <<EOF
用法:
  sudo "${PROJECT_ROOT}/deploy/systemd/pc28ctl.sh" up [executor_id]
  sudo "${PROJECT_ROOT}/deploy/systemd/pc28ctl.sh" restart [executor_id]
  sudo "${PROJECT_ROOT}/deploy/systemd/pc28ctl.sh" down [executor_id]
  sudo "${PROJECT_ROOT}/deploy/systemd/pc28ctl.sh" sync
  "${PROJECT_ROOT}/deploy/systemd/pc28ctl.sh" status [executor_id]
  "${PROJECT_ROOT}/deploy/systemd/pc28ctl.sh" logs [executor_id]

说明:
  - 默认管理 platform + source-sync + auto-settlement + alert + telegram-bot + telegram-report 六个服务
  - 传入 executor_id 后会额外管理对应执行器实例
  - 例如: sudo "${PROJECT_ROOT}/deploy/systemd/pc28ctl.sh" up "executor-001"
EOF
}

require_root() {
    if [[ "${EUID}" -ne 0 ]]; then
        echo "此命令需要 root 权限，请使用 sudo 运行。" >&2
        exit 1
    fi
}

require_env_file() {
    if [[ ! -f "${ENV_FILE}" ]]; then
        echo "缺少配置文件: ${ENV_FILE}" >&2
        exit 1
    fi
}

executor_service_name() {
    local executor_id="${1:-}"
    if [[ -z "${executor_id}" ]]; then
        return 1
    fi
    printf "pc28touzhu-telegram-executor@%s.service\n" "${executor_id}"
}

collect_services() {
    local executor_id="${1:-}"
    printf "%s\n" "${PLATFORM_SERVICE}"
    printf "%s\n" "${SOURCE_SYNC_SERVICE}"
    printf "%s\n" "${AUTO_SETTLEMENT_SERVICE}"
    printf "%s\n" "${ALERT_SERVICE}"
    printf "%s\n" "${BOT_SERVICE}"
    printf "%s\n" "${REPORT_SERVICE}"
    if [[ -n "${executor_id}" ]]; then
        executor_service_name "${executor_id}"
    fi
}

sync_units() {
    require_root
    require_env_file
    cp -f "${SYSTEMD_SOURCE_DIR}/${PLATFORM_SERVICE}" "${SYSTEMD_TARGET_DIR}/${PLATFORM_SERVICE}"
    cp -f "${SYSTEMD_SOURCE_DIR}/${SOURCE_SYNC_SERVICE}" "${SYSTEMD_TARGET_DIR}/${SOURCE_SYNC_SERVICE}"
    cp -f "${SYSTEMD_SOURCE_DIR}/${AUTO_SETTLEMENT_SERVICE}" "${SYSTEMD_TARGET_DIR}/${AUTO_SETTLEMENT_SERVICE}"
    cp -f "${SYSTEMD_SOURCE_DIR}/${ALERT_SERVICE}" "${SYSTEMD_TARGET_DIR}/${ALERT_SERVICE}"
    cp -f "${SYSTEMD_SOURCE_DIR}/${BOT_SERVICE}" "${SYSTEMD_TARGET_DIR}/${BOT_SERVICE}"
    cp -f "${SYSTEMD_SOURCE_DIR}/${REPORT_SERVICE}" "${SYSTEMD_TARGET_DIR}/${REPORT_SERVICE}"
    cp -f "${SYSTEMD_SOURCE_DIR}/${EXECUTOR_TEMPLATE}" "${SYSTEMD_TARGET_DIR}/${EXECUTOR_TEMPLATE}"
    systemctl daemon-reload
}

status_units() {
    local executor_id="${1:-}"
    mapfile -t services < <(collect_services "${executor_id}")
    systemctl status "${services[@]}" --no-pager -l || true
}

logs_units() {
    local executor_id="${1:-}"
    mapfile -t services < <(collect_services "${executor_id}")
    local journal_args=()
    local service
    for service in "${services[@]}"; do
        journal_args+=("-u" "${service}")
    done
    journalctl "${journal_args[@]}" -f
}

up_units() {
    local executor_id="${1:-}"
    sync_units
    mapfile -t services < <(collect_services "${executor_id}")
    systemctl enable --now "${services[@]}"
    status_units "${executor_id}"
}

restart_units() {
    local executor_id="${1:-}"
    sync_units
    mapfile -t services < <(collect_services "${executor_id}")
    systemctl restart "${services[@]}"
    status_units "${executor_id}"
}

down_units() {
    require_root
    local executor_id="${1:-}"
    mapfile -t services < <(collect_services "${executor_id}")
    systemctl stop "${services[@]}"
    status_units "${executor_id}"
}

COMMAND="${1:-}"
EXECUTOR_ID="${2:-}"

case "${COMMAND}" in
    up)
        up_units "${EXECUTOR_ID}"
        ;;
    restart)
        restart_units "${EXECUTOR_ID}"
        ;;
    down)
        down_units "${EXECUTOR_ID}"
        ;;
    sync)
        sync_units
        ;;
    status)
        status_units "${EXECUTOR_ID}"
        ;;
    logs)
        logs_units "${EXECUTOR_ID}"
        ;;
    ""|-h|--help|help)
        usage
        ;;
    *)
        echo "不支持的命令: ${COMMAND}" >&2
        usage
        exit 1
        ;;
esac
