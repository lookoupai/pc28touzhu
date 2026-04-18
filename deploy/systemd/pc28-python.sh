#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

resolve_python_bin() {
    local configured="${PC28_PYTHON_BIN:-}"
    if [[ -n "${configured}" && -x "${configured}" ]]; then
        printf "%s\n" "${configured}"
        return 0
    fi

    local candidate=""
    for candidate in \
        "${PROJECT_ROOT}/.venv/bin/python" \
        "${PROJECT_ROOT}/venv/bin/python"
    do
        if [[ -x "${candidate}" ]]; then
            printf "%s\n" "${candidate}"
            return 0
        fi
    done

    if [[ -x "$(command -v python3)" ]]; then
        command -v python3
        return 0
    fi

    printf "python3\n"
}

cd "${PROJECT_ROOT}"
export PYTHONPATH="src${PYTHONPATH:+:${PYTHONPATH}}"
exec "$(resolve_python_bin)" "$@"
