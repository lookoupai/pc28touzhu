from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from pc28touzhu.config import PROJECT_ROOT


def _deduplicate_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    items: list[Path] = []
    for path in paths:
        normalized = str(path.expanduser())
        if normalized in seen:
            continue
        seen.add(normalized)
        items.append(Path(normalized))
    return items


def project_python_candidates() -> list[Path]:
    configured = str(os.getenv("PC28_PYTHON_BIN") or "").strip()
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured).expanduser())
    candidates.extend(
        [
            PROJECT_ROOT / ".venv/bin/python",
            PROJECT_ROOT / "venv/bin/python",
        ]
    )
    if sys.executable:
        candidates.append(Path(sys.executable).expanduser())
    python3 = shutil.which("python3")
    if python3:
        candidates.append(Path(python3).expanduser())
    return _deduplicate_paths(candidates)


def resolve_project_python() -> str:
    for candidate in project_python_candidates():
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    if sys.executable:
        return str(Path(sys.executable).expanduser())
    return "python3"


def build_telethon_missing_message() -> str:
    current_python = str(Path(sys.executable).expanduser()) if sys.executable else "python3"
    preferred_python = resolve_project_python()
    parts = [
        "未安装 Telethon，请先安装 `Telethon>=1.42,<2`。",
        "当前进程 Python: %s。" % current_python,
    ]
    if preferred_python != current_python:
        parts.append("项目优先解释器: %s。" % preferred_python)
    parts.append(
        "如果依赖装在虚拟环境，请让 platform / executor / bot 服务切到同一个解释器后重启；否则请直接在当前解释器安装依赖。"
    )
    return " ".join(parts)


def resolve_telethon_session_file(session: str) -> Path:
    normalized = Path(str(session or "").strip()).expanduser()
    if normalized.suffix == ".session":
        return normalized
    return normalized.with_name(normalized.name + ".session")


def ensure_telethon_session_writable(session: str) -> Path:
    session_file = resolve_telethon_session_file(session)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    if session_file.exists():
        if not os.access(session_file, os.W_OK):
            raise PermissionError(
                "Telethon session 文件不可写：%s。当前运行用户对该文件没有写权限，请修正属主或权限。"
                % str(session_file)
            )
        return session_file
    if not os.access(session_file.parent, os.W_OK):
        raise PermissionError(
            "Telethon session 目录不可写：%s。当前运行用户无法在该目录创建 session 文件。"
            % str(session_file.parent)
        )
    return session_file
