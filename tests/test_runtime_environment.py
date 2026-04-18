from __future__ import annotations

import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.runtime_environment import (
    build_telethon_missing_message,
    project_python_candidates,
    resolve_project_python,
)


class RuntimeEnvironmentTests(unittest.TestCase):
    def test_env_override_is_preferred_python_candidate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_python = Path(tmpdir) / "python"
            fake_python.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
            fake_python.chmod(fake_python.stat().st_mode | stat.S_IXUSR)
            with patch.dict(os.environ, {"PC28_PYTHON_BIN": str(fake_python)}, clear=False):
                candidates = project_python_candidates()
                self.assertEqual(candidates[0], fake_python)
                self.assertEqual(resolve_project_python(), str(fake_python))

    def test_missing_telethon_message_contains_current_python(self):
        message = build_telethon_missing_message()
        self.assertIn("Telethon>=1.42,<2", message)
        self.assertIn("当前进程 Python:", message)


if __name__ == "__main__":
    unittest.main()
