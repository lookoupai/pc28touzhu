from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any, Dict, List


class ExecutorApiClient:
    def __init__(self, *, base_url: str, token: str, executor_id: str):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.executor_id = executor_id

    def _request(self, method: str, path: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        headers = {
            "Authorization": "Bearer %s" % self.token,
            "X-Executor-Id": self.executor_id,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(self.base_url + path, data=body, method=method.upper(), headers=headers)
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}

    def heartbeat(self, *, version: str, capabilities: Dict[str, Any]) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/executor/heartbeat",
            payload={"version": version, "capabilities": capabilities},
        )

    def pull_jobs(self, *, limit: int) -> List[Dict[str, Any]]:
        path = "/api/executor/jobs/pull?" + urllib.parse.urlencode({"limit": str(limit)})
        payload = self._request("GET", path)
        items = payload.get("items") or []
        return items if isinstance(items, list) else []

    def report_job(self, *, job_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/api/executor/jobs/%s/report" % job_id, payload=payload)

