"""
Pulsivo Salesman Python Client — sales REST API client.

Usage:

    from pulsivo_salesman_client import PulsivoSalesman

    client = PulsivoSalesman("http://localhost:4200")
    profile = client.sales.get_profile("b2c")
    runs = client.sales.list_runs(segment="b2c", limit=5)
    print(profile)
    print(runs)
"""

import json
from typing import Any, Dict, Optional
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class PulsivoSalesmanError(Exception):
    def __init__(self, message: str, status: int = 0, body: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body


def _with_query(path: str, params: Optional[Dict[str, Any]] = None) -> str:
    if not params:
        return path
    filtered = {
        key: value
        for key, value in params.items()
        if value is not None and value != ""
    }
    if not filtered:
        return path
    return f"{path}?{urlencode(filtered)}"


class _Resource:
    def __init__(self, client: "PulsivoSalesman"):
        self._c = client


class PulsivoSalesman:
    """Pulsivo Salesman sales REST client. Zero dependencies — stdlib urllib only."""

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None):
        self.base_url = base_url.rstrip("/")
        self._headers = {"Content-Type": "application/json"}
        if headers:
            self._headers.update(headers)

        self.sales = _SalesResource(self)

    def _request(self, method: str, path: str, body: Any = None) -> Any:
        url = self.base_url + path
        data = json.dumps(body).encode() if body is not None else None
        req = Request(url, data=data, headers=self._headers, method=method)
        try:
            with urlopen(req) as resp:
                ct = resp.headers.get("content-type", "")
                text = resp.read().decode()
                if "application/json" in ct:
                    return json.loads(text)
                return text
        except HTTPError as e:
            body_text = e.read().decode() if e.fp else ""
            raise PulsivoSalesmanError(f"HTTP {e.code}: {body_text}", e.code, body_text) from e

    def health(self) -> Any:
        return self._request("GET", "/api/health")

    def health_detail(self) -> Any:
        return self._request("GET", "/api/health/detail")

    def status(self) -> Any:
        return self._request("GET", "/api/status")

    def version(self) -> Any:
        return self._request("GET", "/api/version")

    def metrics(self) -> str:
        return self._request("GET", "/api/metrics")


class _SalesResource(_Resource):
    def get_profile(self, segment: Optional[str] = None):
        return self._c._request("GET", _with_query("/api/sales/profile", {"segment": segment}))

    def update_profile(self, profile: Dict[str, Any], segment: Optional[str] = None):
        return self._c._request(
            "PUT",
            _with_query("/api/sales/profile", {"segment": segment}),
            profile,
        )

    def autofill_profile(
        self,
        brief: str,
        segment: Optional[str] = None,
        persist: Optional[bool] = None,
    ):
        return self._c._request(
            "POST",
            _with_query("/api/sales/profile/autofill", {"segment": segment}),
            {"brief": brief, "persist": persist},
        )

    def get_onboarding_status(self, segment: Optional[str] = None):
        return self._c._request(
            "GET",
            _with_query("/api/sales/onboarding/status", {"segment": segment}),
        )

    def update_onboarding_brief(
        self,
        brief: str,
        segment: Optional[str] = None,
        persist: Optional[bool] = None,
    ):
        return self._c._request(
            "POST",
            _with_query("/api/sales/onboarding/brief", {"segment": segment}),
            {"brief": brief, "persist": persist},
        )

    def run(self, segment: Optional[str] = None):
        return self._c._request("POST", _with_query("/api/sales/run", {"segment": segment}))

    def get_active_job(self, segment: Optional[str] = None):
        return self._c._request(
            "GET",
            _with_query("/api/sales/jobs/active", {"segment": segment}),
        )

    def get_job(self, job_id: str):
        return self._c._request("GET", f"/api/sales/jobs/{job_id}/progress")

    def retry_job(
        self,
        job_id: str,
        segment: Optional[str] = None,
        force_fresh: bool = False,
    ):
        return self._c._request(
            "POST",
            _with_query(f"/api/sales/jobs/{job_id}/retry", {"segment": segment}),
            {"force_fresh": force_fresh},
        )

    def source_health(self):
        return self._c._request("GET", "/api/sales/source-health")

    def list_runs(
        self,
        segment: Optional[str] = None,
        limit: Optional[int] = None,
        run_id: Optional[str] = None,
    ):
        return self._c._request(
            "GET",
            _with_query(
                "/api/sales/runs",
                {"segment": segment, "limit": limit, "run_id": run_id},
            ),
        )

    def list_leads(
        self,
        segment: Optional[str] = None,
        limit: Optional[int] = None,
        run_id: Optional[str] = None,
    ):
        return self._c._request(
            "GET",
            _with_query(
                "/api/sales/leads",
                {"segment": segment, "limit": limit, "run_id": run_id},
            ),
        )

    def list_prospects(
        self,
        segment: Optional[str] = None,
        limit: Optional[int] = None,
        run_id: Optional[str] = None,
    ):
        return self._c._request(
            "GET",
            _with_query(
                "/api/sales/prospects",
                {"segment": segment, "limit": limit, "run_id": run_id},
            ),
        )

    def get_account_dossier(self, account_id: str):
        return self._c._request("GET", f"/api/sales/accounts/{account_id}/dossier")

    def list_approvals(self, status: Optional[str] = None, limit: Optional[int] = None):
        return self._c._request(
            "GET",
            _with_query("/api/sales/approvals", {"status": status, "limit": limit}),
        )

    def bulk_approve(self, ids):
        return self._c._request("POST", "/api/sales/approvals/bulk-approve", {"ids": ids})

    def edit_approval(self, approval_id: str, edited_payload: Dict[str, Any]):
        return self._c._request(
            "PATCH",
            f"/api/sales/approvals/{approval_id}/edit",
            {"edited_payload": edited_payload},
        )

    def approve(self, approval_id: str):
        return self._c._request("POST", f"/api/sales/approvals/{approval_id}/approve")

    def reject(self, approval_id: str):
        return self._c._request(
            "POST",
            f"/api/sales/approvals/{approval_id}/reject",
            {},
        )

    def list_deliveries(self, limit: Optional[int] = None):
        return self._c._request(
            "GET",
            _with_query("/api/sales/deliveries", {"limit": limit}),
        )
