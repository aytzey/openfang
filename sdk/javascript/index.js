/**
 * @pulsivo-salesman/sdk — Official JavaScript client for the Pulsivo Salesman sales API.
 *
 * Usage:
 *   const { PulsivoSalesman } = require("@pulsivo-salesman/sdk");
 *   const client = new PulsivoSalesman("http://localhost:4200");
 *
 *   const profile = await client.sales.getProfile("b2c");
 *   const runs = await client.sales.listRuns({ segment: "b2c", limit: 5 });
 *   console.log(profile, runs);
 */

"use strict";

class PulsivoSalesmanError extends Error {
  constructor(message, status, body) {
    super(message);
    this.name = "PulsivoSalesmanError";
    this.status = status;
    this.body = body;
  }
}

function buildQuery(params) {
  var query = new URLSearchParams();
  Object.entries(params || {}).forEach(function (entry) {
    var key = entry[0];
    var value = entry[1];
    if (value === undefined || value === null || value === "") return;
    query.set(key, String(value));
  });
  var rendered = query.toString();
  return rendered ? "?" + rendered : "";
}

class PulsivoSalesman {
  /**
   * @param {string} baseUrl - Pulsivo Salesman server URL (e.g. "http://localhost:4200")
   * @param {object} [opts]
   * @param {Record<string, string>} [opts.headers] - Extra headers for every request
   */
  constructor(baseUrl, opts) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this._headers = Object.assign({ "Content-Type": "application/json" }, (opts && opts.headers) || {});
    this.sales = new SalesResource(this);
  }

  async _request(method, path, body) {
    var url = this.baseUrl + path;
    var init = { method: method, headers: Object.assign({}, this._headers) };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
    }
    var res = await fetch(url, init);
    if (!res.ok) {
      var text = await res.text().catch(function () { return ""; });
      throw new PulsivoSalesmanError("HTTP " + res.status + ": " + text, res.status, text);
    }
    var ct = res.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
      return res.json();
    }
    return res.text();
  }

  async health() {
    return this._request("GET", "/api/health");
  }

  async healthDetail() {
    return this._request("GET", "/api/health/detail");
  }

  async status() {
    return this._request("GET", "/api/status");
  }

  async version() {
    return this._request("GET", "/api/version");
  }

  async metrics() {
    return this._request("GET", "/api/metrics");
  }
}

class SalesResource {
  constructor(client) {
    this._c = client;
  }

  async getProfile(segment) {
    return this._c._request("GET", "/api/sales/profile" + buildQuery({ segment: segment }));
  }

  async updateProfile(profile, opts) {
    return this._c._request(
      "PUT",
      "/api/sales/profile" + buildQuery({ segment: opts && opts.segment }),
      profile,
    );
  }

  async autofillProfile(brief, opts) {
    return this._c._request(
      "POST",
      "/api/sales/profile/autofill" + buildQuery({ segment: opts && opts.segment }),
      { brief: brief, persist: opts && opts.persist },
    );
  }

  async getOnboardingStatus(segment) {
    return this._c._request("GET", "/api/sales/onboarding/status" + buildQuery({ segment: segment }));
  }

  async updateOnboardingBrief(brief, opts) {
    return this._c._request(
      "POST",
      "/api/sales/onboarding/brief" + buildQuery({ segment: opts && opts.segment }),
      { brief: brief, persist: opts && opts.persist },
    );
  }

  async run(opts) {
    return this._c._request("POST", "/api/sales/run" + buildQuery({ segment: opts && opts.segment }));
  }

  async getActiveJob(segment) {
    return this._c._request("GET", "/api/sales/jobs/active" + buildQuery({ segment: segment }));
  }

  async getJob(jobId) {
    return this._c._request("GET", "/api/sales/jobs/" + encodeURIComponent(jobId) + "/progress");
  }

  async retryJob(jobId, opts) {
    return this._c._request(
      "POST",
      "/api/sales/jobs/" + encodeURIComponent(jobId) + "/retry" + buildQuery({ segment: opts && opts.segment }),
      { force_fresh: !!(opts && opts.forceFresh) },
    );
  }

  async sourceHealth() {
    return this._c._request("GET", "/api/sales/source-health");
  }

  async listRuns(opts) {
    return this._c._request("GET", "/api/sales/runs" + buildQuery(opts));
  }

  async listLeads(opts) {
    return this._c._request("GET", "/api/sales/leads" + buildQuery({
      segment: opts && opts.segment,
      limit: opts && opts.limit,
      run_id: opts && opts.runId,
    }));
  }

  async listProspects(opts) {
    return this._c._request("GET", "/api/sales/prospects" + buildQuery({
      segment: opts && opts.segment,
      limit: opts && opts.limit,
      run_id: opts && opts.runId,
    }));
  }

  async getAccountDossier(id) {
    return this._c._request("GET", "/api/sales/accounts/" + encodeURIComponent(id) + "/dossier");
  }

  async listApprovals(opts) {
    return this._c._request("GET", "/api/sales/approvals" + buildQuery(opts));
  }

  async bulkApprove(ids) {
    return this._c._request("POST", "/api/sales/approvals/bulk-approve", { ids: ids });
  }

  async editApproval(id, editedPayload) {
    return this._c._request(
      "PATCH",
      "/api/sales/approvals/" + encodeURIComponent(id) + "/edit",
      { edited_payload: editedPayload },
    );
  }

  async approve(id) {
    return this._c._request("POST", "/api/sales/approvals/" + encodeURIComponent(id) + "/approve");
  }

  async reject(id) {
    return this._c._request("POST", "/api/sales/approvals/" + encodeURIComponent(id) + "/reject", {});
  }

  async listDeliveries(opts) {
    return this._c._request("GET", "/api/sales/deliveries" + buildQuery({ limit: opts && opts.limit }));
  }
}

module.exports = {
  PulsivoSalesman: PulsivoSalesman,
  PulsivoSalesmanError: PulsivoSalesmanError,
  SalesResource: SalesResource,
};
