import { describe, expect, it, vi } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { AccountsAdminPanel } from "../components/AccountsAdminPanel";
import { classifyServiceReady, readServiceReady, SERVICE_READY_TIMEOUT_MS } from "./service-ready";
import type { AccountAdminRow } from "./types";

describe("service readiness contract", () => {
  it("separates the updated contract from health on 200 and 503", async () => {
    for (const status of [200, 503]) {
      const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({
        ready: status === 200,
        issues: status === 200 ? [] : ["critical_task_failed"],
        live_control_contract: "owner_requested_mode_v1",
      }), { status }));
      expect(await readServiceReady("http://127.0.0.1:8000/health/ready", request)).toEqual({
        contract: "updated", ready: status === 200,
        issues: status === 200 ? [] : ["critical_task_failed"],
      });
      expect(request.mock.calls[0]?.[1]?.cache).toBe("no-store");
    }
  });

  it("recognizes only the legacy readiness shape as old", () => {
    expect(classifyServiceReady({ ready: true })).toMatchObject({ contract: "old" });
    expect(classifyServiceReady({ ready: false, issues: ["stale"] })).toMatchObject({ contract: "old" });
    expect(classifyServiceReady({ status: "ok" })).toMatchObject({ contract: "unverified" });
    expect(classifyServiceReady({ ready: true, issues: 1 })).toMatchObject({ contract: "unverified" });
    expect(classifyServiceReady("bad")).toMatchObject({ contract: "unreachable" });
  });

  it("classifies timeouts, connection errors and unreadable bodies as unreachable", async () => {
    const timeout = vi.fn<typeof fetch>().mockImplementation((_url, options) => new Promise((_resolve, reject) => {
      options?.signal?.addEventListener("abort", () => reject(new Error("timed out")));
    }));
    expect(SERVICE_READY_TIMEOUT_MS).toBe(2_000);
    expect((await readServiceReady("http://localhost:8000/health/ready", timeout)).contract).toBe("unreachable");
    const disconnected = vi.fn<typeof fetch>().mockRejectedValue(new Error("connection refused"));
    expect((await readServiceReady("http://localhost:8000/health/ready", disconnected)).contract).toBe("unreachable");
    const unreadable = vi.fn<typeof fetch>().mockResolvedValue(new Response("not JSON"));
    expect((await readServiceReady("http://localhost:8000/health/ready", unreadable)).contract).toBe("unreachable");
  });

  it("keeps Request mode visible without enabled or review controls", () => {
    const row: AccountAdminRow = {
      account_id: "acct", is_primary: true, login_email: null, enabled: false,
      execution_order: 0, requested_live_mode: "off", effective_live_mode: "off",
      live_sizing_mode: null, live_sizing_dollar_usd: null, live_sizing_contracts: null,
      live_price_impact_cap_bps: 100, created_at: "", updated_at: "", credentials: null,
    };
    const html = renderToStaticMarkup(createElement(AccountsAdminPanel, { rows: [row] }));
    expect(html).toContain("Request mode");
    expect(html).toContain("Historical enabled: false");
    expect(html).not.toContain("promotion review");
    expect(html).not.toContain("execution enabled");
  });
});
