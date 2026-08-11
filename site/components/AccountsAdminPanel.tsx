"use client";

// Sole account-management UI (#508 Phase C). Every action targets an admin-only route whose
// mutation is one Phase-B RPC. Credential input is write-only and cleared after a successful seal.
import { useState } from "react";

import type { AccountAdminRow } from "@/lib/types";

const inputClass =
  "rounded border border-border bg-panelAlt px-2 py-1 text-text focus:border-accent focus:outline-none";
const buttonClass =
  "rounded border border-border px-2 py-1 text-text hover:border-accent disabled:opacity-40";

async function requestJson(url: string, method: "POST" | "PATCH", body: unknown) {
  const response = await fetch(url, {
    method,
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = (await response.json().catch(() => ({}))) as {
    error?: string;
    fingerprint?: string;
  };
  if (!response.ok) throw new Error(payload.error ?? `HTTP ${response.status}`);
  return payload;
}

function CreateAccount({ first }: { first: boolean }) {
  const [accountId, setAccountId] = useState("");
  const [isPrimary, setIsPrimary] = useState(first);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function create() {
    setBusy(true);
    setStatus(null);
    try {
      await requestJson("/api/accounts", "POST", {
        account_id: accountId,
        is_primary: first ? true : isPrimary,
      });
      window.location.reload();
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "account creation failed");
      setBusy(false);
    }
  }

  return (
    <div className="flex flex-wrap items-end gap-3 text-sm">
      <label className="grid gap-1 text-muted">
        account slug
        <input
          className={inputClass}
          value={accountId}
          onChange={(event) => setAccountId(event.target.value)}
          placeholder="primary"
        />
      </label>
      <label className="flex items-center gap-2 pb-1 text-muted">
        <input
          type="checkbox"
          checked={first || isPrimary}
          disabled={first}
          onChange={(event) => setIsPrimary(event.target.checked)}
        />
        primary
      </label>
      <button
        type="button"
        className={buttonClass}
        disabled={busy || accountId === ""}
        onClick={create}
      >
        {busy ? "Creating…" : "Create account"}
      </button>
      {status ? <span className="text-neg">{status}</span> : null}
      {first ? <span className="text-muted">The first account must be primary.</span> : null}
    </div>
  );
}

function AccountControls({ row }: { row: AccountAdminRow }) {
  const [loginEmail, setLoginEmail] = useState(row.login_email ?? "");
  const [enabled, setEnabled] = useState(row.enabled);
  const [executionOrder, setExecutionOrder] = useState(String(row.execution_order));
  const [sizingMode, setSizingMode] = useState(row.live_sizing_mode ?? "");
  const [dollar, setDollar] = useState(String(row.live_sizing_dollar_usd ?? ""));
  const [contracts, setContracts] = useState(String(row.live_sizing_contracts ?? ""));
  const [impactBps, setImpactBps] = useState(String(row.live_price_impact_cap_bps));
  const [requestedMode, setRequestedMode] = useState<"off" | "live_tiny">(
    row.requested_live_mode,
  );
  const [reason, setReason] = useState("");
  const [evidenceRef, setEvidenceRef] = useState("");
  const [keyId, setKeyId] = useState(row.credentials?.key_id ?? "");
  const [credentialJson, setCredentialJson] = useState("{}");
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState(false);
  const [busy, setBusy] = useState(false);
  const endpoint = `/api/accounts/${encodeURIComponent(row.account_id)}`;

  async function mutate(body: unknown, success: string) {
    setBusy(true);
    setStatus(null);
    setError(false);
    try {
      await requestJson(endpoint, "PATCH", body);
      setStatus(success);
    } catch (mutationError) {
      setError(true);
      setStatus(mutationError instanceof Error ? mutationError.message : "mutation failed");
    } finally {
      setBusy(false);
    }
  }

  async function rotateCredentials() {
    let credentials: unknown;
    try {
      credentials = JSON.parse(credentialJson);
    } catch {
      setError(true);
      setStatus("credential JSON is invalid");
      return;
    }
    if (typeof credentials !== "object" || credentials === null || Array.isArray(credentials)) {
      setError(true);
      setStatus("credential JSON must be one object");
      return;
    }

    setBusy(true);
    setStatus(null);
    setError(false);
    try {
      const result = await requestJson(`${endpoint}/credentials`, "POST", {
        key_id: keyId,
        credentials,
      });
      setCredentialJson("{}");
      setStatus(`credentials sealed (${result.fingerprint ?? "fingerprint pending refresh"})`);
    } catch (rotationError) {
      setError(true);
      setStatus(rotationError instanceof Error ? rotationError.message : "rotation failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-4 rounded-lg border border-border bg-panel p-4 text-sm">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h2 className="font-semibold text-text">
            {row.account_id} {row.is_primary ? <span className="text-accent">· primary</span> : null}
          </h2>
          <p className="text-xs text-muted">
            effective {row.effective_live_mode} · requested {row.requested_live_mode}
          </p>
        </div>
        <div className="text-right text-xs text-muted">
          {row.credentials ? (
            <>
              credentials present · v{row.credentials.bundle_version} · {row.credentials.key_id} ·{" "}
              <span className="font-mono">{row.credentials.fingerprint}</span>
            </>
          ) : (
            "credentials absent"
          )}
        </div>
      </div>

      <div className="grid gap-3 border-t border-border pt-3 lg:grid-cols-[1fr_auto]">
        <label className="grid gap-1 text-muted">
          login email (blank clears access)
          <input
            className={inputClass}
            type="email"
            value={loginEmail}
            onChange={(event) => setLoginEmail(event.target.value)}
          />
        </label>
        <button
          type="button"
          className={`${buttonClass} self-end`}
          disabled={busy}
          onClick={() =>
            mutate(
              { action: "login_email", login_email: loginEmail === "" ? null : loginEmail },
              loginEmail === "" ? "login email cleared" : "login email saved",
            )
          }
        >
          {loginEmail === "" ? "Clear login" : "Save login"}
        </button>
      </div>

      <div className="grid gap-3 border-t border-border pt-3 sm:grid-cols-2 lg:grid-cols-4">
        <label className="flex items-center gap-2 text-muted">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(event) => setEnabled(event.target.checked)}
          />
          execution enabled
        </label>
        <label className="grid gap-1 text-muted">
          execution order
          <input
            className={inputClass}
            type="number"
            step="1"
            value={executionOrder}
            onChange={(event) => setExecutionOrder(event.target.value)}
          />
        </label>
        <label className="grid gap-1 text-muted">
          sizing mode
          <select
            className={inputClass}
            value={sizingMode}
            onChange={(event) => setSizingMode(event.target.value)}
          >
            <option value="">unset</option>
            <option value="kelly">kelly</option>
            <option value="dollar">dollar</option>
            <option value="contract">contract</option>
          </select>
        </label>
        <label className="grid gap-1 text-muted">
          impact cap (bps)
          <input
            className={inputClass}
            type="number"
            step="1"
            value={impactBps}
            onChange={(event) => setImpactBps(event.target.value)}
          />
        </label>
        <label className="grid gap-1 text-muted">
          dollar size
          <input
            className={inputClass}
            type="number"
            step="any"
            value={dollar}
            onChange={(event) => setDollar(event.target.value)}
          />
        </label>
        <label className="grid gap-1 text-muted">
          contracts
          <input
            className={inputClass}
            type="number"
            step="1"
            value={contracts}
            onChange={(event) => setContracts(event.target.value)}
          />
        </label>
        <button
          type="button"
          className={`${buttonClass} self-end`}
          disabled={busy}
          onClick={() =>
            mutate(
              {
                action: "live_settings",
                enabled,
                execution_order: Number(executionOrder),
                live_sizing_mode: sizingMode === "" ? null : sizingMode,
                live_sizing_dollar_usd: dollar === "" ? null : dollar,
                live_sizing_contracts: contracts === "" ? null : contracts,
                live_price_impact_cap_bps: Number(impactBps),
              },
              "live settings saved",
            )
          }
        >
          Save live settings
        </button>
      </div>

      <div className="flex flex-wrap items-end gap-3 border-t border-border pt-3">
        <label className="grid gap-1 text-muted">
          requested live mode
          <select
            className={inputClass}
            value={requestedMode}
            onChange={(event) => setRequestedMode(event.target.value as "off" | "live_tiny")}
          >
            <option value="off">off</option>
            <option value="live_tiny">live_tiny</option>
          </select>
        </label>
        <button
          type="button"
          className={buttonClass}
          disabled={busy}
          onClick={() =>
            mutate(
              { action: "request_mode", requested_mode: requestedMode },
              "live mode requested",
            )
          }
        >
          Request mode
        </button>
      </div>

      <div className="grid gap-3 border-t border-border pt-3 lg:grid-cols-[1fr_1fr_auto_auto]">
        <label className="grid gap-1 text-muted">
          review reason
          <input
            className={inputClass}
            value={reason}
            onChange={(event) => setReason(event.target.value)}
          />
        </label>
        <label className="grid gap-1 text-muted">
          evidence ref
          <input
            className={inputClass}
            value={evidenceRef}
            onChange={(event) => setEvidenceRef(event.target.value)}
          />
        </label>
        <button
          type="button"
          className={`${buttonClass} self-end`}
          disabled={busy}
          onClick={() =>
            mutate(
              { action: "promotion_review", reason, evidence_ref: evidenceRef },
              "promotion review recorded",
            )
          }
        >
          Record review
        </button>
        <button
          type="button"
          className={`${buttonClass} self-end`}
          disabled={busy}
          onClick={() =>
            mutate(
              { action: "revoke_promotion_review", reason },
              "promotion review revoked",
            )
          }
        >
          Revoke review
        </button>
      </div>

      <div className="grid gap-3 border-t border-border pt-3 lg:grid-cols-[minmax(12rem,0.5fr)_1fr_auto]">
        <label className="grid gap-1 text-muted">
          recipient key id
          <input
            className={inputClass}
            value={keyId}
            onChange={(event) => setKeyId(event.target.value)}
          />
        </label>
        <label className="grid gap-1 text-muted">
          credential JSON (write-only)
          <textarea
            className={`${inputClass} min-h-24 font-mono text-xs`}
            value={credentialJson}
            onChange={(event) => setCredentialJson(event.target.value)}
            spellCheck={false}
          />
        </label>
        <button
          type="button"
          className={`${buttonClass} self-end`}
          disabled={busy || keyId === ""}
          onClick={rotateCredentials}
        >
          Seal &amp; rotate
        </button>
      </div>

      {status ? <p className={error ? "text-neg" : "text-pos"}>{status}</p> : null}
    </div>
  );
}

export function AccountsAdminPanel({ rows }: { rows: AccountAdminRow[] }) {
  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-border bg-panel p-4">
        <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted">
          Create account
        </h2>
        <CreateAccount first={rows.length === 0} />
      </div>
      {rows.map((row) => (
        <AccountControls key={row.account_id} row={row} />
      ))}
    </div>
  );
}
