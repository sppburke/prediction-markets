"use client";

// Editable table of `service_config` knobs (#398 WS3, step 17). Each row gets a typed input keyed
// off `value_type`; Save PATCHes /api/config (service-role write, server-side session check) and
// optimistically reflects the result. Values are always stored as strings (the column is `text`).
import { useState } from "react";

import type { ConfigValueType, ServiceConfigRow } from "@/lib/types";

type SaveState = "idle" | "saving" | "saved" | "error";

function TypedInput({
  type,
  value,
  onChange,
}: {
  type: ConfigValueType;
  value: string;
  onChange: (v: string) => void;
}) {
  const cls =
    "w-40 rounded border border-border bg-panelAlt px-2 py-1 text-text focus:border-accent focus:outline-none";
  if (type === "bool") {
    return (
      <select className={cls} value={value} onChange={(e) => onChange(e.target.value)}>
        <option value="true">true</option>
        <option value="false">false</option>
      </select>
    );
  }
  if (type === "integer" || type === "decimal") {
    return (
      <input
        className={cls}
        type="number"
        step={type === "integer" ? "1" : "any"}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    );
  }
  return <input className={cls} type="text" value={value} onChange={(e) => onChange(e.target.value)} />;
}

function ConfigRow({ row }: { row: ServiceConfigRow }) {
  const [value, setValue] = useState(row.value);
  const [state, setState] = useState<SaveState>("idle");
  const [error, setError] = useState<string | null>(null);
  const dirty = value !== row.value;

  async function save() {
    setState("saving");
    setError(null);
    try {
      const res = await fetch("/api/config", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ key: row.key, value }),
      });
      if (!res.ok) {
        const body = (await res.json().catch(() => ({}))) as { error?: string };
        throw new Error(body.error ?? `HTTP ${res.status}`);
      }
      setState("saved");
    } catch (e) {
      setState("error");
      setError(e instanceof Error ? e.message : "save failed");
    }
  }

  return (
    <tr className="border-t border-border align-top">
      <td className="py-2 pr-3 font-medium text-text">{row.key}</td>
      <td className="py-2 pr-3 text-muted">{row.value_type}</td>
      <td className="py-2 pr-3">
        <TypedInput
          type={row.value_type}
          value={value}
          onChange={(v) => {
            setValue(v);
            setState("idle");
          }}
        />
      </td>
      <td className="py-2 pr-3">
        <button
          type="button"
          disabled={!dirty || state === "saving"}
          onClick={save}
          className="rounded border border-border px-2 py-1 text-text enabled:hover:border-accent disabled:opacity-40"
        >
          {state === "saving" ? "Saving…" : "Save"}
        </button>{" "}
        {state === "saved" && <span className="text-pos">saved</span>}
        {state === "error" && <span className="text-neg">{error}</span>}
      </td>
      <td className="py-2 text-muted">{row.description}</td>
    </tr>
  );
}

export function AdminConfigTable({ rows }: { rows: ServiceConfigRow[] }) {
  if (rows.length === 0) {
    return <p className="text-muted">No config rows.</p>;
  }
  return (
    <table className="w-full text-left text-sm">
      <thead>
        <tr className="text-muted">
          <th className="pb-2 pr-3 font-normal">key</th>
          <th className="pb-2 pr-3 font-normal">type</th>
          <th className="pb-2 pr-3 font-normal">value</th>
          <th className="pb-2 pr-3 font-normal"></th>
          <th className="pb-2 font-normal">description</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <ConfigRow key={row.key} row={row} />
        ))}
      </tbody>
    </table>
  );
}
