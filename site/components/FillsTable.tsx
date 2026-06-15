import { formatPrice, formatQty } from "@/lib/format";
import type { PaperFill } from "@/lib/types";

function ts(unixOrIso: number | string | null): string {
  if (unixOrIso === null) return "—";
  const ms = typeof unixOrIso === "number" ? unixOrIso * 1000 : Date.parse(unixOrIso);
  if (!Number.isFinite(ms)) return "—";
  return new Date(ms).toISOString().slice(0, 16).replace("T", " ");
}

/** Recent paper fills for a wallet (read from the anon-readable paper_fills table). */
export function FillsTable({ fills }: { fills: PaperFill[] }) {
  if (fills.length === 0) {
    return <div className="p-4 text-xs text-muted">No fills recorded for this wallet.</div>;
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full min-w-[640px] border-collapse text-xs">
        <thead>
          <tr className="bg-panelAlt text-left text-muted">
            <th className="px-3 py-2 font-medium">Entered</th>
            <th className="px-3 py-2 font-medium">Market</th>
            <th className="px-3 py-2 text-right font-medium">Outcome</th>
            <th className="px-3 py-2 text-right font-medium">Side</th>
            <th className="px-3 py-2 text-right font-medium">Contracts</th>
            <th className="px-3 py-2 text-right font-medium">Fill price</th>
          </tr>
        </thead>
        <tbody>
          {fills.map((f) => (
            <tr key={f.idempotency_key} className="border-t border-border tabular-nums hover:bg-panelAlt">
              <td className="whitespace-nowrap px-3 py-2 text-muted">{ts(f.entry_unix ?? f.inserted_at)}</td>
              <td className="px-3 py-2 font-mono text-[11px]">{f.market_id}</td>
              <td className="px-3 py-2 text-right">{f.outcome_id}</td>
              <td className={`px-3 py-2 text-right ${f.side === "buy" ? "text-pos" : "text-neg"}`}>{f.side}</td>
              <td className="px-3 py-2 text-right">{formatQty(f.contracts)}</td>
              <td className="px-3 py-2 text-right">{formatPrice(f.fill_price)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
