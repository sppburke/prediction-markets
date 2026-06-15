import type { Numeric } from "@/lib/format";
import { signClass } from "@/lib/format";

/** A labelled value. `tone="signed"` colours by the sign of `signOf`. */
export function Stat({
  label,
  value,
  tone = "plain",
  signOf,
  sub,
}: {
  label: string;
  value: string;
  tone?: "plain" | "signed" | "muted";
  signOf?: Numeric;
  sub?: string;
}) {
  let cls = "text-text";
  if (tone === "muted") cls = "text-muted";
  if (tone === "signed") {
    const s = signClass(signOf);
    cls = s === "pos" ? "text-pos" : s === "neg" ? "text-neg" : "text-muted";
  }
  return (
    <div>
      <div className="text-[11px] uppercase tracking-wider text-muted">{label}</div>
      <div className={`text-lg font-semibold tabular-nums ${cls}`}>{value}</div>
      {sub ? <div className="text-[11px] text-muted">{sub}</div> : null}
    </div>
  );
}
