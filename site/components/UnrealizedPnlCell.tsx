// Renders one wallet's unrealized P&L (#398 WS3, step 19). `undefined` (marks not yet loaded, or
// no open position) renders an em-dash via formatUsd(null).
import { formatUsd, signClass, type Numeric } from "@/lib/format";

export function UnrealizedPnlCell({ value }: { value: Numeric }) {
  return <span className={`sign-${signClass(value)}`}>{formatUsd(value, { sign: true })}</span>;
}
