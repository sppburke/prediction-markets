"use client";

// Realized paper P&L per Central Time day (lib/daily-pnl.ts). One list of day cells: a Sunday-first month grid
// from `sm` up, a newest-first list (no padding, weekday header or future days) below it.
import { useState } from "react";

import { type DailyPnl, monthCells, shiftMonth } from "@/lib/daily-pnl";
import { PRECISION, formatUsd, signClass } from "@/lib/format";

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
  "October", "November", "December"];
const SCALE = 10 ** PRECISION.usd;

/** The shown cents (−0 as 0), so sign and tint follow the display: a break-even day is never "-$0.00". */
const cents = (value: number) => Math.round(value * SCALE) / SCALE + 0;

export function PnlCalendar({ days, today }: { days: DailyPnl[]; today: string }) {
  const [month, setMonth] = useState(today.slice(0, 7));
  if (days.length === 0) return <p className="text-xs text-muted">No settled P&L yet.</p>;

  const pnl = new Map(days.map((row) => [row.day, row.pnl]));
  const total = cents(days.reduce((sum, row) => (row.day.startsWith(month) ? sum + row.pnl : sum), 0));
  const name = MONTHS[Number(month.slice(5)) - 1];
  const button = "rounded border border-border px-2 text-muted hover:text-accent disabled:opacity-30";

  return (
    <div>
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <button className={button} aria-label="Previous month" disabled={month <= days[0].day.slice(0, 7)}
            onClick={() => setMonth(shiftMonth(month, -1))}>
            ‹
          </button>
          <span className="w-36 text-center text-sm font-semibold">{`${name} ${month.slice(0, 4)}`}</span>
          <button className={button} aria-label="Next month" disabled={month >= today.slice(0, 7)}
            onClick={() => setMonth(shiftMonth(month, 1))}>
            ›
          </button>
        </div>
        <span className={`text-sm font-semibold tabular-nums sign-${signClass(total)}`}>
          {formatUsd(total, { sign: true })}
        </span>
      </div>
      <div className="flex flex-col-reverse divide-y divide-y-reverse divide-border sm:grid sm:grid-cols-7 sm:gap-1 sm:divide-y-0">
        {WEEKDAYS.map((weekday) => (
          <div key={weekday} className="hidden text-center text-[11px] uppercase tracking-wider text-muted sm:block">
            {weekday}
          </div>
        ))}
        {monthCells(month).map((day, i) => {
          if (day === null) return <div key={`pad-${i}`} className="hidden sm:block" />;
          const raw = pnl.get(day);
          const value = raw === undefined ? undefined : cents(raw);
          const tint = value === undefined || value === 0 ? "" : value > 0 ? "sm:bg-pos/15" : "sm:bg-neg/15";
          return (
            <div key={day}
              className={`${day > today ? "hidden sm:flex" : "flex"} items-center justify-between py-2 text-sm sm:min-h-16 sm:flex-col sm:items-start sm:justify-start sm:rounded sm:border sm:border-border sm:p-1.5 ${tint}`}>
              <span className="text-muted sm:text-[11px]">
                <span className="sm:hidden">{`${WEEKDAYS[new Date(`${day}T12:00:00Z`).getUTCDay()]} ${name.slice(0, 3)} `}</span>
                {Number(day.slice(8))}
              </span>
              {value === undefined ? (
                <span className="text-muted sm:hidden">—</span>
              ) : (
                <span className={`font-semibold tabular-nums sign-${signClass(value)}`}>
                  {formatUsd(value, { sign: true })}
                </span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
