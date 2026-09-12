#!/usr/bin/env python3
"""Read-only schema-one requested-history audit; see docs/26 for its limits."""

import argparse
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
import json
import math
import sqlite3
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen

PAGE_LIMIT = 500
MAX_OFFSET = 5000
# time::OffsetDateTime without large-dates, matching the schema-one converter.
MIN_TIMESTAMP = -377705116800
MAX_TIMESTAMP = 253402300799


def timestamp_seconds(timestamp):
    return timestamp // 1000 if timestamp > 9_999_999_999 else timestamp


def _integer(value, minimum, maximum, name):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"invalid DTO {name}: {value!r}")
    return value


def _decimal(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError(f"invalid DTO decimal: {value!r}")
    # serde_json visits fractional numbers (and integers outside i64/u64) as
    # f64; rust_decimal 1.41 then parses the float's decimal display. Strings
    # instead retain their decimal digits and may use scientific notation.
    if isinstance(value, float) or isinstance(value, int) and not -(2**63) <= value <= 2**64 - 1:
        value = float(value)
        if not math.isfinite(value):
            raise ValueError(f"invalid DTO decimal: {value!r}")
        value = format(Decimal(str(value)), "f")
    text = str(value)
    mantissa, separator, exponent = text.lower().partition("e")
    try:
        result = Decimal(mantissa)
        if not result.is_finite() or result.copy_abs() > Decimal(2**96 - 1):
            raise ValueError(f"invalid DTO decimal: {value!r}")
        # Decimal::from_str rounds excess scale or coefficient digits, with
        # midpoint rounding away from zero, into its 96-bit coefficient.
        scale = min(28, max(0, -result.as_tuple().exponent))
        with localcontext() as context:
            context.prec = 96
            while True:
                rounded = result.quantize(Decimal((0, (1,), -scale)), rounding=ROUND_HALF_UP)
                if rounded.copy_abs().scaleb(scale) <= 2**96 - 1:
                    result = rounded
                    break
                if scale == 0:
                    raise ValueError(f"invalid DTO decimal: {value!r}")
                scale -= 1
            if separator:
                shift = int(exponent)
                if abs(shift) > 28 or shift < 0 and scale - shift > 28:
                    raise ValueError(f"invalid DTO decimal scale: {value!r}")
                result = result.scaleb(shift)
                if result.copy_abs() > 2**96 - 1:
                    raise ValueError(f"invalid DTO decimal: {value!r}")
    except (InvalidOperation, OverflowError) as error:
        raise ValueError(f"invalid DTO decimal: {value!r}") from error
    return result


def parse_page(payload):
    """Validate the entire DTO array before performing any row conversions."""
    raw = json.loads(payload)
    if not isinstance(raw, list):
        raise ValueError("activity response is not an array")
    validated = []
    for row in raw:
        if not isinstance(row, dict):
            raise ValueError("activity DTO is not an object")
        for key in ("transactionHash", "conditionId", "side"):
            if not isinstance(row.get(key), str):
                raise ValueError(f"invalid DTO {key}")
        value = dict(row)
        value["price"] = _decimal(row.get("price"))
        value["size"] = _decimal(row.get("size"))
        value["timestamp"] = _integer(row.get("timestamp"), -(2**63), 2**63 - 1, "timestamp")
        outcome = row.get("outcomeIndex")
        value["outcomeIndex"] = 0 if outcome is None else _integer(outcome, 0, 65535, "outcomeIndex")
        validated.append(value)
    return validated


def convert_row(row):
    """Mirror schema-one row rejection and normalization; rejected rows return None."""
    price, size = row["price"], row["size"]
    second = timestamp_seconds(row["timestamp"])
    if (not 0 <= price <= 1 or size <= 0 or int(size) > 2**64 - 1
            or row["side"].upper() not in ("BUY", "SELL")
            or not MIN_TIMESTAMP <= second <= MAX_TIMESTAMP):
        return None
    return {
        "source_trade_id": row["transactionHash"],
        "market_id": row["conditionId"],
        "outcome_id": row["outcomeIndex"],
        "side": row["side"].lower(),
        "price_str": str(price),
        "contracts": max(1, int(size)),
        "timestamp_unix": second,
    }


def fetch_all_activity(wallet, cutoff, base_url="https://data-api.polymarket.com", fetch=None):
    """DESC backward pages, completing every boundary second before stepping below it.

    Any failed page or saturated second aborts the audit; partial reads are never
    passed to compare(). ``fetch`` is a deterministic transport seam for tests.
    """
    if fetch is None:
        def fetch(url):
            request = Request(url, headers={"User-Agent": "prediction-edge/1.0"})
            with urlopen(request, timeout=30) as response:
                return response.read()

    def page(start, end, offset):
        query = urlencode(dict(user=wallet, type="TRADE", limit=PAGE_LIMIT,
                               offset=offset, sortDirection="DESC", end=end, start=start))
        raw = parse_page(fetch(f"{base_url.rstrip('/')}/activity?{query}"))
        seconds = [timestamp_seconds(r["timestamp"]) for r in raw
                   if MIN_TIMESTAMP <= timestamp_seconds(r["timestamp"]) <= MAX_TIMESTAMP]
        if (len(raw) > PAGE_LIMIT or any(t < start or t > end for t in seconds)
                or (len(raw) == PAGE_LIMIT and not seconds)):
            raise ValueError("activity page has no valid full-page boundary or violates requested bounds")
        rows = [converted for r in raw if (converted := convert_row(r)) is not None]
        return rows, len(raw), min(seconds) if seconds else None

    rows = []
    end = cutoff
    while end >= 1:
        batch, count, boundary = page(1, end, 0)
        if count < PAGE_LIMIT:
            rows.extend(batch)
            break
        rows.extend(r for r in batch if r["timestamp_unix"] > boundary)
        for offset in range(0, MAX_OFFSET + 1, PAGE_LIMIT):
            batch, count, _ = page(boundary, boundary, offset)
            rows.extend(batch)
            if count < PAGE_LIMIT:
                break
        else:
            raise ValueError(f"saturated second {boundary}; audit incomplete")
        end = boundary - 1
    return rows


def load_cache(db, wallet, cutoff):
    """Check marker/frontier and read rows from one consistent read-only snapshot."""
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        con.execute("BEGIN")
        if int(con.execute("PRAGMA user_version").fetchone()[0]) >= 2:
            raise ValueError("audit requires schema one")
        state = con.execute(
            "SELECT backfill_partial, forward_frontier_unix FROM wallets WHERE wallet_hex = ?",
            (wallet,),
        ).fetchone()
        if state is None or state[0] != 0 or state[1] is None or cutoff > state[1]:
            raise ValueError("audit requires backfill_partial = 0 and cutoff <= forward_frontier_unix")
        return [dict(r) for r in con.execute(
            "SELECT source_trade_id, market_id, outcome_id, side, price_str, contracts, timestamp_unix "
            "FROM trades WHERE wallet_hex = ? AND timestamp_unix <= ? ORDER BY source_trade_id",
            (wallet, cutoff),
        )]
    finally:
        con.close()


def compare(venue_rows, cached_rows):
    """Report intra-wallet collisions BEFORE collapsing schema-one transaction ids."""
    grouped = defaultdict(list)
    for row in venue_rows:
        # Equal decimals with different lexical scales denote the same normalized row.
        normalized = {**row, "price_str": str(Decimal(row["price_str"]).normalize())}
        if normalized not in grouped[row["source_trade_id"]]:
            grouped[row["source_trade_id"]].append(normalized)
    cached = {r["source_trade_id"]: r for r in cached_rows}
    collisions = [dict(source_trade_id=tid, normalized_rows=rows,
                       stored_representative=cached.get(tid))
                  for tid, rows in sorted(grouped.items()) if len(rows) > 1]
    venue = {tid: rows[0] for tid, rows in grouped.items()}

    def days(rows):
        # Epoch-day keys cover the converter's full time range, without datetime's
        # narrower year range. Render ordinary operational dates as UTC ISO days.
        from datetime import date, timedelta
        counts = Counter(r["timestamp_unix"] // 86400 for r in rows)
        result = {}
        for day, count in sorted(counts.items()):
            try:
                key = str(date(1970, 1, 1) + timedelta(days=day))
            except OverflowError:
                key = f"UTC epoch day {day}"
            result[key] = count
        return result

    missing = sorted(venue.keys() - cached.keys())
    extra = sorted(cached.keys() - venue.keys())
    return dict(ok=not missing and not extra and not collisions,
                missing_ids=missing, extra_ids=extra, collisions=collisions,
                venue_per_utc_day=days(venue.values()), cache_per_utc_day=days(cached.values()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--cutoff", required=True, type=int)
    parser.add_argument("--base-url", default="https://data-api.polymarket.com")
    args = parser.parse_args()
    try:
        wallet = args.wallet.lower()
        cached = load_cache(args.db, wallet, args.cutoff)
        venue = fetch_all_activity(wallet, args.cutoff, args.base_url)
        result = compare(venue, cached)
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0 if result["ok"] else 1
    except (ValueError, OSError, sqlite3.Error) as error:
        print(f"audit refused: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
