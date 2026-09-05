# 23 — Retired Gamma fee backfill

The historical Gamma-to-`market_fees` design is retired by #545. Fresh caches no
longer create or write that table; existing rows remain untouched and unread for
rollback compatibility. Runtime economics use the compact CLOB fee schedule owned
by `venue-polymarket`.
