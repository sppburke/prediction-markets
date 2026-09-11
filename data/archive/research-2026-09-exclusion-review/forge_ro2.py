import sqlite3, json, time
out = {"observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
c = sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.db?mode=ro", uri=True); c.row_factory = sqlite3.Row
q = lambda s, p=(): [dict(r) for r in c.execute(s, p).fetchall()]
out["all_tombstones"] = q("select wallet_hex, purged_at_unix, reason from purged_wallets")
out["live_flagged_53"] = q("select wallet_hex, is_active, trade_count, source_bits, last_polymarket_fetch_at, discovered_at_unix, last_polymarket_full_at from wallets where is_infra=1")
out["active_trade_volume"] = q("select count(*) n, sum(trade_count) trades from wallets where is_active=1 and is_infra=0")
out["flagged_trade_volume"] = q("select count(*) n, sum(trade_count) trades from wallets where is_infra=1")
out["csvbit_live"] = q("select is_infra, is_active, count(*) n, sum(trade_count) trades from wallets where (source_bits & 4)!=0 group by is_infra, is_active")
a = sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.purge-archive.db?mode=ro", uri=True); a.row_factory = sqlite3.Row
qa = lambda s, p=(): [dict(r) for r in a.execute(s, p).fetchall()]
out["archive_schema"] = qa("select name, sql from sqlite_master where type in ('table','index')")
out["archive_manifest_by_reason"] = qa("select reason, count(*) n, sum(trades_archived) trades, min(purged_at_unix) mn, max(purged_at_unix) mx from purge_manifest group by reason")
out["archive_infra_classes"] = qa("""select (w.source_bits & 4)!=0 as from_csv, w.is_active, (w.last_polymarket_fetch_at is null) as never_fetched, (m.trades_archived=0) as zero_trades,
   count(*) n, sum(m.trades_archived) trades, min(m.purged_at_unix) mn, max(m.purged_at_unix) mx
   from purge_manifest m left join wallets w on w.wallet_hex=m.wallet_hex where m.reason='infra' group by 1,2,3,4 order by n desc""")
out["archive_infra_no_wallet_row"] = qa("select count(*) n from purge_manifest m left join wallets w on w.wallet_hex=m.wallet_hex where m.reason='infra' and w.wallet_hex is null")
out["archive_infra_discovered_hist"] = qa("select w.discovered_at_unix d, count(*) n from purge_manifest m join wallets w on w.wallet_hex=m.wallet_hex where m.reason='infra' group by d order by n desc limit 8")
out["archive_manifest_purged_at_hist"] = qa("select reason, purged_at_unix, count(*) n from purge_manifest group by reason, purged_at_unix order by purged_at_unix")
print(json.dumps(out, default=str))
