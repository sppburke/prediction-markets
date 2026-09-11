import sqlite3, json, os, sys, time
W = """0x1b912b17581b3d544431d13d5a060155a80127dc
0xfd8e46519d0a8f9c35e5010ef4e7f56f7583aea4
0x72e1597864456eda62878413cf3e60c332e4a45d
0x0bf96a1c55e6f47ea84335bc3fc08a89653efd90
0x5a3354a0a35d00d2a1dbe6cea0e37e9c30a2ca0d
0xdf6539d1fadb951a02a999d31a72e0cd7fd9c36d
0xb7ab821f037a4c8deb9b23b8001a4be985d116d0
0x057621bea5fc03a53af38530c95b17a510716232
0xc561b14904d769eda31628082c005362ba60dc22
0x9fbfe50bf171adaa347a5cb2b789b4a6e12ef003
0x31646b754f77b973910e7376b7015cb81fe83c65
0x38d812aff0b79f3bf5da2a477f780bcc163eea7c
0x9c76cdb43fb46454da005fbc82047a64a18ec926
0xdc5bd11896bfb0fb335dad88d99a7e9a6bc3f102
0x330f6bf24e33d8348593bca54017ce83423791b1
0x1f19c48aee80ec95396d91f0d21ac249b8a7f57a
0x06dc51826bc524d9a83770e7de9dd7e005b04524
0x88ec5ba618625d744988f87ab577be53b382ec61
0xdfe29d6ef2a44606cf58967e1fd5854d9e594902
0xaf17116ae2b1476032785a67bd5b7c8c05905c20
0xc33d6aa3eb972639f31e46a6a02201cece380d40
0xa8ae2fb989de545c42c376d61ec887868ca57f3e
0x4e9e342ff236323b43f79c0da642a82bd12f0c30
0xdb5ad26b68d77ae966d29e7180147272ab7a3965
0x5215b36ebe0f78b3114eb998ca8ade49402ab02f
0x8f1dfd0868d056f11f84e0233e1b89527c262fb6
0xc7d02944a76b9f83b199e9090ecc92c82d241f8a
""".split()
out = {"observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
base = "/home/pi/prediction-markets/data/wallet_cache.db"
out["realpath"] = os.path.realpath(base)
out["archive_realpath"] = os.path.realpath("/home/pi/prediction-markets/data/wallet_cache.purge-archive.db")
c = sqlite3.connect(f"file:{base}?mode=ro", uri=True)
c.row_factory = sqlite3.Row
q = lambda s, p=(): [dict(r) for r in c.execute(s, p).fetchall()]
out["user_version"] = q("PRAGMA user_version")
out["schema"] = q("select name, sql from sqlite_master where name in ('wallets','purged_wallets','active_tradeable_wallets','meta','wallet_activation_batches')")
out["tomb_by_reason"] = q("select reason, count(*) n, min(purged_at_unix) mn, max(purged_at_unix) mx from purged_wallets group by reason")
out["is_infra_count"] = q("select is_infra, is_active, count(*) n from wallets group by is_infra, is_active")
out["tomb_and_wallet_row"] = q("select count(*) n from purged_wallets p join wallets w on w.wallet_hex=p.wallet_hex")
out["tomb_and_wallet_row_by_reason_infra"] = q("select p.reason, w.is_infra, w.is_active, count(*) n from purged_wallets p join wallets w on w.wallet_hex=p.wallet_hex group by p.reason, w.is_infra, w.is_active")
ph = ",".join("?"*len(W))
out["cohort_tomb"] = q(f"select * from purged_wallets where wallet_hex in ({ph})", W)
out["cohort_wallets"] = q(f"select * from wallets where wallet_hex in ({ph})", W)
out["cohort_trades"] = q(f"select wallet_hex, count(*) n, min(timestamp_unix) mn, max(timestamp_unix) mx from trades where wallet_hex in ({ph}) group by wallet_hex", W)
out["meta"] = q("select * from meta") if any(r["name"]=="meta" for r in out["schema"]) else None
try:
    a = sqlite3.connect("file:/home/pi/prediction-markets/data/wallet_cache.purge-archive.db?mode=ro", uri=True)
    a.row_factory = sqlite3.Row
    qa = lambda s, p=(): [dict(r) for r in a.execute(s, p).fetchall()]
    out["archive_tables"] = qa("select name from sqlite_master where type='table'")
    out["archive_cohort_wallets"] = qa(f"select * from wallets where wallet_hex in ({ph})", W)
    out["archive_cohort_trades"] = qa(f"select wallet_hex, count(*) n, min(timestamp_unix) mn, max(timestamp_unix) mx from trades where wallet_hex in ({ph}) group by wallet_hex", W)
    try:
        out["archive_cohort_manifest"] = qa(f"select * from purge_manifest where wallet_hex in ({ph})", W)
    except Exception as e:
        out["archive_manifest_err"] = str(e)
except Exception as e:
    out["archive_err"] = str(e)
print(json.dumps(out, default=str))
