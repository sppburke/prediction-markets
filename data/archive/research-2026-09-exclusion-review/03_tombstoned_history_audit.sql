-- Read-only audit of the 27 tombstoned screen matches.
-- Extends trade history to 180 days and exports qualifying first-observed BUY evidence.
-- Still NOT complete activity-ledger reconstruction or a follower execution simulation.
WITH by_market AS (
 SELECT maker AS wallet, condition_id,
 min(block_time) FILTER(WHERE maker_side='BUY') AS first_buy,
 min_by(CAST(ROW(asset_id,price,is_taker_side,tx_hash,evt_index,fee,shares) AS
    ROW(token_id UINT256,price DOUBLE,aggressive BOOLEAN,tx_hash VARBINARY,evt_index INTEGER,fee DOUBLE,shares DOUBLE)),
    ROW(block_time,block_number,tx_hash,evt_index)) FILTER(WHERE maker_side='BUY') AS fb,
 max(block_time) AS last_trade,
 min(block_time) AS first_observed_trade,
 count(*) AS raw_fills,
 count(DISTINCT asset_id) FILTER(WHERE maker_side='BUY') AS bought_outcomes,
 count_if(maker_side='SELL') AS sell_fills,
 sum(amount) FILTER(WHERE maker_side='BUY') AS bought_usd,
 sum(amount) FILTER(WHERE maker_side='SELL') AS sold_usd
 FROM polymarket_polygon.market_trades
 WHERE block_month >= DATE '2026-03-01'
 AND block_time >= TIMESTAMP '2026-03-14 00:00:00'
 AND block_time < TIMESTAMP '2026-09-10 00:00:00'
 AND action='CLOB trade'
 AND maker IN (0x1b912b17581b3d544431d13d5a060155a80127dc,0xfd8e46519d0a8f9c35e5010ef4e7f56f7583aea4,0x72e1597864456eda62878413cf3e60c332e4a45d,0x0bf96a1c55e6f47ea84335bc3fc08a89653efd90,0x5a3354a0a35d00d2a1dbe6cea0e37e9c30a2ca0d,0xdf6539d1fadb951a02a999d31a72e0cd7fd9c36d,0xb7ab821f037a4c8deb9b23b8001a4be985d116d0,0x057621bea5fc03a53af38530c95b17a510716232,0xc561b14904d769eda31628082c005362ba60dc22,0x9fbfe50bf171adaa347a5cb2b789b4a6e12ef003,0x31646b754f77b973910e7376b7015cb81fe83c65,0x38d812aff0b79f3bf5da2a477f780bcc163eea7c,0x9c76cdb43fb46454da005fbc82047a64a18ec926,0xdc5bd11896bfb0fb335dad88d99a7e9a6bc3f102,0x330f6bf24e33d8348593bca54017ce83423791b1,0x1f19c48aee80ec95396d91f0d21ac249b8a7f57a,0x06dc51826bc524d9a83770e7de9dd7e005b04524,0x88ec5ba618625d744988f87ab577be53b382ec61,0xdfe29d6ef2a44606cf58967e1fd5854d9e594902,0xaf17116ae2b1476032785a67bd5b7c8c05905c20,0xc33d6aa3eb972639f31e46a6a02201cece380d40,0xa8ae2fb989de545c42c376d61ec887868ca57f3e,0x4e9e342ff236323b43f79c0da642a82bd12f0c30,0xdb5ad26b68d77ae966d29e7180147272ab7a3965,0x5215b36ebe0f78b3114eb998ca8ade49402ab02f,0x8f1dfd0868d056f11f84e0233e1b89527c262fb6,0xc7d02944a76b9f83b199e9090ecc92c82d241f8a)
 GROUP BY 1,2
), enriched AS (
 SELECT *, min(first_observed_trade) OVER(PARTITION BY wallet) AS wallet_first_observed_trade,
 max(last_trade) OVER(PARTITION BY wallet) AS wallet_last_trade
 FROM by_market
), details AS (
 SELECT token_id, arbitrary(condition_id) AS condition_hex,
 arbitrary(event_market_id) AS event_id, arbitrary(question) AS question,
 arbitrary(tags) AS tags, arbitrary(market_end_time) AS market_end_time,
 arbitrary(resolved_on_timestamp) AS resolved_at,
 arbitrary(settlement_value) AS payout
 FROM polymarket_polygon.market_details GROUP BY 1 HAVING count(*)=1
)
SELECT concat('0x',lower(to_hex(w.wallet))) AS wallet,
 concat('0x',lower(to_hex(w.condition_id))) AS condition_id,
 cast(w.fb.token_id AS varchar) AS token_id,
 first_buy, wallet_first_observed_trade, wallet_last_trade,
 w.fb.price AS leader_price, w.fb.aggressive AS aggressive,
 concat('0x',lower(to_hex(w.fb.tx_hash))) AS tx_hash, w.fb.evt_index AS evt_index,
 w.fb.fee AS leader_fee_usd, w.fb.shares AS first_fill_shares,
 raw_fills, bought_outcomes, sell_fills,bought_usd,sold_usd,
 d.event_id,d.question,d.tags,d.market_end_time,d.resolved_at,d.payout,
 (d.payout-(w.fb.price+0.01))/(w.fb.price+0.01) AS return_proxy
FROM enriched w JOIN details d ON d.token_id=w.fb.token_id
WHERE first_buy >= TIMESTAMP '2026-08-11 00:00:00'
 AND wallet_last_trade >= TIMESTAMP '2026-09-07 00:00:00'
 AND date_diff('second',first_buy,d.market_end_time) >= 60
 AND date_diff('second',first_buy,d.market_end_time) < 172800
 AND w.fb.price+0.01 >= 0.15 AND w.fb.price+0.01 < 0.85
 AND d.payout IN (0.0,0.5,1.0)
 AND first_buy < d.resolved_at
 AND d.resolved_at < TIMESTAMP '2026-09-10 00:00:00'
ORDER BY wallet,first_buy,condition_id
