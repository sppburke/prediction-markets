# research-2026-09-exclusion-review — evidence for `docs/37-WALLET-EXCLUSION-DECISION-RECORD.md`

Findings document: `docs/37-WALLET-EXCLUSION-DECISION-RECORD.md` (issue #589). Evidence class:
**read-only review inputs and derived summaries**; nothing here authorizes a state change.

Provenance: repository revision `ab120d9997f216a27dfdb0199739df695a65c9ac`; ranker-host cache
(`/mnt/storage/prediction-markets/data/wallet_cache.db`, schema one) and its purge archive read
with `mode=ro` URIs on 2026-09-10 21:11–21:52 UTC while the host checkout reported `8ea29a9`;
public Polymarket activity pages fetched 2026-09-10. The `forge_ro*.py` scripts were piped to
`python3 -` over ssh (`forge_ro6.py` reads the CSV address list on stdin) and their stdout captured
verbatim; those raw captures copy cache rows and are therefore **retained privately**, not
committed (git-ignored data root on the development box, `data/research-2026-09-exclusion-review/`,
with its own SHA-256 manifest). `derive_summaries.py <private-root>` regenerates the committed
summaries from them. The retained September 10 research bundle's summary files are copied
unchanged; `manifest.json` carries the SHA-256 of every raw activity page in that bundle (the raw
pages themselves expired with `/tmp`), and `recapture-summary.json` carries the SHA-256 of every
recaptured page (retained privately, about 19 MB).

| file | what it is |
|---|---|
| `cohort_summary.csv` | one derived row per screened wallet (24 cohort + 3 context): CSV label, tombstone and archive timestamps, archived span, min 500-trade window and dense-window count, page spans, recapture outcome, sampled screen metrics, verdict |
| `aggregates.json` | class-level counts used by the record: live tombstone/flag counts, exact CSV intersection, archive classes and volumes, cohort verdict counts, any-window sample, non-CSV control tally, recent ranking-output overlap |
| `controls.json` | density-window results for the four exchange contracts, the five largest CSV members, probe-flagged examples, and the 136 non-CSV infra tombstones with archived trades |
| `derive_summaries.py` | produces the three files above from the private raw captures and the committed bundle files |
| `forge_ro.py` … `forge_ro7.py` | the read-only host queries (schemas and counts; archive classes; sliding windows; random sample; flagged trade rows; exact CSV intersection; recent-output overlap) |
| `recapture.py` / `recapture-summary.json` | 13 monthly-anchored 500-trade pages for each of the four never-fetched wallets: identity check, `end` bound check, span, sides, rule outcome, SHA-256 |
| `tombstone-review.csv` | the September 10 per-wallet review input (historical; its verdict column is superseded by `docs/37` §2) |
| `archive-density.json`, `tombstone-audit-summary.json` | retained bundle summaries: archive oldest/newest-500 spans; Dune first-BUY screen metrics per wallet |
| `manifest.json` | the retained bundle's file hashes, including every original raw page |
| `03_tombstoned_history_audit.sql` | the retained Dune first-BUY audit SQL (execution `01M26ASVHBDNGRJ9M7FCY42GA6`) |

SHA-256 of every committed file:

| file | sha256 |
|---|---|
| `03_tombstoned_history_audit.sql` | `81909aedca923af979b120287f0e5034b6182249379e4b63c1200017c3b2e4c1` |
| `aggregates.json` | `90bfb5473f4b048a16d1cebb4092a700b7cdf542c5b702d3255457a40c4b8489` |
| `archive-density.json` | `c12d25b89ea88105e32d1bd4bb108f88594ea755616aa889d65004dfabaa2ddf` |
| `cohort_summary.csv` | `2e3ff0cb88c602edc3ea5c7dbd98901b757c1a83c987b439f472b32758ba73fe` |
| `controls.json` | `8193675dd92eb1aa39b6b47d7e466a616f4be39d5730f12dc244fc200fe7ca99` |
| `derive_summaries.py` | `55b6e68f61f7e7a76689eb685259011af43c3b34d93bd18864a4c15f58aa626e` |
| `forge_ro.py` | `98d78653b8f7333325329176f6d2f132e1f8cdb7f936a67e7f86b39c9c7c448e` |
| `forge_ro2.py` | `d8df12aa5d4a8e2c93a9e545a4e460e9e0579e097b925ea594e635b48de26183` |
| `forge_ro3.py` | `3b9e000c0f55f0182649cdd8db11478273faa225a8b74459c497cf06411e295a` |
| `forge_ro4.py` | `0faba92651fb02bba3192f22b91cdff473e219cd859a3f956ee6362acba6ac19` |
| `forge_ro5.py` | `cd58ad66c208dcc11effba343cad154672e8fd31decf3f8697474464eeb39972` |
| `forge_ro6.py` | `0e0cd9ac2b7c2e5498c80200d4c23d0ec6fab9e06450b23ad2f494a5485653df` |
| `forge_ro7.py` | `7e0c0496ed339efe316e956dfeba3b6f007c7acf5550a0fcf325ddbc2eb17052` |
| `manifest.json` | `6d51e8f638dc65f95ec55d6ab136dbd967a08e680f491b3bec3acf1ae8e465d1` |
| `recapture-summary.json` | `403267795c09c15c377f83f486fe00bcaac9529be903247f7c3cca5ebc2dec0f` |
| `recapture.py` | `56ea4df95a6a30519c990c1e29bb693bd7133fe182655c6c04b14235e2c50d6a` |
| `tombstone-audit-summary.json` | `935db6976f2d2c65c8d45fbc79bf74b1a2901d66ddc83c2eb50714e4c3b9c928` |
| `tombstone-review.csv` | `4d7f8286619e69415802bf6b06648144110c715203cb66fd7a28d696664cf0bf` |
