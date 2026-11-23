# Shopping Sessions — a native Rust pipeline

Entry for the Builders Buildathon **Databricks · Shopping Sessions** track (team *fortnite pirate token*).

**Result:** placed **357th of 408 teams** in the final (30.1 s on the hidden data set; 1.25 s on practice).

It reads the messy click-stream CSVs in `$DATA_DIR` (`/data`), applies every rule in the track's `rules.md`
(CSV dialect, timestamp forms, linked IDs, dedup before validation, late events, 30-minute sessions,
last-click attribution with 7-day assists), and writes `user_daily/` and `campaign_daily/` as Parquet under
`$OUTPUT_DIR` (`/output`).

## Design

One static binary, Rust standard library only — no crates, no engine to start. All phases run on every core
(`$NCPU` threads) with dynamic work stealing:

| Phase | What it does |
| --- | --- |
| open | `mmap` every input file. |
| A. links | Parse `links/` in parallel into a lock-free hash table of IDs plus a lock-free union-find (link the larger slot under the smaller with CAS). Each person is named by the byte-wise smallest ID (atomic max on the complemented key). |
| B. parse | Parse `events/` and `clicks/` in 4 MB chunks. Every event line becomes a 32-byte record (event_id key, person, ingest_ts, ts, price·category, or "invalid"), radix-partitioned by a hash of `event_id`. Clicks are validated and partitioned by a hash of the person. |
| C. dedup | Per `event_id` partition, keep the copy with the smallest `ingest_ts` (an invalid kept copy drops the event), then repartition survivors by person. |
| D. sessions | Per person partition: counting-sort events and clicks by person, sort each person's events by `ts`, cut sessions at gaps over 30 minutes, credit each session to the latest click in `[start − 7 d, start]` (ties: smallest `click_id`), count each assisting campaign once, aggregate both tables. |
| E. write | A small hand-written Parquet writer (uncompressed PLAIN pages, `DATE` and `STRING` logical types); one `user_daily` file per thread. |

IDs of 1–10 ASCII letters and digits (every ID the generator writes) pack exactly into a 64-bit base-63
number that keeps byte-wise order, so ID comparisons and hashing are integer operations. Any other ID is
interned into a side table, so the pipeline stays exact for every input `rules.md` allows (spaces, tabs, `\r`,
non-ASCII, long IDs) — those just take a slower path.

## Correctness checks

- `reference/reference.py`: an independent, deliberately simple pure-Python implementation of `rules.md`.
- Both implementations pass all 36 starter fixtures and match `dev-answer.zip` exactly (`tools/compare.py`).
- The Rust pipeline is cross-checked against the reference on generator outputs at other seeds and sizes.

```sh
cargo build --release
DATA_DIR=dev OUTPUT_DIR=out target/release/ss
python3 tools/compare.py out dev-answer                       # exact, order-insensitive
tools/fixtures.sh path/to/fixtures target/release/ss          # all 36 edge cases
docker build -t ss . && tools/docker_run.sh ss dev out        # scorer flags
```

## Reused code and references

No third-party code is vendored or linked. The build uses the official `rust` Docker image (pinned by digest)
and the final image is `scratch` with one static binary. The date arithmetic follows Howard Hinnant's public
`days_from_civil` / `civil_from_days` algorithms. The Parquet and Thrift compact-protocol encodings follow the
Apache Parquet format specification.

## License

MIT — see `LICENSE`.
