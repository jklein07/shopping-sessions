# Hand-over: continuing Shopping Sessions on another machine

The goal is the fastest correct run on the Databricks Shopping Sessions track. The practice board times about
20M events; the final runs about 60M events, with a 3-minute cap per run. Submissions close **Sun Nov 22, 11:00 AM ET**.
You get 2 submissions per hour on the track page
(https://www.joinbuilders.org/buildathon/tracks/shopping-sessions; team "fortnite pirate token").
Keep every submitted commit in this public repo.

## Setup (Windows 11 + WSL2, or any Linux x86 box)

The setup needs Docker (Docker Desktop with the WSL2 backend), Git and Python 3. Work inside WSL2 (Ubuntu), on the Linux
filesystem (`~/`), not `/mnt/c`: reading CSVs through `/mnt/c` is very slow.

```sh
sudo apt-get update && sudo apt-get install -y build-essential curl git python3-venv unzip
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && . ~/.cargo/env
git clone https://github.com/jklein07/shopping-sessions && cd shopping-sessions
python3 -m venv ~/venv && ~/venv/bin/pip install polars==1.35.2 pyarrow duckdb==1.5.6
# track files (public URLs): rules, starter (has the 36 fixtures), dev answer, generator
mkdir -p ~/dl && cd ~/dl
for f in rules.md starter-duckdb.zip dev-answer.zip generator.zip; do
  curl -sSLO https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/$f; done
for z in *.zip; do mkdir -p ${z%.zip} && unzip -q -o $z -d ${z%.zip}; done
cd ~/dl/generator/generator && cargo build --release && cp target/release/generate ~/generate
```

## Data

```sh
~/generate --seed 1 --events 1000000  --clicks 250000   --output ~/data/dev   # public dev set (dev-answer.zip)
~/generate --seed 7 --events 20000000 --clicks 5000000  --output ~/data/d20   # ~1.8 GB, practice size
~/generate --seed 9 --events 60000000 --clicks 15000000 --output ~/data/d60   # ~5.4 GB, final size
```

## Build, check, time

```sh
cargo build --release && cargo test --release
FX=~/dl/starter-duckdb/starter-duckdb/fixtures
PY=~/venv/bin/python tools/fixtures.sh $FX target/release/ss            # expect 36 passed
DATA_DIR=~/data/dev OUTPUT_DIR=/tmp/o target/release/ss && ~/venv/bin/python tools/compare.py /tmp/o ~/dl/dev-answer/dev-answer
rm -rf /tmp/o20 && NCPU=8 DATA_DIR=~/data/d20 OUTPUT_DIR=/tmp/o20 target/release/ss   # prints wall ms and cpu-ms per phase
```

`reference/reference.py` is an independent, slow Python implementation of rules.md. Run it on small generated
sets (any seed, at most 2M events) and compare it with `tools/compare.py` before any change that touches the logic.

The scorer-like run: `docker build -t ss . && CPUS=8 MEM=24g tools/docker_run.sh ss ~/data/d20 /tmp/o20d linux/amd64`

### Approximating the scorer (8 vCPUs = 4 Xeon cores with hyperthreading, AVX-512)

On an i9-13900HX the logical CPUs 0–15 are usually the 8 P-cores × 2 threads. For runs closest to the scorer, pin to
4 P-cores / 8 threads: `taskset -c 0-7 env NCPU=8 ...`, or `docker run --cpuset-cpus 0-7 ...`. Under WSL2 these are
VM vCPUs, and Windows decides which physical cores back them, so keep the laptop on AC power with the
best-performance power mode. Compare versions A/B, alternating several runs; absolute times will not match the scorer.

## Where things stand (2026-11-20, 8:10 PM ET)

| Commit | Change | Board (≈20M events) |
| --- | --- | --- |
| b2ff277 | first correct version | 1.86 s |
| ea40960 | SIMD CSV scan, SWAR timestamps, 7-bit ID keys | 1.66 s |
| b0904ad | prefetch-pipelined link lookups | not submitted |
| d1036a8 | per-line 128-byte splitter, table dates, interval assists, scratch reuse | not submitted |
| 09176cf | + overlapped lookups load, finer link chunks, largest-first D, INT32 counts, early unmap | **submit next** |

The board leader was at 1.24 s. A rough model from the two scored runs: board ≈ 0.27 s + 0.29 × (local CPU-seconds
at 20M on an M4 Pro, 8 threads). 09176cf uses about 3.1–3.2 CPU-s, so it should score about 1.2 s; that's a guess.

CPU per phase at 20M (M4 Pro, 8 threads; the Mac is loaded, so ±10%): open 0.15–0.2 s, links 0.17 s, parse 1.85 s,
dedup 0.36 s, sessions 0.55–0.6 s, write 0.08–0.1 s. Wall time is about 0.55–0.75 s when the machine is quiet.
At 60M: 4.2 s wall and 11 s CPU on the loaded Mac, far under the 3-minute cap.

Correctness gates (run all before every submission): `cargo test --release` (including 2M randomized timestamp
checks), the 36 fixtures, dev-answer, and 14 cross-check datasets (seeds 2–13, 15, 21, 272, from 1k to 3M events,
two of them spanning Feb 29 2028) against `reference/reference.py`. Also run with `SS_INTERVAL_CLICKS=0`, which forces
the interval-assist path for every person. On a non-x86 machine, also build `x86_64-apple-darwin` (or run the amd64
image) to exercise the AVX2 path.

Linux measurement recipe (works in a Colima arm64 VM): build the image, `docker cp` `/ss` out of it, and run it in
`rust:1.91-slim-bookworm` with `date +%s%N` around the run. The gap between total time and the last stderr lap is the
teardown at process exit (about 30 ms at 6M events). `SS_FREE=1` times the teardown piece by piece.

## Next ideas, roughly by expected gain

1. Parse is about 60% of wall time. Per line on one M4 core: split 19 ns, `ts` 10 ns, `ingest_ts` 3.5 ns, ID keys 4 ns,
   product 5 ns. Ideas: SWAR product-ID parse; branch-free timestamp-form dispatch.
2. Transparent huge pages for the big record buffers: per-thread arenas with `madvise(MADV_HUGEPAGE)` (or
   `GLIBC_TUNABLES=glibc.malloc.hugetlb=1` via re-exec), for fewer page faults and cheaper exit teardown. The VM test
   was inconclusive (maybe 5%).
3. Sessions (0.55 s CPU): grouping 0.15, sorting 0.19, per-person 0.26. Ideas: radix-sort by ts before the
   counting scatter; smaller hash tables.
4. Dedup (0.36 s CPU): 1024 partitions with a compact key+index table already; prefetch the probes.
5. Links (about 35 ms wall): prefetch slots and parents while inserting.
6. Container start overhead on the scorer is unknown and can't be probed with a no-op entry: wrong output stops a
   submission. Estimate it from the gap between local and board times.
