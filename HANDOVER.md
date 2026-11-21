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

## Where things stand (2026-11-20, 7:50 PM ET)

| Commit | Change | Board |
| --- | --- | --- |
| b2ff277 | first correct version | 1.86 s |
| ea40960 | SIMD CSV scan, SWAR timestamps, 7-bit ID keys | (scoring) |
| next | prefetch-pipelined link lookups (parse CPU 3.3 s → 2.15 s at 20M) | to submit |

The board leader was at 1.24 s when this was written. CPU time per phase at 20M on an M4 Pro, 8 threads:
open 0.16 s, links 0.16 s, parse 2.15 s, dedup 0.35 s, sessions 0.55 s, write 0.11 s.

## Next ideas, roughly by expected gain

1. Parse (still about 75 ns per line without lookups): fewer branches per field; skip validating `ingest_ts` (always
   valid F1 per the rules, but still parse it exactly); cheaper product/campaign lookups; maybe `MAP_POPULATE` or huge pages.
2. Sessions phase (0.55 s): radix-sort big per-person groups; avoid the per-partition `PMap` rebuilds; for assists on
   whales and shared-device persons, use per-campaign intervals instead of scanning the 7-day window.
3. Dedup (0.35 s): smaller table entries; prefetch.
4. Overlap phases: links can run concurrently with event parsing if person lookup moves to the dedup phase.
5. Container start overhead on the scorer is unknown and can't be probed with a no-op entry: wrong output stops a
   submission. Estimate it from the gap between local and board times.
