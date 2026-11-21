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

## Where things stand (2026-11-20, 10:00 PM ET)

| Commit | Change | Board (≈20M events) |
| --- | --- | --- |
| b2ff277 | first correct version | 1.86 s |
| ea40960 | SIMD CSV scan, SWAR timestamps, 7-bit ID keys | 1.66 s |
| a592edd | prefetched link lookups, mask-based CSV split, parallel page release | 1.44 s |
| b0d2093 | + SIMD date-time parse (NEON / SSSE3) | 1.42 s |
| f443325 | + faster Parquet, links overlapped with event parsing, user_daily written per thread | 1.25 s |
| 4582c93 | + partition buffers in huge-page segments | not submitted |
| e827b33 | + partition buffers as block chains from a per-thread huge-page arena | submit 10:08 PM |

The leader was BobBuildIt at 0.99 s (9 PM).

**Timing on x86 (the Legion).** The user's laptop (i9-13900HX, Windows 11, Docker Desktop on WSL2) is reachable as
`ssh legion` (Windows cmd). CPUs 0–7 in its VM are 4 cores × 2 hyperthreads, the scorer's layout, and transparent huge
pages are in `madvise` mode. Run with `--cpuset-cpus 0-7 --memory 12g`. Times there track the board closely
(a592edd ≈1.35 s, board 1.44; b0d2093 ≈1.34 s, board 1.42), so **the board time is almost all our own compute**; the
fixed part is small. The laptop is also the user's machine: timings are noisy, keep jobs short, and A/B by alternating
builds over several rounds.

Docker on the Legion cannot pull images over SSH (the Windows credential helper needs an interactive logon). Workaround
in use: cross-compile static x86-64 binaries on the Mac in the `sscross` image (rust:1.91-slim-bookworm +
gcc-x86-64-linux-gnu; symlink `/usr/x86_64-linux-gnu/lib/*.a` into `/usr/lib/x86_64-linux-gnu/` before linking),
`scp` them to `C:\Users\skyra\ssbench\bin`, and run them in the existing `br-dev` image with the data in the docker
volume `ssdata` (`/d/dev`, `/d/d20`, `/d/d60`, made with the cross-compiled generator). An `ssperf` image (br-dev +
linux-perf + binutils, made with `docker commit`) gives line-level `perf` profiles; write perf.data to `/tmp` inside
the container, not to a Windows bind mount.

**What x86 showed.** With one Vec per partition, about 20% of all CPU was the kernel zeroing and faulting 4 KB pages
for ~1 GB of partition records (expensive in a VM). Partition buffers now come from per-thread huge-page arenas in
fixed-size blocks (`Parts`/`Arena`), touched only as filled: kernel share 15% → 5%, and 20M runs 1.07–1.41 s → 0.81–1.02 s
on the Legion. At 60M: 2.9–3.5 s, peak cgroup memory ≈5.1 GB. Tried and dropped there: 512 or 2048 event partitions
(PBITS 9/11; noise or worse).

Measuring on the Mac: use `/usr/bin/time -l` (instructions, cycles) and `ss bench FILE`; wall time is noisy. The M4 is
about 2× faster per thread than the scorer, and its memory system hides the page-fault and TLB costs that dominate x86.

Correctness gates (run all before every submission): `cargo test --release` (2M random timestamps against a slow
parser, 3M SIMD vs SWAR prefixes, 3.9M random CSV lines fast vs scalar splitter, 1M ID round trips, 2M dense-ID
lookups), the 36 fixtures, dev-answer, and 14 cross-check datasets against `reference/reference.py`
(`shopping-sessions-work/check_all.sh BINARY`), also with `SS_INTERVAL_CLICKS=0`, `NCPU=1` and `NCPU=3`; the x86 build
under Rosetta; the Linux arm64 build in a container (`linux_check.sh`); and the cross-compiled x86 binary on the
Legion's dev set compared with the dev answer.

## Next ideas, roughly by expected gain

1. Timestamps are now the largest single function on x86 (about 11% of samples); F3 epoch values are still SWAR.
2. The partition scatter (about 10% on x86): 1024 write streams per thread. Ideas: software write-combining buffers
   (one cache line per partition, flushed with full-line stores).
3. Dedup at 60M grows faster than the data (tables outgrow L2): prefetch the probes, or a two-level partition only
   for dedup.
4. Sessions: per-person sorting is about a third of the phase.
