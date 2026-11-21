#!/bin/sh
# Run the image like the scorer: docker_run.sh IMAGE DATA_DIR OUTPUT_DIR [PLATFORM]
set -eu
IMAGE="$1"; DATA=$(cd "$2" && pwd); mkdir -p "$3"; OUT=$(cd "$3" && pwd); PLATFORM="${4:-linux/amd64}"
CPUS="${CPUS:-8}"; MEM="${MEM:-24g}"
SCRATCH=$(mktemp -d); chmod 777 "$OUT" "$SCRATCH"
docker run --rm --platform "$PLATFORM" \
  --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
  --user 1000:1000 \
  -v "$DATA":/data:ro -v "$OUT":/output -v "$SCRATCH":/scratch \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -e NCPU="${CPUS%.*}" -e MEM_GB="${MEM%g}" \
  "$IMAGE"
rm -rf "$SCRATCH"
