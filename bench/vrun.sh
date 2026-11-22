#!/bin/sh
# Run the image like the scorer, reading a dataset from the `ssdata` Docker volume.
# Usage: bench/vrun.sh DATASET_NAME   (output lands in volume ssout/DATASET_NAME)
set -eu
DS="$1"
IMAGE="${IMAGE:-shopping-sessions:latest}"
export MSYS_NO_PATHCONV=1
docker volume create ssout >/dev/null
docker run --rm -v ssout:/o debian:bookworm-slim sh -c "rm -rf /o/$DS /o/scratch-$DS && mkdir -p /o/$DS /o/scratch-$DS && chmod 777 /o/$DS /o/scratch-$DS"
start=$(python -c "import time;print(time.time())")
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus 8 --memory 24g --memory-swap 24g --user 1000:1000 \
  --mount type=volume,src=ssdata,dst=/data,volume-subpath=$DS,readonly \
  --mount type=volume,src=ssout,dst=/output,volume-subpath=$DS \
  --mount type=volume,src=ssout,dst=/scratch,volume-subpath=scratch-$DS \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch -e HOME=/scratch \
  -e NCPU=${NCPU:-8} ${EXTRA_ENV:-} -e MEM_GB=24 "$IMAGE"
end=$(python -c "import time;print(time.time())")
python -c "print(\"WALL %.2f s\" % ($end - $start))" >&2
