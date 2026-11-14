#!/usr/bin/env bash
# Run a project script inside the pyspark container.
#
#   ./run.sh spark jobs/02_data_quality.py [args...]   -> spark-submit
#   ./run.sh py    collectors/enrich_collect.py [args]  -> plain python
#
# Environment variables:
#   SPARK_DRIVER_MEMORY   driver memory for local Spark (default 6g)
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "usage: ./run.sh {spark|py} <script> [args...]" >&2
  exit 1
fi

MODE="$1"; SCRIPT="$2"; shift 2
CONTAINER=pyspark_notebook
WORKDIR=/home/jovyan/project
MEM="${SPARK_DRIVER_MEMORY:-6g}"

# Avoid Git Bash on Windows rewriting /home/... paths.
export MSYS_NO_PATHCONV=1

case "$MODE" in
  spark)
    EXTRA=()
    if [[ "$SCRIPT" == *streaming* ]]; then
      SPARK_VERSION=$(docker exec "$CONTAINER" bash -c 'ls "$SPARK_HOME"/jars/' | sed -n 's/^spark-core_2\.13-\(.*\)\.jar$/\1/p' | tr -d '\r')
      EXTRA+=(--packages "org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION}")
    fi
    docker exec -i -u jovyan -w "$WORKDIR" "$CONTAINER" \
      spark-submit --master "${SPARK_MASTER:-local[4]}" --driver-memory "$MEM" \
      --conf spark.sql.shuffle.partitions=64 \
      --conf spark.ui.showConsoleProgress=false \
      ${EXTRA[@]+"${EXTRA[@]}"} "$WORKDIR/$SCRIPT" "$@"
    ;;
  py)
    docker exec -i -u jovyan -w "$WORKDIR" "$CONTAINER" python "$WORKDIR/$SCRIPT" "$@"
    ;;
  *)
    echo "unknown mode: $MODE (use spark or py)" >&2
    exit 1
    ;;
esac
