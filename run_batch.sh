#!/usr/bin/env bash
# Run every batch step in order (Parts 1, 2, 3-targets and 5).
# The enrichment collection (long, network bound) and the streaming job
# are started separately, see PROJECT.md.
set -euo pipefail
cd "$(dirname "$0")"

./run.sh py    jobs/00_download_imdb.py
./run.sh spark jobs/01_ingest.py
./run.sh spark jobs/02_data_quality.py
./run.sh spark jobs/03_exploration.py
./run.sh spark jobs/04a_enrich_targets.py
./run.sh spark jobs/06_open_question.py
python3 tools/assemble_report.py 2>/dev/null || ./run.sh py tools/assemble_report.py
echo "batch pipeline finished - results in results/"
