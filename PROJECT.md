# Project – IMDb & Wikipedia with Spark and Kafka

## Contributors

| Name | E-mail | Git username |
| --- | --- | --- |
| Leila Alkassoum | leila.alkassoum@edu.ece.fr | `LeilaAlk` |
| Andrea Gabrieli | andrea.gabrieli@edu.ece.fr | `Andrea180904` |

## Stack

- Apache Spark 4 (PySpark, batch + Structured Streaming), local mode
- Apache Kafka 4 (KRaft, single broker)
- Docker Compose, using the same base images as the course labs
  (`quay.io/jupyter/pyspark-notebook`, `apache/kafka-native`)
- Data sources: [IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/),
  [Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange),
  [Wikidata](https://www.wikidata.org) (CC0) and
  [English Wikipedia](https://en.wikipedia.org) (CC BY-SA 4.0)

## Repository layout

```text
compose.yaml            Kafka + PySpark containers
docker/Dockerfile       PySpark image + confluent-kafka + requests
run.sh                  ./run.sh {spark|py} <script> [args]  (runs inside the container)
run_batch.sh            all batch steps in order
jobs/
  common/               configuration, Spark session, result writers
  00_download_imdb.py   download the IMDb TSV files
  01_ingest.py          TSV -> Parquet "bronze" (all strings)
  02_data_quality.py    Part 1: missing values, quality checks, cleaning -> "silver"
  03_exploration.py     Part 2: the 8 exploration questions
  04a_enrich_targets.py Part 3: select films / people to enrich (Spark)
  04b_enrich_process.py Part 3: clean, validate, join enrichment data (Spark)
  05_streaming.py       Part 4: Structured Streaming job (Kafka -> flagged edits)
  06_open_question.py   Part 5: open question
collectors/
  enrich_collect.py     Part 3: raw data gathering from Wikidata / Wikipedia (no Spark)
producer/
  admin.py              create the Kafka topic
  wikistream_producer.py  Wikimedia EventStreams -> Kafka
tools/
  make_sample_imdb.py   small synthetic IMDb dataset for smoke tests
  assemble_report.py    copy results/*.md into REPORT.md
results/                generated result tables (Markdown + CSV)
data/                   generated data, not versioned (raw, bronze, silver, quarantine, gold...)
```

Data layers: `raw` (downloaded files) → `bronze` (Parquet, untouched strings) →
`silver` (typed, validated, with a `dq_flags` column) + `quarantine` (rejected
rows with their `dq_reason`) → `gold` (enriched / analytical tables).

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2) with **at least 8 GB of RAM**
  allocated to Docker, and about 15 GB of free disk space
- Git and a bash shell (Linux/macOS terminal, or Git Bash / WSL on Windows)
- Internet access (IMDb files, Maven package for the Kafka connector,
  Wikimedia APIs)

## Setup

```bash
git clone <repo-url> && cd <repo>
chmod +x run.sh run_batch.sh

# Linux only: make files created in the container belong to you
export UID GID=$(id -g)

docker compose up -d --build
docker compose ps          # kafka and pyspark_notebook must be "running"
```

JupyterLab is also available (optional): `docker logs pyspark_notebook` prints
the URL with its token.

If Docker has less than 8 GB of memory, lower the Spark driver memory:
`export SPARK_DRIVER_MEMORY=4g` before using `./run.sh`.

## Optional: smoke test on synthetic data (≈ 5 minutes)

Run this **before** downloading the real data. It generates a tiny IMDb-like
dataset (with deliberate quality problems) in `data/raw/imdb`.

```bash
./run.sh py tools/make_sample_imdb.py --out data/raw/imdb
./run.sh spark jobs/01_ingest.py
./run.sh spark jobs/02_data_quality.py
./run.sh spark jobs/03_exploration.py
# then wipe everything generated from the sample:
rm -rf data/* results/*
```

## Running the project

All commands are run from the repository root on the host machine; `run.sh`
executes them inside the `pyspark_notebook` container.

### Parts 1 and 2 – preparation and exploration

```bash
./run.sh py    jobs/00_download_imdb.py      # ~1.5 GB download
./run.sh spark jobs/01_ingest.py             # TSV -> bronze Parquet
./run.sh spark jobs/02_data_quality.py       # Part 1 -> silver + quarantine
./run.sh spark jobs/03_exploration.py        # Part 2 (all questions)
./run.sh spark jobs/03_exploration.py --only q4,q5   # (a subset)
```

(`./run_batch.sh` runs these steps plus `04a` and `06` in one go.)

### Part 3 – enrichment

```bash
./run.sh spark jobs/04a_enrich_targets.py                 # targets (Spark)
./run.sh py collectors/enrich_collect.py --contact <your-email> --limit 30   # quick test
./run.sh py collectors/enrich_collect.py --contact <your-email>              # full run
./run.sh spark jobs/04b_enrich_process.py                 # cleaning + lineage (Spark)
```

The collector is network-bound (one Wikipedia request per film, serial, with a
delay, as required by the Wikimedia API etiquette) and takes about an hour for
the full 1980–2025 scope. It is resumable: stop it with `Ctrl+C` and start it
again, already collected ids are skipped. `04a` accepts `--year-min`,
`--year-max`, `--top-n` and `--principals-per-film` to change the scope
(for example the whole dataset) without changing the code.

### Part 4 – streaming (two terminals, 20–30 minutes)

```bash
# terminal 1: topic + producer (keeps only enwiki/frwiki article edits)
./run.sh py producer/admin.py --reset
./run.sh py producer/wikistream_producer.py --duration 25

# terminal 2: Spark Structured Streaming job
./run.sh spark jobs/05_streaming.py --duration 22 --fresh
```

Flagged edits are printed in terminal 2 and written to `data/streaming/flagged`.
When the job stops it writes `results/04_streaming_*.md` (counts per matching
rule, top pages, closed windows, throughput and late-data metrics).
`InterruptedException` stack traces printed while the queries are stopped at the
end are expected.

The streaming job uses the enriched Wikipedia page titles of Part 3 when they
exist, so run Part 3 first if possible.

### Part 5 – open question

```bash
./run.sh spark jobs/06_open_question.py
```

### Report

```bash
python3 tools/assemble_report.py      # or: ./run.sh py tools/assemble_report.py
```

copies every `results/*.md` table into the matching block of `REPORT.md`.
Full results (beyond the rows shown in Markdown) are in `results/csv/`.

## Stopping

```bash
docker compose down        # add -v to also remove the containers' volumes
```

## Use of AI tools

| Tool | Used for | Not used for |
| --- | --- | --- |
| Claude (Anthropic), chat assistant | Generating the initial version of the code: Docker setup, `run.sh`, all jobs in `jobs/`, the collector, the Kafka producer, the sample-data generator and the report-assembly tool. The code was then reviewed, run and adapted by the team. | The explanations, reasoning and methodological sections of `REPORT.md`, which are written by the team without AI. |

<!-- Edit the table above so that it reflects exactly how you used AI tools. -->
