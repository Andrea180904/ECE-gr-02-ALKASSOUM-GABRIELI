"""Part 4 - Structured Streaming: flag live Wikipedia edits about IMDb.

Pipeline
  Kafka topic `wikistreams` (raw Wikimedia recentchange JSON)
    -> parse with an explicit schema, event time = event `timestamp`
    -> watermark + de-duplication of re-delivered events (meta.id)
    -> stream-static joins with IMDb lookups (broadcast):
         * exact page match with the enriched Wikipedia pages (Part 3)
         * normalised page title  vs  IMDb titles (primary/original/akas)
         * normalised page title  vs  IMDb people names
       + the disambiguation suffix of the page, e.g. "(2010 film)",
         "(actor)", "(album)", is used as a hint for or against a match
    -> sinks: flagged events (Parquet + console) and windowed counts
       (Parquet, append mode = only windows closed by the watermark)

Late data: event time comes from the event itself. A watermark of
--watermark (default 10 minutes) bounds the state: events older than
max(event_time) - watermark are dropped from the aggregations and the
de-duplication, and Spark reports them as `numRowsDroppedByWatermark`
(saved in results/04_streaming_metrics.md).

Run (producer in another terminal):
  ./run.sh spark jobs/05_streaming.py --duration 20 --fresh
Test without Kafka (folder of JSON lines files):
  ./run.sh spark jobs/05_streaming.py --source files --input-dir <dir>
"""

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pyspark  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType, IntegerType, LongType, StringType, StructField, StructType,
)

from common.config import (  # noqa: E402
    GOLD_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC, RESULTS_DIR, STREAMING_DIR,
)
from common.output import save_result, save_text  # noqa: E402
from common.spark import get_spark, normalize_text, read_silver  # noqa: E402

TITLE_TYPES = ["movie", "tvMovie", "tvSeries", "tvMiniSeries"]
PERSON_CATEGORIES = ["actor", "actress", "director", "writer", "producer",
                     "composer"]
AKAS_REGIONS = ["US", "GB", "FR"]
FILM_HINT = (r"film|movie|series|miniseries|sitcom|documentary|anime|"
             r"soap opera|téléfilm|série|feuilleton")
PERSON_HINT = (r"actor|actress|director|filmmaker|screenwriter|producer|"
               r"composer|comedian|acteur|actrice|réalisat|scénariste|"
               r"producteur|productrice|compositeur")

EVENT_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("comment", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("wiki", StringType()),
    StructField("server_name", StringType()),
    StructField("length", StructType([
        StructField("old", LongType()), StructField("new", LongType())])),
    StructField("revision", StructType([
        StructField("old", LongType()), StructField("new", LongType())])),
    StructField("meta", StructType([
        StructField("id", StringType()), StructField("dt", StringType()),
        StructField("domain", StringType())])),
])


# ---------------------------------------------------------------- lookups
def build_lookups(spark, min_votes: int, min_credits: int):
    """Static IMDb lookup tables keyed by a normalised text."""
    basics = read_silver(spark, "title_basics").filter(
        F.col("titleType").isin(*TITLE_TYPES))
    ratings = read_silver(spark, "title_ratings").select("tconst",
                                                         "numVotes")
    titles = basics.join(ratings, "tconst").filter(
        F.col("numVotes") >= min_votes).select(
        "tconst", "titleType", "primaryTitle", "originalTitle", "numVotes",
        "startYear").cache()

    akas = read_silver(spark, "title_akas").filter(
        F.col("region").isin(*AKAS_REGIONS)).select(
        F.col("titleId").alias("tconst"), F.col("title").alias("label"))
    labels = (
        titles.select("tconst", F.col("primaryTitle").alias("label"))
        .unionByName(titles.select("tconst",
                                   F.col("originalTitle").alias("label")))
        .unionByName(akas.join(titles.select("tconst"), "tconst"))
        .withColumn("key", normalize_text(F.col("label")))
        .filter(F.length("key") >= 2).select("key", "tconst").distinct()
    )
    title_lookup = (
        labels.join(titles, "tconst")
        .groupBy("key").agg(
            F.max(F.struct("numVotes", "tconst", "titleType",
                           "primaryTitle", "startYear")).alias("best"),
            F.count("*").alias("n_candidates"))
        .select(
            F.col("key").alias("t_key"),
            F.col("best.tconst").alias("t_id"),
            F.col("best.primaryTitle").alias("t_label"),
            F.col("best.titleType").alias("t_type"),
            F.col("best.numVotes").alias("t_votes"),
            F.col("best.startYear").alias("t_year"),
            F.col("n_candidates").alias("t_candidates"))
    )

    principals = read_silver(spark, "title_principals").filter(
        F.col("category").isin(*PERSON_CATEGORIES))
    names = read_silver(spark, "name_basics").select("nconst",
                                                     "primaryName")
    credits = (
        principals.join(titles.select("tconst", "numVotes"), "tconst")
        .groupBy("nconst").agg(
            F.countDistinct("tconst").alias("n_credits"),
            F.sum("numVotes").alias("votes"))
        .filter(F.col("n_credits") >= min_credits)
        .join(names, "nconst")
        .withColumn("key", normalize_text(F.col("primaryName")))
    )
    person_lookup = credits.groupBy("key").agg(
        F.max(F.struct("votes", "n_credits", "nconst", "primaryName"))
        .alias("best")).select(
        F.col("key").alias("p_key"),
        F.col("best.nconst").alias("p_id"),
        F.col("best.primaryName").alias("p_label"),
        F.col("best.n_credits").alias("p_credits"))

    # Exact Wikipedia page titles collected in Part 3 (if available)
    pages = []
    for table, id_col, label_col in [("enriched_films", "tconst", "title"),
                                     ("enriched_people", "nconst", "name")]:
        path = GOLD_DIR / table
        if path.exists():
            pages.append(
                spark.read.parquet(str(path))
                .filter(F.col("page_title").isNotNull())
                .select(F.lower("page_title").alias("e_page"),
                        F.col(id_col).alias("e_id"),
                        F.col(label_col).alias("e_label")))
    if pages:
        page_lookup = pages[0]
        for extra in pages[1:]:
            page_lookup = page_lookup.unionByName(extra)
        page_lookup = page_lookup.dropDuplicates(["e_page"])
    else:
        page_lookup = spark.createDataFrame(
            [], "e_page string, e_id string, e_label string")

    out = {}
    for name, df in [("titles", title_lookup), ("people", person_lookup),
                     ("pages", page_lookup)]:
        path = str(STREAMING_DIR / "lookups" / name)
        df.write.mode("overwrite").parquet(path)
        out[name] = spark.read.parquet(path).cache()
        print(f"lookup {name}: {out[name].count():,} keys")
    titles.unpersist()
    return out["titles"], out["people"], out["pages"]


# ---------------------------------------------------------------- stream
def read_source(spark, args) -> DataFrame:
    if args.source == "kafka":
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
            .select(F.col("value").cast("string").alias("value"))
        )
    return (spark.readStream.schema("value string")
            .option("maxFilesPerTrigger", 1).text(args.input_dir))


def flag_events(raw: DataFrame, titles, people, pages, args) -> DataFrame:
    ev = raw.select(F.from_json("value", EVENT_SCHEMA).alias("e")).select(
        "e.*")
    ev = (
        ev.filter(F.col("title").isNotNull() & F.col("timestamp").isNotNull())
        .filter((F.col("namespace") == 0)
                & F.col("type").isin("edit", "new"))
        .withColumn("event_time", F.timestamp_seconds("timestamp"))
        .withColumn("event_id", F.coalesce(F.col("meta.id"),
                                           F.col("id").cast("string")))
        .withColumn("size_delta",
                    F.coalesce(F.col("length.new"), F.lit(0))
                    - F.coalesce(F.col("length.old"), F.lit(0)))
        .withWatermark("event_time", args.watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .withColumn("hint", F.lower(F.regexp_extract(
            "title", r"\(([^()]*)\)\s*$", 1)))
        .withColumn("key", normalize_text(F.regexp_replace(
            "title", r"\s*\([^()]*\)\s*$", "")))
        .withColumn("page_key", F.lower("title"))
        .withColumn("n_words", F.size(F.split("key", " ")))
    )
    ev = (
        ev.join(F.broadcast(pages), F.col("page_key") == F.col("e_page"),
                "left")
        .join(F.broadcast(titles), F.col("key") == F.col("t_key"), "left")
        .join(F.broadcast(people), F.col("key") == F.col("p_key"), "left")
    )
    hint = F.col("hint")
    no_hint = hint == ""
    match_type = (
        F.when(F.col("e_id").isNotNull() & (F.col("wiki") == "enwiki"),
               "enriched_page")
        .when(F.col("t_id").isNotNull() & hint.rlike(FILM_HINT),
              "title_with_film_hint")
        .when(F.col("p_id").isNotNull() & hint.rlike(PERSON_HINT),
              "person_with_role_hint")
        .when(F.col("t_id").isNotNull() & no_hint
              & (F.col("t_votes") >= args.popular_votes)
              & (F.length("key") >= 4), "popular_title_exact")
        .when(F.col("p_id").isNotNull() & no_hint
              & (F.col("n_words") >= 2)
              & (F.col("p_credits") >= args.person_min_credits),
              "person_exact")
    )
    ev = ev.withColumn("match_type", match_type)
    is_page = F.col("match_type") == "enriched_page"
    is_title = F.col("match_type").isin("title_with_film_hint",
                                        "popular_title_exact")
    return (
        ev.withColumn("imdb_id", F.when(is_page, F.col("e_id"))
                      .when(is_title, F.col("t_id"))
                      .otherwise(F.col("p_id")))
        .withColumn("imdb_label", F.when(is_page, F.col("e_label"))
                    .when(is_title, F.col("t_label"))
                    .otherwise(F.col("p_label")))
        .withColumn("is_relevant", F.col("match_type").isNotNull())
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["kafka", "files"],
                        default="kafka")
    parser.add_argument("--input-dir", default="")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--duration", type=float, default=20,
                        help="minutes before the queries are stopped")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--window", default="5 minutes")
    parser.add_argument("--trigger", default="10 seconds")
    parser.add_argument("--min-votes", type=int, default=1000,
                        help="IMDb titles with fewer votes are ignored")
    parser.add_argument("--popular-votes", type=int, default=50_000,
                        help="votes needed to match a title without hint")
    parser.add_argument("--person-min-credits", type=int, default=5)
    parser.add_argument("--fresh", action="store_true",
                        help="delete previous checkpoints and outputs")
    args = parser.parse_args()

    if args.fresh:
        for sub in ["checkpoints", "flagged", "window_counts"]:
            shutil.rmtree(STREAMING_DIR / sub, ignore_errors=True)
        (RESULTS_DIR / "streaming_progress.jsonl").unlink(missing_ok=True)

    kafka_pkg = ("org.apache.spark:spark-sql-kafka-0-10_2.13:"
                 + pyspark.__version__)
    spark = get_spark("05_streaming",
                      packages=kafka_pkg if args.source == "kafka" else "")
    titles, people, pages = build_lookups(spark, args.min_votes, 2)
    # Small micro-batches: few shuffle partitions = fewer state-store files
    # and lower latency. (Fixed in the checkpoint at the first run.)
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    events = flag_events(read_source(spark, args), titles, people, pages,
                         args)
    ckpt = STREAMING_DIR / "checkpoints"

    flagged = events.filter("is_relevant").select(
        "event_time", "wiki", "title", "type", "user", "bot", "size_delta",
        "match_type", "imdb_id", "imdb_label", "t_votes", "comment",
        "event_id")
    q_flagged = (
        flagged.writeStream.format("parquet").outputMode("append")
        .option("path", str(STREAMING_DIR / "flagged"))
        .option("checkpointLocation", str(ckpt / "flagged"))
        .trigger(processingTime=args.trigger).queryName("flagged_to_parquet")
        .start()
    )
    q_console = (
        flagged.select("event_time", "wiki", "title", "match_type",
                       "imdb_id", "imdb_label")
        .writeStream.format("console").outputMode("append")
        .option("truncate", False).option("numRows", 50)
        .option("checkpointLocation", str(ckpt / "console"))
        .trigger(processingTime=args.trigger).queryName("flagged_console")
        .start()
    )
    windows = (
        events.groupBy(F.window("event_time", args.window), "wiki")
        .agg(F.count("*").alias("events"),
             F.sum(F.col("is_relevant").cast("int")).alias("imdb_events"),
             F.approx_count_distinct("title").alias("distinct_pages"))
        .select(F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"), "wiki", "events",
                "imdb_events", "distinct_pages")
    )
    q_windows = (
        windows.writeStream.format("parquet").outputMode("append")
        .option("path", str(STREAMING_DIR / "window_counts"))
        .option("checkpointLocation", str(ckpt / "window_counts"))
        .trigger(processingTime=args.trigger).queryName("window_counts")
        .start()
    )
    queries = [q_flagged, q_console, q_windows]

    progress_file = RESULTS_DIR / "streaming_progress.jsonl"
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    deadline = time.time() + args.duration * 60
    seen = set()
    print(f"running for {args.duration} minutes...")
    while time.time() < deadline and all(q.isActive for q in queries):
        time.sleep(5)
        with open(progress_file, "a", encoding="utf-8") as fh:
            for q in queries:
                for p in q.recentProgress:
                    # PySpark >= 4 returns objects, older versions dicts
                    p = json.loads(p.json) if hasattr(p, "json") else p
                    key = (p["name"], p["batchId"])
                    if key not in seen:
                        seen.add(key)
                        fh.write(json.dumps(p, default=str) + "\n")
    for q in queries:
        if q.exception():
            print(f"query {q.name} failed: {q.exception()}")
        q.stop()

    summarize(spark, progress_file, args)
    spark.stop()


# ---------------------------------------------------------------- report
def summarize(spark, progress_file: Path, args) -> None:
    flagged_path = STREAMING_DIR / "flagged"
    if flagged_path.exists() and any(flagged_path.glob("*.parquet")):
        flagged = spark.read.parquet(str(flagged_path)).cache()
        save_result(
            flagged.groupBy("match_type").agg(
                F.count("*").alias("edits"),
                F.countDistinct("title").alias("pages"))
            .orderBy(F.desc("edits")),
            "04_streaming_by_match_type",
            "Flagged edits by matching rule")
        save_result(
            flagged.groupBy("wiki").agg(F.count("*").alias("edits"))
            .orderBy(F.desc("edits")),
            "04_streaming_by_wiki", "Flagged edits by wiki")
        save_result(
            flagged.groupBy("title", "imdb_id", "imdb_label", "match_type")
            .agg(F.count("*").alias("edits")).orderBy(F.desc("edits")),
            "04_streaming_top_pages", "Most edited IMDb-related pages",
            max_md_rows=20)
        save_result(
            flagged.orderBy("event_time").select(
                "event_time", "wiki", "title", "match_type", "imdb_id",
                "imdb_label", "user", "size_delta"),
            "04_streaming_flagged_sample", "Flagged events (first rows)",
            max_md_rows=25)
    else:
        save_text("04_streaming_by_match_type",
                  "_No flagged event was written._")

    win_path = STREAMING_DIR / "window_counts"
    if win_path.exists() and any(win_path.glob("*.parquet")):
        save_result(
            spark.read.parquet(str(win_path))
            .withColumn("pct_imdb", F.round(
                100 * F.col("imdb_events") / F.col("events"), 2))
            .orderBy("window_start", "wiki"),
            "04_streaming_windows",
            f"Events per {args.window} window (closed windows only, "
            f"watermark {args.watermark})", max_md_rows=40)

    # Throughput / latency / late data, from StreamingQueryProgress
    if not progress_file.exists():
        return
    rows = [json.loads(line) for line in progress_file.read_text(
        encoding="utf-8").splitlines() if line.strip()]
    lines = ["**Streaming metrics (from StreamingQueryProgress)**", "",
             "| query | batches | input rows | avg input rows/s | "
             "avg processed rows/s | avg batch ms | max batch ms | "
             "rows dropped by watermark | max state rows |",
             "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
    by_query = {}
    for p in rows:
        by_query.setdefault(p["name"], []).append(p)
    for name, ps in sorted(by_query.items()):
        active = [p for p in ps if p.get("numInputRows", 0) > 0] or ps
        dur = [p["durationMs"].get("triggerExecution", 0) for p in ps]
        dropped = sum(s.get("numRowsDroppedByWatermark", 0)
                      for p in ps for s in p.get("stateOperators", []))
        state = max([s.get("numRowsTotal", 0) for p in ps
                     for s in p.get("stateOperators", [])] or [0])
        avg = lambda xs: sum(xs) / len(xs) if xs else 0  # noqa: E731
        n_in = sum(p.get("numInputRows", 0) for p in ps)
        in_rate = avg([p.get("inputRowsPerSecond") or 0 for p in active])
        out_rate = avg([p.get("processedRowsPerSecond") or 0
                        for p in active])
        lines.append(
            f"| {name} | {len(ps)} | {n_in:,} | {in_rate:.1f} | "
            f"{out_rate:.1f} | {avg(dur):.0f} | {max(dur or [0]):,} | "
            f"{dropped:,} | {state:,} |")
    first = min((p["timestamp"] for p in rows), default="?")
    last = max((p["timestamp"] for p in rows), default="?")
    lines += ["", f"_Run from {first} to {last}; watermark "
                  f"{args.watermark}, window {args.window}, trigger "
                  f"{args.trigger}._"]
    save_text("04_streaming_metrics", "\n".join(lines))


if __name__ == "__main__":
    main()
