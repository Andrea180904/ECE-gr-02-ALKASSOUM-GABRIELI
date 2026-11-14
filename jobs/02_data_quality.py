"""Part 1 - Data preparation: data-quality assessment and cleaning.

1. Percentage of NULL / missing / empty values for every column of every
   IMDb table, and the five worst columns across the whole dataset.
2. Detection of other data-quality issues (impossible dates, orphan
   foreign keys, duplicates, out-of-range values...).
3. Cleaning: produces a typed "silver" layer used by every other job,
   plus a "quarantine" layer holding the rejected rows and their reason.

Handling policy implemented here (summarised in results/01_quality_issues):
  * duplicates on the primary key           -> keep one row
  * values that cannot be cast to a number  -> NULL + flag
  * impossible values (death < birth, ...)  -> offending value(s) set to
                                               NULL + flag in `dq_flags`
  * orphan rows in child tables             -> moved to quarantine
  * suspicious but possible values          -> kept + flag in `dq_flags`

Run with:  ./run.sh spark jobs/02_data_quality.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import (  # noqa: E402
    BRONZE_DIR, CURRENT_YEAR, FIRST_FILM_YEAR, IMDB_TABLES, QUARANTINE_DIR,
    SILVER_DIR,
)
from common.output import save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402

MAX_LIFESPAN = 122  # longest documented human lifespan
SERIES_TYPES = ["tvSeries", "tvMiniSeries"]
ARCHIVE_CATEGORIES = ["archive_footage", "archive_sound"]


class IssueLog:
    """Collects (table, check, affected rows, total rows, action)."""

    def __init__(self):
        self.rows = []

    def add(self, table, check, affected, total, action, description=""):
        pct = 100.0 * affected / total if total else 0.0
        description = description or check.replace("_", " ")
        self.rows.append(
            (table, check, description, int(affected), float(round(pct, 4)),
             action)
        )
        print(f"  [{table}] {check}: {affected:,} ({pct:.4f}%)")


def missing(col_name: str):
    c = F.col(col_name)
    return c.isNull() | (F.trim(c) == "")


def to_int(col_name: str):
    return F.col(col_name).try_cast("int")


def flags(**conditions):
    """Array of the names of the conditions that are true for the row."""
    return F.array_compact(
        F.array(*[F.when(cond, F.lit(name)) for name, cond in
                  conditions.items()])
    )


def flag_counts(df: DataFrame) -> dict:
    rows = (
        df.select(F.explode("dq_flags").alias("flag"))
        .groupBy("flag").count().collect()
    )
    return {r["flag"]: r["count"] for r in rows}


def invalid_numeric(df: DataFrame, cols) -> dict:
    """Number of non-null raw values that are not valid integers/doubles."""
    exprs = [
        F.sum(
            F.when(F.col(c).isNotNull() & F.col(c).try_cast("double").isNull(),
                   1).otherwise(0)
        ).alias(c)
        for c in cols
    ]
    row = df.agg(*exprs).collect()[0]
    return {c: row[c] or 0 for c in cols}


def write(df: DataFrame, layer: Path, name: str, spark) -> DataFrame:
    path = str(layer / name)
    df.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


# --------------------------------------------------------------------------
# 1. Missing values
# --------------------------------------------------------------------------
def missing_values_report(spark) -> None:
    rows = []
    for table in IMDB_TABLES:
        df = spark.read.parquet(str(BRONZE_DIR / table))
        aggs = [F.count(F.lit(1)).alias("__total")] + [
            F.sum(F.when(missing(c), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]
        res = df.agg(*aggs).collect()[0]
        total = res["__total"]
        for c in df.columns:
            n_missing = res[c] or 0
            pct = 100.0 * n_missing / total if total else 0.0
            rows.append((table, c, total, n_missing, round(pct, 4)))

    report = spark.createDataFrame(
        rows,
        "table string, column string, total_rows long, missing_rows long, "
        "missing_pct double",
    )
    save_result(
        report.orderBy("table", F.desc("missing_pct")),
        "01_missing_values",
        "Missing values (NULL, '\\N' or empty string) per column",
        max_md_rows=60,
    )
    save_result(
        report.orderBy(F.desc("missing_pct"), "table", "column").limit(5),
        "01_top5_missing_columns",
        "Top 5 columns with the highest share of missing values",
    )


# --------------------------------------------------------------------------
# 2 + 3. Quality checks and cleaning, table by table
# --------------------------------------------------------------------------
def clean_title_basics(spark, log: IssueLog) -> DataFrame:
    t = "title_basics"
    raw = spark.read.parquet(str(BRONZE_DIR / t))
    total = raw.count()

    dups = total - raw.select("tconst").distinct().count()
    log.add(t, "duplicate_tconst", dups, total, "kept one row per tconst",
            "same primary key appears more than once")

    bad = invalid_numeric(
        raw, ["isAdult", "startYear", "endYear", "runtimeMinutes"]
    )
    for c, n in bad.items():
        log.add(t, f"non_numeric_{c}", n, total, "set to NULL",
                f"{c} is not a valid number")

    df = (
        raw.dropDuplicates(["tconst"])
        .withColumn("isAdult", to_int("isAdult"))
        .withColumn("startYear", to_int("startYear"))
        .withColumn("endYear", to_int("endYear"))
        .withColumn("runtimeMinutes", to_int("runtimeMinutes"))
    )
    df = df.withColumn(
        "dq_flags",
        flags(
            invalid_is_adult=~F.col("isAdult").isin(0, 1),
            end_before_start=F.col("endYear") < F.col("startYear"),
            start_before_first_film=F.col("startYear") < FIRST_FILM_YEAR,
            start_in_future=F.col("startYear") > CURRENT_YEAR,
            non_positive_runtime=F.col("runtimeMinutes") <= 0,
            runtime_over_1000_min=F.col("runtimeMinutes") > 1000,
            missing_primary_title=missing("primaryTitle"),
            missing_genres=missing("genres"),
        ),
    )
    has = lambda f: F.array_contains("dq_flags", f)  # noqa: E731
    df = (
        df.withColumn("isAdult",
                      F.when(has("invalid_is_adult"), None)
                      .otherwise(F.col("isAdult")))
        .withColumn("endYear",
                    F.when(has("end_before_start"), None)
                    .otherwise(F.col("endYear")))
        .withColumn("startYear",
                    F.when(has("start_before_first_film"), None)
                    .otherwise(F.col("startYear")))
        .withColumn("runtimeMinutes",
                    F.when(has("non_positive_runtime"), None)
                    .otherwise(F.col("runtimeMinutes")))
        .withColumn("genres",
                    F.when(missing("genres"), F.array().cast("array<string>"))
                    .otherwise(F.split("genres", ",")))
        .withColumn("primaryTitle",
                    F.when(missing("primaryTitle"), F.col("originalTitle"))
                    .otherwise(F.col("primaryTitle")))
    )
    df = write(df, SILVER_DIR, t, spark)

    actions = {
        "invalid_is_adult": ("isAdult not 0/1", "set to NULL"),
        "end_before_start": ("series ends before it starts",
                             "endYear set to NULL"),
        "start_before_first_film": (f"startYear < {FIRST_FILM_YEAR}",
                                    "startYear set to NULL"),
        "start_in_future": (f"startYear > {CURRENT_YEAR} (announced titles)",
                            "kept, flagged; excluded by year filters"),
        "non_positive_runtime": ("runtime <= 0 minutes",
                                 "runtime set to NULL"),
        "runtime_over_1000_min": ("runtime > 1000 minutes (experimental)",
                                  "kept, flagged"),
        "missing_primary_title": ("no primary title",
                                  "replaced by originalTitle"),
        "missing_genres": ("no genre", "empty genre array"),
    }
    counts = flag_counts(df)
    for f_name, (desc, action) in actions.items():
        log.add(t, f_name, counts.get(f_name, 0), total, action, desc)

    # Normalised bridge table title <-> genre
    genres = df.select("tconst", F.explode("genres").alias("genre"))
    write(genres, SILVER_DIR, "title_genres", spark)
    return df


def clean_name_basics(spark, log: IssueLog, basics: DataFrame) -> DataFrame:
    t = "name_basics"
    raw = spark.read.parquet(str(BRONZE_DIR / t))
    total = raw.count()

    dups = total - raw.select("nconst").distinct().count()
    log.add(t, "duplicate_nconst", dups, total, "kept one row per nconst",
            "same primary key appears more than once")
    for c, n in invalid_numeric(raw, ["birthYear", "deathYear"]).items():
        log.add(t, f"non_numeric_{c}", n, total, "set to NULL",
                f"{c} is not a valid number")

    df = (
        raw.dropDuplicates(["nconst"])
        .withColumn("birthYear", to_int("birthYear"))
        .withColumn("deathYear", to_int("deathYear"))
    )
    b, d = F.col("birthYear"), F.col("deathYear")
    df = df.withColumn(
        "dq_flags",
        flags(
            death_before_birth=d < b,
            birth_in_future=b > CURRENT_YEAR,
            death_in_future=d > CURRENT_YEAR,
            lifespan_over_max=(d - b) > MAX_LIFESPAN,
            alive_over_max_age=d.isNull() & ((CURRENT_YEAR - b)
                                             > MAX_LIFESPAN),
            missing_name=missing("primaryName"),
        ),
    )
    has = lambda f: F.array_contains("dq_flags", f)  # noqa: E731
    impossible_life = has("death_before_birth") | has("lifespan_over_max")
    df = (
        df.withColumn(
            "birthYear",
            F.when(impossible_life | has("birth_in_future"), None)
            .otherwise(b))
        .withColumn(
            "deathYear",
            F.when(impossible_life | has("death_in_future"), None)
            .otherwise(d))
        .withColumn("primaryProfession",
                    F.when(missing("primaryProfession"),
                           F.array().cast("array<string>"))
                    .otherwise(F.split("primaryProfession", ",")))
        .withColumn("knownForTitles",
                    F.when(missing("knownForTitles"),
                           F.array().cast("array<string>"))
                    .otherwise(F.split("knownForTitles", ",")))
    )

    # knownForTitles referencing titles that do not exist
    known = df.select("nconst",
                      F.explode("knownForTitles").alias("tconst"))
    unknown_known_for = known.join(
        basics.select("tconst"), "tconst", "left_anti").count()
    n_known = known.count()

    df = write(df, SILVER_DIR, t, spark)
    actions = {
        "death_before_birth": ("deathYear < birthYear",
                               "birth and death years set to NULL"),
        "lifespan_over_max": (f"lifespan > {MAX_LIFESPAN} years",
                              "birth and death years set to NULL"),
        "birth_in_future": (f"birthYear > {CURRENT_YEAR}",
                            "birthYear set to NULL"),
        "death_in_future": (f"deathYear > {CURRENT_YEAR}",
                            "deathYear set to NULL"),
        "alive_over_max_age": (f"no deathYear but older than {MAX_LIFESPAN}",
                               "kept, flagged (death probably not recorded)"),
        "missing_name": ("no primaryName", "kept, flagged"),
    }
    counts = flag_counts(df)
    for f_name, (desc, action) in actions.items():
        log.add(t, f_name, counts.get(f_name, 0), total, action, desc)
    log.add(t, "unknown_known_for_title", unknown_known_for, n_known,
            "kept (column not used in the analyses)",
            "knownForTitles id missing from title_basics "
            "(% of knownFor references)")
    return df


def quarantine(df: DataFrame, name: str, reason: str, spark) -> int:
    df = df.withColumn("dq_reason", F.lit(reason))
    path = str(QUARANTINE_DIR / name)
    df.write.mode("append").parquet(path)
    return df.count()


def clean_title_ratings(spark, log, basics) -> DataFrame:
    t = "title_ratings"
    raw = spark.read.parquet(str(BRONZE_DIR / t))
    total = raw.count()
    dups = total - raw.select("tconst").distinct().count()
    log.add(t, "duplicate_tconst", dups, total, "kept one row per tconst")
    for c, n in invalid_numeric(raw, ["averageRating", "numVotes"]).items():
        log.add(t, f"non_numeric_{c}", n, total, "row quarantined")

    df = (
        raw.dropDuplicates(["tconst"])
        .withColumn("averageRating", F.col("averageRating").try_cast("double"))
        .withColumn("numVotes", F.col("numVotes").try_cast("long"))
    )
    invalid = (
        F.col("averageRating").isNull() | F.col("numVotes").isNull()
        | (F.col("averageRating") < 1) | (F.col("averageRating") > 10)
        | (F.col("numVotes") <= 0)
    )
    n_bad = quarantine(df.filter(invalid), t, "rating_out_of_range", spark)
    log.add(t, "rating_or_votes_out_of_range", n_bad, total,
            "row quarantined", "rating outside [1, 10] or votes <= 0")
    df = df.filter(~invalid)

    orphans = df.join(basics.select("tconst"), "tconst", "left_anti")
    n_orphan = quarantine(orphans, t, "orphan_tconst", spark)
    log.add(t, "orphan_tconst", n_orphan, total, "row quarantined",
            "rating for a title missing from title_basics")
    df = df.join(basics.select("tconst"), "tconst", "left_semi")
    return write(df.withColumn("dq_flags", F.array().cast("array<string>")),
                 SILVER_DIR, t, spark)


def clean_title_episode(spark, log, basics) -> DataFrame:
    t = "title_episode"
    raw = spark.read.parquet(str(BRONZE_DIR / t))
    total = raw.count()
    dups = total - raw.select("tconst").distinct().count()
    log.add(t, "duplicate_tconst", dups, total, "kept one row per tconst")
    for c, n in invalid_numeric(raw,
                                ["seasonNumber", "episodeNumber"]).items():
        log.add(t, f"non_numeric_{c}", n, total, "set to NULL")

    df = (
        raw.dropDuplicates(["tconst"])
        .withColumn("seasonNumber", to_int("seasonNumber"))
        .withColumn("episodeNumber", to_int("episodeNumber"))
    )
    ids = basics.select("tconst")
    parents = basics.select(F.col("tconst").alias("parentTconst"),
                            F.col("titleType").alias("parentType"))

    self_parent = df.filter(F.col("tconst") == F.col("parentTconst"))
    n_self = quarantine(self_parent, t, "self_parent", spark)
    log.add(t, "episode_is_its_own_parent", n_self, total, "row quarantined")
    df = df.filter(F.col("tconst") != F.col("parentTconst"))

    orphan_ep = df.join(ids, "tconst", "left_anti")
    n1 = quarantine(orphan_ep, t, "orphan_episode_tconst", spark)
    log.add(t, "orphan_episode_tconst", n1, total, "row quarantined",
            "episode id missing from title_basics")
    df = df.join(ids, "tconst", "left_semi")

    orphan_parent = df.join(parents, "parentTconst", "left_anti")
    n2 = quarantine(orphan_parent, t, "orphan_parent_tconst", spark)
    log.add(t, "orphan_parent_tconst", n2, total, "row quarantined",
            "parent series missing from title_basics")
    df = df.join(parents, "parentTconst", "inner")

    df = df.withColumn(
        "dq_flags",
        flags(
            parent_not_a_series=~F.col("parentType").isin(*SERIES_TYPES),
            non_positive_season=F.col("seasonNumber") <= 0,
            non_positive_episode=F.col("episodeNumber") <= 0,
        ),
    ).drop("parentType")
    has = lambda f: F.array_contains("dq_flags", f)  # noqa: E731
    df = (
        df.withColumn("seasonNumber",
                      F.when(has("non_positive_season"), None)
                      .otherwise(F.col("seasonNumber")))
        .withColumn("episodeNumber",
                    F.when(has("non_positive_episode"), None)
                    .otherwise(F.col("episodeNumber")))
    )
    df = write(df, SILVER_DIR, t, spark)
    counts = flag_counts(df)
    log.add(t, "parent_not_a_series", counts.get("parent_not_a_series", 0),
            total, "kept, flagged", "parent is not tvSeries/tvMiniSeries")
    log.add(t, "non_positive_season", counts.get("non_positive_season", 0),
            total, "seasonNumber set to NULL")
    log.add(t, "non_positive_episode", counts.get("non_positive_episode", 0),
            total, "episodeNumber set to NULL")
    return df


def clean_title_principals(spark, log, basics, names) -> DataFrame:
    t = "title_principals"
    raw = spark.read.parquet(str(BRONZE_DIR / t))
    total = raw.count()
    dups = total - raw.select("tconst", "ordering").distinct().count()
    log.add(t, "duplicate_tconst_ordering", dups, total,
            "kept one row per (tconst, ordering)")

    df = (
        raw.dropDuplicates(["tconst", "ordering"])
        .withColumn("ordering", to_int("ordering"))
    )
    orphan_t = df.join(basics.select("tconst"), "tconst", "left_anti")
    n1 = quarantine(orphan_t, t, "orphan_tconst", spark)
    log.add(t, "orphan_tconst", n1, total, "row quarantined",
            "credit for a title missing from title_basics")
    df = df.join(basics.select("tconst"), "tconst", "left_semi")

    orphan_n = df.join(names.select("nconst"), "nconst", "left_anti")
    n2 = quarantine(orphan_n, t, "orphan_nconst", spark)
    log.add(t, "orphan_nconst", n2, total, "row quarantined",
            "credit for a person missing from name_basics")
    df = df.join(names.select("nconst"), "nconst", "left_semi")

    # Cross-table consistency: credited before being born / long after death
    info = (
        df.join(basics.select("tconst", "startYear"), "tconst", "left")
        .join(names.select("nconst", "birthYear", "deathYear"), "nconst",
              "left")
    )
    df = info.withColumn(
        "dq_flags",
        flags(
            credited_before_birth=F.col("startYear") < F.col("birthYear"),
            credited_10y_after_death=(
                (F.col("startYear") > F.col("deathYear") + 10)
                & ~F.col("category").isin(*ARCHIVE_CATEGORIES)
            ),
        ),
    ).drop("startYear", "birthYear", "deathYear")
    df = write(df, SILVER_DIR, t, spark)
    counts = flag_counts(df)
    log.add(t, "credited_before_birth",
            counts.get("credited_before_birth", 0), total, "kept, flagged",
            "title released before the person was born")
    log.add(t, "credited_10y_after_death",
            counts.get("credited_10y_after_death", 0), total,
            "kept, flagged",
            "non-archive credit > 10 years after death")
    return df


def clean_title_crew(spark, log, basics, names) -> None:
    t = "title_crew"
    raw = spark.read.parquet(str(BRONZE_DIR / t))
    total = raw.count()
    dups = total - raw.select("tconst").distinct().count()
    log.add(t, "duplicate_tconst", dups, total, "kept one row per tconst")
    df = raw.dropDuplicates(["tconst"])

    orphan = df.join(basics.select("tconst"), "tconst", "left_anti")
    n1 = quarantine(orphan, t, "orphan_tconst", spark)
    log.add(t, "orphan_tconst", n1, total, "row quarantined")
    df = df.join(basics.select("tconst"), "tconst", "left_semi")
    df = write(df.withColumn("dq_flags", F.array().cast("array<string>")),
               SILVER_DIR, t, spark)

    known = names.select("nconst")
    for role in ["directors", "writers"]:
        bridge = (
            df.select("tconst", F.explode(F.split(role, ",")).alias("nconst"))
            .withColumn("nconst", F.trim("nconst"))
        )
        n_refs = bridge.count()
        empty = bridge.filter(F.col("nconst") == "")
        n_empty = empty.count()
        bridge = bridge.filter(F.col("nconst") != "")
        unknown = bridge.join(known, "nconst", "left_anti")
        n_unknown = quarantine(unknown, f"title_{role}", "unknown_nconst",
                               spark)
        log.add(t, f"empty_{role}_id", n_empty, n_refs, "id removed",
                "(% of references)")
        log.add(t, f"unknown_{role}_id", n_unknown, n_refs,
                "reference quarantined",
                "person id missing from name_basics (% of references)")
        bridge = bridge.join(known, "nconst", "left_semi").distinct()
        write(bridge, SILVER_DIR, f"title_{role}", spark)


def clean_title_akas(spark, log, basics) -> None:
    t = "title_akas"
    raw = spark.read.parquet(str(BRONZE_DIR / t))
    total = raw.count()
    dups = total - raw.select("titleId", "ordering").distinct().count()
    log.add(t, "duplicate_titleid_ordering", dups, total,
            "kept one row per (titleId, ordering)")
    df = (
        raw.dropDuplicates(["titleId", "ordering"])
        .withColumn("ordering", to_int("ordering"))
        .withColumn("isOriginalTitle", to_int("isOriginalTitle"))
    )
    ids = basics.select(F.col("tconst").alias("titleId"))
    orphan = df.join(ids, "titleId", "left_anti")
    n1 = quarantine(orphan, t, "orphan_titleId", spark)
    log.add(t, "orphan_titleId", n1, total, "row quarantined",
            "alternative title for a missing title")
    df = df.join(ids, "titleId", "left_semi").withColumn(
        "dq_flags",
        flags(invalid_is_original=~F.col("isOriginalTitle").isin(0, 1)),
    )
    df = write(df, SILVER_DIR, t, spark)
    counts = flag_counts(df)
    log.add(t, "invalid_is_original_title",
            counts.get("invalid_is_original", 0), total, "kept, flagged")


def main() -> None:
    spark = get_spark("02_data_quality")
    import shutil
    shutil.rmtree(QUARANTINE_DIR, ignore_errors=True)

    print("== 1. missing values ==")
    missing_values_report(spark)

    print("== 2. quality checks + cleaning ==")
    log = IssueLog()
    basics = clean_title_basics(spark, log)
    names = clean_name_basics(spark, log, basics)
    clean_title_ratings(spark, log, basics)
    clean_title_episode(spark, log, basics)
    clean_title_principals(spark, log, basics, names)
    clean_title_crew(spark, log, basics, names)
    clean_title_akas(spark, log, basics)

    issues = spark.createDataFrame(
        log.rows,
        "table string, check string, description string, affected_rows long, "
        "affected_pct double, action string",
    )
    save_result(
        issues.filter(F.col("affected_rows") > 0)
        .orderBy("table", F.desc("affected_rows")),
        "01_quality_issues",
        "Data-quality issues detected and how they were handled",
        max_md_rows=100,
    )
    save_result(
        issues.orderBy("table", "check"),
        "01_quality_checks_all",
        "All data-quality checks (including those with 0 affected rows)",
        max_md_rows=100,
    )

    # Bronze vs silver vs quarantine row counts
    summary = []
    for table in IMDB_TABLES:
        n_bronze = spark.read.parquet(str(BRONZE_DIR / table)).count()
        silver = spark.read.parquet(str(SILVER_DIR / table))
        n_silver = silver.count()
        n_flagged = silver.filter(F.size("dq_flags") > 0).count()
        qpath = QUARANTINE_DIR / table
        n_quar = (spark.read.parquet(str(qpath)).count()
                  if qpath.exists() else 0)
        summary.append((table, n_bronze, n_silver, n_quar, n_flagged))
    save_result(
        spark.createDataFrame(
            summary,
            "table string, bronze_rows long, silver_rows long, "
            "quarantined_rows long, flagged_rows long",
        ),
        "01_cleaning_summary",
        "Row counts before and after cleaning",
    )
    spark.stop()


if __name__ == "__main__":
    main()
