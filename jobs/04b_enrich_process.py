"""Part 3 (step 3/3) - Clean, validate and join the enrichment data (Spark).

Input : raw JSON-lines written by collectors/enrich_collect.py
Output: data/gold/enriched_films            one row per target film
        data/gold/enriched_people           one row per target person
        data/gold/enriched_film_principals  film -> its enriched principals

Every enriched record carries its lineage (source URLs, retrieval time,
licence) and a `notes` array that flags missing / suspicious information.

Run with:  ./run.sh spark jobs/04b_enrich_process.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType, LongType, StringType, StructField, StructType,
)

from common.config import ENRICH_DIR, GOLD_DIR  # noqa: E402
from common.output import save_result  # noqa: E402
from common.spark import get_spark, read_silver  # noqa: E402

RAW = ENRICH_DIR / "raw"
LICENCE_WIKIPEDIA = "CC BY-SA 4.0 (Wikipedia)"
LICENCE_WIKIDATA = "CC0 1.0 (Wikidata)"
MIN_SYNOPSIS_WORDS = 40
MIN_BIO_WORDS = 15

WIKIDATA_SCHEMA = StructType([
    StructField("imdb_id", StringType()),
    StructField("entity_type", StringType()),
    StructField("wikidata_uri", StringType()),
    StructField("enwiki_url", StringType()),
    StructField("description", StringType()),
    StructField("birth", StringType()),
    StructField("death", StringType()),
    StructField("birth_place", StringType()),
    StructField("source", StringType()),
    StructField("source_url", StringType()),
    StructField("retrieved_at", StringType()),
])
WIKIPEDIA_SCHEMA = StructType([
    StructField("imdb_id", StringType()),
    StructField("entity_type", StringType()),
    StructField("extract_mode", StringType()),
    StructField("requested_title", StringType()),
    StructField("page_title", StringType()),
    StructField("pageid", LongType()),
    StructField("revid", LongType()),
    StructField("missing", BooleanType()),
    StructField("extract", StringType()),
    StructField("source", StringType()),
    StructField("source_url", StringType()),
    StructField("retrieved_at", StringType()),
])

PLOT_REGEX = (
    r"(?s)\n==\s*(?:Plot|Plot summary|Synopsis|Summary|Premise|Storyline)"
    r"\s*==\s*\n(.*?)(?=\n==[^=]|$)"
)
INTRO_REGEX = r"(?s)^(.*?)(?=\n==|$)"


def read_jsonl(spark, name: str, schema: StructType) -> DataFrame:
    path = RAW / name
    if not path.exists():
        print(f"WARNING: {path} not found - treated as empty")
        return spark.createDataFrame([], schema)
    # FAILFAST would abort on a truncated last line; PERMISSIVE keeps going
    # and corrupt lines get all-NULL fields, removed below.
    return (spark.read.schema(schema).option("mode", "PERMISSIVE")
            .json(str(path)).filter(F.col("imdb_id").isNotNull()))


def clean_text(col):
    """Collapse whitespace, drop sub-headings and citation markers."""
    c = F.regexp_replace(col, r"={2,}[^=\n]*={2,}", " ")
    c = F.regexp_replace(c, r"\[\d+\]", "")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    return F.when(c == "", None).otherwise(c)


def word_count(col):
    return F.when(col.isNull(), 0).otherwise(F.size(F.split(col, " ")))


def to_date(col):
    """Wikidata time literal '1964-09-02T00:00:00Z' -> date."""
    return F.to_date(F.try_to_timestamp(
        F.regexp_replace(col, r"^\+", ""),
        F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")))


def latest(df: DataFrame, key: str) -> DataFrame:
    """Keep the most recently retrieved record per key (re-runs append)."""
    w = Window.partitionBy(key).orderBy(F.desc("retrieved_at"))
    return df.withColumn("_rk", F.row_number().over(w)).filter(
        "_rk = 1").drop("_rk")


def wikidata_entities(spark, file_name: str) -> DataFrame:
    """Raw SPARQL bindings -> one row per IMDb id (Spark de-duplication).

    An id can have several bindings (several articles, birth dates or
    birth places); we keep the first value in a deterministic order and
    count the alternatives so that ambiguity can be flagged.
    """
    wd = read_jsonl(spark, file_name, WIKIDATA_SCHEMA)
    return wd.groupBy("imdb_id").agg(
        F.min("wikidata_uri").alias("wikidata_uri"),
        F.countDistinct("wikidata_uri").alias("n_wikidata_items"),
        F.min("enwiki_url").alias("enwiki_url"),
        F.first("description", ignorenulls=True).alias("description"),
        F.min(to_date(F.col("birth"))).alias("birth_date"),
        F.countDistinct("birth").alias("n_birth_values"),
        F.min(to_date(F.col("death"))).alias("death_date"),
        F.min("birth_place").alias("birth_place"),
        F.max("retrieved_at").alias("wikidata_retrieved_at"),
    )


def process_films(spark) -> DataFrame:
    targets = spark.read.parquet(str(GOLD_DIR / "enrich_targets_films"))
    film_info = targets.groupBy("tconst").agg(
        F.first("primaryTitle").alias("title"),
        F.first("year").alias("year"),
        F.first("numVotes").alias("num_votes"),
        F.array_sort(F.collect_list(F.struct(
            "genre", F.col("rank_in_genre_year").alias("rank"))))
        .alias("top10_in"),
    )
    wd = wikidata_entities(spark, "wikidata_films.jsonl")
    pages = latest(read_jsonl(spark, "wikipedia_films.jsonl",
                              WIKIPEDIA_SCHEMA), "imdb_id")
    pages = pages.select(
        "imdb_id", "page_title", "missing",
        F.col("source_url").alias("wikipedia_url"),
        F.col("retrieved_at").alias("wikipedia_retrieved_at"),
        clean_text(F.regexp_extract("extract", PLOT_REGEX, 1)).alias("plot"),
        clean_text(F.regexp_extract("extract", INTRO_REGEX, 1))
        .alias("intro"),
    )
    df = (
        film_info
        .join(wd, film_info.tconst == wd.imdb_id, "left").drop("imdb_id")
        .join(pages, film_info.tconst == pages.imdb_id, "left")
        .drop("imdb_id")
        .withColumn("synopsis", F.coalesce("plot", "intro"))
        .withColumn("synopsis_section",
                    F.when(F.col("plot").isNotNull(), "Plot")
                    .when(F.col("intro").isNotNull(), "Introduction"))
        .withColumn("synopsis_words", word_count(F.col("synopsis")))
    )
    notes = F.array_compact(F.array(
        F.when(F.col("wikidata_uri").isNull(),
               F.lit("NO_WIKIDATA_ITEM_FOR_IMDB_ID")),
        F.when(F.col("n_wikidata_items") > 1,
               F.lit("SEVERAL_WIKIDATA_ITEMS_FIRST_KEPT")),
        F.when(F.col("wikidata_uri").isNotNull()
               & F.col("enwiki_url").isNull(),
               F.lit("NO_ENGLISH_WIKIPEDIA_ARTICLE")),
        F.when(F.col("enwiki_url").isNotNull()
               & F.col("wikipedia_url").isNull(),
               F.lit("ARTICLE_NOT_FETCHED_YET")),
        F.when(F.col("missing"), F.lit("ARTICLE_MISSING_ON_WIKIPEDIA")),
        F.when(F.col("plot").isNull() & F.col("intro").isNotNull(),
               F.lit("NO_PLOT_SECTION_INTRO_USED")),
        F.when(F.col("synopsis").isNull(), F.lit("SYNOPSIS_MISSING")),
        F.when(F.col("synopsis").isNotNull()
               & (F.col("synopsis_words") < MIN_SYNOPSIS_WORDS),
               F.lit("SYNOPSIS_SHORT")),
    ))
    df = df.withColumn("notes", notes).withColumn(
        "lineage",
        F.struct(
            F.array_compact(F.array("wikipedia_url", "wikidata_uri"))
            .alias("source_urls"),
            F.col("wikipedia_retrieved_at").alias("wikipedia_retrieved_at"),
            F.col("wikidata_retrieved_at").alias("wikidata_retrieved_at"),
            F.array_compact(F.array(
                F.when(F.col("wikipedia_url").isNotNull(),
                       F.lit(LICENCE_WIKIPEDIA)),
                F.when(F.col("wikidata_uri").isNotNull(),
                       F.lit(LICENCE_WIKIDATA)))).alias("licences"),
        ),
    ).select(
        "tconst", "title", "year", "num_votes", "top10_in",
        "description", "synopsis", "synopsis_section", "synopsis_words",
        "wikidata_uri", "wikipedia_url", "page_title", "lineage", "notes",
    )
    path = str(GOLD_DIR / "enriched_films")
    df.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def process_people(spark) -> DataFrame:
    targets = spark.read.parquet(str(GOLD_DIR / "enrich_targets_people"))
    names = read_silver(spark, "name_basics").select(
        "nconst", "primaryName", "birthYear", "deathYear",
        "primaryProfession")
    wd = wikidata_entities(spark, "wikidata_people.jsonl")
    pages = latest(read_jsonl(spark, "wikipedia_people.jsonl",
                              WIKIPEDIA_SCHEMA), "imdb_id").select(
        "imdb_id", "missing", "page_title",
        F.col("source_url").alias("wikipedia_url"),
        F.col("retrieved_at").alias("wikipedia_retrieved_at"),
        clean_text(F.col("extract")).alias("biography"),
    )
    people = targets.select("nconst").distinct().join(names, "nconst",
                                                      "left")
    df = (
        people.join(wd, people.nconst == wd.imdb_id, "left").drop("imdb_id")
        .join(pages, people.nconst == pages.imdb_id, "left")
        .drop("imdb_id")
        .withColumn("bio_words", word_count(F.col("biography")))
    )
    notes = F.array_compact(F.array(
        F.when(F.col("wikidata_uri").isNull(),
               F.lit("NO_WIKIDATA_ITEM_FOR_IMDB_ID")),
        F.when(F.col("n_wikidata_items") > 1,
               F.lit("SEVERAL_WIKIDATA_ITEMS_FIRST_KEPT")),
        F.when(F.col("wikidata_uri").isNotNull()
               & F.col("enwiki_url").isNull(),
               F.lit("NO_ENGLISH_WIKIPEDIA_ARTICLE")),
        F.when(F.col("enwiki_url").isNotNull()
               & F.col("wikipedia_url").isNull(),
               F.lit("ARTICLE_NOT_FETCHED_YET")),
        F.when(F.col("missing"), F.lit("ARTICLE_MISSING_ON_WIKIPEDIA")),
        F.when(F.col("biography").isNull(), F.lit("BIOGRAPHY_MISSING")),
        F.when(F.col("biography").isNotNull()
               & (F.col("bio_words") < MIN_BIO_WORDS),
               F.lit("BIOGRAPHY_SHORT")),
        F.when(F.col("birth_date").isNull(), F.lit("BIRTH_DATE_MISSING")),
        F.when(F.col("n_birth_values") > 1,
               F.lit("SEVERAL_BIRTH_DATES_EARLIEST_KEPT")),
        F.when(F.year("birth_date") != F.col("birthYear"),
               F.lit("BIRTH_YEAR_DIFFERS_FROM_IMDB")),
    ))
    df = df.withColumn("notes", notes).withColumn(
        "lineage",
        F.struct(
            F.array_compact(F.array("wikipedia_url", "wikidata_uri"))
            .alias("source_urls"),
            F.col("wikipedia_retrieved_at").alias("wikipedia_retrieved_at"),
            F.col("wikidata_retrieved_at").alias("wikidata_retrieved_at"),
            F.array_compact(F.array(
                F.when(F.col("wikipedia_url").isNotNull(),
                       F.lit(LICENCE_WIKIPEDIA)),
                F.when(F.col("wikidata_uri").isNotNull(),
                       F.lit(LICENCE_WIKIDATA)))).alias("licences"),
        ),
    ).select(
        "nconst", F.col("primaryName").alias("name"),
        F.col("birthYear").alias("imdb_birth_year"),
        F.col("deathYear").alias("imdb_death_year"),
        F.col("primaryProfession").alias("professions"),
        "description", "birth_date", "death_date", "birth_place",
        "biography", "bio_words", "wikidata_uri", "wikipedia_url",
        "page_title", "lineage", "notes",
    )
    path = str(GOLD_DIR / "enriched_people")
    df.write.mode("overwrite").parquet(path)
    df = spark.read.parquet(path)

    links = targets.join(
        df.select("nconst", "name",
                  F.col("biography").isNotNull().alias("has_biography")),
        "nconst", "left")
    links.write.mode("overwrite").parquet(
        str(GOLD_DIR / "enriched_film_principals"))
    return df


def coverage(df: DataFrame, entity: str, text_col: str) -> DataFrame:
    total = F.count("*")
    return df.agg(
        F.lit(entity).alias("entity"),
        total.alias("targets"),
        F.sum(F.col("wikidata_uri").isNotNull().cast("int"))
        .alias("with_wikidata"),
        F.sum(F.col("wikipedia_url").isNotNull().cast("int"))
        .alias("with_wikipedia"),
        F.sum(F.col(text_col).isNotNull().cast("int")).alias("with_text"),
        F.round(100 * F.avg(F.col(text_col).isNotNull().cast("int")), 1)
        .alias("pct_with_text"),
        F.sum((F.size("notes") > 0).cast("int")).alias("with_notes"),
    )


def main() -> None:
    spark = get_spark("04b_enrich_process")
    films = process_films(spark)
    people = process_people(spark)

    save_result(
        coverage(films, "films", "synopsis").unionByName(
            coverage(people, "people", "biography")),
        "03_enrichment_coverage", "Enrichment coverage")
    save_result(
        films.groupBy("synopsis_section").count().orderBy(F.desc("count")),
        "03_enrichment_synopsis_sections",
        "Where the film synopsis comes from")
    notes = (
        films.select(F.lit("film").alias("entity"),
                     F.explode("notes").alias("note"))
        .unionByName(people.select(F.lit("person").alias("entity"),
                                   F.explode("notes").alias("note")))
        .groupBy("entity", "note").count()
        .orderBy("entity", F.desc("count"))
    )
    save_result(notes, "03_enrichment_notes",
                "Programmatic notes flagging missing / doubtful information")
    save_result(
        films.filter(F.col("synopsis").isNotNull())
        .orderBy(F.desc("num_votes"))
        .select("tconst", "title", "year", "synopsis_section",
                F.concat(F.substring("synopsis", 1, 160), F.lit("..."))
                .alias("synopsis_excerpt"),
                F.col("lineage.source_urls")[0].alias("source")),
        "03_enrichment_films_sample", "Sample of enriched films",
        max_md_rows=8, max_csv_rows=200)
    save_result(
        people.filter(F.col("biography").isNotNull())
        .orderBy("nconst")
        .select("nconst", "name", "birth_date", "birth_place",
                F.concat(F.substring("biography", 1, 140), F.lit("..."))
                .alias("biography_excerpt"),
                F.col("lineage.source_urls")[0].alias("source")),
        "03_enrichment_people_sample", "Sample of enriched people",
        max_md_rows=8, max_csv_rows=200)
    spark.stop()


if __name__ == "__main__":
    main()
