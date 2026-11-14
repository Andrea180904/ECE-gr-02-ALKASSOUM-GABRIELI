"""Part 2 - Data exploration (8 questions) on the cleaned silver layer.

Run all:        ./run.sh spark jobs/03_exploration.py
Run a subset:   ./run.sh spark jobs/03_exploration.py --only q1,q5

Definitions shared by the questions (see common/config.py):
  * feature-length movie = titleType 'movie', non-adult,
    runtime > FEATURE_MIN_RUNTIME minutes
  * "popularity" of a movie = its number of IMDb votes
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import (  # noqa: E402
    FEATURE_MIN_RUNTIME, YEAR_MAX, YEAR_MIN,
)
from common.output import save_result, save_text  # noqa: E402
from common.spark import (  # noqa: E402
    feature_movies, get_spark, normalize_text, read_silver,
)

# ---- parameters -----------------------------------------------------------
Z_OUTLIER = 3.0            # Q1: |z-score| above which a runtime is "notable"
TOP_GENRES = 3             # Q2/Q3
HIT_RATING = 8.0           # Q4
FLOP_AVG_RATING = 6.0      # Q4
MIN_VOTES_RELIABLE = 1000  # Q4: ignore movies whose rating rests on few votes
SUCCESS_RATING = 7.0       # Q6: "successful" film
SUCCESS_VOTES = 10_000     # Q6
MIN_LANGUAGES = 3          # Q6
CAREER_YEARS = 20          # Q7
CAREER_MOVIES = 15         # Q7
EARLY_SHARE = 1 / 3        # Q7: peak within the first third of the career
MIN_VOTES_Q8 = 100         # Q8
MIN_FILMS_EACH_SIDE = 3    # Q8: per-person comparison multi vs single role

# Region -> main language (ISO 639-1), used when akas.language is missing.
REGION_LANGUAGE = {
    "US": "en", "GB": "en", "AU": "en", "CA": "en", "IE": "en", "NZ": "en",
    "FR": "fr", "BE": "fr", "CH": "de", "DE": "de", "AT": "de", "IT": "it",
    "ES": "es", "MX": "es", "AR": "es", "CO": "es", "CL": "es", "PE": "es",
    "VE": "es", "PT": "pt", "BR": "pt", "JP": "ja", "KR": "ko", "CN": "zh",
    "TW": "zh", "HK": "zh", "IN": "hi", "RU": "ru", "SU": "ru", "UA": "uk",
    "PL": "pl", "CZ": "cs", "CSHH": "cs", "SK": "sk", "HU": "hu", "RO": "ro",
    "BG": "bg", "RS": "sr", "YUCS": "sr", "HR": "hr", "GR": "el", "TR": "tr",
    "IR": "fa", "EG": "ar", "SA": "ar", "MA": "ar", "DZ": "ar", "TN": "ar",
    "LB": "ar", "IL": "he", "NL": "nl", "SE": "sv", "NO": "no", "DK": "da",
    "FI": "fi", "IS": "is", "EE": "et", "LV": "lv", "LT": "lt", "TH": "th",
    "VN": "vi", "ID": "id", "MY": "ms", "PH": "tl", "PK": "ur", "BD": "bn",
    "LK": "si", "NG": "en", "ZA": "en", "XWG": "de", "DDDE": "de",
}
LANGUAGE_ALIASES = {"cmn": "zh", "yue": "zh", "qbn": "nl", "qbp": "pt"}


# ---------------------------------------------------------------------------
def q1_runtime_outliers(movies: DataFrame) -> None:
    fm = movies.filter(F.col("runtimeMinutes").isNotNull()).select(
        "tconst", "primaryTitle", "year", "runtimeMinutes",
        F.explode("genres").alias("genre"),
    )
    stats = fm.groupBy("genre").agg(
        F.count("*").alias("n_movies"),
        F.avg("runtimeMinutes").alias("avg_runtime"),
        F.stddev_samp("runtimeMinutes").alias("std_runtime"),
        F.percentile_approx("runtimeMinutes", 0.5).alias("median_runtime"),
    )
    scored = fm.join(stats, "genre").withColumn(
        "z_score",
        F.try_divide(F.col("runtimeMinutes") - F.col("avg_runtime"),
                     F.col("std_runtime")),
    ).filter(F.abs("z_score") >= Z_OUTLIER).cache()

    summary = (
        scored.groupBy("genre").agg(
            F.sum(F.when(F.col("z_score") > 0, 1).otherwise(0))
            .alias("n_longer"),
            F.sum(F.when(F.col("z_score") < 0, 1).otherwise(0))
            .alias("n_shorter"),
        )
        .join(stats, "genre", "right")
        .fillna(0, ["n_longer", "n_shorter"])
        .withColumn("pct_outliers", F.round(
            100 * (F.col("n_longer") + F.col("n_shorter"))
            / F.col("n_movies"), 2))
        .select("genre", "n_movies", F.round("avg_runtime", 1)
                .alias("avg_runtime"), F.round("std_runtime", 1)
                .alias("std_runtime"), "median_runtime", "n_longer",
                "n_shorter", "pct_outliers")
        .orderBy(F.desc("pct_outliers"))
    )
    save_result(
        summary, "02_q1_runtime_outliers_by_genre",
        "Q1 - Feature films whose runtime is notably different from their "
        "genre average (|z| >= %.0f)" % Z_OUTLIER,
        max_md_rows=40,
    )
    w = Window.partitionBy("genre").orderBy(F.desc(F.abs("z_score")))
    examples = (
        scored.withColumn("rk", F.row_number().over(w))
        .filter("rk <= 3")
        .select("genre", "primaryTitle", "year", "runtimeMinutes",
                F.round("avg_runtime", 1).alias("genre_avg"),
                F.round("z_score", 2).alias("z_score"))
        .orderBy("genre", F.desc(F.abs("z_score")))
    )
    save_result(examples, "02_q1_runtime_outliers_examples",
                "Q1 - Most extreme runtime outliers (top 3 per genre)",
                max_md_rows=45)
    scored.unpersist()


def genre_year_stats(movies: DataFrame) -> DataFrame:
    gy = movies.filter(
        F.col("year").between(YEAR_MIN, YEAR_MAX)
        & F.col("numVotes").isNotNull()
    ).select("tconst", "year", "averageRating", "numVotes", "runtimeMinutes",
             F.explode("genres").alias("genre"))
    stats = gy.groupBy("year", "genre").agg(
        F.count("*").alias("n_movies"),
        F.sum("numVotes").alias("total_votes"),
        F.round(F.avg("averageRating"), 2).alias("avg_rating"),
    )
    w = Window.partitionBy("year").orderBy(
        F.desc("total_votes"), F.desc("n_movies"), "genre")
    stats = stats.withColumn("rank", F.row_number().over(w))
    return gy, stats


def q2_q3_top_genres(movies: DataFrame) -> None:
    gy, stats = genre_year_stats(movies)
    stats = stats.cache()
    top = stats.filter(F.col("rank") <= TOP_GENRES)

    save_result(
        top.select("year", "rank", "genre", "n_movies", "total_votes",
                   "avg_rating").orderBy("year", "rank"),
        "02_q2_top3_genres_long",
        "Q2 - Top 3 genres per year (ranked by total IMDb votes)",
        max_md_rows=30,
    )
    wide = (
        top.groupBy("year").pivot("rank", [1, 2, 3])
        .agg(F.first("genre")).orderBy("year")
    )
    wide = wide.select("year", *[F.col(str(i)).alias(f"top_{i}")
                                 for i in range(1, TOP_GENRES + 1)])
    save_result(wide, "02_q2_top3_genres",
                "Q2 - Top 3 genres per year, 1980-2025 (by total votes)",
                max_md_rows=50)
    freq = (
        top.groupBy("genre").agg(
            F.count("*").alias("years_in_top3"),
            F.sum(F.when(F.col("rank") == 1, 1).otherwise(0))
            .alias("years_ranked_first"))
        .orderBy(F.desc("years_in_top3"))
    )
    save_result(freq, "02_q2_top3_frequency",
                "Q2 - How often each genre reached the yearly top 3")

    # Q3: top-3 genres vs all other genres, per year
    labelled = gy.join(stats.select("year", "genre", "rank"),
                       ["year", "genre"]).withColumn(
        "group", F.when(F.col("rank") <= TOP_GENRES, "top3")
        .otherwise("rest"))
    year_tot = labelled.groupBy("year").agg(
        F.sum("numVotes").alias("year_votes"),
        F.count("*").alias("year_rows"))
    grp = (
        labelled.groupBy("year", "group").agg(
            F.countDistinct("genre").alias("n_genres"),
            F.count("*").alias("n_movies"),
            F.sum("numVotes").alias("votes"),
            F.avg("averageRating").alias("avg_rating"),
            F.avg("numVotes").alias("avg_votes_per_movie"),
            F.avg("runtimeMinutes").alias("avg_runtime"))
        .join(year_tot, "year")
        .withColumn("share_of_votes", F.col("votes") / F.col("year_votes"))
        .withColumn("share_of_movies",
                    F.col("n_movies") / F.col("year_rows"))
    )
    piv = grp.groupBy("year").pivot("group", ["top3", "rest"]).agg(
        F.first("n_genres").alias("genres"),
        F.first("share_of_movies").alias("movie_share"),
        F.first("share_of_votes").alias("vote_share"),
        F.first("avg_rating").alias("rating"),
        F.first("avg_votes_per_movie").alias("votes_per_movie"),
        F.first("avg_runtime").alias("runtime"),
    )
    comp = piv.select(
        "year",
        "rest_genres",
        F.round(100 * F.col("top3_movie_share"), 1).alias("top3_pct_movies"),
        F.round(100 * F.col("top3_vote_share"), 1).alias("top3_pct_votes"),
        F.round("top3_rating", 2).alias("top3_avg_rating"),
        F.round("rest_rating", 2).alias("rest_avg_rating"),
        F.round("top3_votes_per_movie", 0).alias("top3_votes_per_movie"),
        F.round("rest_votes_per_movie", 0).alias("rest_votes_per_movie"),
        F.round("top3_runtime", 1).alias("top3_avg_runtime"),
        F.round("rest_runtime", 1).alias("rest_avg_runtime"),
    ).orderBy("year").cache()
    save_result(comp, "02_q3_top3_vs_rest",
                "Q3 - Top 3 genres vs the remaining genres, per year "
                "(a movie counts once per genre)", max_md_rows=50)
    overall = comp.agg(
        F.round(F.avg("top3_pct_movies"), 1).alias("avg_top3_pct_movies"),
        F.round(F.avg("top3_pct_votes"), 1).alias("avg_top3_pct_votes"),
        F.round(F.avg("top3_avg_rating"), 2).alias("avg_top3_rating"),
        F.round(F.avg("rest_avg_rating"), 2).alias("avg_rest_rating"),
        F.round(F.avg(F.col("top3_votes_per_movie")
                      / F.col("rest_votes_per_movie")), 2)
        .alias("votes_per_movie_ratio"),
        F.sum(F.when(F.col("top3_avg_rating") > F.col("rest_avg_rating"), 1)
              .otherwise(0)).alias("years_top3_rated_higher"),
        F.count("*").alias("years"),
    )
    save_result(overall, "02_q3_top3_vs_rest_overall",
                "Q3 - Summary over all years")
    stats.unpersist()
    comp.unpersist()


def q4_one_hit_directors(spark, movies: DataFrame) -> None:
    directors = read_silver(spark, "title_directors")
    names = read_silver(spark, "name_basics").select("nconst", "primaryName")
    rated = movies.filter(F.col("numVotes") >= MIN_VOTES_RELIABLE)
    dm = directors.join(rated, "tconst")
    per_dir = dm.groupBy("nconst").agg(
        F.sum(F.when(F.col("averageRating") >= HIT_RATING, 1).otherwise(0))
        .alias("n_hits"),
        F.max(F.when(F.col("averageRating") >= HIT_RATING,
                     F.struct("averageRating", "primaryTitle", "year",
                              "numVotes"))).alias("hit"),
        F.sum(F.when(F.col("averageRating") < HIT_RATING, 1).otherwise(0))
        .alias("n_other"),
        F.avg(F.when(F.col("averageRating") < HIT_RATING,
                     F.col("averageRating"))).alias("avg_other"),
    )
    res = (
        per_dir.filter(
            (F.col("n_hits") == 1) & (F.col("n_other") >= 2)
            & (F.col("avg_other") <= FLOP_AVG_RATING))
        .join(names, "nconst")
        .select(
            F.col("primaryName").alias("director"),
            F.col("hit.primaryTitle").alias("hit_movie"),
            F.col("hit.year").alias("hit_year"),
            F.col("hit.averageRating").alias("hit_rating"),
            F.col("hit.numVotes").alias("hit_votes"),
            F.col("n_other").alias("n_other_movies"),
            F.round("avg_other", 2).alias("avg_rating_other"),
        )
        .orderBy(F.desc("hit_votes"))
    )
    save_result(
        res, "02_q4_one_hit_directors",
        f"Q4 - Directors with exactly one movie rated >= {HIT_RATING} and "
        f">= 2 other movies averaging <= {FLOP_AVG_RATING} "
        f"(movies with >= {MIN_VOTES_RELIABLE:,} votes)",
        max_md_rows=40,
    )


def q5_series_decline(spark) -> None:
    basics = read_silver(spark, "title_basics")
    episodes = read_silver(spark, "title_episode").filter(
        F.col("seasonNumber").isNotNull())
    ratings = read_silver(spark, "title_ratings").select(
        "tconst", "averageRating", "numVotes")
    series = basics.filter(F.col("titleType") == "tvSeries").select(
        F.col("tconst").alias("parentTconst"),
        F.col("primaryTitle").alias("series"),
        F.col("startYear").alias("series_start"))
    ep = episodes.join(series, "parentTconst")

    season_sizes = ep.groupBy("parentTconst", "seasonNumber").agg(
        F.count("*").alias("n_episodes"))
    per_series = season_sizes.groupBy("parentTconst").agg(
        F.sum(F.when(F.col("n_episodes") >= 10, 1).otherwise(0))
        .alias("seasons_10plus"),
        F.max("seasonNumber").alias("last_season"),
        F.count("*").alias("n_seasons"),
    ).filter(F.col("seasons_10plus") >= 3)

    rated = ep.join(ratings, "tconst").join(per_series, "parentTconst")
    w_worst = Window.partitionBy("parentTconst").orderBy(
        F.asc("averageRating"), F.desc("numVotes"), "tconst")
    w_best = Window.partitionBy("parentTconst").orderBy(
        F.desc("averageRating"), F.desc("numVotes"), "tconst")
    ranked = (rated.withColumn("worst_rk", F.row_number().over(w_worst))
              .withColumn("best_rk", F.row_number().over(w_best)))
    worst = ranked.filter("worst_rk = 1").select(
        "parentTconst", F.col("seasonNumber").alias("worst_season"),
        F.col("episodeNumber").alias("worst_episode"),
        F.col("averageRating").alias("worst_rating"))
    best = ranked.filter("best_rk = 1").select(
        "parentTconst", F.col("seasonNumber").alias("best_season"),
        F.col("episodeNumber").alias("best_episode"),
        F.col("averageRating").alias("best_rating"))
    series_votes = ratings.select(F.col("tconst").alias("parentTconst"),
                                  F.col("numVotes").alias("series_votes"))
    res = (
        per_series.join(worst, "parentTconst").join(best, "parentTconst")
        .join(series, "parentTconst")
        .join(series_votes, "parentTconst", "left")
        .filter((F.col("worst_season") == F.col("last_season"))
                & (F.col("best_season") <= 2))
        .select("series", "series_start", "n_seasons", "seasons_10plus",
                "best_season", "best_episode", "best_rating",
                "worst_season", "worst_episode", "worst_rating",
                "series_votes")
        .orderBy(F.desc_nulls_last("series_votes"))
    )
    save_result(
        res, "02_q5_series_decline",
        "Q5 - TV series (>= 3 seasons of >= 10 episodes) whose worst "
        "episode is in the last season and best episode in seasons 1-2",
        max_md_rows=40,
    )


def movie_languages(spark, movies: DataFrame) -> DataFrame:
    """Infer the original language of each movie from title_akas."""
    akas = read_silver(spark, "title_akas")
    region_map = spark.createDataFrame(
        list(REGION_LANGUAGE.items()), "region string, region_language string")
    alias_map = F.create_map(*[F.lit(x) for kv in LANGUAGE_ALIASES.items()
                               for x in kv])
    orig = movies.select(F.col("tconst").alias("titleId"),
                         normalize_text(F.col("originalTitle"))
                         .alias("orig_norm"))
    cand = akas.join(orig, "titleId").filter(
        normalize_text(F.col("title")) == F.col("orig_norm"))
    direct = cand.filter(F.col("language").isNotNull()).select(
        "titleId",
        F.coalesce(alias_map[F.col("language")], F.col("language"))
        .alias("language"), F.lit(1).alias("priority"))
    by_region = cand.filter(F.col("region").isNotNull()).join(
        region_map, "region").select(
        "titleId", F.col("region_language").alias("language"),
        F.lit(2).alias("priority"))
    allc = direct.unionByName(by_region).groupBy("titleId", "language").agg(
        F.min("priority").alias("priority"), F.count("*").alias("n"))
    w = Window.partitionBy("titleId").orderBy("priority", F.desc("n"),
                                              "language")
    return (allc.withColumn("rk", F.row_number().over(w)).filter("rk = 1")
            .select(F.col("titleId").alias("tconst"), "language"))


def q6_multilingual_actors(spark, movies: DataFrame) -> None:
    principals = read_silver(spark, "title_principals").filter(
        F.col("category").isin("actor", "actress"))
    names = read_silver(spark, "name_basics").select("nconst", "primaryName")
    langs = movie_languages(spark, movies).cache()

    n_movies = movies.filter(F.col("numVotes").isNotNull()).count()
    n_lang = langs.join(movies.filter(F.col("numVotes").isNotNull()),
                        "tconst").count()
    dist = langs.groupBy("language").count().orderBy(F.desc("count"))
    save_result(dist, "02_q6_language_distribution",
                f"Q6 - Inferred original language of rated feature films "
                f"(coverage: {n_lang:,}/{n_movies:,} = "
                f"{100 * n_lang / max(n_movies, 1):.1f}%)", max_md_rows=20)

    hits = movies.filter((F.col("averageRating") >= SUCCESS_RATING)
                         & (F.col("numVotes") >= SUCCESS_VOTES)).join(
        langs, "tconst")
    credits = principals.select("tconst", "nconst").distinct().join(
        hits, "tconst")
    per_actor = credits.groupBy("nconst").agg(
        F.countDistinct("language").alias("n_languages"),
        F.array_sort(F.collect_set("language")).alias("languages"),
        F.count("*").alias("n_successful_films"),
        F.slice(F.array_sort(F.collect_list(
            F.struct(F.col("numVotes") * -1, "primaryTitle"))), 1, 3)
        .alias("_top"),
    ).cache()
    distribution = per_actor.groupBy("n_languages").agg(
        F.count("*").alias("n_actors")).orderBy("n_languages")
    save_result(distribution, "02_q6_languages_per_actor",
                f"Q6 - Actors/actresses by number of distinct languages of "
                f"their successful films (rating >= {SUCCESS_RATING}, "
                f">= {SUCCESS_VOTES:,} votes)")
    res = (
        per_actor.filter(F.col("n_languages") >= MIN_LANGUAGES)
        .join(names, "nconst")
        .select(F.col("primaryName").alias("name"), "n_languages",
                "languages", "n_successful_films",
                F.col("_top.primaryTitle").alias("best_known_hits"))
        .orderBy(F.desc("n_languages"), F.desc("n_successful_films"))
    )
    save_result(res, "02_q6_multilingual_actors",
                f"Q6 - Actors/actresses with successful films in at least "
                f"{MIN_LANGUAGES} languages", max_md_rows=40)
    langs.unpersist()
    per_actor.unpersist()


def q7_early_peak(spark, movies: DataFrame) -> None:
    principals = read_silver(spark, "title_principals").filter(
        F.col("category").isin("actor", "actress"))
    names = read_silver(spark, "name_basics").select("nconst", "primaryName")
    rated = movies.filter(F.col("numVotes").isNotNull()
                          & F.col("year").isNotNull())
    # Votes are normalised by the yearly average to remove the strong bias
    # towards recent movies (IMDb users vote more for recent releases).
    year_avg = rated.groupBy("year").agg(F.avg("numVotes").alias("year_avg"))
    am = (
        principals.select("tconst", "nconst").distinct()
        .join(rated, "tconst").join(year_avg, "year")
        .withColumn("rel_pop", F.col("numVotes") / F.col("year_avg"))
    )
    career = am.groupBy("nconst").agg(
        F.min("year").alias("first_year"), F.max("year").alias("last_year"),
        F.countDistinct("tconst").alias("n_movies"),
    ).filter(((F.col("last_year") - F.col("first_year")) >= CAREER_YEARS)
             & (F.col("n_movies") >= CAREER_MOVIES))
    per_year = am.join(career.select("nconst"), "nconst").groupBy(
        "nconst", "year").agg(
        F.sum("rel_pop").alias("year_pop"),
        F.max(F.struct("rel_pop", "primaryTitle")).alias("best"))
    w = Window.partitionBy("nconst").orderBy(F.desc("year_pop"), "year")
    peak = per_year.withColumn("rk", F.row_number().over(w)).filter(
        "rk = 1").select("nconst", F.col("year").alias("peak_year"),
                         F.col("best.primaryTitle").alias("peak_movie"),
                         F.round("year_pop", 2).alias("peak_relative_pop"))
    res = (
        career.join(peak, "nconst").join(names, "nconst")
        .withColumn("span", F.col("last_year") - F.col("first_year"))
        .withColumn("peak_position",
                    F.round((F.col("peak_year") - F.col("first_year"))
                            / F.col("span"), 3))
        .cache()
    )
    total = res.count()
    early = res.filter(F.col("peak_position") <= EARLY_SHARE)
    n_early = early.count()
    save_text(
        "02_q7_summary",
        f"**Q7 - Summary**\n\nActors/actresses with a career of at least "
        f"{CAREER_YEARS} years and at least {CAREER_MOVIES} rated feature "
        f"films: **{total:,}**. Popularity peak in the first third of the "
        f"career: **{n_early:,}** ({100 * n_early / max(total, 1):.1f}%).",
    )
    save_result(
        early.select(F.col("primaryName").alias("name"), "first_year",
                     "last_year", "n_movies", "peak_year", "peak_movie",
                     "peak_position", "peak_relative_pop")
        .orderBy(F.desc("n_movies"), "peak_position"),
        "02_q7_early_peak_actors",
        "Q7 - Actors/actresses (20+ years, 15+ movies) whose popularity "
        "peaked early (peak_position = share of career elapsed at peak)",
        max_md_rows=40,
    )
    res.unpersist()


ROLE_MAP = {
    "actor": "actor", "actress": "actor", "director": "director",
    "writer": "writer", "producer": "producer", "composer": "composer",
    "cinematographer": "cinematographer", "editor": "editor",
    "production_designer": "production_designer",
}


def q8_multi_talented(spark, movies: DataFrame) -> None:
    principals = read_silver(spark, "title_principals")
    role_map = F.create_map(*[F.lit(x) for kv in ROLE_MAP.items()
                              for x in kv])
    from_principals = principals.select(
        "tconst", "nconst", role_map[F.col("category")].alias("role")
    ).filter(F.col("role").isNotNull())
    from_crew = (
        read_silver(spark, "title_directors")
        .withColumn("role", F.lit("director"))
        .unionByName(read_silver(spark, "title_writers")
                     .withColumn("role", F.lit("writer")))
    )
    rated = movies.filter(F.col("numVotes") >= MIN_VOTES_Q8).select(
        "tconst", "averageRating", "numVotes")
    credits = (
        from_principals.unionByName(from_crew).distinct()
        .join(rated, "tconst")
        .groupBy("tconst", "nconst", "averageRating", "numVotes")
        .agg(F.array_sort(F.collect_set("role")).alias("roles"))
        .withColumn("n_roles", F.size("roles"))
        .withColumn("multi", F.col("n_roles") >= 2)
        .cache()
    )
    names = read_silver(spark, "name_basics").select("nconst", "primaryName")

    single = credits.filter(~F.col("multi")).agg(
        F.round(F.avg("averageRating"), 3).alias("r"),
        F.percentile_approx("numVotes", 0.5).alias("v")).collect()[0]
    combos = (
        credits.filter("multi")
        .withColumn("combination", F.concat_ws(" + ", "roles"))
        .groupBy("combination").agg(
            F.count("*").alias("n_credits"),
            F.countDistinct("nconst").alias("n_people"),
            F.round(F.avg("averageRating"), 3).alias("avg_rating"),
            F.percentile_approx("numVotes", 0.5).alias("median_votes"))
        .withColumn("rating_vs_single_role",
                    F.round(F.col("avg_rating") - F.lit(single["r"]), 3))
        .orderBy(F.desc("n_credits"))
    )
    save_result(
        combos, "02_q8_role_combinations",
        f"Q8 - Performance of films where one person holds several roles "
        f"(baseline single-role credits: avg rating {single['r']}, "
        f"median votes {single['v']:,})", max_md_rows=25,
    )

    per_person = credits.groupBy("nconst").agg(
        F.sum(F.col("multi").cast("int")).alias("n_multi_role_films"),
        F.sum((~F.col("multi")).cast("int")).alias("n_single_role_films"),
        F.avg(F.when(F.col("multi"), F.col("averageRating")))
        .alias("avg_rating_multi"),
        F.avg(F.when(~F.col("multi"), F.col("averageRating")))
        .alias("avg_rating_single"),
        F.array_sort(F.array_distinct(F.flatten(F.collect_list(
            F.when(F.col("multi"), F.col("roles"))))))
        .alias("roles_combined"),
    ).filter(F.col("n_multi_role_films") >= MIN_FILMS_EACH_SIDE).cache()
    talents = (
        per_person.join(names, "nconst")
        .withColumn("delta", F.round(F.col("avg_rating_multi")
                                     - F.col("avg_rating_single"), 2))
        .select(F.col("primaryName").alias("name"), "roles_combined",
                "n_multi_role_films", "n_single_role_films",
                F.round("avg_rating_multi", 2).alias("avg_rating_multi"),
                F.round("avg_rating_single", 2).alias("avg_rating_single"),
                "delta")
    )
    save_result(
        talents.orderBy(F.desc("n_multi_role_films")),
        "02_q8_multi_talented_people",
        f"Q8 - Multi-talented professionals (>= {MIN_FILMS_EACH_SIDE} films "
        f"holding several roles)", max_md_rows=30,
    )
    comparable = talents.filter(
        F.col("n_single_role_films") >= MIN_FILMS_EACH_SIDE)
    agg = comparable.agg(
        F.count("*").alias("people"),
        F.round(100 * F.avg((F.col("delta") > 0).cast("int")), 1)
        .alias("pct_better_when_multi"),
        F.round(F.avg("delta"), 3).alias("mean_delta"),
        F.percentile_approx("delta", 0.5).alias("median_delta"),
    )
    save_result(
        agg, "02_q8_multi_vs_single_summary",
        f"Q8 - Same person, multi-role vs single-role films (people with "
        f">= {MIN_FILMS_EACH_SIDE} films on each side)",
    )
    credits.unpersist()
    per_person.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default="",
                        help="comma separated subset, e.g. q1,q5")
    args = parser.parse_args()
    selected = {q.strip().lower() for q in args.only.split(",") if q.strip()}
    run = lambda q: not selected or q in selected  # noqa: E731

    spark = get_spark("03_exploration")
    movies = feature_movies(spark, FEATURE_MIN_RUNTIME).cache()
    print(f"feature-length movies: {movies.count():,}")

    if run("q1"):
        q1_runtime_outliers(movies)
    if run("q2") or run("q3"):
        q2_q3_top_genres(movies)
    if run("q4"):
        q4_one_hit_directors(spark, movies)
    if run("q5"):
        q5_series_decline(spark)
    if run("q6"):
        q6_multilingual_actors(spark, movies)
    if run("q7"):
        q7_early_peak(spark, movies)
    if run("q8"):
        q8_multi_talented(spark, movies)
    spark.stop()


if __name__ == "__main__":
    main()
