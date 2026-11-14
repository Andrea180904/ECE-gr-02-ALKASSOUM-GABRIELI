"""Central configuration: paths and analysis parameters shared by all jobs."""

import os
from pathlib import Path

PROJECT_ROOT = Path(
    os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2])
)

DATA_DIR = PROJECT_ROOT / "data"
RAW_IMDB_DIR = DATA_DIR / "raw" / "imdb"
BRONZE_DIR = DATA_DIR / "bronze"  # source data as Parquet, all strings
SILVER_DIR = DATA_DIR / "silver"  # cleaned, typed, validated
QUARANTINE_DIR = DATA_DIR / "quarantine"  # rows rejected by cleaning
GOLD_DIR = DATA_DIR / "gold"  # analytical / enriched outputs
ENRICH_DIR = DATA_DIR / "enrichment"
STREAMING_DIR = DATA_DIR / "streaming"
RESULTS_DIR = PROJECT_ROOT / "results"

IMDB_BASE_URL = "https://datasets.imdbws.com"

# file name -> ordered list of columns (as documented by IMDb)
IMDB_TABLES = {
    "name_basics": (
        "name.basics.tsv.gz",
        ["nconst", "primaryName", "birthYear", "deathYear",
         "primaryProfession", "knownForTitles"],
    ),
    "title_akas": (
        "title.akas.tsv.gz",
        ["titleId", "ordering", "title", "region", "language", "types",
         "attributes", "isOriginalTitle"],
    ),
    "title_basics": (
        "title.basics.tsv.gz",
        ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
         "startYear", "endYear", "runtimeMinutes", "genres"],
    ),
    "title_crew": (
        "title.crew.tsv.gz",
        ["tconst", "directors", "writers"],
    ),
    "title_episode": (
        "title.episode.tsv.gz",
        ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
    ),
    "title_principals": (
        "title.principals.tsv.gz",
        ["tconst", "ordering", "nconst", "category", "job", "characters"],
    ),
    "title_ratings": (
        "title.ratings.tsv.gz",
        ["tconst", "averageRating", "numVotes"],
    ),
}

# Analysis parameters
CURRENT_YEAR = 2026
FIRST_FILM_YEAR = 1874  # earliest known motion picture
YEAR_MIN = 1980
YEAR_MAX = 2025
FEATURE_MIN_RUNTIME = 40  # feature film = runtime strictly above 40 minutes

# Streaming
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "wikistreams")
