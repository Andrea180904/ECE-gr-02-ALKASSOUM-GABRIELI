"""Ingest the raw IMDb TSV files into a Parquet "bronze" layer.

Every column is kept as a string so that no information is lost before
the data-quality step (a failed cast would silently become NULL).
IMDb encodes missing values as the literal "\\N", which is mapped to NULL.

Run with:  ./run.sh spark jobs/01_ingest.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType, StructField, StructType  # noqa: E402

from common.config import BRONZE_DIR, IMDB_TABLES, RAW_IMDB_DIR  # noqa: E402
from common.output import save_result  # noqa: E402
from common.spark import get_spark  # noqa: E402


def main() -> None:
    spark = get_spark("01_ingest")
    counts = []
    for table, (file_name, columns) in IMDB_TABLES.items():
        schema = StructType(
            [StructField(c, StringType(), True) for c in columns]
        )
        source = RAW_IMDB_DIR / file_name
        df = (
            spark.read.option("sep", "\t")
            .option("header", "true")
            .option("nullValue", "\\N")
            # IMDb TSV files are not quoted: titles may contain '"'
            .option("quote", "")
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(str(source))
        )
        target = BRONZE_DIR / table
        df.write.mode("overwrite").parquet(str(target))
        n = spark.read.parquet(str(target)).count()
        print(f"{table}: {n:,} rows -> {target}")
        counts.append((table, file_name, n, len(columns)))

    summary = spark.createDataFrame(
        counts, "table string, source_file string, rows long, columns int"
    ).orderBy(F.desc("rows"))
    save_result(summary, "00_ingestion", "Ingested IMDb tables")
    spark.stop()


if __name__ == "__main__":
    main()
