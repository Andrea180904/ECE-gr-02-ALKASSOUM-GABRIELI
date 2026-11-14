"""Save query results as Markdown (for REPORT.md) and CSV (full result)."""

import csv
import datetime as dt
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from common.config import RESULTS_DIR


def _fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value % 1:
            return f"{value:,.3f}".rstrip("0").rstrip(".")
        return f"{value:,.0f}"
    if isinstance(value, Decimal):
        return _fmt(float(value))
    if isinstance(value, int) and not isinstance(value, bool):
        # no separator below 10,000 so that years stay readable (1980)
        return f"{value:,}" if abs(value) >= 10_000 else str(value)
    if isinstance(value, (list, tuple)):
        return ", ".join(_fmt(v) for v in value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text


def rows_to_markdown(columns, rows) -> str:
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_fmt(v) for v in row) + " |")
    return "\n".join(lines)


def save_result(
    df: DataFrame,
    name: str,
    title: str,
    description: str = "",
    max_md_rows: int = 30,
    max_csv_rows: int = 100_000,
) -> list:
    """Collect (a bounded number of) rows, write results/<name>.md and
    results/csv/<name>.csv, print the Markdown table and return the rows."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / "csv").mkdir(exist_ok=True)

    # Render timestamps in the session time zone (UTC) instead of letting
    # Python convert them to the local time zone of the machine.
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            df = df.withColumn(field.name, F.date_format(
                field.name, "yyyy-MM-dd HH:mm:ss"))
    rows = df.limit(max_csv_rows).collect()
    columns = df.columns
    total = len(rows)

    with open(RESULTS_DIR / "csv" / f"{name}.csv", "w", newline="",
              encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        for row in rows:
            writer.writerow(
                ["|".join(map(str, v)) if isinstance(v, list) else v
                 for v in row]
            )

    shown = rows[:max_md_rows]
    md = [f"**{title}**", ""]
    if description:
        md += [description, ""]
    md.append(rows_to_markdown(columns, shown))
    md.append("")
    if total > len(shown):
        md.append(
            f"_Showing {len(shown)} of {total:,} rows. "
            f"Full result: `results/csv/{name}.csv`._"
        )
    else:
        md.append(f"_{total:,} rows._")
    text = "\n".join(md) + "\n"
    (RESULTS_DIR / f"{name}.md").write_text(text, encoding="utf-8")

    print(f"\n=== {title} ===")
    print(text)
    return rows


def save_text(name: str, text: str) -> None:
    """Write free-form Markdown (e.g. key metrics) to results/<name>.md."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTS_DIR / f"{name}.md").write_text(text.strip() + "\n",
                                            encoding="utf-8")
    print(f"\n=== {name} ===\n{text}")
