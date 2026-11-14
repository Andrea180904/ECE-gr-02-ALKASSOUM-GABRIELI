"""Download the IMDb non-commercial datasets into data/raw/imdb/.

Plain Python (no Spark). Files that already exist are skipped unless
--force is given. Run with:  ./run.sh py jobs/00_download_imdb.py
"""

import argparse
import shutil
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common.config import (  # noqa: E402
    IMDB_BASE_URL, IMDB_TABLES, RAW_IMDB_DIR,
)


def download(url: str, dest: Path) -> None:
    tmp = dest.with_suffix(dest.suffix + ".part")
    request = urllib.request.Request(
        url, headers={"User-Agent": "ece-big-data-student-project"}
    )
    with urllib.request.urlopen(request) as response, open(tmp, "wb") as fh:
        total = int(response.headers.get("Content-Length", 0))
        done = 0
        while True:
            chunk = response.read(1 << 20)
            if not chunk:
                break
            fh.write(chunk)
            done += len(chunk)
            if total:
                print(f"\r  {dest.name}: {done / total:6.1%}", end="")
    print()
    shutil.move(tmp, dest)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true",
                        help="re-download existing files")
    args = parser.parse_args()

    RAW_IMDB_DIR.mkdir(parents=True, exist_ok=True)
    for file_name, _ in IMDB_TABLES.values():
        dest = RAW_IMDB_DIR / file_name
        if dest.exists() and not args.force:
            print(f"skip {file_name} (already downloaded)")
            continue
        print(f"downloading {file_name}")
        download(f"{IMDB_BASE_URL}/{file_name}", dest)
    print("done:", RAW_IMDB_DIR)


if __name__ == "__main__":
    main()
