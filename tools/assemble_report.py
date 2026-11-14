"""Copy the generated result tables (results/*.md) into REPORT.md.

REPORT.md contains markers such as

    <!-- RESULTS:02_q4_one_hit_directors -->
    ...anything here is replaced...
    <!-- /RESULTS:02_q4_one_hit_directors -->

Only the content between markers is touched: the methodological
explanations written by the team are never modified.

Run with:  ./run.sh py tools/assemble_report.py   (or python3 directly)
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "REPORT.md"
RESULTS = ROOT / "results"
PATTERN = re.compile(
    r"(<!-- RESULTS:(?P<name>[\w-]+) -->)(.*?)(<!-- /RESULTS:(?P=name) -->)",
    re.DOTALL,
)


def main() -> None:
    text = REPORT.read_text(encoding="utf-8")
    filled, missing = [], []

    def replace(match):
        name = match.group("name")
        path = RESULTS / f"{name}.md"
        if path.exists():
            body = path.read_text(encoding="utf-8").strip()
            filled.append(name)
        else:
            body = f"_Result `{name}` not generated yet._"
            missing.append(name)
        return f"{match.group(1)}\n{body}\n{match.group(4)}"

    REPORT.write_text(PATTERN.sub(replace, text), encoding="utf-8")
    print(f"filled {len(filled)} result blocks")
    if missing:
        print("missing:", ", ".join(missing))


if __name__ == "__main__":
    main()
