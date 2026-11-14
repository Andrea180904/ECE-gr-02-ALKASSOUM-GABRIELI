"""Part 3 (step 2/3) - Gather raw enrichment data (plain Python, no Spark).

Sources (human-authored, open licences):
  * Wikidata  (CC0)       : IMDb id (property P345) -> Wikidata item,
                            English Wikipedia article, short description,
                            birth/death date and birth place for people.
  * Wikipedia (CC BY-SA)  : full plain text of film articles (the "Plot"
                            section is extracted later, in Spark) and the
                            introduction of people articles (biography).

The raw API answers are appended, unmodified, to JSON-lines files in
data/enrichment/raw/. All cleaning, de-duplication and schema enforcement
happen in Spark (jobs/04b_enrich_process.py).

Politeness: a descriptive User-Agent, serial requests with a minimum delay,
`maxlag` on the MediaWiki API, exponential back-off on 429/5xx and
`Retry-After` support. The script is resumable: ids already present in the
output files are skipped, so it can be stopped and restarted at any time.

Run with:  ./run.sh py collectors/enrich_collect.py --contact you@ece.fr
           (add --limit 50 for a quick test)
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT",
                                   Path(__file__).resolve().parents[1]))
ENRICH_DIR = PROJECT_ROOT / "data" / "enrichment"
RAW_DIR = ENRICH_DIR / "raw"

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_ENTITY = "http://www.wikidata.org/entity/"
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
ENWIKI_PREFIX = "https://en.wikipedia.org/wiki/"

SPARQL_BATCH = 150
INTRO_BATCH = 20  # MediaWiki limit for intro extracts per request

FILM_QUERY = """
SELECT ?imdb ?item ?article ?description WHERE {
  VALUES ?imdb { %s }
  ?item wdt:P345 ?imdb .
  OPTIONAL { ?article schema:about ?item ;
                      schema:isPartOf <https://en.wikipedia.org/> . }
  OPTIONAL { ?item schema:description ?description .
             FILTER(LANG(?description) = "en") }
}"""

PERSON_QUERY = """
SELECT ?imdb ?item ?article ?description ?birth ?death ?birthPlace WHERE {
  VALUES ?imdb { %s }
  ?item wdt:P345 ?imdb .
  OPTIONAL { ?article schema:about ?item ;
                      schema:isPartOf <https://en.wikipedia.org/> . }
  OPTIONAL { ?item schema:description ?description .
             FILTER(LANG(?description) = "en") }
  OPTIONAL { ?item wdt:P569 ?birth . }
  OPTIONAL { ?item wdt:P570 ?death . }
  OPTIONAL { ?item wdt:P19 ?bp .
             ?bp rdfs:label ?birthPlace . FILTER(LANG(?birthPlace) = "en") }
}"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Client:
    """HTTP client with throttling, retries and back-off."""

    def __init__(self, contact: str, delay: float):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = (
            f"ECE-BigData-StudentProject/1.0 (contact: {contact}) "
            f"python-requests/{requests.__version__}"
        )
        self.delay = delay
        self.last = 0.0

    def get_json(self, url: str, params: dict, retries: int = 6) -> dict:
        backoff = 5.0
        for attempt in range(retries):
            wait = self.delay - (time.monotonic() - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.monotonic()
            try:
                resp = self.session.get(url, params=params, timeout=60)
            except requests.RequestException as exc:
                print(f"  network error ({exc}), retry in {backoff:.0f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            if resp.status_code == 200:
                data = resp.json()
                # MediaWiki maxlag: server asks us to slow down
                if isinstance(data, dict) and \
                        data.get("error", {}).get("code") == "maxlag":
                    time.sleep(float(resp.headers.get("Retry-After", 5)))
                    continue
                return data
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                pause = float(retry_after) if retry_after and \
                    retry_after.isdigit() else backoff
                print(f"  HTTP {resp.status_code}, pause {pause:.0f}s "
                      f"(attempt {attempt + 1}/{retries})")
                time.sleep(pause)
                backoff *= 2
                continue
            resp.raise_for_status()
        raise RuntimeError(f"giving up on {url} after {retries} attempts")


def read_ids(folder: Path) -> list:
    ids = []
    for part in sorted(folder.glob("part-*")):
        ids += [line.strip() for line in part.read_text().splitlines()
                if line.strip()]
    return sorted(set(ids))


def done_ids(path: Path, key: str = "imdb_id") -> set:
    if not path.exists():
        return set()
    seen = set()
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            try:
                seen.add(json.loads(line)[key])
            except (json.JSONDecodeError, KeyError):
                continue  # truncated last line after an interruption
    return seen


def append(path: Path, records: list) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def title_from_url(url: str) -> str:
    return urllib.parse.unquote(url[len(ENWIKI_PREFIX):]).replace("_", " ")


# ---------------------------------------------------------------- Wikidata
def parse_sparql(data: dict, ids: list, entity: str) -> list:
    """One raw record per SPARQL binding + one empty record per id that
    has no Wikidata item (so that it is not queried again)."""
    retrieved = now()
    records, found = [], set()
    for b in data.get("results", {}).get("bindings", []):
        val = lambda k: b.get(k, {}).get("value")  # noqa: E731
        found.add(val("imdb"))
        records.append({
            "imdb_id": val("imdb"), "entity_type": entity,
            "wikidata_uri": val("item"), "enwiki_url": val("article"),
            "description": val("description"), "birth": val("birth"),
            "death": val("death"), "birth_place": val("birthPlace"),
            "source": "wikidata", "source_url": WIKIDATA_SPARQL,
            "retrieved_at": retrieved,
        })
    for missing in set(ids) - found:
        records.append({
            "imdb_id": missing, "entity_type": entity, "wikidata_uri": None,
            "enwiki_url": None, "description": None, "birth": None,
            "death": None, "birth_place": None, "source": "wikidata",
            "source_url": WIKIDATA_SPARQL, "retrieved_at": retrieved,
        })
    return records


def collect_wikidata(client, ids, entity, out: Path) -> None:
    todo = [i for i in ids if i not in done_ids(out)]
    print(f"wikidata/{entity}: {len(ids) - len(todo)} done, "
          f"{len(todo)} to fetch")
    query = FILM_QUERY if entity == "film" else PERSON_QUERY
    for start in range(0, len(todo), SPARQL_BATCH):
        batch = todo[start:start + SPARQL_BATCH]
        values = " ".join(f'"{i}"' for i in batch)
        data = client.get_json(
            WIKIDATA_SPARQL,
            {"query": query % values, "format": "json"},
        )
        append(out, parse_sparql(data, batch, entity))
        print(f"  {min(start + SPARQL_BATCH, len(todo))}/{len(todo)}")


def wikipedia_targets(wikidata_file: Path) -> dict:
    """imdb_id -> list of distinct English Wikipedia article URLs."""
    targets = {}
    with open(wikidata_file, encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            if rec.get("enwiki_url"):
                targets.setdefault(rec["imdb_id"], set()).add(
                    rec["enwiki_url"])
    return {k: sorted(v) for k, v in targets.items()}


# --------------------------------------------------------------- Wikipedia
def page_record(imdb_id, entity, requested, page, mode) -> dict:
    title = page.get("title", requested)
    revid = page.get("lastrevid")
    permalink = (
        f"https://en.wikipedia.org/w/index.php?title="
        f"{urllib.parse.quote(title.replace(' ', '_'))}&oldid={revid}"
        if revid else ENWIKI_PREFIX + urllib.parse.quote(
            title.replace(" ", "_"))
    )
    return {
        "imdb_id": imdb_id, "entity_type": entity, "extract_mode": mode,
        "requested_title": requested, "page_title": title,
        "pageid": page.get("pageid"), "revid": revid,
        "missing": bool(page.get("missing", False)),
        "extract": page.get("extract"), "source": "wikipedia",
        "source_url": permalink, "retrieved_at": now(),
    }


def resolve_titles(data: dict) -> dict:
    """requested title -> final title, following normalisation/redirects."""
    query = data.get("query", {})
    mapping = {}
    for item in query.get("normalized", []):
        mapping[item["from"]] = item["to"]
    redirects = {r["from"]: r["to"] for r in query.get("redirects", [])}
    resolved = {}
    for page in query.get("pages", []):
        resolved[page.get("title")] = page
    return mapping, redirects, resolved


def lookup(requested, mapping, redirects, resolved):
    title = mapping.get(requested, requested)
    title = redirects.get(title, title)
    return resolved.get(title, {"title": requested, "missing": True})


def collect_films(client, wikidata_file: Path, out: Path, limit) -> None:
    targets = wikipedia_targets(wikidata_file)
    done = done_ids(out)
    todo = [(i, url) for i, urls in sorted(targets.items())
            if i not in done for url in urls[:1]]
    if limit:
        todo = todo[:limit]
    print(f"wikipedia/film: {len(done)} done, {len(todo)} to fetch")
    for n, (imdb_id, url) in enumerate(todo, start=1):
        requested = title_from_url(url)
        data = client.get_json(WIKIPEDIA_API, {
            "action": "query", "prop": "extracts|info", "explaintext": 1,
            "exsectionformat": "wiki", "titles": requested,
            "redirects": 1, "format": "json", "formatversion": 2,
            "maxlag": 5,
        })
        mapping, redirects, resolved = resolve_titles(data)
        page = lookup(requested, mapping, redirects, resolved)
        append(out, [page_record(imdb_id, "film", requested, page, "full")])
        if n % 50 == 0:
            print(f"  {n}/{len(todo)}")


def collect_people(client, wikidata_file: Path, out: Path, limit) -> None:
    targets = wikipedia_targets(wikidata_file)
    done = done_ids(out)
    todo = [(i, title_from_url(urls[0])) for i, urls in
            sorted(targets.items()) if i not in done]
    if limit:
        todo = todo[:limit]
    print(f"wikipedia/person: {len(done)} done, {len(todo)} to fetch")
    for start in range(0, len(todo), INTRO_BATCH):
        batch = todo[start:start + INTRO_BATCH]
        data = client.get_json(WIKIPEDIA_API, {
            "action": "query", "prop": "extracts|info", "exintro": 1,
            "explaintext": 1, "exlimit": INTRO_BATCH,
            "titles": "|".join(t for _, t in batch), "redirects": 1,
            "format": "json", "formatversion": 2, "maxlag": 5,
        })
        mapping, redirects, resolved = resolve_titles(data)
        append(out, [
            page_record(i, "person", t,
                        lookup(t, mapping, redirects, resolved), "intro")
            for i, t in batch
        ])
        print(f"  {min(start + INTRO_BATCH, len(todo))}/{len(todo)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contact", required=True,
                        help="e-mail put in the User-Agent (API policy)")
    parser.add_argument("--delay", type=float, default=0.25,
                        help="minimum seconds between Wikipedia requests")
    parser.add_argument("--limit", type=int, default=0,
                        help="max number of films/people (0 = all), "
                             "for quick tests")
    parser.add_argument("--step", default="all",
                        choices=["all", "wikidata", "films", "people"])
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    film_ids = read_ids(ENRICH_DIR / "targets" / "films")
    people_ids = read_ids(ENRICH_DIR / "targets" / "people")
    if not film_ids:
        sys.exit("no targets: run jobs/04a_enrich_targets.py first")
    if args.limit:
        film_ids, people_ids = (film_ids[:args.limit],
                                people_ids[:args.limit])

    wd_client = Client(args.contact, delay=1.0)  # SPARQL: 1 query / second
    wp_client = Client(args.contact, delay=args.delay)
    wd_films = RAW_DIR / "wikidata_films.jsonl"
    wd_people = RAW_DIR / "wikidata_people.jsonl"

    if args.step in ("all", "wikidata"):
        collect_wikidata(wd_client, film_ids, "film", wd_films)
        collect_wikidata(wd_client, people_ids, "person", wd_people)
    if args.step in ("all", "films"):
        collect_films(wp_client, wd_films,
                      RAW_DIR / "wikipedia_films.jsonl", args.limit)
    if args.step in ("all", "people"):
        collect_people(wp_client, wd_people,
                       RAW_DIR / "wikipedia_people.jsonl", args.limit)
    print("done ->", RAW_DIR)


if __name__ == "__main__":
    main()
