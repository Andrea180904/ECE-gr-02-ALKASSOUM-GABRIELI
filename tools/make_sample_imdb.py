"""Generate a small synthetic IMDb-like dataset (same files and schemas).

Useful to smoke-test the whole pipeline in a few minutes before running it
on the real (multi-GB) dataset. It deliberately contains data-quality
problems (orphans, impossible dates, duplicates, empty strings...).

    ./run.sh py tools/make_sample_imdb.py --out data/raw/imdb_sample
    IMDB_RAW_DIR=... is NOT needed: pass --out data/raw/imdb to replace
    the real files (do not do that once the real data is downloaded).
"""

import argparse
import gzip
import random
from pathlib import Path

GENRES = ["Drama", "Comedy", "Action", "Thriller", "Romance", "Horror",
          "Crime", "Documentary", "Adventure", "Sci-Fi", "Animation",
          "Family", "Mystery", "Fantasy", "Biography"]
COUNTRIES = [("US", "en", 0.55), ("GB", "en", 0.08), ("FR", "fr", 0.1),
             ("JP", "ja", 0.06), ("IN", "hi", 0.06), ("KR", "ko", 0.05),
             ("ES", "es", 0.04), ("DE", "de", 0.03), ("IT", "it", 0.03)]
WORDS = ["Night", "City", "Love", "Shadow", "River", "Last", "Dream",
         "Storm", "Silent", "Golden", "Lost", "Heart", "Fire", "Winter",
         "Secret", "Road", "Star", "Blue", "Dark", "Home", "War", "Sun"]
FIRST = ["Anna", "Paul", "Marie", "John", "Yuki", "Raj", "Min", "Lucia",
         "Hans", "Sofia", "Pierre", "Emma", "Kenji", "Priya", "Carlos",
         "Laura", "David", "Chloe", "Omar", "Ines"]
LAST = ["Martin", "Smith", "Tanaka", "Kumar", "Kim", "Garcia", "Muller",
        "Rossi", "Dubois", "Brown", "Lee", "Lopez", "Bernard", "Sato"]
N = "\\N"


def write(path: Path, header, rows) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        fh.write("\t".join(header) + "\n")
        for row in rows:
            fh.write("\t".join(N if v is None else str(v) for v in row) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/raw/imdb")
    parser.add_argument("--movies", type=int, default=4000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    rnd = random.Random(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # ---------------- people ----------------
    people = []
    for i in range(1, 1501):
        birth = rnd.randint(1925, 2000)
        death = birth + rnd.randint(50, 95) if rnd.random() < 0.15 else None
        if death and death > 2026:
            death = None
        name = f"{rnd.choice(FIRST)} {rnd.choice(LAST)} {i}"
        people.append([f"nm{i:07d}", name, birth, death, "actor", None])
    # data-quality traps
    people[10][3] = people[10][2] - 30          # died before birth
    people[11][3] = people[11][2] - 1
    people[12][2] = 2031                        # born in the future
    people[13][1] = ""                          # empty name
    actors = [p[0] for p in people[:900]]
    directors = [p[0] for p in people[900:1150]]
    writers = [p[0] for p in people[1150:1400]]
    for p in people[900:1150]:
        p[4] = "director,writer"
    for p in people[1150:1400]:
        p[4] = "writer"

    # ---------------- titles ----------------
    basics, ratings, principals, crew, akas = [], [], [], [], []
    # fixed director-actor duos (for the open question)
    duos = [(directors[i], actors[i]) for i in range(20)]
    for i in range(1, args.movies + 1):
        tconst = f"tt{i:07d}"
        year = rnd.randint(1972, 2026)
        runtime = max(41, int(rnd.gauss(105, 18)))
        if rnd.random() < 0.01:
            runtime = rnd.randint(240, 600)     # very long outliers
        if rnd.random() < 0.003:
            runtime = 0                          # invalid runtime
        ttype = "movie" if rnd.random() < 0.9 else rnd.choice(
            ["short", "tvMovie"])
        if ttype == "short":
            runtime = rnd.randint(5, 30)
        genres = rnd.sample(GENRES, rnd.randint(1, 3))
        if genres[0] == "Animation":
            runtime = max(41, runtime - 15)
        title = " ".join(rnd.sample(WORDS, rnd.randint(1, 3)))
        r = rnd.random()
        acc = 0
        for country, lang, w in COUNTRIES:
            acc += w
            if r <= acc:
                break
        original = title if lang == "en" else f"{title} ({lang})"
        basics.append([tconst, ttype, title, original, 0, year, None,
                       runtime if runtime else 0,
                       ",".join(genres) if rnd.random() > 0.01 else None])
        # ratings
        if rnd.random() < 0.92 and year <= 2026:
            rating = round(min(10, max(1, rnd.gauss(6.3, 1.1))), 1)
            votes = int(rnd.lognormvariate(8, 1.8)) + 5
            ratings.append([tconst, rating, votes])
        # principals: 4 cast, 1 director, 1 writer
        if i <= len(duos) * 5:
            director, lead = duos[(i - 1) % len(duos)]
        else:
            director, lead = rnd.choice(directors), rnd.choice(actors)
        cast = [lead] + rnd.sample(actors, 3)
        for o, a in enumerate(cast, start=1):
            cat = "actress" if int(a[2:]) % 2 else "actor"
            principals.append([tconst, o, a, cat, None, '["Role"]'])
        principals.append([tconst, 5, director, "director", None, None])
        writer = rnd.choice(writers + [director] * 60)
        principals.append([tconst, 6, writer, "writer", "screenplay", None])
        if rnd.random() < 0.05:                  # director also acts
            principals.append([tconst, 7, director, "actor", None, '["Self"]'])
        crew.append([tconst, director, writer])
        # akas
        akas.append([tconst, 1, original, None, None, "original", None, 1])
        akas.append([tconst, 2, original, country,
                     lang if rnd.random() < 0.4 else None, None, None, 0])
        for k, (c, l, _) in enumerate(rnd.sample(COUNTRIES, 2), start=3):
            akas.append([tconst, k, f"{title} [{c}]", c, None,
                         "imdbDisplay", None, 0])

    # planted case for the "one hit director" question
    hit_dir = directors[-1]
    for j, rating in enumerate([8.6, 5.2, 5.8, 5.5, 6.0]):
        tconst = f"tt8{j:06d}"
        basics.append([tconst, "movie", f"Planted {j}", f"Planted {j}", 0,
                       1995 + j, None, 100, "Drama"])
        ratings.append([tconst, rating, 20000])
        principals.append([tconst, 1, rnd.choice(actors), "actor", None, None])
        principals.append([tconst, 2, hit_dir, "director", None, None])
        crew.append([tconst, hit_dir, None])

    # ---------------- TV series + episodes ----------------
    episodes = []
    ep_id = 9_000_000
    for s in range(1, 31):
        series = f"tt{8_500_000 + s}"
        basics.append([series, "tvSeries", f"Series {s}", f"Series {s}", 0,
                       2000 + s % 20, 2010 + s % 15 if s % 3 else None,
                       45, "Drama,Crime"])
        ratings.append([series, round(rnd.uniform(6, 9), 1),
                        rnd.randint(1000, 500000)])
        n_seasons = rnd.randint(2, 6)
        for season in range(1, n_seasons + 1):
            for ep in range(1, rnd.randint(8, 14) + 1):
                ep_id += 1
                et = f"tt{ep_id}"
                basics.append([et, "tvEpisode", f"Episode {ep}",
                               f"Episode {ep}", 0, 2000 + s % 20 + season,
                               None, 45, "Drama"])
                rating = round(rnd.uniform(6.5, 8.5), 1)
                # planted pattern: best early, worst in last season
                if s <= 3 and season == 1 and ep == 2:
                    rating = 9.8
                if s <= 3 and season == n_seasons and ep == 3:
                    rating = 3.1
                ratings.append([et, rating, rnd.randint(50, 5000)])
                episodes.append([et, series, season, ep])
    for s in range(1, 4):  # make sure planted series have >=3 long seasons
        series = f"tt{8_500_000 + s}"
        for season in range(1, 5):
            for ep in range(1, 13):
                ep_id += 1
                et = f"tt{ep_id}"
                basics.append([et, "tvEpisode", f"Extra {ep}", f"Extra {ep}",
                               0, 2005 + season, None, 45, "Drama"])
                rating = round(rnd.uniform(6.5, 8.5), 1)
                if season == 4 and ep == 5:
                    rating = 2.0
                ratings.append([et, rating, rnd.randint(50, 5000)])
                episodes.append([et, series, season + 10, ep])

    # ---------------- more data-quality traps ----------------
    basics.append(basics[5])                         # duplicate tconst
    basics.append(["tt7777777", "tvSeries", "Bad Years", "Bad Years", 0,
                   2015, 2010, 40, "Drama"])         # end before start
    ratings.append(["tt9999999", 7.0, 100])          # orphan rating
    ratings.append(["tt0000003", 11.5, 10])          # out-of-range rating
    principals.append(["tt0000001", 9, "nm9999999", "actor", None, None])
    episodes.append(["tt9999998", "tt0000000", 1, 1])  # orphan episode
    crew.append(["tt0000002", "nm8888888", ""])      # unknown director
    people[20][5] = "tt0000001,tt9999990"            # unknown knownFor

    write(out / "name.basics.tsv.gz",
          ["nconst", "primaryName", "birthYear", "deathYear",
           "primaryProfession", "knownForTitles"], people)
    write(out / "title.basics.tsv.gz",
          ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult",
           "startYear", "endYear", "runtimeMinutes", "genres"], basics)
    write(out / "title.ratings.tsv.gz",
          ["tconst", "averageRating", "numVotes"], ratings)
    write(out / "title.principals.tsv.gz",
          ["tconst", "ordering", "nconst", "category", "job", "characters"],
          principals)
    write(out / "title.crew.tsv.gz", ["tconst", "directors", "writers"], crew)
    write(out / "title.episode.tsv.gz",
          ["tconst", "parentTconst", "seasonNumber", "episodeNumber"],
          episodes)
    write(out / "title.akas.tsv.gz",
          ["titleId", "ordering", "title", "region", "language", "types",
           "attributes", "isOriginalTitle"], akas)
    print("sample dataset written to", out)


if __name__ == "__main__":
    main()
