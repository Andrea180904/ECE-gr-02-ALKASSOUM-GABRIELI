# Report – IMDb & Wikipedia with Spark and Kafka

<!--
HOW TO USE THIS FILE
- Result blocks (between the RESULTS markers) are filled automatically by
  `python3 tools/assemble_report.py` from results/*.md. Do not edit them by hand.
- Every "TODO (team)" must be written by the team. AI tools are NOT allowed for
  explanations or reasoning in this file (course rule).
- Delete these comments before submitting.
-->

## Contents

1. [Data preparation](#1-data-preparation)
2. [Data exploration](#2-data-exploration)
3. [Data enrichment](#3-data-enrichment)
4. [Streaming data processing](#4-streaming-data-processing)
5. [Open question](#5-open-question)

## 1. Data preparation

### Ingested data

<!-- RESULTS:00_ingestion -->
**Ingested IMDb tables**

| table | source_file | rows | columns |
| --- | --- | --- | --- |
| title_principals | title.principals.tsv.gz | 102,756,358 | 6 |
| title_akas | title.akas.tsv.gz | 60,424,702 | 8 |
| name_basics | name.basics.tsv.gz | 15,816,005 | 6 |
| title_basics | title.basics.tsv.gz | 12,963,111 | 9 |
| title_crew | title.crew.tsv.gz | 12,963,111 | 3 |
| title_episode | title.episode.tsv.gz | 10,034,862 | 4 |
| title_ratings | title.ratings.tsv.gz | 1,732,169 | 3 |

_7 rows._
<!-- /RESULTS:00_ingestion -->

### Missing values per column

<!-- RESULTS:01_missing_values -->
**Missing values (NULL, '\N' or empty string) per column**

| table | column | total_rows | missing_rows | missing_pct |
| --- | --- | --- | --- | --- |
| name_basics | deathYear | 15,816,005 | 15,550,475 | 98.321 |
| name_basics | birthYear | 15,816,005 | 15,130,128 | 95.663 |
| name_basics | primaryProfession | 15,816,005 | 3,220,639 | 20.363 |
| name_basics | knownForTitles | 15,816,005 | 1,917,578 | 12.124 |
| name_basics | primaryName | 15,816,005 | 293 | 0.002 |
| name_basics | nconst | 15,816,005 | 0 | 0 |
| title_akas | attributes | 60,424,702 | 60,107,437 | 99.475 |
| title_akas | types | 60,424,702 | 40,393,110 | 66.849 |
| title_akas | language | 60,424,702 | 19,665,115 | 32.545 |
| title_akas | region | 60,424,702 | 13,002,884 | 21.519 |
| title_akas | isOriginalTitle | 60,424,702 | 0 | 0 |
| title_akas | titleId | 60,424,702 | 0 | 0 |
| title_akas | ordering | 60,424,702 | 0 | 0 |
| title_akas | title | 60,424,702 | 0 | 0 |
| title_basics | endYear | 12,963,111 | 12,798,642 | 98.731 |
| title_basics | runtimeMinutes | 12,963,111 | 8,050,574 | 62.104 |
| title_basics | startYear | 12,963,111 | 1,487,320 | 11.473 |
| title_basics | genres | 12,963,111 | 548,144 | 4.229 |
| title_basics | tconst | 12,963,111 | 0 | 0 |
| title_basics | titleType | 12,963,111 | 0 | 0 |
| title_basics | primaryTitle | 12,963,111 | 0 | 0 |
| title_basics | originalTitle | 12,963,111 | 0 | 0 |
| title_basics | isAdult | 12,963,111 | 0 | 0 |
| title_crew | writers | 12,963,111 | 6,434,239 | 49.635 |
| title_crew | directors | 12,963,111 | 5,793,144 | 44.69 |
| title_crew | tconst | 12,963,111 | 0 | 0 |
| title_episode | seasonNumber | 10,034,862 | 2,126,237 | 21.189 |
| title_episode | episodeNumber | 10,034,862 | 2,126,237 | 21.189 |
| title_episode | tconst | 10,034,862 | 0 | 0 |
| title_episode | parentTconst | 10,034,862 | 0 | 0 |
| title_principals | job | 102,756,358 | 82,882,023 | 80.659 |
| title_principals | characters | 102,756,358 | 52,691,057 | 51.278 |
| title_principals | nconst | 102,756,358 | 0 | 0 |
| title_principals | category | 102,756,358 | 0 | 0 |
| title_principals | tconst | 102,756,358 | 0 | 0 |
| title_principals | ordering | 102,756,358 | 0 | 0 |
| title_ratings | tconst | 1,732,169 | 0 | 0 |
| title_ratings | averageRating | 1,732,169 | 0 | 0 |
| title_ratings | numVotes | 1,732,169 | 0 | 0 |

_39 rows._
<!-- /RESULTS:01_missing_values -->

### Five columns with the highest share of missing values

<!-- RESULTS:01_top5_missing_columns -->
**Top 5 columns with the highest share of missing values**

| table | column | total_rows | missing_rows | missing_pct |
| --- | --- | --- | --- | --- |
| title_akas | attributes | 60,424,702 | 60,107,437 | 99.475 |
| title_basics | endYear | 12,963,111 | 12,798,642 | 98.731 |
| name_basics | deathYear | 15,816,005 | 15,550,475 | 98.321 |
| name_basics | birthYear | 15,816,005 | 15,130,128 | 95.663 |
| title_principals | job | 102,756,358 | 82,882,023 | 80.659 |

_5 rows._
<!-- /RESULTS:01_top5_missing_columns -->

### Other data-quality issues and how they are handled

<!-- RESULTS:01_quality_issues -->
**Data-quality issues detected and how they were handled**

| table | check | description | affected_rows | affected_pct | action |
| --- | --- | --- | --- | --- | --- |
| name_basics | alive_over_max_age | no deathYear but older than 122 | 2537 | 0.016 | kept, flagged (death probably not recorded) |
| name_basics | missing_name | no primaryName | 293 | 0.002 | kept, flagged |
| name_basics | unknown_known_for_title | knownForTitles id missing from title_basics (% of knownFor references) | 68 | 0 | kept (column not used in the analyses) |
| name_basics | death_before_birth | deathYear < birthYear | 28 | 0 | birth and death years set to NULL |
| title_akas | orphan_titleId | alternative title for a missing title | 8 | 0 | row quarantined |
| title_basics | missing_genres | no genre | 548,144 | 4.229 | empty genre array |
| title_basics | start_in_future | startYear > 2026 (announced titles) | 6238 | 0.048 | kept, flagged; excluded by year filters |
| title_basics | runtime_over_1000_min | runtime > 1000 minutes (experimental) | 442 | 0.003 | kept, flagged |
| title_basics | non_positive_runtime | runtime <= 0 minutes | 4 | 0 | runtime set to NULL |
| title_crew | unknown_writers_id | person id missing from name_basics (% of references) | 1159 | 0.008 | reference quarantined |
| title_crew | unknown_directors_id | person id missing from name_basics (% of references) | 430 | 0.004 | reference quarantined |
| title_episode | non_positive_episode | non positive episode | 7130 | 0.071 | episodeNumber set to NULL |
| title_episode | orphan_parent_tconst | parent series missing from title_basics | 5 | 0 | row quarantined |
| title_principals | credited_10y_after_death | non-archive credit > 10 years after death | 164,868 | 0.16 | kept, flagged |
| title_principals | orphan_nconst | credit for a person missing from name_basics | 7262 | 0.007 | row quarantined |
| title_principals | credited_before_birth | title released before the person was born | 3109 | 0.003 | kept, flagged |

_16 rows._
<!-- /RESULTS:01_quality_issues -->

<!-- RESULTS:01_cleaning_summary -->
**Row counts before and after cleaning**

| table | bronze_rows | silver_rows | quarantined_rows | flagged_rows |
| --- | --- | --- | --- | --- |
| name_basics | 15,816,005 | 15,816,005 | 0 | 2858 |
| title_akas | 60,424,702 | 60,424,694 | 8 | 0 |
| title_basics | 12,963,111 | 12,963,111 | 0 | 554,584 |
| title_crew | 12,963,111 | 12,963,111 | 0 | 0 |
| title_episode | 10,034,862 | 10,034,857 | 5 | 7130 |
| title_principals | 102,756,358 | 102,749,096 | 7262 | 167,977 |
| title_ratings | 1,732,169 | 1,732,169 | 0 | 0 |

_7 rows._
<!-- /RESULTS:01_cleaning_summary -->

### Methodological explanation

- **Chosen method:** TODO (team)
- **Why:** TODO (team)
- **Alternatives considered and trade-offs:** TODO (team)

## 2. Data exploration

Common definitions: TODO (team) – explain what a feature-length movie is and
how popularity and success are measured.

### 2.1 Runtime notably different from the genre average

<!-- RESULTS:02_q1_runtime_outliers_by_genre -->
**Q1 - Feature films whose runtime is notably different from their genre (robust z-score: |0.6745 (x - median) / MAD| > 3.5; short_below / long_above = runtime limits in minutes)**

| genre | n_movies | avg_runtime | std_runtime | median_runtime | mad_runtime | short_below | long_above | n_shorter | n_longer | pct_outliers |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Game-Show | 7 | 79.6 | 35.4 | 67 | 15 | -11 | 145 | 0 | 1 | 14.29 |
| Action | 40,409 | 101.2 | 33.8 | 94 | 12 | 32 | 156 | 0 | 1787 | 4.42 |
| Adventure | 24,027 | 93 | 30.6 | 90 | 11 | 33 | 147 | 0 | 805 | 3.35 |
| Thriller | 34,075 | 96.1 | 21.8 | 93 | 10 | 41 | 145 | 2 | 1042 | 3.06 |
| Reality-TV | 297 | 92.1 | 85.2 | 90 | 15 | 12 | 168 | 0 | 9 | 3.03 |
| Comedy | 95,799 | 93.3 | 28.7 | 91 | 10 | 39 | 143 | 0 | 2710 | 2.83 |
| Musical | 8503 | 100.1 | 33.3 | 94 | 14 | 21 | 167 | 0 | 240 | 2.82 |
| Crime | 34,429 | 95.7 | 25.6 | 93 | 11 | 36 | 150 | 0 | 919 | 2.67 |
| War | 8049 | 94.8 | 31.3 | 92 | 13 | 25 | 159 | 0 | 206 | 2.56 |
| Sci-Fi | 9779 | 91.2 | 30.5 | 90 | 10 | 38 | 142 | 0 | 241 | 2.46 |
| Fantasy | 12,601 | 94.1 | 31.9 | 91 | 12 | 29 | 153 | 0 | 294 | 2.33 |
| Romance | 42,228 | 99.2 | 25.1 | 96 | 12 | 34 | 158 | 0 | 908 | 2.15 |
| Horror | 30,573 | 88.7 | 22.3 | 88 | 9 | 41 | 135 | 4 | 647 | 2.13 |
| Western | 6354 | 76.5 | 28.7 | 70 | 15 | -8 | 148 | 0 | 117 | 1.84 |
| Family | 15,971 | 92.3 | 31.4 | 90 | 13 | 23 | 157 | 0 | 281 | 1.76 |
| Animation | 8336 | 82.7 | 26.8 | 81 | 12 | 19 | 143 | 0 | 144 | 1.73 |
| Drama | 196,230 | 96.1 | 37 | 93 | 13 | 26 | 160 | 0 | 3350 | 1.71 |
| News | 1409 | 77.6 | 32.2 | 75 | 15 | -3 | 153 | 0 | 22 | 1.56 |
| History | 13,637 | 94.3 | 52.9 | 90 | 18 | -3 | 183 | 0 | 209 | 1.53 |
| Sport | 6819 | 88.9 | 37.4 | 90 | 14 | 17 | 163 | 0 | 100 | 1.47 |
| Music | 13,982 | 88.4 | 57.7 | 87 | 16 | 4 | 170 | 0 | 200 | 1.43 |
| Mystery | 16,436 | 94.4 | 50.4 | 92 | 12 | 30 | 154 | 0 | 231 | 1.41 |
| Documentary | 116,260 | 78 | 223.4 | 74 | 16 | -9 | 157 | 0 | 1318 | 1.13 |
| Biography | 16,111 | 89.2 | 29.4 | 88 | 17 | 0 | 176 | 0 | 173 | 1.07 |
| Talk-Show | 102 | 88.1 | 33.7 | 81 | 21 | -28 | 190 | 0 | 1 | 0.98 |
| Film-Noir | 878 | 81.9 | 13.8 | 81 | 10 | 29 | 133 | 0 | 2 | 0.23 |

_26 rows._
<!-- /RESULTS:02_q1_runtime_outliers_by_genre -->

<!-- RESULTS:02_q1_runtime_outliers_examples -->
**Q1 - Most extreme runtime outliers (top 2 longer and top 2 shorter per genre)**

| genre | side | primaryTitle | year | runtimeMinutes | genre_median | robust_z |
| --- | --- | --- | --- | --- | --- | --- |
| Action | longer | h36: | 2017 | 2160 | 94 | 116.13 |
| Action | longer | Burning of the Red Lotus Temple | 1928 | 1620 | 94 | 85.77 |
| Adventure | longer | Ooio Aifujairea: Lifedesign | 2025 | 1440 | 90 | 82.78 |
| Adventure | longer | The Longest Movie on Earth!?: The Narwallus Movie! | 2016 | 600 | 90 | 31.27 |
| Animation | longer | Tom and Jerry: The Golden Era Anthology | 1958 | 817 | 81 | 41.37 |
| Animation | longer | Disappeared Quipu | 2018 | 490 | 81 | 22.99 |
| Biography | longer | The Steps: A Memoir | 2026 | 542 | 88 | 18.01 |
| Biography | longer | O.J.: Made in America | 2016 | 467 | 88 | 15.04 |
| Comedy | longer | London EC1 | 2015 | 5460 | 91 | 362.14 |
| Comedy | longer | Azgrab: The Documentary | 2011 | 1669 | 91 | 106.44 |
| Crime | longer | His Last Stand |  | 990 | 93 | 55 |
| Crime | longer | Lemons | 2026 | 751 | 93 | 40.35 |
| Documentary | longer | Logistics | 2012 | 51,420 | 74 | 2,164.55 |
| Documentary | longer | Ambiancé | 2020 | 43,200 | 74 | 1,818.03 |
| Drama | longer | Hunger! | 2015 | 6000 | 93 | 306.48 |
| Drama | longer | Matrjoschka | 2006 | 5700 | 93 | 290.92 |
| Family | longer | World Peace & Prayer Day | 2011 | 2400 | 90 | 119.85 |
| Family | longer | De filosoof van Haeghem | 1967 | 385 | 90 | 15.31 |
| Fantasy | longer | Burning of the Red Lotus Temple | 1928 | 1620 | 91 | 85.94 |
| Fantasy | longer | Funusion | 2016 | 1440 | 91 | 75.83 |
| Film-Noir | longer | Not as a Stranger | 1955 | 135 | 81 | 3.64 |
| Film-Noir | longer | Hangmen Also Die! | 1943 | 134 | 81 | 3.57 |
| Game-Show | longer | The Netflix Slam | 2024 | 150 | 67 | 3.73 |
| History | longer | Saynatakuna: Masks | 2013 | 2905 | 90 | 105.48 |
| History | longer | The Longest Most Meaningless Movie in the World | 1970 | 2880 | 90 | 104.55 |
| Horror | longer | 24 Hour Psycho | 1993 | 1440 | 88 | 101.32 |
| Horror | longer | Amy's Night Out | 2007 | 675 | 88 | 43.99 |
| Horror | shorter | Melchiad Koloman | 1920 | 41 | 88 | -3.52 |
| Horror | shorter | Dark Mansion | 2019 | 41 | 88 | -3.52 |
| Music | longer | The Cure for Insomnia | 1987 | 5220 | 87 | 216.39 |
| Music | longer | Rock Milestones: Metallica - The Halcyon Years | 2007 | 2007 | 87 | 80.94 |
| Musical | longer | Sigur rós: Route One | 2016 | 1440 | 94 | 64.85 |
| Musical | longer | La Flor | 2018 | 808 | 94 | 34.4 |
| Mystery | longer | London EC1 | 2015 | 5460 | 92 | 301.73 |
| Mystery | longer | How Does David Lynch Do It? | 2010 | 1800 | 92 | 96 |
| News | longer | Australia Votes 2022 | 2022 | 387 | 75 | 14.03 |
| News | longer | Australia Votes 2019 | 2019 | 387 | 75 | 14.03 |
| Reality-TV | longer | Sky TV | 1966 | 1440 | 90 | 60.71 |
| Reality-TV | longer | Craziest Vacation Ever | 2020 | 317 | 90 | 10.21 |
| Romance | longer | The Man of Her Dreams | 2023 | 750 | 96 | 36.76 |
| Romance | longer | The Longest Movie on Earth!?: The Narwallus Movie! | 2016 | 600 | 96 | 28.33 |
| Sci-Fi | longer | Funusion | 2016 | 1440 | 90 | 91.06 |
| Sci-Fi | longer | Split | 2004 | 700 | 90 | 41.14 |
| Sport | longer | Azgrab: The Documentary | 2011 | 1669 | 90 | 76.07 |
| Sport | longer | NHL: Washington Capitals 10 Greatest Games | 2010 | 1200 | 90 | 53.48 |
| Talk-Show | longer | The Conciousnous Paradigm | 2015 | 190 | 81 | 3.5 |
| Thriller | longer | Final Call | 2018 | 619 | 93 | 35.48 |
| Thriller | longer | Barrabas | 1919 | 489 | 93 | 26.71 |
| Thriller | shorter | Dasher | 2026 | 41 | 93 | -3.51 |
| Thriller | shorter | Redwood Diaries | 2026 | 41 | 93 | -3.51 |
| War | longer | The Journey | 1987 | 873 | 92 | 40.52 |
| War | longer | Europa: The Last Battle | 2017 | 746 | 92 | 33.93 |
| Western | longer | The Mystery Rider | 1928 | 380 | 70 | 13.94 |
| Western | longer | The Moon Riders | 1920 | 360 | 70 | 13.04 |

_54 rows._
<!-- /RESULTS:02_q1_runtime_outliers_examples -->

### 2.2 Top three genres per year (1980–2025)

<!-- RESULTS:02_q2_top3_genres -->
**Q2 - Top 3 genres per year, 1980-2025 (by total votes)**

| year | top_1 | top_2 | top_3 |
| --- | --- | --- | --- |
| 1980 | Drama | Adventure | Horror |
| 1981 | Adventure | Drama | Horror |
| 1982 | Drama | Sci-Fi | Action |
| 1983 | Drama | Action | Adventure |
| 1984 | Action | Drama | Adventure |
| 1985 | Comedy | Adventure | Drama |
| 1986 | Drama | Adventure | Comedy |
| 1987 | Drama | Comedy | Action |
| 1988 | Drama | Comedy | Action |
| 1989 | Comedy | Drama | Adventure |
| 1990 | Drama | Comedy | Crime |
| 1991 | Drama | Crime | Comedy |
| 1992 | Drama | Comedy | Crime |
| 1993 | Drama | Comedy | Crime |
| 1994 | Drama | Crime | Comedy |
| 1995 | Drama | Crime | Comedy |
| 1996 | Drama | Comedy | Crime |
| 1997 | Drama | Action | Comedy |
| 1998 | Drama | Comedy | Crime |
| 1999 | Drama | Comedy | Crime |
| 2000 | Drama | Comedy | Adventure |
| 2001 | Drama | Comedy | Adventure |
| 2002 | Drama | Adventure | Action |
| 2003 | Drama | Action | Comedy |
| 2004 | Drama | Action | Comedy |
| 2005 | Drama | Comedy | Action |
| 2006 | Drama | Comedy | Thriller |
| 2007 | Drama | Comedy | Action |
| 2008 | Drama | Action | Comedy |
| 2009 | Drama | Comedy | Action |
| 2010 | Drama | Action | Adventure |
| 2011 | Drama | Action | Comedy |
| 2012 | Drama | Action | Comedy |
| 2013 | Drama | Action | Comedy |
| 2014 | Drama | Action | Adventure |
| 2015 | Drama | Action | Adventure |
| 2016 | Drama | Action | Adventure |
| 2017 | Drama | Action | Comedy |
| 2018 | Drama | Action | Adventure |
| 2019 | Drama | Action | Comedy |
| 2020 | Drama | Comedy | Action |
| 2021 | Drama | Action | Adventure |
| 2022 | Drama | Action | Comedy |
| 2023 | Drama | Action | Comedy |
| 2024 | Drama | Action | Comedy |
| 2025 | Drama | Action | Comedy |

_46 rows._
<!-- /RESULTS:02_q2_top3_genres -->

<!-- RESULTS:02_q2_top3_frequency -->
**Q2 - How often each genre reached the yearly top 3**

| genre | years_in_top3 | years_ranked_first |
| --- | --- | --- |
| Drama | 46 | 42 |
| Comedy | 34 | 2 |
| Action | 29 | 1 |
| Adventure | 16 | 1 |
| Crime | 9 | 0 |
| Horror | 2 | 0 |
| Sci-Fi | 1 | 0 |
| Thriller | 1 | 0 |

_8 rows._
<!-- /RESULTS:02_q2_top3_frequency -->

### 2.3 Top three genres compared with the other genres

<!-- RESULTS:02_q3_top3_vs_rest_overall -->
**Q3 - Summary over all years**

| avg_top3_pct_movies | avg_top3_pct_votes | avg_top3_rating | avg_rest_rating | votes_per_movie_ratio | years_top3_rated_higher | years |
| --- | --- | --- | --- | --- | --- | --- |
| 44.2 | 44.7 | 6.04 | 6.09 | 1.07 | 17 | 46 |

_1 rows._
<!-- /RESULTS:02_q3_top3_vs_rest_overall -->

<!-- RESULTS:02_q3_top3_vs_rest -->
**Q3 - Top 3 genres vs the remaining genres, per year (a movie counts once per genre)**

| year | rest_genres | top3_pct_movies | top3_pct_votes | top3_avg_rating | rest_avg_rating | top3_votes_per_movie | rest_votes_per_movie | top3_avg_runtime | rest_avg_runtime |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1980 | 20 | 39.1 | 44.4 | 6.03 | 6 | 5,861 | 4,707 | 101.1 | 100.1 |
| 1981 | 18 | 38.4 | 46.6 | 6.11 | 6.1 | 3,985 | 2,854 | 100.1 | 98.5 |
| 1982 | 18 | 40.9 | 43.8 | 6.06 | 5.94 | 5,142 | 4,578 | 102.3 | 97.5 |
| 1983 | 18 | 43.4 | 45.9 | 6.04 | 5.97 | 4,435 | 4,010 | 102.2 | 96.6 |
| 1984 | 18 | 42.1 | 44.2 | 6.08 | 6.02 | 6,300 | 5,780 | 102.1 | 95.6 |
| 1985 | 18 | 49.4 | 51.6 | 6.05 | 5.96 | 6,192 | 5,681 | 100.6 | 99.5 |
| 1986 | 18 | 49.2 | 46.2 | 5.98 | 5.96 | 5,118 | 5,758 | 98.3 | 96.8 |
| 1987 | 18 | 54.7 | 42.6 | 5.89 | 5.95 | 4,483 | 7,291 | 100.4 | 102 |
| 1988 | 18 | 52.6 | 45.1 | 5.88 | 5.92 | 5,014 | 6,777 | 100.5 | 96.1 |
| 1989 | 18 | 46.6 | 49.5 | 6.04 | 5.83 | 5,898 | 5,259 | 101.3 | 99.2 |
| 1990 | 18 | 51.5 | 45.6 | 5.95 | 5.8 | 6,525 | 8,277 | 101.3 | 98.5 |
| 1991 | 18 | 50.7 | 44.2 | 6.05 | 5.93 | 6,018 | 7,833 | 102.6 | 98.6 |
| 1992 | 18 | 50.9 | 46.3 | 6.03 | 6.03 | 6,968 | 8,369 | 102.3 | 99 |
| 1993 | 18 | 50 | 44.8 | 6 | 6.02 | 8,538 | 10,530 | 103.1 | 100 |
| 1994 | 18 | 51.1 | 58.3 | 6 | 5.92 | 13,572 | 10,153 | 101.7 | 99.4 |
| 1995 | 18 | 48.7 | 49 | 6.04 | 5.95 | 13,032 | 12,897 | 102.4 | 98.2 |
| 1996 | 19 | 49.5 | 44.9 | 6.09 | 6.05 | 8,293 | 9,958 | 102 | 98.9 |
| 1997 | 18 | 52.4 | 44.1 | 6 | 6.11 | 10,780 | 15,051 | 102.6 | 95.9 |
| 1998 | 18 | 50.9 | 51.5 | 6.04 | 5.95 | 11,941 | 11,691 | 102.1 | 96.9 |
| 1999 | 18 | 51.3 | 43.6 | 6 | 5.96 | 12,281 | 16,737 | 101.9 | 98.6 |
| 2000 | 18 | 46.1 | 45 | 6 | 5.99 | 11,650 | 12,171 | 100.6 | 97.7 |
| 2001 | 18 | 46.7 | 42.4 | 6.06 | 6 | 12,487 | 14,906 | 101 | 98 |
| 2002 | 21 | 37.9 | 42.4 | 6.08 | 6.13 | 15,330 | 12,707 | 103.6 | 96.5 |
| 2003 | 20 | 50.7 | 45.5 | 6.01 | 6.14 | 11,495 | 14,148 | 102.7 | 94.9 |
| 2004 | 20 | 48.9 | 41.3 | 6.03 | 6.23 | 11,913 | 16,198 | 102.4 | 98.2 |
| 2005 | 20 | 48.1 | 40.4 | 6.04 | 6.22 | 9,306 | 12,744 | 101 | 94.4 |
| 2006 | 19 | 47.2 | 42.7 | 6 | 6.18 | 10,568 | 12,656 | 101.8 | 95.5 |
| 2007 | 20 | 45.1 | 38.3 | 6.03 | 6.25 | 9,944 | 13,126 | 100.4 | 93.6 |
| 2008 | 19 | 45.4 | 40.4 | 5.95 | 6.24 | 9,483 | 11,629 | 100.5 | 92.8 |
| 2009 | 19 | 44.8 | 41.3 | 6.02 | 6.27 | 8,572 | 9,886 | 98.4 | 91.2 |
| 2010 | 20 | 33 | 40 | 6.13 | 6.25 | 10,759 | 7,957 | 98.7 | 91 |
| 2011 | 20 | 42.8 | 46.1 | 6.06 | 6.35 | 9,055 | 7,911 | 97.4 | 91.5 |
| 2012 | 20 | 41.8 | 42.7 | 6.11 | 6.33 | 7,964 | 7,658 | 97.1 | 95.9 |
| 2013 | 20 | 41.1 | 41.5 | 6.09 | 6.3 | 8,320 | 8,175 | 97.2 | 89.7 |
| 2014 | 22 | 30.8 | 47 | 6.17 | 6.28 | 12,007 | 6,022 | 98.2 | 91.5 |
| 2015 | 20 | 31.8 | 46.6 | 6.1 | 6.2 | 9,273 | 4,948 | 98.8 | 92.6 |
| 2016 | 20 | 32.1 | 44.1 | 6.1 | 6.15 | 9,163 | 5,492 | 98.4 | 92.5 |
| 2017 | 21 | 41.7 | 45 | 6.01 | 6.17 | 6,695 | 5,854 | 99.6 | 92.3 |
| 2018 | 21 | 32.5 | 44.3 | 6.03 | 6.04 | 8,051 | 4,876 | 100.9 | 93.5 |
| 2019 | 21 | 41.9 | 43.6 | 5.95 | 6.12 | 6,279 | 5,845 | 101.1 | 95.5 |
| 2020 | 21 | 39 | 44.7 | 5.86 | 6.04 | 4,249 | 3,356 | 97.3 | 94.9 |
| 2021 | 21 | 32.3 | 46.4 | 5.98 | 6.03 | 7,575 | 4,164 | 100.1 | 92.8 |
| 2022 | 21 | 41.7 | 46.4 | 6.07 | 6.16 | 5,361 | 4,428 | 102.7 | 95.2 |
| 2023 | 21 | 42.9 | 44.9 | 6.1 | 6.17 | 4,536 | 4,170 | 103.2 | 95.5 |
| 2024 | 22 | 41.3 | 42.9 | 6.09 | 6.13 | 3,775 | 3,539 | 102.1 | 95.5 |
| 2025 | 21 | 41.4 | 39.3 | 6.22 | 6.23 | 3,405 | 3,715 | 102.8 | 96.8 |

_46 rows._
<!-- /RESULTS:02_q3_top3_vs_rest -->

### 2.4 Directors with a single movie rated 8.0 or more

<!-- RESULTS:02_q4_one_hit_directors -->
**Q4 - Directors with exactly one movie rated >= 8.0 and >= 2 other movies averaging <= 6.0 (movies with >= 1,000 votes)**

| director | hit_movie | hit_year | hit_rating | hit_votes | n_other_movies | avg_rating_other |
| --- | --- | --- | --- | --- | --- | --- |
| Irvin Kershner | Star Wars: Episode V - The Empire Strikes Back | 1980 | 8.7 | 1,533,246 | 9 | 5.94 |
| James McTeigue | V for Vendetta | 2005 | 8.1 | 1,261,890 | 4 | 5.98 |
| M. Night Shyamalan | The Sixth Sense | 1999 | 8.2 | 1,177,664 | 15 | 5.87 |
| Richard Kelly | Donnie Darko | 2001 | 8 | 925,429 | 2 | 5.45 |
| Frank Miller | Sin City | 2005 | 8 | 825,749 | 2 | 5.65 |
| Robert Rodriguez | Sin City | 2005 | 8 | 825,749 | 22 | 5.94 |
| Martin Campbell | Casino Royale | 2006 | 8 | 758,848 | 16 | 5.92 |
| Tate Taylor | The Help | 2011 | 8.1 | 535,090 | 6 | 5.88 |
| Garth Davis | Lion | 2016 | 8 | 270,324 | 2 | 5.65 |
| Wai Keung Lau | Infernal Affairs | 2002 | 8 | 139,919 | 21 | 6 |
| Vincent Paronnaud | Persepolis | 2007 | 8 | 107,518 | 3 | 5.7 |
| Billy Bob Thornton | Sling Blade | 1996 | 8 | 105,240 | 3 | 5.93 |
| Predrag Antonijevic | Dara of Jasenovac | 2020 | 8 | 82,510 | 5 | 5.7 |
| Umesh Shukla | OMG: Oh My God! | 2012 | 8.1 | 72,478 | 6 | 5.6 |
| Vikas Bahl | Queen | 2013 | 8.1 | 72,027 | 6 | 5.83 |
| Mert Baykal | Pardon | 2005 | 8.1 | 24,943 | 3 | 5.9 |
| Kamal Tabrizi | The Lizard | 2004 | 8.4 | 24,814 | 4 | 5.53 |
| Ajay Bahl | Section 375 | 2019 | 8 | 21,953 | 3 | 4.63 |
| J.P. Dutta | Border | 1997 | 8 | 20,782 | 4 | 5.33 |
| Dharani | Ghilli | 2004 | 8.2 | 19,437 | 5 | 5.44 |
| Karthick Naren | Dhuruvangal Pathinaaru | 2016 | 8.1 | 19,419 | 2 | 5.15 |
| Puri Jagannadh | Pokiri | 2006 | 8 | 17,308 | 18 | 5.8 |
| Nalan Kumarasamy | Soodhu Kavvum | 2013 | 8.2 | 13,775 | 2 | 5.95 |
| Sinan Çetin | Abbas in Flower | 1982 | 8 | 12,169 | 7 | 4.84 |
| Gunasekhar | Okkadu | 2003 | 8 | 11,854 | 6 | 5.38 |
| R. Madhesh | Mudhalvan | 1999 | 8.4 | 9596 | 3 | 5.87 |
| Ezhil | Thulladha Manamum Thullum | 1999 | 8.3 | 7363 | 2 | 4.55 |
| Ravikanth Perepu | Kshanam | 2016 | 8.1 | 6712 | 2 | 5.95 |
| Sangeeth Sivan | Yoddha | 1992 | 8.4 | 5499 | 4 | 5.25 |
| Harish Vyas | Om Ka Hari | 2026 | 9.8 | 5027 | 2 | 5.75 |
| Christopher Ashley | Come from Away | 2021 | 8.5 | 5015 | 2 | 5.6 |
| Denis Sanders | Elvis: That's the Way It Is | 1970 | 8 | 3536 | 2 | 5.6 |
| Ananth Mahadevan | Phule | 2025 | 8.1 | 3248 | 4 | 5.15 |
| David Hogan | Sign 'o' the Times | 1987 | 8 | 3059 | 2 | 4.6 |
| Albert Magnoli | Sign 'o' the Times | 1987 | 8 | 3059 | 4 | 5.8 |
| Prince | Sign 'o' the Times | 1987 | 8 | 3059 | 2 | 4.9 |
| Johny Antony | C.I.D. Moosa | 2003 | 8.1 | 2817 | 2 | 5.8 |
| Carlos Bolado | Promises | 2001 | 8.3 | 2796 | 2 | 5.85 |
| Suseenthiran | Jeeva | 2014 | 8 | 2448 | 5 | 5.96 |
| Satish Paramvedha | Seetha Kalyana Vaibhogame | 2024 | 8.4 | 2362 | 2 | 5.8 |

_Showing 40 of 47 rows. Full result: `results/csv/02_q4_one_hit_directors.csv`._
<!-- /RESULTS:02_q4_one_hit_directors -->

### 2.5 TV series: best episode early, worst episode in the last season

<!-- RESULTS:02_q5_series_decline -->
**Q5 - TV series (>= 3 seasons of >= 10 episodes) whose worst episode is in the last season and best episode in seasons 1-2**

| series | series_start | n_seasons | seasons_10plus | best_season | best_episode | best_rating | worst_season | worst_episode | worst_rating | series_votes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| House of Cards | 2013 | 6 | 5 | 2 | 1 | 9.5 | 6 | 8 | 2.5 | 565,406 |
| Narcos | 2015 | 3 | 3 | 2 | 10 | 9.4 | 3 | 1 | 7.8 | 539,829 |
| The Flash | 2014 | 9 | 9 | 1 | 15 | 9.6 | 9 | 6 | 3.8 | 390,086 |
| You | 2018 | 5 | 5 | 2 | 9 | 9.1 | 5 | 10 | 5.3 | 378,790 |
| 13 Reasons Why | 2017 | 4 | 4 | 1 | 13 | 9.2 | 4 | 4 | 5.1 | 344,920 |
| The Umbrella Academy | 2019 | 4 | 3 | 2 | 10 | 8.9 | 4 | 6 | 4.8 | 319,761 |
| Two and a Half Men | 2003 | 12 | 12 | 2 | 23 | 8.7 | 12 | 15 | 3.6 | 302,186 |
| True Blood | 2008 | 7 | 7 | 2 | 8 | 8.8 | 7 | 10 | 5.4 | 274,302 |
| Heroes | 2006 | 4 | 4 | 1 | 20 | 8.9 | 4 | 15 | 6.8 | 267,709 |
| Jessica Jones | 2015 | 3 | 3 | 1 | 9 | 9 | 3 | 2 | 7.1 | 243,305 |
| Demon Slayer: Kimetsu no Yaiba | 2019 | 4 | 3 | 2 | 17 | 9.8 | 4 | 4 | 7.1 | 241,451 |
| One Punch Man | 2015 | 3 | 3 | 1 | 12 | 9.5 | 3 | 6 | 1.6 | 232,771 |
| 24 | 2001 | 8 | 8 | 1 | 24 | 9.3 | 8 | 9 | 7.6 | 208,922 |
| Silicon Valley | 2014 | 6 | 3 | 1 | 8 | 9.3 | 6 | 4 | 7.3 | 185,377 |
| The Originals | 2013 | 5 | 5 | 1 | 22 | 9.5 | 5 | 9 | 8 | 163,853 |
| Chuck | 2007 | 5 | 5 | 2 | 22 | 9.5 | 5 | 2 | 7.4 | 155,120 |
| Lie to Me | 2009 | 3 | 3 | 1 | 12 | 8.8 | 3 | 5 | 7 | 136,850 |
| The Man in the High Castle | 2015 | 4 | 4 | 2 | 10 | 9.2 | 4 | 10 | 6 | 127,472 |
| Weeds | 2005 | 8 | 8 | 2 | 12 | 8.9 | 8 | 9 | 6.9 | 124,121 |
| Under the Dome | 2013 | 3 | 3 | 1 | 1 | 7.6 | 3 | 13 | 6.1 | 120,107 |
| Designated Survivor | 2016 | 3 | 3 | 1 | 10 | 8.5 | 3 | 4 | 6.5 | 117,435 |
| Atypical | 2017 | 4 | 3 | 2 | 10 | 9.1 | 4 | 1 | 7.4 | 115,794 |
| Dead to Me | 2019 | 3 | 3 | 1 | 10 | 8.6 | 3 | 1 | 7.4 | 112,408 |
| The Twilight Zone | 1959 | 5 | 5 | 2 | 6 | 9.1 | 5 | 27 | 5.6 | 111,577 |
| Narcos: Mexico | 2018 | 3 | 3 | 1 | 10 | 9 | 3 | 2 | 7.4 | 103,609 |
| Star Trek | 1966 | 3 | 3 | 1 | 28 | 9.2 | 3 | 4 | 5.2 | 102,817 |
| Falling Skies | 2011 | 5 | 5 | 2 | 10 | 8.2 | 5 | 10 | 5.9 | 100,153 |
| Monty Python's Flying Circus | 1969 | 4 | 3 | 2 | 2 | 8.9 | 4 | 1 | 7.2 | 88,097 |
| Scandal | 2012 | 7 | 6 | 2 | 22 | 8.8 | 7 | 18 | 6.1 | 85,495 |
| Veronica Mars | 2004 | 4 | 3 | 1 | 22 | 9.1 | 4 | 8 | 5.8 | 84,524 |
| iZombie | 2015 | 5 | 5 | 2 | 19 | 9.1 | 5 | 4 | 7.3 | 80,181 |
| Ballers | 2015 | 5 | 3 | 2 | 10 | 8.5 | 5 | 2 | 7.1 | 78,999 |
| The Last Ship | 2014 | 5 | 5 | 2 | 12 | 9.1 | 5 | 5 | 6.1 | 71,441 |
| The Last Man on Earth | 2015 | 4 | 4 | 2 | 10 | 8.2 | 4 | 7 | 6.3 | 69,483 |
| Good Girls | 2018 | 4 | 4 | 1 | 10 | 8.4 | 4 | 16 | 5.9 | 68,949 |
| The Great | 2020 | 3 | 3 | 2 | 10 | 8.7 | 3 | 9 | 7.3 | 67,016 |
| Quantico | 2015 | 3 | 3 | 1 | 1 | 7.7 | 3 | 5 | 4.8 | 66,400 |
| Star Trek: Enterprise | 2001 | 4 | 4 | 2 | 2 | 8.6 | 4 | 22 | 5.3 | 65,496 |
| Scorpion | 2014 | 4 | 4 | 2 | 10 | 8.6 | 4 | 22 | 6.2 | 64,741 |
| Bloodline | 2015 | 3 | 3 | 1 | 12 | 9.1 | 3 | 9 | 3.6 | 64,667 |

_Showing 40 of 566 rows. Full result: `results/csv/02_q5_series_decline.csv`._
<!-- /RESULTS:02_q5_series_decline -->

### 2.6 Actors and actresses successful in several languages

<!-- RESULTS:02_q6_language_distribution -->
**Q6 - Inferred original language of rated feature films (coverage: 278,734/317,982 = 87.7%)**

| language | count |
| --- | --- |
| en | 242,657 |
| es | 28,232 |
| de | 25,706 |
| fr | 23,272 |
| it | 14,382 |
| ja | 9296 |
| pt | 8859 |
| zh | 5640 |
| hi | 5311 |
| ko | 4943 |
| tr | 4877 |
| sv | 3944 |
| fa | 3689 |
| ar | 3669 |
| nl | 3249 |
| cs | 2919 |
| da | 2679 |
| hu | 2532 |
| tl | 1902 |
| id | 1893 |

_Showing 20 of 65 rows. Full result: `results/csv/02_q6_language_distribution.csv`._
<!-- /RESULTS:02_q6_language_distribution -->

<!-- RESULTS:02_q6_languages_per_actor -->
**Q6 - Actors/actresses by number of distinct languages of their successful films (rating >= 7.0, >= 10,000 votes)**

| n_languages | n_actors |
| --- | --- |
| 1 | 25,713 |
| 2 | 1123 |
| 3 | 185 |
| 4 | 17 |
| 5 | 5 |
| 6 | 1 |

_6 rows._
<!-- /RESULTS:02_q6_languages_per_actor -->

<!-- RESULTS:02_q6_multilingual_actors -->
**Q6 - Actors/actresses with successful films in at least 3 languages**

| name | n_languages | languages | n_successful_films | best_known_hits |
| --- | --- | --- | --- | --- |
| Toshirō Mifune | 6 | de, en, fr, it, nl, sv | 7 | Seven Samurai, High and Low, Samurai Rebellion |
| Şener Şen | 5 | de, en, fr, ja, tr | 15 | The Bandit, Hunting Season, The Chaos Class Failed the Class |
| Song Kang-ho | 5 | en, fr, it, ko, tr | 11 | Parasite, Snowpiercer, Memories of Murder |
| Tony Leung Chiu-wai | 5 | de, en, es, fr, zh | 10 | Shang-Chi and the Legend of the Ten Rings, Hero, In the Mood for Love |
| Bruno Ganz | 5 | da, de, en, fr, sv | 9 | Downfall, The Reader, Wings of Desire |
| Tatsuya Nakadai | 5 | de, en, fr, sv, tr | 7 | Ran, Harakiri, High and Low |
| Max von Sydow | 4 | da, en, fr, sv | 17 | Shutter Island, Minority Report, The Exorcist |
| Gérard Depardieu | 4 | en, es, fr, it | 12 | La Vie En Rose, Mesrine: Killer Instinct, Ruby & Quentin |
| Ayşen Gruda | 4 | de, fr, ja, tr | 8 | Coming Soon, Tosun Pasha, The King of the Street Cleaners |
| Cem Yılmaz | 4 | de, en, ja, tr | 8 | The Water Diviner, G.O.R.A., A.R.O.G |
| Christian Berkel | 4 | da, de, en, zh | 7 | Downfall, The Man from U.N.C.L.E., Valkyrie |
| Bibi Andersson | 4 | da, en, fr, sv | 7 | The Seventh Seal, Persona, Wild Strawberries |
| Liv Ullmann | 4 | da, en, fr, sv | 7 | Persona, Autumn Sonata, Cries & Whispers |
| Trine Dyrholm | 4 | da, de, en, ja | 7 | The Celebration, Who Am I, A Royal Affair |
| Takeshi Kaneshiro | 4 | ca, de, fr, zh | 5 | Chungking Express, Fallen Angels, The Warlords |
| Ryu Seung-ryong | 4 | de, en, it, ko | 5 | Miracle in Cell No. 7, 20th Century Girl, Extreme Job |
| Miki Manojlović | 4 | bs, en, hr, sr | 4 | Underground, The Wounds, The Trap |
| Ma Dong-seok | 4 | en, es, ko, tr | 4 | Train to Busan, The Gangster, the Cop, the Devil, The Outlaws |
| Ryūnosuke Kamiki | 4 | de, en, fr, ja | 4 | Godzilla Minus One, Suzume, March of the Penguins |
| Bogdan Diklic | 4 | en, hr, it, sr | 4 | No Man's Land, The Marathon Family, The Trap |
| Farah Zeynep Abdullah | 4 | de, en, ja, sv | 4 | The Butterfly's Dream, Whisper If I Forget, Champion |
| Ahmet Mümtaz Taylan | 4 | de, en, fr, sv | 4 | Once Upon a Time in Anatolia, Mortal World, The Butterfly's Dream |
| Çetin Tekindor | 4 | de, fr, ja, sv | 4 | My Father and My Son, Ayla: The Daughter of War, Hunting Season |
| Robert De Niro | 3 | en, es, it | 34 | Joker, The Godfather Part II, GoodFellas |
| Henry Fonda | 3 | en, es, it | 18 | 12 Angry Men, Once Upon a Time in the West, The Grapes of Wrath |
| Stellan Skarsgård | 3 | en, no, sv | 17 | The Avengers, Good Will Hunting, Dune |
| Nikolaj Lie Kaas | 3 | da, en, ja | 14 | Frankenstein, Riders of Justice, Adam's Apples |
| Kemal Sunal | 3 | de, en, tr | 14 | The Chaos Class, The Chaos Class Failed the Class, Tosun Pasha |
| Javier Bardem | 3 | en, es, fr | 13 | No Country for Old Men, Dune, Dune: Part Two |
| Naomi Watts | 3 | en, es, hi | 13 | Birdman or (The Unexpected Virtue of Ignorance), King Kong, Mulholland Drive |
| Penélope Cruz | 3 | en, es, it | 12 | Blow, Volver, All About My Mother |
| Antonio Banderas | 3 | en, es, fr | 12 | Shrek 2, Interview with the Vampire: The Vampire Chronicles, Puss in Boots: The Last Wish |
| Adile Naşit | 3 | de, fr, tr | 11 | The Chaos Class, The Chaos Class Failed the Class, Tosun Pasha |
| Ingrid Bergman | 3 | en, it, sv | 10 | Casablanca, Notorious, Murder on the Orient Express |
| Marcello Mastroianni | 3 | en, fr, it | 10 | 8½, La Dolce Vita, La Notte |
| Terence Hill | 3 | es, fr, it | 10 | The Leopard, My Name Is Nobody, They Call Me Trinity |
| Alain Delon | 3 | en, fr, it | 10 | Le Samouraï, The Leopard, The Red Circle |
| Jeanne Moreau | 3 | en, es, fr | 10 | Jules and Jim, Elevator to the Gallows, The Trial |
| Diane Kruger | 3 | de, en, fr | 9 | Inglourious Basterds, Mr. Nobody, The Infiltrator |
| Gunnar Björnstrand | 3 | da, en, sv | 9 | The Seventh Seal, Persona, Wild Strawberries |

_Showing 40 of 208 rows. Full result: `results/csv/02_q6_multilingual_actors.csv`._
<!-- /RESULTS:02_q6_multilingual_actors -->

### 2.7 Long careers with an early popularity peak

<!-- RESULTS:02_q7_summary -->
**Q7 - Summary**

Actors/actresses with a career of at least 20 years and at least 15 rated feature films: **20,517**. Popularity peak in the first third of the career: **8,095** (39.5%).
<!-- /RESULTS:02_q7_summary -->

<!-- RESULTS:02_q7_early_peak_actors -->
**Q7 - Actors/actresses (20+ years, 15+ movies) whose popularity peaked early (peak_position = share of career elapsed at peak)**

| name | first_year | last_year | n_movies | peak_year | peak_movie | peak_position | peak_relative_pop |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Jagathy Sreekumar | 1978 | 2022 | 313 | 1989 | Kireedam | 0.25 | 3.5 |
| Raymond Hatton | 1914 | 1959 | 216 | 1917 | A Romance of the Redwoods | 0.067 | 19.53 |
| Ku Feng | 1963 | 2020 | 214 | 1978 | Five Deadly Venoms | 0.263 | 5.9 |
| Dharmendra | 1961 | 2026 | 212 | 1975 | Sholay | 0.215 | 29.41 |
| Michael Madsen | 1982 | 2026 | 203 | 1992 | Reservoir Dogs | 0.227 | 231.48 |
| Soumitra Chatterjee | 1959 | 2025 | 192 | 1959 | The World of Apu | 0 | 12.53 |
| Roy Barcroft | 1938 | 1966 | 189 | 1945 | The Vampire's Ghost | 0.25 | 2.29 |
| Innocent Vareed Thekkethala | 1982 | 2023 | 187 | 1993 | Manichitrathazhu | 0.268 | 3.66 |
| John Carradine | 1930 | 1989 | 178 | 1940 | The Grapes of Wrath | 0.169 | 73.38 |
| José Luis López Vázquez | 1947 | 2007 | 174 | 1963 | The Executioner | 0.267 | 5.43 |
| Satyendra Kapoor | 1963 | 2003 | 173 | 1975 | Sholay | 0.3 | 29.35 |
| Ranjeet Bedi | 1970 | 2022 | 169 | 1982 | Satte Pe Satta | 0.231 | 2.57 |
| J. Farrell MacDonald | 1915 | 1951 | 167 | 1927 | Sunrise | 0.333 | 57.51 |
| Oskar Sima | 1929 | 1967 | 166 | 1929 | Three Loves | 0 | 0.94 |
| Türkan Şoray | 1960 | 2015 | 164 | 1977 | The Girl with the Red Scarf | 0.309 | 8.66 |
| Johnny Mack Brown | 1927 | 1965 | 155 | 1929 | Coquette | 0.053 | 10.41 |
| Tom Sizemore | 1989 | 2026 | 155 | 1998 | Saving Private Ryan | 0.243 | 203.83 |
| Francisco Rabal | 1950 | 2003 | 154 | 1961 | Viridiana | 0.208 | 22.84 |
| Farid Shawqi | 1947 | 1996 | 154 | 1958 | Cairo Station | 0.224 | 5.14 |
| Michel Galabru | 1958 | 2013 | 151 | 1964 | The Gendarme of Saint-Tropez | 0.109 | 8.98 |
| George 'Gabby' Hayes | 1929 | 1950 | 150 | 1934 | Blue Steel | 0.238 | 21.15 |
| J. Carrol Naish | 1931 | 1971 | 147 | 1943 | Sahara | 0.3 | 26.98 |
| Ernie Adams | 1926 | 1952 | 147 | 1934 | It Happened One Night | 0.308 | 201.36 |
| Charles Starrett | 1930 | 1952 | 146 | 1932 | The Mask of Fu Manchu | 0.091 | 6.32 |
| Joe Estevez | 1986 | 2026 | 145 | 1990 | Soultaker | 0.1 | 0.81 |
| Hülya Koçyiğit | 1963 | 2007 | 142 | 1963 | Dry Summer | 0 | 4.43 |
| Bernard Blier | 1938 | 1989 | 142 | 1947 | Jenny Lamour | 0.176 | 7.66 |
| Lyle Talbot | 1932 | 1959 | 139 | 1932 | Three on a Match | 0 | 18.25 |
| Thilakan | 1981 | 2013 | 139 | 1991 | Sandesham | 0.313 | 4.07 |
| Luis Aguilar | 1944 | 1992 | 138 | 1951 | A.T.M.: ¡¡A toda máquina!! | 0.146 | 1.14 |
| John Litel | 1930 | 1963 | 137 | 1938 | Jezebel | 0.242 | 46.48 |
| Michel Piccoli | 1949 | 2014 | 137 | 1967 | Belle de Jour | 0.277 | 46.29 |
| Keith David | 1982 | 2026 | 135 | 1982 | The Thing | 0 | 177.07 |
| M.N. Nambiar | 1947 | 2006 | 135 | 1957 | Mayabazar | 0.169 | 3.22 |
| N.T. Rama Rao | 1949 | 1993 | 135 | 1957 | Mayabazar | 0.182 | 3.22 |
| Fuzzy Knight | 1932 | 1965 | 134 | 1938 | The Cowboy and the Lady | 0.182 | 4.98 |
| Barton MacLane | 1933 | 1968 | 134 | 1941 | The Maltese Falcon | 0.229 | 138.29 |
| Malcolm McDowell | 1968 | 2026 | 133 | 1971 | A Clockwork Orange | 0.052 | 509.92 |
| Sreenivasan | 1977 | 2025 | 133 | 1988 | Chithram | 0.229 | 3.05 |
| Theo Lingen | 1930 | 1977 | 131 | 1931 | M | 0.021 | 149.39 |

_Showing 40 of 8,095 rows. Full result: `results/csv/02_q7_early_peak_actors.csv`._
<!-- /RESULTS:02_q7_early_peak_actors -->

### 2.8 Multi-talented film professionals

<!-- RESULTS:02_q8_role_combinations -->
**Q8 - Performance of films where one person holds several roles (baseline single-role credits: avg rating 5.868, median votes 552)**

| combination | n_credits | n_people | avg_rating | median_votes | rating_vs_single_role |
| --- | --- | --- | --- | --- | --- |
| director + writer | 52,887 | 27,267 | 6.003 | 516 | 0.135 |
| director + producer + writer | 16,545 | 11,105 | 5.772 | 459 | -0.096 |
| producer + writer | 10,888 | 7759 | 5.573 | 596 | -0.295 |
| director + producer | 8301 | 5226 | 6.039 | 545 | 0.171 |
| actor + writer | 7937 | 6078 | 5.628 | 478 | -0.24 |
| actor + producer | 7123 | 5158 | 5.306 | 508 | -0.562 |
| director + editor + writer | 4352 | 3327 | 5.766 | 394 | -0.102 |
| actor + director + writer | 4079 | 2832 | 5.753 | 469 | -0.115 |
| director + editor + producer + writer | 3863 | 3074 | 5.495 | 318 | -0.373 |
| actor + producer + writer | 2452 | 1968 | 5.122 | 440 | -0.746 |
| actor + director + producer + writer | 2424 | 1907 | 5.321 | 388 | -0.547 |
| director + editor | 1831 | 1484 | 5.778 | 371 | -0.09 |
| actor + director | 1516 | 1196 | 5.719 | 364 | -0.149 |
| cinematographer + producer | 1509 | 1230 | 5.556 | 318 | -0.312 |
| editor + producer | 1475 | 1224 | 5.658 | 318 | -0.21 |
| actor + composer | 1406 | 1227 | 5.688 | 330 | -0.18 |
| editor + writer | 1339 | 1078 | 6.338 | 378 | 0.47 |
| cinematographer + director + editor + producer + writer | 1331 | 1095 | 5.605 | 264 | -0.263 |
| cinematographer + editor | 1320 | 1083 | 5.457 | 266 | -0.411 |
| cinematographer + director | 1231 | 924 | 5.996 | 337 | 0.128 |
| cinematographer + director + writer | 1113 | 903 | 6.107 | 282 | 0.239 |
| director + editor + producer | 1040 | 882 | 5.966 | 298 | 0.098 |
| cinematographer + director + producer + writer | 895 | 769 | 6.062 | 317 | 0.194 |
| cinematographer + director + editor + writer | 863 | 706 | 5.571 | 265 | -0.297 |
| actor + director + editor + producer + writer | 725 | 639 | 5.1 | 266 | -0.768 |

_Showing 25 of 201 rows. Full result: `results/csv/02_q8_role_combinations.csv`._
<!-- /RESULTS:02_q8_role_combinations -->

<!-- RESULTS:02_q8_multi_vs_single_summary -->
**Q8 - Same person, multi-role vs single-role films (people with >= 3 films on each side)**

| people | pct_better_when_multi | mean_delta | median_delta |
| --- | --- | --- | --- |
| 6944 | 51.6 | 0.016 | 0.03 |

_1 rows._
<!-- /RESULTS:02_q8_multi_vs_single_summary -->

<!-- RESULTS:02_q8_multi_talented_people -->
**Q8 - Multi-talented professionals (>= 3 films holding several roles)**

| name | roles_combined | n_multi_role_films | n_single_role_films | avg_rating_multi | avg_rating_single | delta |
| --- | --- | --- | --- | --- | --- | --- |
| Jing Wong | actor, director, producer, writer | 122 | 74 | 5.65 | 5.79 | -0.14 |
| Jesús Franco | actor, cinematographer, composer, director, editor, producer, writer | 112 | 22 | 4.66 | 4.42 | 0.24 |
| Yōji Yamada | director, writer | 63 | 7 | 6.89 | 6.87 | 0.02 |
| Jean-Luc Godard | actor, cinematographer, director, editor, producer, writer | 62 | 11 | 6.43 | 6.16 | 0.27 |
| Giannis Dalianidis | actor, director, producer, writer | 61 | 12 | 5.98 | 5.94 | 0.04 |
| Hark Tsui | actor, director, editor, producer, production_designer, writer | 56 | 31 | 6.28 | 6.18 | 0.1 |
| Carlo Vanzina | director, producer, writer | 56 | 12 | 4.84 | 4.89 | -0.06 |
| Woody Allen | actor, composer, director, writer | 54 | 8 | 6.89 | 5.69 | 1.2 |
| Joe D'Amato | cinematographer, director, editor, producer, writer | 54 | 25 | 4.46 | 5.12 | -0.67 |
| Cheh Chang | director, producer, writer | 54 | 22 | 6.46 | 6.35 | 0.11 |
| Scott Chambers | actor, director, editor, producer, writer | 50 | 50 | 3.06 | 3.17 | -0.12 |
| Godfrey Ho | actor, director, producer, production_designer, writer | 49 | 21 | 4.16 | 4.55 | -0.39 |
| Luc Besson | cinematographer, director, editor, producer, writer | 48 | 26 | 6.18 | 6.19 | -0.02 |
| Claude Chabrol | actor, director, producer, writer | 48 | 24 | 6.53 | 6.13 | 0.4 |
| Roger Corman | actor, director, producer, writer | 46 | 62 | 5.35 | 5.03 | 0.32 |
| Claude Lelouch | actor, cinematographer, director, editor, producer, writer | 45 | 9 | 6.3 | 6.31 | -0.01 |
| Kader Khan | actor, writer | 45 | 126 | 5.73 | 5.35 | 0.38 |
| Neri Parenti | director, writer | 44 | 3 | 4.87 | 5.1 | -0.23 |
| Werner Herzog | actor, cinematographer, director, producer, writer | 44 | 11 | 7.09 | 6.47 | 0.61 |
| Mario Monicelli | director, writer | 44 | 34 | 6.67 | 6.61 | 0.06 |
| Julien Duvivier | director, producer, writer | 43 | 6 | 6.83 | 6.75 | 0.08 |
| Sergey A. | actor, cinematographer, composer, director, editor, producer, production_designer, writer | 43 | 0 | 6.76 |  |  |
| Frederick Wiseman | director, editor, producer, writer | 42 | 2 | 7.26 | 6.15 | 1.11 |
| Dino Risi | director, writer | 42 | 15 | 6.35 | 6.57 | -0.21 |
| Ram Gopal Varma | actor, director, editor, producer, writer | 42 | 44 | 5.58 | 5.57 | 0.01 |
| Clint Eastwood | actor, composer, director, producer | 42 | 29 | 6.95 | 6.72 | 0.23 |
| Alekos Sakellarios | actor, director, producer, writer | 41 | 16 | 6.96 | 5.93 | 1.03 |
| Steno | director, writer | 41 | 37 | 6.19 | 6.02 | 0.17 |
| Charles Band | actor, director, producer, writer | 41 | 38 | 4.12 | 4.58 | -0.46 |
| Alberto Sordi | actor, director, producer, writer | 41 | 63 | 6.14 | 6.65 | -0.5 |

_Showing 30 of 13,233 rows. Full result: `results/csv/02_q8_multi_talented_people.csv`._
<!-- /RESULTS:02_q8_multi_talented_people -->

### Methodological explanation

- **Chosen method:** TODO (team) – for each question, explain the
  definitions and thresholds used.
- **Why:** TODO (team)
- **Alternatives considered and trade-offs:** TODO (team)

## 3. Data enrichment

### Scope

<!-- RESULTS:03_enrichment_targets -->
**Enrichment scope: top 10 films per genre per year (1980-2025) and their first 3 principals**

| item | n |
| --- | --- |
| film x genre x year slots | 9832 |
| distinct films | 6821 |
| film principals | 20,446 |
| distinct people | 10,157 |
| films with < 3 principals | 7 |

_5 rows._
<!-- /RESULTS:03_enrichment_targets -->

### Coverage and flagged gaps

<!-- RESULTS:03_enrichment_coverage -->
**Enrichment coverage**

| entity | targets | with_wikidata | with_wikipedia | with_text | pct_with_text | with_notes |
| --- | --- | --- | --- | --- | --- | --- |
| films | 6821 | 6579 | 6331 | 6330 | 92.8 | 941 |
| people | 10,157 | 8437 | 7602 | 7601 | 74.8 | 3251 |

_2 rows._
<!-- /RESULTS:03_enrichment_coverage -->

<!-- RESULTS:03_enrichment_synopsis_sections -->
**Where the film synopsis comes from**

| synopsis_section | count |
| --- | --- |
| Plot | 5968 |
|  | 491 |
| Introduction | 362 |

_3 rows._
<!-- /RESULTS:03_enrichment_synopsis_sections -->

<!-- RESULTS:03_enrichment_notes -->
**Programmatic notes flagging missing / doubtful information**

| entity | note | count |
| --- | --- | --- |
| film | SYNOPSIS_MISSING | 491 |
| film | NO_PLOT_SECTION_INTRO_USED | 362 |
| film | NO_ENGLISH_WIKIPEDIA_ARTICLE | 248 |
| film | NO_WIKIDATA_ITEM_FOR_IMDB_ID | 242 |
| film | SYNOPSIS_SHORT | 137 |
| film | SEVERAL_WIKIDATA_ITEMS_FIRST_KEPT | 5 |
| person | BIOGRAPHY_MISSING | 2556 |
| person | BIRTH_DATE_MISSING | 2024 |
| person | NO_WIKIDATA_ITEM_FOR_IMDB_ID | 1720 |
| person | NO_ENGLISH_WIKIPEDIA_ARTICLE | 835 |
| person | BIRTH_YEAR_DIFFERS_FROM_IMDB | 252 |
| person | BIOGRAPHY_SHORT | 250 |
| person | SEVERAL_BIRTH_DATES_EARLIEST_KEPT | 190 |
| person | SEVERAL_WIKIDATA_ITEMS_FIRST_KEPT | 13 |

_14 rows._
<!-- /RESULTS:03_enrichment_notes -->

### Samples

<!-- RESULTS:03_enrichment_films_sample -->
**Sample of enriched films**

| tconst | title | year | synopsis_section | synopsis_excerpt | source |
| --- | --- | --- | --- | --- | --- |
| tt0111161 | The Shawshank Redemption | 1994 | Plot | In 1947, Portland, Maine, banker Andy Dufresne arrives at Shawshank State Prison to serve two consecutive life sentences for murdering his wife and her lover. H... | https://en.wikipedia.org/w/index.php?title=The_Shawshank_Redemption&oldid=1384720571 |
| tt0468569 | The Dark Knight | 2008 | Plot | A gang of masked criminals rob a mafia-owned bank in Gotham City, betraying and killing each other until the sole survivor, the Joker, reveals himself as the ma... | https://en.wikipedia.org/w/index.php?title=The_Dark_Knight&oldid=1384066085 |
| tt1375666 | Inception | 2010 | Plot | Dom Cobb and Arthur are professional thieves who perform corporate espionage using experimental dream-sharing technology to infiltrate their targets' subconscio... | https://en.wikipedia.org/w/index.php?title=Inception&oldid=1385346213 |
| tt0137523 | Fight Club | 1999 | Plot | The unnamed Narrator works an office job as an insurance claims adjuster. He sees a doctor for his disordered sleep, complaining that he falls asleep unexpected... | https://en.wikipedia.org/w/index.php?title=Fight_Club&oldid=1385277614 |
| tt0816692 | Interstellar | 2014 | Plot | In the near future, humanity faces extinction due to dust storms and widespread crop blights. Joseph Cooper, a widowed former NASA test pilot, works as a farmer... | https://en.wikipedia.org/w/index.php?title=Interstellar_%28film%29&oldid=1384598366 |
| tt0109830 | Forrest Gump | 1994 | Plot | In 1981, a feather lands at a bus stop in Savannah, Georgia; Forrest Gump collects it, then recounts his life story to strangers on a bus bench. In the fictiona... | https://en.wikipedia.org/w/index.php?title=Forrest_Gump&oldid=1385610424 |
| tt0110912 | Pulp Fiction | 1994 | Plot | A young Butch Coolidge is visited by Captain Koons, an Air Force pilot. Koons reveals a gold watch to Butch and explains that the watch is a family heirloom and... | https://en.wikipedia.org/w/index.php?title=Pulp_Fiction&oldid=1385190039 |
| tt0133093 | The Matrix | 1999 | Plot | In an unnamed city in 1999, Thomas Anderson, a computer programmer known as "Neo" in hacking circles, investigates the mystery of the "Matrix", bringing him to ... | https://en.wikipedia.org/w/index.php?title=The_Matrix&oldid=1384970784 |

_Showing 8 of 200 rows. Full result: `results/csv/03_enrichment_films_sample.csv`._
<!-- /RESULTS:03_enrichment_films_sample -->

<!-- RESULTS:03_enrichment_people_sample -->
**Sample of enriched people**

| nconst | name | birth_date | birth_place | biography_excerpt | source |
| --- | --- | --- | --- | --- | --- |
| nm0000002 | Lauren Bacall | 1924-09-16 | The Bronx | Betty Joan Perske (September 16, 1924 – August 12, 2014), known professionally as Lauren Bacall ( bə-KAWL), was an American actress. She was... | https://en.wikipedia.org/w/index.php?title=Lauren_Bacall&oldid=1383532971 |
| nm0000004 | John Belushi | 1949-01-24 | Humboldt Park | John Adam Belushi ( bə-LOO-shee; January 24, 1949 – March 5, 1982) was an American comedian, actor, and musician. He was one of seven Saturd... | https://en.wikipedia.org/w/index.php?title=John_Belushi&oldid=1383285377 |
| nm0000005 | Ingmar Bergman | 1918-07-14 | Uppsala | Ernst Ingmar Bergman (14 July 1918 – 30 July 2007) was a Swedish filmmaker and theatre director. He is considered one of the greatest and mo... | https://en.wikipedia.org/w/index.php?title=Ingmar_Bergman&oldid=1385115612 |
| nm0000008 | Marlon Brando | 1924-04-03 | Omaha | Marlon Brando ( ; April 3, 1924 – July 1, 2004) was an American actor. Widely regarded as one of the greatest and most influential performer... | https://en.wikipedia.org/w/index.php?title=Marlon_Brando&oldid=1385107819 |
| nm0000009 | Richard Burton | 1925-11-10 | Pontrhydyfen | Richard Walter Burton (; né Jenkins Jr.; 10 November 1925 – 5 August 1984) was a Welsh actor. Noted for his powerful presence and baritone v... | https://en.wikipedia.org/w/index.php?title=Richard_Burton&oldid=1385195999 |
| nm0000012 | Bette Davis | 1908-04-05 | Lowell | Ruth Elizabeth "Bette" Davis (; April 5, 1908 – October 6, 1989) was an American actress of film, television, and theater. Regarded as one o... | https://en.wikipedia.org/w/index.php?title=Bette_Davis&oldid=1381285548 |
| nm0000017 | Marlene Dietrich | 1901-12-27 | Rote Insel | Marie Magdalene "Marlene" Dietrich (, German: [maʁˈleːnə ˈdiːtʁɪç] ; 27 December 1901 – 6 May 1992) was a German and American actress and si... | https://en.wikipedia.org/w/index.php?title=Marlene_Dietrich&oldid=1385577963 |
| nm0000018 | Kirk Douglas | 1916-12-09 | Amsterdam | Kirk Douglas (born Issur Danielovitch; December 9, 1916 – February 5, 2020) was an American actor and filmmaker. After an impoverished child... | https://en.wikipedia.org/w/index.php?title=Kirk_Douglas&oldid=1384022142 |

_Showing 8 of 200 rows. Full result: `results/csv/03_enrichment_people_sample.csv`._
<!-- /RESULTS:03_enrichment_people_sample -->

### Methodological explanation

- **Chosen method:** TODO (team)
- **Why:** TODO (team)
- **Alternatives considered and trade-offs:** TODO (team)
- **Provenance, licences and rate limits:** TODO (team)
- **Scalability to the full IMDb dataset:** TODO (team)

## 4. Streaming data processing

### Results of the test run

<!-- RESULTS:04_streaming_metrics -->
**Streaming metrics (from StreamingQueryProgress)**

| query | batches | input rows | avg input rows/s | avg processed rows/s | avg batch ms | max batch ms | rows dropped by watermark | max state rows |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| flagged_console | 130 | 2,488 | 1.6 | 11.8 | 1470 | 4,666 | 0 | 2,026 |
| flagged_to_parquet | 131 | 2,488 | 1.6 | 5.8 | 3447 | 6,832 | 0 | 2,026 |
| window_counts | 130 | 2,488 | 1.6 | 5.5 | 3647 | 6,898 | 0 | 2,026 |

_Run from 2026-11-14T20:29:19.840Z to 2026-11-14T20:51:00.000Z; watermark 10 minutes, window 5 minutes, trigger 10 seconds._
<!-- /RESULTS:04_streaming_metrics -->

<!-- RESULTS:04_streaming_by_match_type -->
**Flagged edits by matching rule**

| match_type | edits | pages |
| --- | --- | --- |
| person_exact | 29 | 23 |
| popular_title_exact | 17 | 11 |
| title_with_film_hint | 16 | 12 |

_3 rows._
<!-- /RESULTS:04_streaming_by_match_type -->

<!-- RESULTS:04_streaming_by_wiki -->
**Flagged edits by wiki**

| wiki | edits |
| --- | --- |
| enwiki | 49 |
| frwiki | 13 |

_2 rows._
<!-- /RESULTS:04_streaming_by_wiki -->

<!-- RESULTS:04_streaming_top_pages -->
**Most edited IMDb-related pages**

| title | imdb_id | imdb_label | match_type | edits |
| --- | --- | --- | --- | --- |
| All Creatures Great and Small (2020 TV series) | tt0075472 | All Creatures Great & Small | title_with_film_hint | 3 |
| Stagecoach | tt0031971 | Stagecoach | popular_title_exact | 3 |
| Rien que pour vos cheveux | tt0960144 | You Don't Mess with the Zohan | popular_title_exact | 3 |
| The Child (2026 film) | tt0456396 | The Child | title_with_film_hint | 3 |
| Emily Hampshire | nm0358922 | Emily Hampshire | person_exact | 2 |
| Sharon Tate | nm0001790 | Sharon Tate | person_exact | 2 |
| Here Be Monsters! | tt0787474 | The Boxtrolls | popular_title_exact | 2 |
| Queen | tt3322420 | Queen | popular_title_exact | 2 |
| Sean Astin | nm0000276 | Sean Astin | person_exact | 2 |
| Belén Cuesta | nm3177382 | Belén Cuesta | person_exact | 2 |
| Eddie Murphy | nm0000552 | Eddie Murphy | person_exact | 2 |
| Ice Cube | nm0001084 | Ice Cube | person_exact | 2 |
| Charles Hallahan | nm0356251 | Charles Hallahan | person_exact | 1 |
| Yonggary (1999 film) | tt0272425 | Reptilian | title_with_film_hint | 1 |
| Campbell Scott | nm0001714 | Campbell Scott | person_exact | 1 |
| John Malkovich | nm0000518 | John Malkovich | person_exact | 1 |
| Prince of Darkness (film) | tt0093777 | Prince of Darkness | title_with_film_hint | 1 |
| Gene Roddenberry | nm0734472 | Gene Roddenberry | person_exact | 1 |
| Marianne Basler | nm0059974 | Marianne Basler | person_exact | 1 |
| The Sentinel (série télévisée) | tt0443632 | The Sentinel | title_with_film_hint | 1 |

_Showing 20 of 46 rows. Full result: `results/csv/04_streaming_top_pages.csv`._
<!-- /RESULTS:04_streaming_top_pages -->

<!-- RESULTS:04_streaming_windows -->
**Events per 5 minutes window (closed windows only, watermark 10 minutes)**

| window_start | window_end | wiki | events | imdb_events | distinct_pages | pct_imdb |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-11-14 20:25:00 | 2026-11-14 20:30:00 | enwiki | 449 | 9 | 377 | 2 |
| 2026-11-14 20:25:00 | 2026-11-14 20:30:00 | frwiki | 91 | 3 | 79 | 3.3 |
| 2026-11-14 20:30:00 | 2026-11-14 20:35:00 | enwiki | 365 | 9 | 330 | 2.47 |
| 2026-11-14 20:30:00 | 2026-11-14 20:35:00 | frwiki | 101 | 3 | 81 | 2.97 |
| 2026-11-14 20:35:00 | 2026-11-14 20:40:00 | enwiki | 394 | 9 | 335 | 2.28 |
| 2026-11-14 20:35:00 | 2026-11-14 20:40:00 | frwiki | 88 | 3 | 74 | 3.41 |

_6 rows._
<!-- /RESULTS:04_streaming_windows -->

<!-- RESULTS:04_streaming_flagged_sample -->
**Flagged events (first rows)**

| event_time | wiki | title | match_type | imdb_id | imdb_label | user | size_delta |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-11-14 20:25:25 | enwiki | Yonggary (1999 film) | title_with_film_hint | tt0272425 | Reptilian | Moakdoge | 20 |
| 2026-11-14 20:25:33 | enwiki | Gene Roddenberry | person_exact | nm0734472 | Gene Roddenberry | Starst90 | 4 |
| 2026-11-14 20:25:35 | enwiki | Little Bear (TV series) | title_with_film_hint | tt0245637 | Little Bear | ~2026-59016-42 | 37 |
| 2026-11-14 20:25:47 | enwiki | Lorne Balfe | person_exact | nm1154632 | Lorne Balfe | ~2026-60385-34 | 4 |
| 2026-11-14 20:25:53 | enwiki | Charles Hallahan | person_exact | nm0356251 | Charles Hallahan | Inert77 | -72 |
| 2026-11-14 20:26:12 | frwiki | The Sentinel (série télévisée) | title_with_film_hint | tt0443632 | The Sentinel | CodexBot | 90 |
| 2026-11-14 20:26:50 | enwiki | In the Heights | popular_title_exact | tt1321510 | In the Heights | Cringechancellor | 0 |
| 2026-11-14 20:27:03 | frwiki | Emily Hampshire | person_exact | nm0358922 | Emily Hampshire | Iamabaggins | 1454 |
| 2026-11-14 20:27:38 | frwiki | Holly Taylor | person_exact | nm4111426 | Holly Taylor | ~2026-60108-00 | 85 |
| 2026-11-14 20:27:38 | enwiki | Hamilton Camp | person_exact | nm0131985 | Hamilton Camp | ~2026-54945-86 | 1 |
| 2026-11-14 20:29:41 | enwiki | Room on the Broom (film) | title_with_film_hint | tt2484120 | Room on the Broom | TheBusGuy1634 | -18 |
| 2026-11-14 20:29:44 | enwiki | Bully (2001 film) | title_with_film_hint | tt0242193 | Bully | Ranggo Dogg | 11 |
| 2026-11-14 20:30:06 | enwiki | Timothy Dalton | person_exact | nm0001096 | Timothy Dalton | Halbared | 90 |
| 2026-11-14 20:30:46 | frwiki | Emily Hampshire | person_exact | nm0358922 | Emily Hampshire | Iamabaggins | 19 |
| 2026-11-14 20:31:02 | enwiki | Werner Herzog | person_exact | nm0001348 | Werner Herzog | Dolomede | -18 |
| 2026-11-14 20:32:27 | enwiki | My Name Is Earl | popular_title_exact | tt0460091 | My Name Is Earl | ~2026-60364-59 | -7 |
| 2026-11-14 20:33:16 | enwiki | Wolf | popular_title_exact | tt0111742 | Wolf | LittleJerry | -354 |
| 2026-11-14 20:33:50 | enwiki | Stagecoach | popular_title_exact | tt0031971 | Stagecoach | RobertSimons | 13 |
| 2026-11-14 20:33:55 | enwiki | The Child (2026 film) | title_with_film_hint | tt0456396 | The Child | Asqueladd | 598 |
| 2026-11-14 20:34:08 | frwiki | Sean Astin | person_exact | nm0000276 | Sean Astin | Hajun is Boundless | 2 |
| 2026-11-14 20:34:31 | frwiki | Sean Astin | person_exact | nm0000276 | Sean Astin | Hajun is Boundless | 0 |
| 2026-11-14 20:34:39 | enwiki | Stagecoach | popular_title_exact | tt0031971 | Stagecoach | RobertSimons | 4 |
| 2026-11-14 20:34:39 | enwiki | Eddie Murphy | person_exact | nm0000552 | Eddie Murphy | TheRocknRollPat | 11 |
| 2026-11-14 20:34:55 | enwiki | Eddie Murphy | person_exact | nm0000552 | Eddie Murphy | TheRocknRollPat | 9 |
| 2026-11-14 20:35:26 | enwiki | John Malkovich | person_exact | nm0000518 | John Malkovich | Peripteros | -1072 |

_Showing 25 of 62 rows. Full result: `results/csv/04_streaming_flagged_sample.csv`._
<!-- /RESULTS:04_streaming_flagged_sample -->

### Filtering strategy

TODO (team)

### Handling of late-arriving data

TODO (team) – required by the instructions.

### Methodological explanation

- **Chosen method:** TODO (team)
- **Why:** TODO (team)
- **Alternatives considered and trade-offs:** TODO (team)

## 5. Open question

### Question, why it matters, data used

TODO (team)

### Results

<!-- RESULTS:05_overall -->
**Open question - overall result**

| metric | value |
| --- | --- |
| duos analysed | 2,069 |
| avg rating together | 6.680 |
| avg baseline (apart) | 6.464 |
| mean lift | +0.216 (std. error 0.012) |
| median lift | +0.230 |
| duos with positive lift | 1,420 (68.6%) |
| sign test z-score (H0: 50% positive) | +16.95 |
<!-- /RESULTS:05_overall -->

<!-- RESULTS:05_by_decade -->
**Lift by decade of the first collaboration**

| decade | duos | avg_joint_rating | avg_baseline | mean_lift | pct_positive |
| --- | --- | --- | --- | --- | --- |
| 1910 | 4 | 6.664 | 6.849 | -0.185 | 25 |
| 1920 | 16 | 7.014 | 6.907 | 0.106 | 56.3 |
| 1930 | 162 | 6.925 | 6.816 | 0.108 | 63 |
| 1940 | 127 | 6.913 | 6.71 | 0.203 | 74 |
| 1950 | 122 | 6.892 | 6.749 | 0.143 | 68.9 |
| 1960 | 177 | 6.831 | 6.571 | 0.259 | 70.1 |
| 1970 | 181 | 6.835 | 6.578 | 0.257 | 68 |
| 1980 | 186 | 6.803 | 6.488 | 0.316 | 73.7 |
| 1990 | 305 | 6.693 | 6.383 | 0.311 | 77.4 |
| 2000 | 415 | 6.587 | 6.31 | 0.277 | 71.3 |
| 2010 | 344 | 6.345 | 6.269 | 0.076 | 58.1 |
| 2020 | 30 | 5.763 | 5.788 | -0.026 | 46.7 |

_12 rows._
<!-- /RESULTS:05_by_decade -->

<!-- RESULTS:05_by_collaboration_rank -->
**Rating of the n-th film made together (relative to the duo's baseline)**

| collab_bucket | films | avg_rating | avg_lift |
| --- | --- | --- | --- |
| 1 | 2069 | 6.918 | 0.454 |
| 2 | 2069 | 6.763 | 0.299 |
| 3 | 2069 | 6.515 | 0.051 |
| 4 | 815 | 6.509 | 0.04 |
| 5+ | 923 | 6.543 | 0.046 |

_5 rows._
<!-- /RESULTS:05_by_collaboration_rank -->

<!-- RESULTS:05_top_duos -->
**Duos with the highest lift**

| director_name | actor_name | n_joint | first_year | last_year | joint_avg | director_apart | actor_apart | lift |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Mehmet Ada Öztekin | Sarp Akkaya | 3 | 2021 | 2024 | 8.17 | 6.68 | 4.75 | 2.45 |
| Rainer Werner Fassbinder | Ulli Lommel | 3 | 1969 | 1976 | 6.83 | 7.07 | 1.7 | 2.45 |
| Francis Ford Coppola | Diane Keaton | 3 | 1972 | 1990 | 8.57 | 6.29 | 6.15 | 2.34 |
| Peter Jackson | Elijah Wood | 3 | 2001 | 2003 | 8.9 | 7.38 | 6.1 | 2.16 |
| Francis Ford Coppola | Al Pacino | 3 | 1972 | 1990 | 8.57 | 6.29 | 6.53 | 2.16 |
| Vivek Agnihotri | Mithun Chakraborty | 3 | 2019 | 2025 | 7.77 | 5.44 | 5.78 | 2.16 |
| Kevin Smith | Brian O'Halloran | 3 | 1994 | 2022 | 7.07 | 6.08 | 3.9 | 2.08 |
| Peter Jackson | Orlando Bloom | 3 | 2001 | 2003 | 8.9 | 7.38 | 6.29 | 2.06 |
| K. Selvaraghavan | Sonia Agarwal | 3 | 2003 | 2006 | 8.1 | 6.7 | 5.53 | 1.98 |
| M. Rajesh | Santhanam | 3 | 2009 | 2012 | 6.93 | 4.23 | 5.84 | 1.9 |
| Neeraj Pandey | Anupam Kher | 3 | 2008 | 2016 | 8.03 | 6.26 | 6.1 | 1.85 |
| James Cameron | Michael Biehn | 3 | 1984 | 1989 | 8 | 7.08 | 5.22 | 1.85 |
| M. Night Shyamalan | Bruce Willis | 3 | 1999 | 2019 | 7.37 | 5.7 | 5.4 | 1.82 |
| Rajkumar Santoshi | Meenakshi Sheshadri | 3 | 1990 | 1996 | 7.7 | 6.41 | 5.5 | 1.75 |
| Robert Zemeckis | Christopher Lloyd | 4 | 1985 | 1990 | 7.88 | 6.79 | 5.54 | 1.71 |

_Showing 15 of 2,069 rows. Full result: `results/csv/05_top_duos.csv`._
<!-- /RESULTS:05_top_duos -->

<!-- RESULTS:05_bottom_duos -->
**Duos with the lowest lift**

| director_name | actor_name | n_joint | first_year | last_year | joint_avg | director_apart | actor_apart | lift |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Doga Can Anafarta | Altan Erkekli | 3 | 2019 | 2021 | 2.27 | 4.57 | 6.61 | -3.32 |
| Jingming Guo | Kai Ko | 3 | 2013 | 2014 | 3.27 | 5.75 | 6.87 | -3.04 |
| Jingming Guo | Amber Kuo | 3 | 2013 | 2014 | 3.27 | 5.75 | 6.23 | -2.72 |
| Jingming Guo | Mi Yang | 3 | 2013 | 2014 | 3.27 | 5.75 | 6.12 | -2.67 |
| Edward Drake | Bruce Willis | 7 | 2021 | 2023 | 3.46 | 5.05 | 5.63 | -1.88 |
| Prabhu Deva | Salman Khan | 3 | 2009 | 2021 | 3.8 | 5.59 | 5.52 | -1.75 |
| George Gallo | Morgan Freeman | 3 | 2019 | 2021 | 4.53 | 5.99 | 6.54 | -1.73 |
| Neri Parenti | Massimo Ghini | 3 | 2005 | 2008 | 4.33 | 5.34 | 6.75 | -1.71 |
| Kartal Tibet | Halit Akçatepe | 3 | 1985 | 2004 | 5.43 | 6.62 | 7.62 | -1.69 |
| Matt Eskandari | Bruce Willis | 4 | 2019 | 2022 | 3.7 | 5.13 | 5.54 | -1.64 |

_Showing 10 of 2,069 rows. Full result: `results/csv/05_bottom_duos.csv`._
<!-- /RESULTS:05_bottom_duos -->

<!-- RESULTS:05_most_frequent_duos -->
**Most frequent duos**

| director_name | actor_name | n_joint | first_year | last_year | joint_avg | director_apart | actor_apart | lift |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Kartal Tibet | Kemal Sunal | 26 | 1976 | 1990 | 6.91 | 5.4 | 7.37 | 0.52 |
| Gerald Thomas | Sidney James | 21 | 1960 | 1974 | 6.27 | 5.45 | 6.83 | 0.13 |
| Akira Kurosawa | Toshirō Mifune | 16 | 1948 | 1965 | 7.81 | 7.25 | 7.22 | 0.58 |
| David Dhawan | Govinda | 16 | 1990 | 2009 | 6 | 4.96 | 5.56 | 0.74 |
| Priyadarshan | Mohanlal | 15 | 1985 | 2021 | 7.51 | 6.62 | 6.87 | 0.77 |
| John Ford | John Wayne | 14 | 1939 | 1963 | 7.25 | 6.9 | 6.32 | 0.64 |
| Erik Balling | Morten Grunwald | 14 | 1967 | 1981 | 6.94 | 6.75 | 6.15 | 0.49 |
| Terence Fisher | Peter Cushing | 13 | 1957 | 1974 | 6.48 | 6.28 | 5.96 | 0.36 |
| Neri Parenti | Paolo Villaggio | 13 | 1980 | 1996 | 5.98 | 4.3 | 6.21 | 0.73 |
| Rohit Shetty | Ajay Devgn | 12 | 2003 | 2024 | 5.77 | 5.27 | 5.91 | 0.18 |
| Satyajit Ray | Soumitra Chatterjee | 12 | 1959 | 1989 | 7.95 | 7.91 | 7.72 | 0.13 |
| Neri Parenti | Christian De Sica | 12 | 1985 | 2008 | 4.4 | 6.02 | 5.25 | -1.24 |
| Woody Allen | Mia Farrow | 11 | 1982 | 1992 | 7.16 | 6.86 | 6.17 | 0.65 |
| Terence Fisher | Christopher Lee | 11 | 1957 | 1968 | 6.46 | 6.32 | 5.81 | 0.4 |
| Roy William Neill | Basil Rathbone | 11 | 1942 | 1946 | 6.82 | 6.7 | 6.54 | 0.2 |

_Showing 15 of 2,069 rows. Full result: `results/csv/05_most_frequent_duos.csv`._
<!-- /RESULTS:05_most_frequent_duos -->

### Key takeaways

TODO (team)

### Methodology, why it was chosen, alternatives considered

TODO (team)
