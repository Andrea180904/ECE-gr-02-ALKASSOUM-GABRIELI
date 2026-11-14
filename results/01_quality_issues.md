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
