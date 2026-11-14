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
<!-- /RESULTS:00_ingestion -->

### Missing values per column

<!-- RESULTS:01_missing_values -->
<!-- /RESULTS:01_missing_values -->

### Five columns with the highest share of missing values

<!-- RESULTS:01_top5_missing_columns -->
<!-- /RESULTS:01_top5_missing_columns -->

### Other data-quality issues and how they are handled

<!-- RESULTS:01_quality_issues -->
<!-- /RESULTS:01_quality_issues -->

<!-- RESULTS:01_cleaning_summary -->
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
<!-- /RESULTS:02_q1_runtime_outliers_by_genre -->

<!-- RESULTS:02_q1_runtime_outliers_examples -->
<!-- /RESULTS:02_q1_runtime_outliers_examples -->

### 2.2 Top three genres per year (1980–2025)

<!-- RESULTS:02_q2_top3_genres -->
<!-- /RESULTS:02_q2_top3_genres -->

<!-- RESULTS:02_q2_top3_frequency -->
<!-- /RESULTS:02_q2_top3_frequency -->

### 2.3 Top three genres compared with the other genres

<!-- RESULTS:02_q3_top3_vs_rest_overall -->
<!-- /RESULTS:02_q3_top3_vs_rest_overall -->

<!-- RESULTS:02_q3_top3_vs_rest -->
<!-- /RESULTS:02_q3_top3_vs_rest -->

### 2.4 Directors with a single movie rated 8.0 or more

<!-- RESULTS:02_q4_one_hit_directors -->
<!-- /RESULTS:02_q4_one_hit_directors -->

### 2.5 TV series: best episode early, worst episode in the last season

<!-- RESULTS:02_q5_series_decline -->
<!-- /RESULTS:02_q5_series_decline -->

### 2.6 Actors and actresses successful in several languages

<!-- RESULTS:02_q6_language_distribution -->
<!-- /RESULTS:02_q6_language_distribution -->

<!-- RESULTS:02_q6_languages_per_actor -->
<!-- /RESULTS:02_q6_languages_per_actor -->

<!-- RESULTS:02_q6_multilingual_actors -->
<!-- /RESULTS:02_q6_multilingual_actors -->

### 2.7 Long careers with an early popularity peak

<!-- RESULTS:02_q7_summary -->
<!-- /RESULTS:02_q7_summary -->

<!-- RESULTS:02_q7_early_peak_actors -->
<!-- /RESULTS:02_q7_early_peak_actors -->

### 2.8 Multi-talented film professionals

<!-- RESULTS:02_q8_role_combinations -->
<!-- /RESULTS:02_q8_role_combinations -->

<!-- RESULTS:02_q8_multi_vs_single_summary -->
<!-- /RESULTS:02_q8_multi_vs_single_summary -->

<!-- RESULTS:02_q8_multi_talented_people -->
<!-- /RESULTS:02_q8_multi_talented_people -->

### Methodological explanation

- **Chosen method:** TODO (team) – for each question, explain the
  definitions and thresholds used.
- **Why:** TODO (team)
- **Alternatives considered and trade-offs:** TODO (team)

## 3. Data enrichment

### Scope

<!-- RESULTS:03_enrichment_targets -->
<!-- /RESULTS:03_enrichment_targets -->

### Coverage and flagged gaps

<!-- RESULTS:03_enrichment_coverage -->
<!-- /RESULTS:03_enrichment_coverage -->

<!-- RESULTS:03_enrichment_synopsis_sections -->
<!-- /RESULTS:03_enrichment_synopsis_sections -->

<!-- RESULTS:03_enrichment_notes -->
<!-- /RESULTS:03_enrichment_notes -->

### Samples

<!-- RESULTS:03_enrichment_films_sample -->
<!-- /RESULTS:03_enrichment_films_sample -->

<!-- RESULTS:03_enrichment_people_sample -->
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
<!-- /RESULTS:04_streaming_metrics -->

<!-- RESULTS:04_streaming_by_match_type -->
<!-- /RESULTS:04_streaming_by_match_type -->

<!-- RESULTS:04_streaming_by_wiki -->
<!-- /RESULTS:04_streaming_by_wiki -->

<!-- RESULTS:04_streaming_top_pages -->
<!-- /RESULTS:04_streaming_top_pages -->

<!-- RESULTS:04_streaming_windows -->
<!-- /RESULTS:04_streaming_windows -->

<!-- RESULTS:04_streaming_flagged_sample -->
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
<!-- /RESULTS:05_overall -->

<!-- RESULTS:05_by_decade -->
<!-- /RESULTS:05_by_decade -->

<!-- RESULTS:05_by_collaboration_rank -->
<!-- /RESULTS:05_by_collaboration_rank -->

<!-- RESULTS:05_top_duos -->
<!-- /RESULTS:05_top_duos -->

<!-- RESULTS:05_bottom_duos -->
<!-- /RESULTS:05_bottom_duos -->

<!-- RESULTS:05_most_frequent_duos -->
<!-- /RESULTS:05_most_frequent_duos -->

### Key takeaways

TODO (team)

### Methodology, why it was chosen, alternatives considered

TODO (team)
