**Streaming metrics (from StreamingQueryProgress)**

| query | batches | input rows | avg input rows/s | avg processed rows/s | avg batch ms | max batch ms | rows dropped by watermark | max state rows |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| flagged_console | 130 | 2,488 | 1.6 | 11.8 | 1470 | 4,666 | 0 | 2,026 |
| flagged_to_parquet | 131 | 2,488 | 1.6 | 5.8 | 3447 | 6,832 | 0 | 2,026 |
| window_counts | 130 | 2,488 | 1.6 | 5.5 | 3647 | 6,898 | 0 | 2,026 |

_Run from 2026-11-14T20:29:19.840Z to 2026-11-14T20:51:00.000Z; watermark 10 minutes, window 5 minutes, trigger 10 seconds._
