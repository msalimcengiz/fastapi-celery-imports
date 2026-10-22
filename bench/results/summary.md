No faults injected. Each run: 20 imports of 100000 rows (1% invalid), all uploaded back to back, 1,000-row chunks.
Machine: Apple M4, Docker VM with 4 CPUs and 8 GiB shared by every container (API, workers, RabbitMQ, Postgres).

| import workers × processes | imports | rows | valid rows | invalid rows (reported by line) | products stored | seconds (first upload → last finished) | rows/s | per-import p50 s | per-import max s | imports not in the expected state |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 × 4 | 20 | 2000000 | 1980000 | 20000 | 1980000 | 34.5 | **57,971** | 32.6 | 34.0 | 0 |
| 2 × 4 | 20 | 2000000 | 1980000 | 20000 | 1980000 | 34.1 | **58,651** | 31.6 | 33.3 | 0 |
| 4 × 4 | 20 | 2000000 | 1980000 | 20000 | 1980000 | 32.8 | **60,976** | 29.9 | 31.9 | 0 |
