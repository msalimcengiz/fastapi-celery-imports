55 imports of 20000 rows (1% invalid), one uploaded per second = 1100000 rows in 1100 chunks. SIGKILLed 18 import-worker containers (every 3 s) and RabbitMQ once; 0.02 of chunk attempts failed on purpose.

| check | violations |
|---|---:|
| imports not finished | **0** |
| imports FAILED | **0** |
| chunks not DONE | **0** |
| imports whose ok + error rows ≠ total, or ≠ the expected 19800 / 200 | **0** |
| imports whose product count ≠ ok rows (lost or double-applied chunk) | **0** |
| imports whose error-table count ≠ error rows | **0** |

Chunks that ran more than once (redelivered after a kill, or retried): **42** of 1100. Imports re-planned by the sweeper: 0.

**PASS**
