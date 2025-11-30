# Distributed Web Crawler

A decentralized, fault-tolerant web crawler built with Python, Redis, and MongoDB. Designed for horizontal scalability with no single point of failure.

## Performance

| Metric | Value |
|--------|-------|
| Max Workers | 500+ (tested) |
| Throughput | 50K URLs/sec |
| Memory (10M URLs) | 14 MB (Bloom filter) |
| Storage Savings | 90% (zlib compression) |

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Worker 1   │     │   Worker 2   │     │   Worker N   │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────────────────┼────────────────────┘
                            ▼
                 ┌──────────────────┐
                 │      Redis       │
                 │  (Coordination)  │
                 │  • URL Frontier  │
                 │  • Bloom Filter  │
                 │  • Domain Locks  │
                 └────────┬─────────┘
                          │
                 ┌────────▼─────────┐
                 │     MongoDB      │
                 │    (Storage)     │
                 └──────────────────┘
```

**Key Design Decisions:**
- **Decentralized workers** – No master bottleneck, workers coordinate via Redis
- **Bloom filter deduplication** – 98% memory reduction vs Redis Sets
- **Distributed politeness** – Per-domain rate limiting using Redis locks
- **Batch writes** – 20x faster MongoDB inserts

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start Redis and MongoDB
redis-server &
mongod --dbpath=/tmp/mongodb &

# Launch crawler (10 workers, 100 pages each)
./scripts/start_v3.sh 10 100

# Stop crawler
./scripts/stop_v3.sh
```

## Project Structure

```
src/
├── v3/
│   ├── worker_v3.py          # Autonomous crawler workers
│   ├── bloom_filter.py       # Memory-efficient URL deduplication
│   ├── politeness.py         # Distributed rate limiting
│   └── optimized_storage.py  # Compressed MongoDB storage
├── config.py                 # Configuration
└── robots_handler_async.py   # Async robots.txt parsing

scripts/
├── start_v3.sh               # Launch workers
└── stop_v3.sh                # Graceful shutdown
```

## Core Components

### Bloom Filter
Probabilistic URL deduplication with 0.1% false positive rate. Uses 14 MB for 10M URLs vs 1.4 GB with Redis Sets.

### Politeness Manager
Redis-based distributed locks enforce per-domain crawl delays. Respects robots.txt and automatically recovers stale URLs on worker failure.

### Optimized Storage
Zlib compression reduces storage by 90%. Batch inserts provide 20x write speedup. Split collections enable fast metadata queries.

## Monitoring

```bash
# Real-time stats
python3 src/v3/master_v3.py monitor

# Queue size
redis-cli ZCARD crawler:frontier

# Crawled pages count
mongosh web_crawler --eval "db.pages_metadata.countDocuments()"
```

## Requirements

- Python 3.8+
- Redis 6.0+
- MongoDB 4.4+

## License

MIT
