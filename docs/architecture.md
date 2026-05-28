# Architecture Deep-Dive

## System Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   Hacker News Firebase API          Reddit Public JSON API      │
│   (no auth, free, ~500 stories/hr)  (no auth, rate-limited)    │
└─────────────────┬───────────────────────────┬───────────────────┘
                  │                           │
                  ▼                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    KAFKA (Docker, local)                        │
│                                                                 │
│   Topic: raw-posts (3 partitions)    Topic: dead-letter-queue   │
│                                                                 │
│   Producer features:                                           │
│   • Idempotent keys (MD5 of source:id)                         │
│   • acks=all for durability                                     │
│   • Snappy compression                                          │
│   • In-memory dedup (seen_ids set)                              │
│   • DLQ routing for malformed records                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              SPARK STRUCTURED STREAMING                         │
│                          (local[*])                             │
│                                                                 │
│   Bronze ──► Silver ──► (Gold via dbt)                         │
│                                                                 │
│   Bronze: raw Parquet (schema-inferred, append-only)           │
│   Silver: foreachBatch handler:                                 │
│     1. Parse JSON → StructType schema                           │
│     2. Drop nulls, deduplicate on (id, source)                  │
│     3. VADER + TextBlob sentiment enrichment                    │
│     4. Write partitioned Parquet (source / date)                │
│                                                                 │
│   Trigger: every 30 seconds (micro-batch)                       │
│   Checkpoints: local filesystem (restart-safe)                  │
└────────────────────────┬────────────────────────────────────────┘
                         │ (Parquet files)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    dbt + DuckDB                                  │
│                                                                 │
│   silver/stg_posts.sql                                          │
│     → read_parquet() over Silver layer                          │
│     → type casting, COALESCE, TRIM                              │
│                                                                 │
│   gold/sentiment_trends.sql                                     │
│     → 5-minute windowed aggregations per source                 │
│     → avg compound, post count, % of window                     │
│                                                                 │
│   gold/topic_leaderboard.sql                                    │
│     → tokenize titles, filter stop words                        │
│     → keyword frequency, avg sentiment, positive %              │
│                                                                 │
│   DuckDB: columnar, zero-config, no server needed               │
└────────────────────────┬────────────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
┌──────────────┐ ┌─────────────┐ ┌──────────────┐
│   AIRFLOW    │ │  STREAMLIT  │ │  KAFKA UI    │
│              │ │  DASHBOARD  │ │              │
│  DAG: every  │ │             │ │  Topic lag,  │
│  15 minutes  │ │  Sentiment  │ │  throughput, │
│              │ │  trends,    │ │  partitions  │
│  Tasks:      │ │  keywords,  │ │              │
│  • Kafka     │ │  KPI cards  │ │  :8090       │
│    health    │ │             │ │              │
│  • dbt run   │ │  :8501      │ └──────────────┘
│  • dbt test  │ └─────────────┘
│  • DQ checks │
│  • Alerting  │
│  :8080       │
└──────────────┘
```

---

## Medallion Architecture

| Layer  | Format   | Location              | Trigger          |
|--------|----------|-----------------------|------------------|
| Bronze | Parquet  | data/bronze/posts/    | Every 30s (Spark)|
| Silver | Parquet  | data/silver/posts/    | Every 30s (Spark)|
| Gold   | DuckDB   | data/gold/*.duckdb    | Every 15min (Airflow → dbt)|

---

## Sentiment Model: Why VADER + TextBlob Ensemble?

**VADER** (Valence Aware Dictionary and sEntiment Reasoner):
- Designed specifically for social media text
- Handles slang, ALL CAPS, punctuation!!!
- Sub-millisecond latency
- Rule-based = fully explainable

**TextBlob**:
- Machine-learning backed (Naive Bayes on movie reviews corpus)
- Better for formal, longer-form text
- Provides subjectivity score (factual vs opinionated)

**Ensemble logic:**
```
compound = 0.6 * vader_compound + 0.4 * textblob_polarity
confidence = model_agreement * signal_magnitude
label = MIXED if models strongly disagree else POSITIVE/NEGATIVE/NEUTRAL
```

**Why not GPT-4 / Claude API?**
- Latency: LLM API ~500-2000ms vs VADER <1ms per document
- Cost: at 500 posts/hr, API calls would cost $5-50/day
- Throughput: API rate limits would bottleneck the stream
- For production: use a fine-tuned DistilBERT on HuggingFace (still free)

---

## Production Trade-offs (For Interview Discussions)

### Why Kafka over direct API → Spark?
Kafka decouples producers and consumers. If Spark goes down, messages are retained
and replayed. Direct connection = lost data during consumer downtime.

### Why foreachBatch instead of Spark UDFs for sentiment?
VADER uses native Python objects that can't be serialized by Spark's Pickle.
`foreachBatch` gives us a micro-batch DataFrame we can `.collect()` and process
in Python, then write back to Spark. Trade-off: data must fit in driver memory.
For large scale, use a proper Spark UDF with a pure-Python model.

### Why DuckDB over PostgreSQL for Gold?
DuckDB is columnar (OLAP), processes Parquet natively, and needs zero config.
For a local portfolio project, it's 10x faster for analytical queries.
In production, swap for Snowflake or BigQuery.

### Why Parquet for Bronze/Silver?
Columnar storage means we only read the columns we query (compression ~10:1).
Parquet is the de-facto standard for data lake layers (Iceberg, Delta, Hudi all use it).
Schema evolution is supported via optional fields.

### Dead-Letter Queue pattern
Any message that fails parsing/enrichment is routed to the DLQ topic instead
of crashing the consumer. This is critical for production reliability — one
malformed record should not stop the stream.

---

## Extending to Production

```
Local                           Production equivalent
─────                           ─────────────────────
Kafka (Docker)           →      Confluent Cloud / AWS MSK
Spark local[*]           →      AWS EMR / Databricks
Parquet on disk          →      S3 / GCS / ADLS
DuckDB                   →      Snowflake / BigQuery
Airflow (Docker)         →      MWAA / Astronomer
Streamlit                →      Databricks SQL Dashboard / Looker
VADER + TextBlob         →      HuggingFace DistilBERT (fine-tuned)
```
