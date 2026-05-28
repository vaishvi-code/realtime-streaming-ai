-- models/silver/stg_posts.sql
{{ config(materialized='view') }}

SELECT
    id,
    source,
    TRIM(title)                           AS title,
    COALESCE(TRIM(text), '')              AS body,
    url,
    CAST(score AS INTEGER)                AS score,
    author,
    CAST(num_comments AS INTEGER)         AS num_comments,
    sentiment_label,
    CAST(sentiment_compound   AS DOUBLE)  AS sentiment_compound,
    CAST(vader_pos            AS DOUBLE)  AS vader_positive,
    CAST(vader_neg            AS DOUBLE)  AS vader_negative,
    CAST(vader_neu            AS DOUBLE)  AS vader_neutral,
    CAST(textblob_polarity    AS DOUBLE)  AS textblob_polarity,
    CAST(textblob_subjectivity AS DOUBLE) AS textblob_subjectivity,
    CAST(sentiment_confidence AS DOUBLE)  AS sentiment_confidence,
    TRY_CAST(ingested_at AS TIMESTAMP)    AS ingested_at,
    processed_at,
    date_partition

FROM read_parquet('../data/silver/posts/**/*.parquet', hive_partitioning=true)

WHERE id IS NOT NULL AND title IS NOT NULL AND title != ''
