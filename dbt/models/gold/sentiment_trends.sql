-- models/gold/sentiment_trends.sql
-- Gold layer: 5-minute windowed sentiment aggregations per source
-- This is what powers the live dashboard charts

{{
  config(
    materialized = 'table'
  )
}}

WITH windowed AS (
    SELECT
        source,
        sentiment_label,
        date_trunc('hour', ingested_at)     AS hour_window,
        -- 5-minute buckets
        date_trunc('minute', ingested_at)
            - INTERVAL (EXTRACT(MINUTE FROM ingested_at)::INT % 5) MINUTE
                                            AS five_min_window,

        COUNT(*)                            AS post_count,
        AVG(sentiment_compound)             AS avg_sentiment,
        AVG(sentiment_confidence)           AS avg_confidence,
        SUM(score)                          AS total_score,
        SUM(num_comments)                   AS total_comments,
        MAX(ingested_at)                    AS latest_ingested_at

    FROM {{ ref('stg_posts') }}
    WHERE ingested_at IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

with_pct AS (
    SELECT
        *,
        ROUND(
            100.0 * post_count / SUM(post_count) OVER (PARTITION BY five_min_window, source),
            2
        ) AS pct_of_window
    FROM windowed
)

SELECT
    source,
    sentiment_label,
    hour_window,
    five_min_window,
    post_count,
    ROUND(avg_sentiment, 4)    AS avg_sentiment_score,
    ROUND(avg_confidence, 4)   AS avg_confidence,
    total_score,
    total_comments,
    pct_of_window,
    latest_ingested_at,
    CURRENT_TIMESTAMP           AS dbt_updated_at

FROM with_pct
ORDER BY five_min_window DESC, source, sentiment_label
