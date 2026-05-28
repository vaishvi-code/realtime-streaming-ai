-- models/gold/topic_leaderboard.sql
-- Gold layer: top keywords/topics by frequency and average sentiment

{{
  config(
    materialized = 'table'
  )
}}

WITH tokenized AS (
    SELECT
        source,
        sentiment_label,
        sentiment_compound,
        score,
        -- Extract individual words from title (simple tokenisation)
        UNNEST(
            string_split(
                regexp_replace(lower(title), '[^a-z0-9 ]', '', 'g'),
                ' '
            )
        ) AS word
    FROM {{ ref('stg_posts') }}
    WHERE title IS NOT NULL
),

filtered AS (
    SELECT *
    FROM tokenized
    WHERE
        -- Remove common stop words
        word NOT IN (
            'the','a','an','and','or','but','in','on','at','to','for',
            'of','with','by','from','is','it','this','that','are','was',
            'be','as','i','we','you','he','she','they','has','have',
            'had','not','no','so','if','do','did','get','got','can',
            'will','its','our','my','your','his','her','their','how',
            'what','when','where','why','who','all','more','also','just',
            'been','about','up','out','into','than','then','them','would',
            'could','should','new','one','two','after','before','over'
        )
        AND LENGTH(word) > 3
        AND word != ''
),

aggregated AS (
    SELECT
        word                            AS keyword,
        source,
        COUNT(*)                        AS mention_count,
        AVG(sentiment_compound)         AS avg_sentiment,
        AVG(score)                      AS avg_post_score,
        SUM(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 ELSE 0 END) AS positive_count,
        SUM(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 ELSE 0 END) AS negative_count,
        SUM(CASE WHEN sentiment_label = 'NEUTRAL'  THEN 1 ELSE 0 END) AS neutral_count
    FROM filtered
    GROUP BY 1, 2
)

SELECT
    keyword,
    source,
    mention_count,
    ROUND(avg_sentiment, 4)         AS avg_sentiment_score,
    ROUND(avg_post_score, 1)        AS avg_post_score,
    positive_count,
    negative_count,
    neutral_count,
    ROUND(
        100.0 * positive_count / NULLIF(mention_count, 0), 1
    )                               AS positive_pct,
    CURRENT_TIMESTAMP               AS dbt_updated_at

FROM aggregated
WHERE mention_count >= 2
ORDER BY mention_count DESC
