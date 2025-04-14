WITH ranked as (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY DATE(TIMESTAMP_SECONDS(Timestamp_unix)) ORDER BY Timestamp_unix DESC) close_rank,
    ROW_NUMBER() OVER(PARTITION BY DATE(TIMESTAMP_SECONDS(Timestamp_unix)) ORDER BY Timestamp_unix ASC) open_rank,
  FROM {{ref('stg_btcusd')}}
)

SELECT 
  DATE(TIMESTAMP_SECONDS(Timestamp_unix)) `date`,
  ANY_VALUE(CASE WHEN open_rank=1 THEN Open END) as open,
  MAX(High) as high,
  MIN(Low) as low,
  ANY_VALUE(CASE WHEN close_rank=1 THEN Close END) as close,
  SUM(Volume) as volume
FROM ranked
GROUP BY `date`
ORDER BY `date`