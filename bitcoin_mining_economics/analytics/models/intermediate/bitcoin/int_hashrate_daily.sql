SELECT 
  DATE(TIMESTAMP_SECONDS(Timestamp_unix)) `date`,
  AVG(avgHashrate) as avg_hashrate
FROM {{ref('stg_hashrate')}}
GROUP BY `date`
ORDER BY `date` 