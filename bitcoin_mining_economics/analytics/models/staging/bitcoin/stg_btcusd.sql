SELECT  
  CAST(`Timestamp` AS INT64) `Timestamp_unix`,
  Open,
  High,
  Low,
  Close,
  Volume
FROM {{source('btc_src', 'btcusd_src')}}