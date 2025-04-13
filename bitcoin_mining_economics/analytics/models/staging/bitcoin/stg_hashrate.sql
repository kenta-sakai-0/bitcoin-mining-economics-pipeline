SELECT  
  `timestamp` as Timestamp_unix,
  avgHashrate 
FROM {{source('btc_src', 'hashrate_src')}}