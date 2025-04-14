SELECT
  btcusd.date,
  btcusd.open,
  btcusd.high,
  btcusd.low,
  btcusd.close,
  btcusd.volume,
  br.block_reward,
  hr.avg_hashrate,
  (
    (btcusd.close * br.block_reward * 144) / hr.avg_hashrate
  ) * 1e15 AS hashprice_usd_per_ph_day
FROM {{ref('int_btcusd_daily')}} btcusd
LEFT JOIN {{ref('int_hashrate_daily')}} hr 
  ON btcusd.date = hr.date
LEFT JOIN {{ref('int_block_reward')}} br 
  ON btcusd.date = br.date
