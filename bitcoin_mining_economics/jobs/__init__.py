import dagster as dg 

btc_hashrate_daily = dg.AssetSelection.groups("btc_hashrate")
btc_hashrate_job = dg.define_asset_job(
    name = "btc_hashrate_job",
    selection=btc_hashrate_daily 
)