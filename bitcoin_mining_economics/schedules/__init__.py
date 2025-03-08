import dagster as dg 
from jobs import btc_hashrate_job

hashrate_update_schedule = dg.ScheduleDefinition(
    job = btc_hashrate_job, 
    cron_schedule= "0 0 * * *"
)