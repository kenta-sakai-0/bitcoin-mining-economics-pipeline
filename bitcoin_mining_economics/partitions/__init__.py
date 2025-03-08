import dagster as dg
from assets import constants

start_date = constants.START_DATE
end_date = "2025-02-28"

monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)
