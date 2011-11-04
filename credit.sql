select 
  *,
  (credit_lag - credit) credit_diff
from
(select 
  circuit_id,
  meter_time, 
  gateway_time,
  credit, 
  watthours,
  coalesce(
    lag(credit, 1) over (partition by circuit_id order by meter_time), 0
  ) credit_lag
from
  clean_log
) bc
