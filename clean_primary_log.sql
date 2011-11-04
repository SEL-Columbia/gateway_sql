select distinct
  pl.circuit_id,
  l.date meter_time,
  first_value(pl.created) over circuit_time gateway_time,
  first_value(pl.credit) over circuit_time credit,
  first_value(pl.watthours) over circuit_time watthours
from
  log l,
  primary_log pl
where
  l.id=pl.id
window circuit_time as (partition by pl.circuit_id, l.date order by pl.created desc)
