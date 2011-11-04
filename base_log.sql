select distinct
  circuit_id,
  first_value(gateway_time) over circuit_window last_gwy_time,
  first_value(watthours) over circuit_window last_watthours,
  first_value(credit) over circuit_window last_credit
from 
  clean_log 
window circuit_window as (partition by circuit_id order by gateway_time desc)

