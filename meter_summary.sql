-- ASSUMES CLEAN_LOG table exists
-- can be created via clean_primary_log.sql
select 
  bm.ip_address, 
  bm.pin, 
  bl.last_gwy_time, 
  bm.status, 
  bm.lang, 
  coalesce(bjac.count_credits, 0) num_recharges, 
  bc.sum_credit, 
  bl.last_credit, 
  bl.last_watthours 
from 
  -- meter.sql
  -- gather circuit, account info & meter_id
  (select 
    circuit.id circuit_id,
    circuit.ip_address,
    circuit.status,
    circuit.pin,
    circuit.credit,
    account.lang, 
    meter.id meter_id
  from 
    circuit
  join
    account on (account.id=circuit.account_id) 
  join
    meter on (meter.id=circuit.meter)) bm 
join 
  -- base_log.sql
  -- get the latest values of time, watthours, credit (by gateway_time) 
  -- for each circuit
  (select distinct
    circuit_id,
    first_value(gateway_time) over circuit_window last_gwy_time,
    first_value(watthours) over circuit_window last_watthours,
    first_value(credit) over circuit_window last_credit
  from 
    clean_log 
  window circuit_window as (partition by circuit_id order by gateway_time desc)) bl 
on bm.circuit_id=bl.circuit_id 
left outer join 
  -- get the sum of credits for each circuit
  (select 
    sum(credit_diff) sum_credit, 
    circuit_id 
  from 
    -- credit.sql
    -- gets the credit differentials
    (select 
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
    ) bcr 
  where credit_diff > 0 and 
        meter_time > (current_date - 30) and 
        meter_time <= current_date 
  group by circuit_id) bc 
on bm.circuit_id=bc.circuit_id 
left outer join 
  -- jobs_addcredit.sql
  -- get the count of add_credit xactions per circuit
  (select 
    count(credit) count_credits, 
    circuit_id 
  from 
    (select 
      jobs.id,
      jobs.start,
      jobs.end,
      jobs.circuit_id,
      ac.credit
    from
      jobs
    join
      addcredit ac
    on jobs.id=ac.id) bjacr 
  where start > (current_date - 30) 
  group by circuit_id) bjac 
on bm.circuit_id=bjac.circuit_id 
where bm.meter_id=:meter_id 
order by ip_address;
