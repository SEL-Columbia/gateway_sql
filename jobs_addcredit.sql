select 
  jobs.id,
  jobs.start,
  jobs.end,
  jobs.circuit_id,
  ac.credit
from
  jobs
join
  addcredit ac
on jobs.id=ac.id
