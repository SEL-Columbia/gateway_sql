select 
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
  meter on (meter.id=circuit.meter)
