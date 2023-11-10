select
    cast(dv_hkey_sat_customer as string) as error_value,
    'dv_hkey_sat_customer' as error_column,
    'unique' as error_code
from auto_dv_integration.sat_customer_2
group by dv_hkey_sat_customer
having count(1) > 1
union all
select
    cast(dv_hkey_sat_customer as string) as error_value,
    'dv_hkey_sat_customer' as error_column,
    'not_null' as error_code
from auto_dv_integration.sat_customer_2
where dv_hkey_sat_customer is null
union all
select
    cast(dv_hkey_hub_customer as string) as error_value,
    'dv_hkey_hub_customer' as error_column,
    'not_null' as error_code
from auto_dv_integration.sat_customer_2
where dv_hkey_hub_customer is null
union all
select
    cast(dv_hkey_hub_customer as string) as error_value,
    'dv_hkey_hub_customer' as error_column,
    'orphan' as error_code
from auto_dv_integration.sat_customer_2 a
where not exists (
    select 1 from auto_dv_integration.hub_customer b
    where a.dv_hkey_hub_customer = b.dv_hkey_hub_customer
)