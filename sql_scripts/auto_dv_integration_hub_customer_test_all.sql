select
    cast(dv_hkey_hub_customer as string) as error_value,
    'dv_hkey_hub_customer' as error_column,
    'unique' as error_code
from auto_dv_integration.hub_customer
group by dv_hkey_hub_customer
having count(1) > 1
union all
select
    cast(dv_hkey_hub_customer as string) as error_value,
    'dv_hkey_hub_customer' as error_column,
    'not_null' as error_code
from auto_dv_integration.hub_customer
where dv_hkey_hub_customer is null