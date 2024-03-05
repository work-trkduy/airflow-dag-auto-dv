select
    cast(dv_hkey_lnk_customer_branch as string) as error_value,
    'dv_hkey_lnk_customer_branch' as error_column,
    'unique' as error_code
from auto_dv_integration.lnk_customer_branch
group by dv_hkey_lnk_customer_branch
having count(1) > 1
union all
select
    cast(dv_hkey_lnk_customer_branch as string) as error_value,
    'dv_hkey_lnk_customer_branch' as error_column,
    'not_null' as error_code
from auto_dv_integration.lnk_customer_branch
where dv_hkey_lnk_customer_branch is null
union all
select
    cast(dv_hkey_hub_customer as string) as error_value,
    'dv_hkey_hub_customer' as error_column,
    'orphan' as error_code
from auto_dv_integration.lnk_customer_branch a
where not exists (
    select 1 from auto_dv_integration.hub_customer b
    where a.dv_hkey_hub_customer = b.dv_hkey_hub_customer
)
union all
select
    cast(dv_hkey_hub_branch as string) as error_value,
    'dv_hkey_hub_branch' as error_column,
    'orphan' as error_code
from auto_dv_integration.lnk_customer_branch a
where not exists (
    select 1 from auto_dv_integration.hub_branch b
    where a.dv_hkey_hub_branch = b.dv_hkey_hub_branch
)