select
    cast(dv_hkey_hub_branch as string) as error_value,
    'dv_hkey_hub_branch' as error_column,
    'unique' as error_code
from auto_dv_integration.hub_branch
group by dv_hkey_hub_branch
having count(1) > 1
union all
select
    cast(dv_hkey_hub_branch as string) as error_value,
    'dv_hkey_hub_branch' as error_column,
    'not_null' as error_code
from auto_dv_integration.hub_branch
where dv_hkey_hub_branch is null