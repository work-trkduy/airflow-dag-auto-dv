select
    cast(dv_hkey_lnk_customer_branch as string) as error_value,
    'dv_hkey_lnk_customer_branch' as error_column,
    'unique' as error_code
from auto_dv_integration.lnk_customer_branch_2
group by dv_hkey_lnk_customer_branch
having count(1) > 1
union all
select
    cast(dv_hkey_lnk_customer_branch as string) as error_value,
    'dv_hkey_lnk_customer_branch' as error_column,
    'not_null' as error_code
from auto_dv_integration.lnk_customer_branch_2
where dv_hkey_lnk_customer_branch is null