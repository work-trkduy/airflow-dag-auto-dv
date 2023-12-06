with cte_stg_lnk as (
    select
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(upper(cast(br_cd as string))), ''), '-1')|| '#~!' || 'test', 256) as dv_hkey_lnk_customer_branch,
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1')|| '#~!' || '', 256) as dv_hkey_hub_customer,
	sha2(coalesce(nullif(rtrim(upper(cast(br_cd as string))), ''), '-1')|| '#~!' || '', 256) as dv_hkey_hub_branch,
        current_timestamp() as dv_kaf_ldt,
	monotonically_increasing_id() as dv_kaf_ofs,
	'I' as dv_cdc_ops,
	current_timestamp() as dv_src_ldt,
	'test' as dv_src_rec,
	current_timestamp() as dv_ldt
    from $auto_dv_psa.customer
    where cst_no is not null and br_cd is not null
),
cte_stg_lnk_latest_records as (
select * from (
    select
        *,
        row_number() over (
            partition by dv_hkey_lnk_customer_branch
            order by dv_src_ldt asc, dv_kaf_ldt asc, dv_kaf_ofs asc
        ) as row_num
    from cte_stg_lnk
)
where row_num = 1
),
cte_stg_lnk_existed_keys (
    select dv_hkey_lnk_customer_branch
    from cte_stg_lnk src
    where exists (
        select 1
        from auto_dv_integration.lnk_customer_branch tgt
        where tgt.dv_hkey_lnk_customer_branch = src.dv_hkey_lnk_customer_branch
    )
)
select
    dv_hkey_lnk_customer_branch,
    dv_hkey_hub_customer,
	dv_hkey_hub_branch,
    dv_kaf_ldt,
	dv_kaf_ofs,
	dv_cdc_ops,
	dv_src_ldt,
	dv_src_rec,
	dv_ldt
from cte_stg_lnk_latest_records src
where not exists (
    select 1
    from cte_stg_lnk_existed_keys tgt
    where tgt.dv_hkey_lnk_customer_branch = src.dv_hkey_lnk_customer_branch
)