with cte_stg_lsate as (
    select
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(upper(cast(br_cd as string))), ''), '-1')|| '#~!' || 'test', 256) as dv_hkey_lnk_customer_branch,
        cst_no is not null and br_cd is not null as hkey_lnk_not_null,
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1')|| '#~!' || 'test', 256) as dv_hkey_hub_customer,
        current_timestamp() as dv_kaf_ldt,
		monotonically_increasing_id() as dv_kaf_ofs,
		'I' as dv_cdc_ops,
		current_timestamp() as dv_src_ldt,
		'test' as dv_src_rec,
		current_timestamp() as dv_ldt,
        current_timestamp() as dv_startts,
        1 as from_stg
    from $auto_dv_psa.customer
),
cte_current_effectivity as (
    select
        dv_hkey_lnk_customer_branch,
        dv_hkey_hub_customer,
        dv_kaf_ldt, dv_kaf_ofs, dv_cdc_ops, dv_src_ldt, dv_src_rec, dv_ldt,
        dv_startts,
        0 as from_stg
    from (
        select
            lsate.*,
            lnk.dv_hkey_hub_customer,
            row_number() over (
                partition by lsate.dv_hkey_lnk_customer_branch
                order by lsate.dv_src_ldt desc, lsate.dv_kaf_ldt desc, lsate.dv_kaf_ofs desc
            ) as row_num
        from auto_dv_integration.lsate_lnk_customer_branch_2 lsate
        join auto_dv_integration.lnk_customer_branch_2 lnk
            on lsate.dv_hkey_lnk_customer_branch = lnk.dv_hkey_lnk_customer_branch
    )
    where row_num = 1 and dv_endts = timestamp'9999-12-31'
),
cte_stg_hkey_drv as (
    select
        dv_hkey_lnk_customer_branch,
        dv_hkey_hub_customer,
        dv_kaf_ldt, dv_kaf_ofs, dv_cdc_ops, dv_src_ldt, dv_src_rec, dv_ldt,
        dv_startts
    from (
        select
            *,
            row_number() over (
                partition by lsate.dv_hkey_hub_customer
                order by dv_src_ldt asc, dv_kaf_ldt asc, dv_kaf_ofs asc
            ) as row_num
        from cte_stg_lsate lsate
    )
    where row_num = 1
),
cte_new_effectivity as (
    select
        dv_hkey_lnk_customer_branch,
        dv_startts,
        lead(dv_startts, 1, timestamp'9999-12-31') over (
            partition by dv_hkey_hub_customer
            order by dv_src_ldt asc, dv_kaf_ldt asc, dv_kaf_ofs asc
        ) dv_endts,
        dv_kaf_ldt, dv_kaf_ofs, dv_cdc_ops, dv_src_ldt, dv_src_rec, dv_ldt
    from (
        select
            *,
            lag(dv_hkey_lnk_customer_branch, 1, null) over (
                partition by dv_hkey_hub_customer
                order by dv_src_ldt asc, dv_kaf_ldt asc, dv_kaf_ofs asc
            ) as prev_hkey_lnk
        from (
            select * from cte_current_effectivity
            union all
            select
                dv_hkey_lnk_customer_branch,
                dv_hkey_hub_customer,
                dv_kaf_ldt, dv_kaf_ofs, dv_cdc_ops, dv_src_ldt, dv_src_rec, dv_ldt,
                dv_startts,
                from_stg
            from cte_stg_lsate
            where hkey_lnk_not_null
        )
    )
    where not (prev_hkey_lnk <=> dv_hkey_lnk_customer_branch) and from_stg = 1
),
cte_close_effectivity as (
    select
        curr.dv_hkey_lnk_customer_branch,
        curr.dv_startts,
        stg.dv_startts as dv_endts,
        stg.dv_kaf_ldt,
        stg.dv_kaf_ofs,
        stg.dv_cdc_ops,
        stg.dv_src_ldt,
        stg.dv_src_rec,
        stg.dv_ldt
        from cte_current_effectivity curr
    join cte_stg_hkey_drv stg
        on curr.dv_hkey_hub_customer = stg.dv_hkey_hub_customer
        and curr.dv_hkey_lnk_customer_branch != stg.dv_hkey_lnk_customer_branch
)
select * from cte_new_effectivity
union all
select * from cte_close_effectivity