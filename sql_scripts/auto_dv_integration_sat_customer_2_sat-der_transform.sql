with cte_stg_sat as (
    select
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(upper(cast(br_cd as string))), ''), '-1')|| '#~!' || 'test' || '#~!' || coalesce(nullif(rtrim(cast(create_dt as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_timestamp() as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_timestamp() as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(monotonically_increasing_id() as string)), ''), '-1'), 256) as dv_hkey_sat_customer,
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(upper(cast(br_cd as string))), ''), '-1')|| '#~!' || 'test', 256) as dv_hkey_hub_customer,
        sha2(coalesce(nullif(rtrim(cast(cst_nm as string)), ''), repeat('0',16)) || '#~!' || coalesce(nullif(rtrim(cast(cst_full_nm as string)), ''), repeat('0',16)) || '#~!' || coalesce(nullif(rtrim(cast(cst_type as string)), ''), repeat('0',16)), 256) as dv_hsh_dif,
        create_dt,
        cst_nm,
		cst_full_nm,
		cst_type,
        current_timestamp() as dv_kaf_ldt,
		monotonically_increasing_id() as dv_kaf_ofs,
		'I' as dv_cdc_ops,
		current_timestamp() as dv_src_ldt,
		'test' as dv_src_rec,
		current_timestamp() as dv_ldt,
        'test' as dv_ccd,
        -1 as row_num
    from $auto_dv_psa.customer
    where 1=1
),
cte_sat_der_set_row_num as (
    select
        dv_hkey_sat_customer,
        dv_hkey_hub_customer,
        dv_hsh_dif,
        create_dt,
        cst_nm,
		cst_full_nm,
		cst_type,
        dv_kaf_ldt,
		dv_kaf_ofs,
		dv_cdc_ops,
		dv_src_ldt,
		dv_src_rec,
		dv_ldt,
        dv_ccd,
        row_number() over (
            partition by dv_hkey_hub_customer, create_dt
            order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc
        ) as row_num
    from auto_dv_integration.sat_der_customer_2 tgt
    where exists (
        select 1
        from cte_stg_sat src
        where tgt.dv_hkey_hub_customer = src.dv_hkey_hub_customer
            and tgt.create_dt = src.create_dt
            )
),
cte_sat_der_latest_records as (
    select *
    from cte_sat_der_set_row_num
    where row_num = 1
),
cte_stg_sat_der_dedup_hsh_dif as (
    select *
    from (
        select
            *,
            lag(dv_hsh_dif, 1, null) over w as prev_hash_diff,
            lag(dv_cdc_ops, 1, null) over w as prev_cdc_ops
        from (
            select * from cte_stg_sat
            union
            select * from cte_sat_der_latest_records
        )
        window w as (
            partition by dv_hkey_hub_customer, create_dt
            order by dv_src_ldt asc, dv_kaf_ldt asc, dv_kaf_ofs asc
        )
    )
    where
        prev_hash_diff != dv_hsh_dif or
        prev_hash_diff is null or
        lower(dv_cdc_ops) = lower('d') or
        lower(prev_cdc_ops) = lower('d')
)
select
    dv_hkey_sat_customer,
    dv_hkey_hub_customer,
    dv_hsh_dif,
    create_dt,
    cst_nm,
	cst_full_nm,
	cst_type,
    dv_kaf_ldt,
	dv_kaf_ofs,
	dv_cdc_ops,
	dv_src_ldt,
	dv_src_rec,
	dv_ldt,
    dv_ccd
from cte_stg_sat_der_dedup_hsh_dif
where row_num = -1