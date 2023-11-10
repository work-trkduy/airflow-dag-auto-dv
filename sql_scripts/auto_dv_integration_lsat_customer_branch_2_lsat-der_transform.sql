with cte_stg_lsat as (
    select
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(upper(cast(br_cd as string))), ''), '-1')|| '#~!' || 'test' || '#~!' || coalesce(nullif(rtrim(cast(phone as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_timestamp() as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_timestamp() as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(monotonically_increasing_id() as string)), ''), '-1'), 256) as dv_hkey_lsat_customer_branch,
        sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1') || '#~!' || coalesce(nullif(rtrim(upper(cast(br_cd as string))), ''), '-1')|| '#~!' || 'test', 256) as dv_hkey_lnk_customer_branch,
        sha2(coalesce(nullif(rtrim(cast(id_number as string)), ''), repeat('0',16)) || '#~!' || coalesce(nullif(rtrim(cast(type_of_id as string)), ''), repeat('0',16)) || '#~!' || coalesce(nullif(rtrim(cast(date_of_issue as string)), ''), repeat('0',16)) || '#~!' || coalesce(nullif(rtrim(cast(place_of_issue as string)), ''), repeat('0',16)), 256) as dv_hsh_dif,
        phone,
        id_number,
		type_of_id,
		date_of_issue,
		place_of_issue,
        current_timestamp() as dv_kaf_ldt,
		monotonically_increasing_id() as dv_kaf_ofs,
		'I' as dv_cdc_ops,
		current_timestamp() as dv_src_ldt,
		'test' as dv_src_rec,
		current_timestamp() as dv_ldt,
        'test' as dv_ccd,
        -1 as row_num
    from $auto_dv_psa.customer
),
cte_lsat_der_set_row_num as (
    select
        dv_hkey_lsat_customer_branch,
        dv_hkey_lnk_customer_branch,
        dv_hsh_dif,
        phone,
        id_number,
		type_of_id,
		date_of_issue,
		place_of_issue,
        dv_kaf_ldt,
		dv_kaf_ofs,
		dv_cdc_ops,
		dv_src_ldt,
		dv_src_rec,
		dv_ldt,
        dv_ccd,
        row_number() over (
            partition by dv_hkey_lnk_customer_branch, phone
            order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc
        ) as row_num
    from auto_dv_integration.lsat_der_customer_branch_2 tgt
    where exists (
        select 1
        from cte_stg_lsat src
        where tgt.dv_hkey_lnk_customer_branch = src.dv_hkey_lnk_customer_branch
            and tgt.phone = src.phone
            )
),
cte_lsat_der_latest_records as (
    select *
    from cte_lsat_der_set_row_num
    where row_num = 1
),
cte_stg_lsat_der_dedup_hsh_dif as (
    select *
    from (
        select
            *,
            lag(dv_hsh_dif, 1, null) over w as prev_hash_diff,
            lag(dv_cdc_ops, 1, null) over w as prev_cdc_ops
        from (
            select * from cte_stg_lsat
            union
            select * from cte_lsat_der_latest_records
        )
        window w as (
            partition by dv_hkey_lnk_customer_branch, phone
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
    dv_hkey_lsat_customer_branch,
    dv_hkey_lnk_customer_branch,
    dv_hsh_dif,
    phone,
    id_number,
	type_of_id,
	date_of_issue,
	place_of_issue,
    dv_kaf_ldt,
	dv_kaf_ofs,
	dv_cdc_ops,
	dv_src_ldt,
	dv_src_rec,
	dv_ldt,
    dv_ccd
from cte_stg_lsat_der_dedup_hsh_dif
where row_num = -1