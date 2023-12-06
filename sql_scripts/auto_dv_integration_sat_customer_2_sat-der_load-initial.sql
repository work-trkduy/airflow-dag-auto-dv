insert into auto_dv_integration.sat_der_customer_2 (
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
	dv_ldt
)
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
	current_timestamp() as dv_ldt
from $auto_dv_psa.customer
where
    current_timestamp() >= $v_end_date
    and cst_no is not null
    and br_cd is not null
    and create_dt is not null
    