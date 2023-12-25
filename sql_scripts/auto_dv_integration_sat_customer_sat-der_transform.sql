select
    sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1')|| '#~!' || 'test' || '#~!' || coalesce(nullif(rtrim(cast(current_timestamp() as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(current_timestamp() as string)), ''), '-1') || '#~!' || coalesce(nullif(rtrim(cast(monotonically_increasing_id() as string)), ''), '-1'), 256) as dv_hkey_sat_customer,
    sha2(coalesce(nullif(rtrim(upper(cast(cst_no as string))), ''), '-1')|| '#~!' || 'test', 256) as dv_hkey_hub_customer,
    sha2(coalesce(nullif(rtrim(cast(cst_nm as string)), ''), repeat('0',16)) || '#~!' || coalesce(nullif(rtrim(cast(cst_full_nm as string)), ''), repeat('0',16)) || '#~!' || coalesce(nullif(rtrim(cast(cst_type as string)), ''), repeat('0',16)), 256) as dv_hsh_dif,
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
where 1=1
    and cst_no is not null
    