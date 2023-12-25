with cte_sat_der_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by dv_hkey_hub_customer
                order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
        from auto_dv_integration.sat_der_customer
        where dv_src_ldt >= $v_from_date and dv_src_ldt < $v_end_date
    )
    where row_num = 1
),
cte_sat_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by dv_hkey_hub_customer
                order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
        from auto_dv_integration.sat_snp_customer
    )
    where row_num = 1
)
select
    dv_hkey_sat_customer,
    dv_hkey_hub_customer,
    dv_hsh_dif,
    cst_nm,
    cst_full_nm,
    cst_type,
    dv_kaf_ldt,
    dv_kaf_ofs,
    dv_cdc_ops,
    dv_src_ldt,
    dv_src_rec,
    dv_ldt
from cte_sat_der_latest_records sat_der
where not exists (
    select 1 from cte_sat_latest_records sat_snp
    where
        sat_der.dv_hkey_hub_customer = sat_snp.dv_hkey_hub_customer
        and lower(sat_der.dv_cdc_ops) != 'd'
        and lower(sat_snp.dv_cdc_ops) != 'd'
)