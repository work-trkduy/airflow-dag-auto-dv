with cte_stg_sat_snp as (
with cte_sat_der_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by dv_hkey_hub_customer, create_dt
                order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
        from auto_dv_integration.sat_der_customer_2
        where dv_src_ldt >= $v_from_date and dv_src_ldt < $v_end_date
    )
    where row_num = 1
),
cte_sat_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by dv_hkey_hub_customer, create_dt
                order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
        from auto_dv_integration.sat_snp_customer_2
    )
    where row_num = 1
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
    dv_ldt
from cte_sat_der_latest_records sat_der
where not exists (
    select 1 from cte_sat_latest_records sat_snp
    where
        sat_der.dv_hkey_hub_customer = sat_snp.dv_hkey_hub_customer
        and sat_der.create_dt = sat_snp.create_dt
        and lower(sat_der.dv_cdc_ops) != 'd'
        and lower(sat_snp.dv_cdc_ops) != 'd'
)
)
merge into auto_dv_integration.sat_snp_customer_2 tgt
using cte_stg_sat_snp src
on tgt.dv_hkey_hub_customer = src.dv_hkey_hub_customer
    and tgt.create_dt = src.create_dt
when matched then update set *
when not matched then insert *