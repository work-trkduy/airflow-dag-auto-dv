with cte_stg_lsat_snp as (
with cte_lsat_der_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by dv_hkey_lnk_customer_branch
                order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
        from auto_dv_integration.lsat_der_customer_branch
        where dv_src_ldt >= $v_from_date and dv_src_ldt < $v_end_date
    )
    where row_num = 1
),
cte_lsat_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by dv_hkey_lnk_customer_branch
                order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
        from auto_dv_integration.lsat_snp_customer_branch
    )
    where row_num = 1
)
select
    dv_hkey_lsat_customer_branch,
    dv_hkey_lnk_customer_branch,
    dv_hsh_dif,
    id_number,
    type_of_id,
    date_of_issue,
    place_of_issue,
    dv_kaf_ldt,
    dv_kaf_ofs,
    dv_cdc_ops,
    dv_src_ldt,
    dv_src_rec,
    dv_ldt
from cte_lsat_der_latest_records lsat_der
where not exists (
    select 1 from cte_lsat_latest_records lsat_snp
    where
        lsat_der.dv_hkey_lnk_customer_branch = lsat_snp.dv_hkey_lnk_customer_branch
        and lsat_der.dv_hsh_dif = lsat_snp.dv_hsh_dif
        and lower(lsat_der.dv_cdc_ops) not in ('d','t')
        and lower(lsat_snp.dv_cdc_ops) not in ('d','t')
)
)
merge into auto_dv_integration.lsat_snp_customer_branch tgt
using cte_stg_lsat_snp src
on tgt.dv_hkey_lnk_customer_branch = src.dv_hkey_lnk_customer_branch
when matched then update set *
when not matched then insert *