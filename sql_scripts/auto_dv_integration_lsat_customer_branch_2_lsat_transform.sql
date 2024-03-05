with cte_lsat_der_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by dv_hkey_lnk_customer_branch, phone
                order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
        from auto_dv_integration.lsat_der_customer_branch_2
        where dv_src_ldt >= $v_from_date and dv_src_ldt < $v_end_date
    )
    where row_num = 1
),
cte_lsat_latest_records as (
    select * from (
        select
            *,
            row_number() over (
                partition by dv_hkey_lnk_customer_branch, phone
                order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
        from auto_dv_integration.lsat_customer_branch_2
    )
    where row_num = 1
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
    dv_ldt
from cte_lsat_der_latest_records lsat_der
where not exists (
    select 1 from cte_lsat_latest_records lsat
    where
        lsat_der.dv_hkey_lnk_customer_branch = lsat.dv_hkey_lnk_customer_branch
        and lsat_der.phone = lsat.phone
        and lsat_der.dv_hsh_dif = lsat.dv_hsh_dif
        and lower(lsat_der.dv_cdc_ops) not in ('d','t')
        and lower(lsat.dv_cdc_ops) not in ('d','t')
)