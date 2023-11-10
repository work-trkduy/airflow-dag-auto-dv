insert into auto_dv_integration.sat_customer_2 (
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
)
with cte_latest_datelastmaint as (
    select datelastmaint
    from (
        select *, row_number() over (partition by 1 order by run_date desc) as row_num
        from auto_dv_metadata.logging_etl_batch_job
    ) a
    where row_num = 1
),
cte_sat_der_set_row_num as (
    select
        sat_der.*,
        row_number() over (
            partition by dv_hkey_hub_customer, create_dt
            order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc) as row_num
    from auto_dv_integration.sat_der_customer_2 sat_der
    join cte_latest_datelastmaint log on sat_der.dv_src_ldt <= log.datelastmaint
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
from cte_sat_der_set_row_num sat_der
where row_num != 1 or not exists (
    select 1 from auto_dv_integration.sat_snp_customer_2 sat_snp
    where sat_der.dv_hkey_hub_customer = sat_snp.dv_hkey_hub_customer
        and sat_der.create_dt = sat_snp.create_dt
        )