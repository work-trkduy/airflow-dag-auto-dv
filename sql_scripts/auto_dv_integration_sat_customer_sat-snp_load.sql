with cte_stg_sat_snp as (
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
		dv_ldt,
        dv_ccd,
        row_number() over (
            partition by dv_hkey_hub_customer
            order by dv_src_ldt desc, dv_kaf_ldt desc, dv_kaf_ofs desc
        ) as row_num
    from auto_dv_integration.sat_der_customer sat_der
    join cte_latest_datelastmaint log on sat_der.dv_src_ldt <= log.datelastmaint
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
	dv_ldt,
    dv_ccd
from cte_sat_der_set_row_num
where row_num = 1
)
merge into auto_dv_integration.sat_snp_customer tgt
using cte_stg_sat_snp src
on tgt.dv_hkey_hub_customer = src.dv_hkey_hub_customer
when matched then update set *
when not matched then insert *