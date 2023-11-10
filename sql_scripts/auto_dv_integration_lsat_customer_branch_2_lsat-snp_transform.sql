with cte_latest_datelastmaint as (
    select datelastmaint
    from (
        select *, row_number() over (partition by 1 order by run_date desc) as row_num
        from auto_dv_metadata.logging_etl_batch_job
    ) a
    where row_num = 1
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
    from auto_dv_integration.lsat_der_customer_branch_2 sat_der
    join cte_latest_datelastmaint log on sat_der.dv_src_ldt <= log.datelastmaint
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
from cte_lsat_der_set_row_num
where row_num = 1