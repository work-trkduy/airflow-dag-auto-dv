create table if not exists auto_dv_integration.lsate_lnk_customer_branch_2 (
    dv_hkey_lnk_customer_branch string,
    dv_startts timestamp,
    dv_endts timestamp,
    dv_kaf_ldt timestamp,
	dv_kaf_ofs bigint,
	dv_cdc_ops string,
	dv_src_ldt timestamp,
	dv_src_rec string,
	dv_ldt timestamp
)
using iceberg
tblproperties ('read.parquet.vectorization.enabled' = 'true'
        , 'read.parquet.vectorization.batch-size' = '10000'
        , 'hive.engine.enabled' = 'true'
        )
partitioned by (days(dv_src_ldt))