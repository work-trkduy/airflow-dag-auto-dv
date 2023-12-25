create table if not exists auto_dv_integration.hub_branch (
    dv_hkey_hub_branch string,
    br_cd bigint,
    dv_kaf_ldt timestamp,
    dv_kaf_ofs bigint,
    dv_cdc_ops string,
    dv_src_ldt timestamp,
    dv_src_rec string,
    dv_ldt timestamp,
    dv_ccd string
)
using iceberg
tblproperties (
    'read.parquet.vectorization.enabled' = 'true', 
    'read.parquet.vectorization.batch-size' = '10000', 
    'hive.engine.enabled' = 'true'
)
partitioned by (days(dv_src_ldt))