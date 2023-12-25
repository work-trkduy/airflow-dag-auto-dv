create table if not exists auto_dv_integration.sat_der_customer (
    dv_hkey_sat_customer string,
    dv_hkey_hub_customer string,
    dv_hsh_dif string,
    cst_nm string,
    cst_full_nm string,
    cst_type string,
    dv_kaf_ldt timestamp,
    dv_kaf_ofs bigint,
    dv_cdc_ops string,
    dv_src_ldt timestamp,
    dv_src_rec string,
    dv_ldt timestamp
)
using iceberg
tblproperties (
    'read.parquet.vectorization.enabled' = 'true', 
    'read.parquet.vectorization.batch-size' = '10000', 
    'hive.engine.enabled' = 'true'
)
partitioned by (days(dv_src_ldt))