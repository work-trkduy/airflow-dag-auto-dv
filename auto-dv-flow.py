import os
from glob import glob
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = 'Auto-DV',
    start_date=datetime(2023, 11, 7),
    schedule_interval=None
)

spark_conf = {
    "spark.submit.deployMode": "cluster",
    "spark.yarn.queue": "root.default",
    "spark.yarn.jars": "hdfs://192.168.1.9/spark/jars/*",
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
    "spark.hadoop.hive.metastore.uris": "thrift://192.168.1.9:9083",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type": "hive",
    "spark.sql.catalog.spark_catalog.warehouse": "/hive/warehouse",
    "spark.sql.defaultCatalog": "spark_catalog",
    "spark.sql.warehouse.dir":"/hive/warehouse",
    "spark.dynamicAllocation.enabled": "true",
    "spark.driver.cores": "1",
    "spark.driver.memory": "2g",
    "spark.driver.maxResultSize": "0",
    "spark.executor.cores": "2",
    "spark.executor.memory": "2g",
    "spark.executor.instances": "2",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "8",
}


def create_spark_submit_op(
    task_id,
    app_path=None,
    spark_conf=spark_conf,
    conn_id="spark-default",
    files=None,
    app_args=None
):
    if app_path is None:
        app_path = f"/opt/airflow/dags/auto_dv/tasks/{task_id}.py"
    
    return SparkSubmitOperator(
        task_id=task_id,
        application=app_path,
        name=f"auto-dv-{task_id}",
        conf=spark_conf,
        conn_id=conn_id,
        files=files,
        application_args=app_args,
        dag=dag
    )

start_task = DummyOperator(task_id='start', dag=dag)

sync_input_batch_data_task = create_spark_submit_op(task_id="sync-input-batch-data")

prepare_etl_scripts_task = BashOperator(
    task_id='prepare-etl-scripts',
    bash_command='python /opt/airflow/dags/auto_dv/tasks/prepare-etl-scripts.py',
    dag=dag
)

# PythonOperator(
#     task_id="prepare-etl-scripts",
#     python_callable=prepare_etl_scripts,
#     op_kwargs={},
#     dag=dag,
# )

create_dv_tables_task = create_spark_submit_op(
    task_id="create-dv-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_create.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_create.sql"))
)

create_metadata_tables_task = create_spark_submit_op(task_id="create-metadata-tables")

update_logging_etl_batch_job_task = create_spark_submit_op(task_id="update-logging-etl-batch-job")

load_hub_tables_task = create_spark_submit_op(
    task_id="load-hub-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_hub_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_hub_load.sql"))
)

test_hub_tables_task = create_spark_submit_op(
    task_id="test-hub-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_hub_*_test_all.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_hub_*_test_all.sql"))
)

load_lnk_tables_task = create_spark_submit_op(
    task_id="load-lnk-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lnk_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lnk_load.sql"))
)

test_lnk_tables_task = create_spark_submit_op(
    task_id="test-lnk-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lnk_*_test_all.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lnk_*_test_all.sql"))
)

load_lsate_tables_task = create_spark_submit_op(
    task_id="load-lsate-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lsate_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lsate_load.sql"))
)

initial_load_sat_der_tables_task = create_spark_submit_op(
    task_id="intial-load-sat-der-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_sat-der_load-initial.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_sat-der_load-initial.sql"))
)

initial_load_sat_tables_task = create_spark_submit_op(
    task_id="intial-load-sat-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_sat_load-initial.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_sat_load-initial.sql"))
)

initial_load_sat_snp_tables_task = create_spark_submit_op(
    task_id="intial-load-sat-snp-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_sat-snp_load-initial.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_sat-snp_load-initial.sql"))
)

load_sat_der_tables_task = create_spark_submit_op(
    task_id="load-sat-der-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_sat-der_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_sat-der_load.sql"))
)

load_sat_tables_task = create_spark_submit_op(
    task_id="load-sat-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_sat_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_sat_load.sql"))
)

load_sat_snp_tables_task = create_spark_submit_op(
    task_id="load-sat-snp-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_sat-snp_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_sat-snp_load.sql"))
)

test_sat_tables_task = create_spark_submit_op(
    task_id="test-sat-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_sat_*_test_all.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_sat_*_test_all.sql"))
)

initial_load_lsat_der_tables_task = create_spark_submit_op(
    task_id="intial-load-lsat-der-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lsat-der_load-initial.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lsat-der_load-initial.sql"))
)

initial_load_lsat_tables_task = create_spark_submit_op(
    task_id="intial-load-lsat-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lsat_load-initial.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lsat_load-initial.sql"))
)

initial_load_lsat_snp_tables_task = create_spark_submit_op(
    task_id="intial-load-lsat-snp-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lsat-snp_load-initial.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lsat-snp_load-initial.sql"))
)

load_lsat_der_tables_task = create_spark_submit_op(
    task_id="load-lsat-der-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lsat-der_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lsat-der_load.sql"))
)

load_lsat_tables_task = create_spark_submit_op(
    task_id="load-lsat-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lsat_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lsat_load.sql"))
)

load_lsat_snp_tables_task = create_spark_submit_op(
    task_id="load-lsat-snp-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lsat-snp_load.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lsat-snp_load.sql"))
)

test_lsat_tables_task = create_spark_submit_op(
    task_id="test-lsat-tables",
    app_path="/opt/airflow/dags/auto_dv/tasks/exec-sql-scripts.py",
    files="/opt/airflow/dags/auto_dv/sql_scripts/*_lsat_*_test_all.sql",
    app_args=map(os.path.basename, glob("/opt/airflow/dags/auto_dv/sql_scripts/*_lsat_*_test_all.sql"))
)

start_task >> sync_input_batch_data_task
start_task >> prepare_etl_scripts_task >> create_dv_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> load_hub_tables_task >> test_hub_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> load_lnk_tables_task >> test_lnk_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> load_lsate_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> initial_load_sat_der_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> initial_load_sat_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> initial_load_sat_snp_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> initial_load_lsat_der_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> initial_load_lsat_tables_task
(sync_input_batch_data_task, create_dv_tables_task) >> initial_load_lsat_snp_tables_task

start_task >> create_metadata_tables_task >> update_logging_etl_batch_job_task

(initial_load_sat_der_tables_task, initial_load_sat_tables_task, initial_load_sat_snp_tables_task) >> load_sat_der_tables_task
(load_sat_der_tables_task, update_logging_etl_batch_job_task) >> load_sat_tables_task
(load_sat_der_tables_task, update_logging_etl_batch_job_task) >> load_sat_snp_tables_task
load_sat_tables_task >> test_sat_tables_task

(initial_load_lsat_der_tables_task, initial_load_lsat_tables_task, initial_load_lsat_snp_tables_task) >> load_lsat_der_tables_task
(load_lsat_der_tables_task, update_logging_etl_batch_job_task) >> load_lsat_tables_task
(load_lsat_der_tables_task, update_logging_etl_batch_job_task) >> load_lsat_snp_tables_task
load_lsat_tables_task >> test_lsat_tables_task