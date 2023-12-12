import os
from glob import glob
from auto_dv.model_handler.helper import SQLScriptRenderer

home_path = "/opt/airflow/dags/auto_dv"

def prepare_etl_scripts(home_path = home_path):
    etl_scripts_dict = {}
    
    for entity_name, entity_path in [(f.name, f.path) for f in os.scandir(f"{home_path}/models") if f.is_dir()]:

        for config_path in glob(f"{home_path}/model_configs_examples/{entity_name}_*.yaml"):
            template_files = list(map(os.path.basename, glob(f"{entity_path}/*.sql")))
            template_paths = [f"models/{entity_name}/{template_file}" for template_file in template_files] + ["models/test/test_all.sql"]

            options = {
                'file_loader': home_path,
                'convention_path': f"{home_path}/model_configs_examples/dv_hard_rules.yaml",
                'tbl_properties_path' : f'{home_path}/model_configs_examples/dv_tblproperties.yaml',
                'config_path': config_path,
                'template_paths': template_paths
            }
            renderer = SQLScriptRenderer(options)
            renderer.render_all_sql_templates()
            etl_scripts_dict.update(renderer.sql_scripts)

    [os.remove(f) for f in glob(f"{home_path}/sql_scripts/*.sql")]
    for name, sql_text in etl_scripts_dict.items():
        f = open(f"{home_path}/sql_scripts/{name}.sql", "w")
        f.write(sql_text)
        f.close()

try:
    prepare_etl_scripts()
except Exception as e:
    print("ERROR:", e)
    raise e