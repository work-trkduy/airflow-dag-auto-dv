import os
from glob import glob
from auto_dv.model_handler.helper import SQLScriptRenderer

home_path = "/opt/airflow/dags/auto_dv"

def prepare_etl_scripts(home_path = home_path):
    etl_scripts_dict = {}
    
    for entity_name, entity_path in [(f.name, f.path) for f in os.scandir(f"{home_path}/models") if f.is_dir()]:

        for config_path in glob(f"{home_path}/schema_configs/{entity_name}_*.yaml"):
            for template_file in map(os.path.basename, glob(f"{entity_path}/*.sql")):
                # if "lsate" in template_file:
                #     continue    
                options = {
                    'file_loader': home_path,
                    'template_path': f"models/{entity_name}/{template_file}",
                    'convention_path': f"{home_path}/models/dv_hard_rules.yaml",
                    'tbl_properties_path' : f'{home_path}/models/dv_tblproperties.yaml',
                    'config_path': config_path
                }
                renderer = SQLScriptRenderer(options)
                renderer.render_all_sql_template()
                etl_scripts_dict.update(renderer.sql_template)
            
            options = {
                'file_loader': home_path,
                'template_path': f"models/test/test_all.sql",
                'convention_path': f"{home_path}/models/dv_hard_rules.yaml",
                'tbl_properties_path' : f'{home_path}/models/dv_tblproperties.yaml',
                'config_path': config_path
            }
            renderer = SQLScriptRenderer(options)
            renderer.render_all_sql_template()
            etl_scripts_dict.update(renderer.sql_template)

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