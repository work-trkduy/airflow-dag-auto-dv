import yaml
import os
from abc import ABC, abstractmethod
import jinja2
import json
import re


class ConfigReader(ABC):
    @abstractmethod
    def read(self, config_path):
        pass


class ConfigEntityReader(ConfigReader):
    def read(self, config_path):
        with open(config_path) as file:
            model_config = yaml.safe_load(file)
        return model_config


class ConfigConventionReader(ConfigReader):
    def read(self, config_path):
        with open(config_path) as file:
            model_config = yaml.safe_load(file)
        return model_config


class TemplateReader(ConfigReader):
    def read(self, config_path):
        with open(config_path) as file:
            sql_template = file.read()
        return sql_template


class SQLScriptRenderer:
    def __init__(self, options):
        # jinja2 env and template
        self.file_loader = jinja2.FileSystemLoader(options["file_loader"])
        self.jinja_env = jinja2.Environment(loader=self.file_loader, extensions=['jinja2.ext.do'])
        self.jinja_env.filters["to_json"] = lambda a: json.dumps(a)
        self.jinja_env.filters["from_json"] = lambda a: json.loads(a)
        self.jinja_env.filters["replace_prefix"] = lambda a, old, new: re.sub(r"\A("+old+r"_)?", new+"_", a, 1)
        self.templates = {os.path.basename(p).split('.')[0]: self.jinja_env.get_template(p) for p in options["template_paths"]}

        # convention
        self.convention_reader = ConfigConventionReader()
        self.convention = self.convention_reader.read(options['convention_path'])
        self.tbl_properties = self.convention_reader.read(options['tbl_properties_path'])

        # config
        self.config_reader = ConfigEntityReader()
        self.config = self.config_reader.read(options['config_path'])

        self.sql_scripts = {}

    def render(self, template, model):
        rendered_script = template.render(
            dv_system=self.convention['dv_system'],
            tbl_properties=self.tbl_properties['dv_tblproperties'],
            model=model
        )
        return rendered_script

    def render_all_sql_templates(self):
        for operation, template in self.templates.items():
            rendered_script = self.render(template, self.config)
            if not re.fullmatch(r"\s*", rendered_script):
                self.sql_scripts[f"{self.config['target_schema']}_{self.config['target']}_{operation}"] = rendered_script