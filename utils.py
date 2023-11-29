import os
from pathlib import Path

def process_artifacts_archive(bentoservice, bentoservice_root_dir):
    bentoservice.postprocess(bentoservice_root_dir)
    
def process_service_version(bentoservice, bentoservice_root_dir):
    service_version_text = '''
    @api(input=JsonInput())
    def service_version(self, *args):
        """
        Версия данного Bento сервиса.
        """
        return self.version'''
    
    import yaml
    bentoml_yaml = Path(bentoservice_root_dir) / "bentoml.yml"
    with open(bentoml_yaml, 'r') as f:
        bentoml_info = yaml.safe_load(f)
        
    bentoservice_file = Path(bentoservice_root_dir) / bentoservice.__class__.__name__ / bentoml_info['metadata']['module_file']

    with open(bentoservice_file, 'a') as f:
        f.write(service_version_text)