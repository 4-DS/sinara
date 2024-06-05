import os
from pathlib import Path
import hashlib
import json

def process_artifacts_archive(bentoservice, bentoservice_root_dir):
    """
    Process artifacts archive
    bentoservice: the bentoservice instance
    bentoservice_root_dir: the root directory of bentoservice
    """
    
    bentoservice.postprocess(bentoservice_root_dir)
    
def process_service_version(bentoservice, bentoservice_root_dir):
    """
    Process service version
    bentoservice: the bentoservice instance
    bentoservice_root_dir: the root directory of bentoservice
    """

    service_version_text = '''
    @api(input=JsonInput())
    def service_version(self, *args):
        """
        THis BentoService version
        """
        return self.version'''
    
    import yaml
    bentoml_yaml = Path(bentoservice_root_dir) / "bentoml.yml"
    with open(bentoml_yaml, 'r') as f:
        bentoml_info = yaml.safe_load(f)
        
    bentoservice_file = Path(bentoservice_root_dir) / bentoservice.__class__.__name__ / bentoml_info['metadata']['module_file']

    with open(bentoservice_file, 'a') as f:
        f.write(service_version_text)

def compute_md5(file_name):
    """
    Compute md5 of a file
    """
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def save_bentoservice_profile(bentoservice_root_dir, bentoservice_profile):
    """
    Save bentoservice profile
    bentoservice_root_dir: the root directory of bentoservice
    bentoservice_profile: the bentoservice profile to be saved
    """
    profile_file = os.path.join(bentoservice_root_dir, 'bentoservice_profile.json')
    with open(profile_file, 'w+') as f:
        json.dump(bentoservice_profile, f)
