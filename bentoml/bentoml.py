from ..fs import SinaraFileSystem
from ..substep import get_curr_run_id, get_curr_notebook_name
from .utils import process_artifacts_archive, process_service_version, save_bentoservice_profile
import os
import shutil
from pathlib import Path
import logging
import time
import requests
import bentoml
import json
import yaml
import re
from subprocess import STDOUT, PIPE, run, Popen
import dataclasses

# def get_curr_run_id():
#     if "DSML_CURR_RUN_ID" not in os.environ:
#         run_id = reset_curr_run_id()
#     else:
#         run_id = os.environ["DSML_CURR_RUN_ID"]
#     return run_id

def get_sinara_step_tmp_path():
    return f"{os.getcwd()}/tmp"

def save_bentoservice( bentoservice, *, substep = None, path, service_version = None ):
    
    # Correct 'ensure_python' method in bentoml-init.sh
    def fix_bentoml_013_2(filepath):
        
        fix = 'IFS=. read -r major minor build <<< "${PY_VERSION_SAVED}"; DESIRED_PY_VERSION=$major.$minor; '
        with open(filepath, "r+") as f:
            file_content = f.read()
            fixed_file_content = re.sub('DESIRED_PY_VERSION=.*', fix, file_content, flags = re.M)
            #print(fixed_file_content)
            f.seek(0)
            f.write(fixed_file_content)
            f.truncate()

    ''' save to fs model packed as a BentoService Python object '''
    
    fspath = path
    
    runid = get_curr_run_id()
    bentoservice_name = os.path.basename(fspath)
    
    if not service_version and not substep:
        raise Exception("There is no service_version or substep provided")

    if not service_version:
        #<env>.<product>.<zone>.<ml_model>:<ml_model_run_id>
        service_version = f'{substep.env_name}.{substep.pipeline_name}.{substep.zone_name}.{bentoservice_name}.{runid}'
    
    save_info = [f'BENTO_SERVICE={service_version}']
    
    #write bento service to tmp dir
    tmppath = get_sinara_step_tmp_path()
    bentoservice_dir = f"{tmppath}/{runid}/{bentoservice_name}"
    
    shutil.rmtree(bentoservice_dir, ignore_errors=True)
    os.makedirs(bentoservice_dir, exist_ok=True)
    
    if not hasattr(bentoservice, 'service_version'):
        bentoservice.set_version(service_version)
        try:
            # replace 'service_version' artifact
            bentoservice.pack('service_version', service_version)
        except KeyError:
            pass

    bentoservice.save_to_dir(bentoservice_dir)
        
    fix_bentoml_013_2(f'{bentoservice_dir}/bentoml-init.sh')

    save_info_file = os.path.join(bentoservice_dir, 'save_info.txt')

    with open(save_info_file, 'w+') as f:
        f.writelines(line + '\n' for line in save_info)
        
    if bentoservice.name == 'BentoArchive':
        process_artifacts_archive(bentoservice, bentoservice_dir)
        
    if not hasattr(bentoservice, 'service_version'):
        process_service_version(bentoservice, bentoservice_dir)
    
    if hasattr(bentoservice, 'service_profile'):
        save_bentoservice_profile(bentoservice_dir, bentoservice.service_profile)
        shutil.copytree("sinara/bentoservice", f"{bentoservice_dir}/ModelService/sinara/bentoservice")
    
    #make zip file for bento service
    bentoservice_zipfile =  f"{tmppath}/{runid}_{bentoservice_name}.model" 
    shutil.make_archive(bentoservice_zipfile, 'zip', bentoservice_dir)

    #write zip file to fs
    
    fs = SinaraFileSystem.FileSystem()
    fs.makedirs(fspath)
    fs.put(f"{bentoservice_zipfile}.zip", f"{fspath}/model.zip")
    fs.touch(f"{fspath}/_SUCCESS")
    
    #remove zip file from tmp
    os.remove(f"{bentoservice_zipfile}.zip")
    
def load_bentoservice(path, bentoservice_name: str = None):
    # read zip file from dir
    runid = get_curr_run_id()
    if bentoservice_name is None:
        bentoservice_name = os.path.basename(path)
    tmppath = get_sinara_step_tmp_path()
    bentoservice_zipfile =  f"{tmppath}/{runid}_{bentoservice_name}.model.zip"
    bentoservice_zipfile_crc = f"{tmppath}/.{runid}_{bentoservice_name}.model.zip.crc"
    
    fs = SinaraFileSystem.FileSystem()
    if not fs.exists(f"{path}/_SUCCESS"):
        raise Exception("There is no _SUCCESS file for '{path}'")
    
    fs.get(f"{path}/model.zip", bentoservice_zipfile)
    
    # unpack zip archive   
    bentoservice_dir = f"{tmppath}/{runid}/{bentoservice_name}"
    shutil.unpack_archive(bentoservice_zipfile, bentoservice_dir)
        
    # remove zip file from tmp
    os.remove(bentoservice_zipfile)
    try:
        os.remove(bentoservice_zipfile_crc)
    except:
        pass #crc file doesn't exisis
    
    #load bentoml service
    return bentoml.load_from_dir(bentoservice_dir)

def start_dev_bentoservice( bentoservice, use_popen = False, debug = False, port = 5000 ):
   #fix of bentoservice import bug
    __import__(bentoservice.__class__.__module__)

    if use_popen:
        bentoservice_dir = bentoservice._bento_service_bundle_path
        bentoservice_cmd = ["python", "-m", "bentoml", "serve", "--port", str(port), bentoservice_dir]
        if debug:
            bentoservice_cmd.insert(-1, "--debug")
        bentoservice.process = Popen(bentoservice_cmd)
    else:
        if port != 5000:
            raise Exception("use_popen parameter should be True to use port other than 5000")
        bentoservice.start_dev_server(debug=debug)
    
    #wait 30 sec for bentoservice is really started
    ex = None
    for i in range(30):
        try:
            healthz = requests.get(f"http://127.0.0.1:{port}/healthz")
            healthz.raise_for_status()
        except Exception as e:
            ex = e
            time.sleep(1)
            continue
        else:
            ex = None
            time.sleep(1) # sometimes healthz is up, but other methods in intermediate state
            break
    if ex:
        stop_dev_bentoservice( bentoservice )
        raise ex
    

def stop_dev_bentoservice( bentoservice ):
    bentoservice.stop_dev_server()

def save_bentoartifact_to_tmp(bentoservice, 
                               artifact_name="model", 
                               artifact_file_path=""):
    ''' save bentoservice artifact to local cache, bentoservice has to be loaded beforehand  '''
    
    if "." in os.path.basename(artifact_file_path):
        os.makedirs(os.path.dirname(artifact_file_path), exist_ok = True)
    else:
    # Considering user wants to save an original artifact to directory instead of custom file path
    # see artifacts/binary_artifacts.py for details
        os.makedirs(artifact_file_path, exist_ok = True)
    
    bentoservice.artifacts[artifact_name].save(artifact_file_path)
    
def extract_artifacts_from_bentoservice(bentoservice_path, dest_folder=None):
    
    # read zip file from dir
    runid = get_curr_run_id()
    bentoservice_name = os.path.basename(bentoservice_path)
    tmppath = get_sinara_step_tmp_path()
    bentoservice_zipfile =  f"{tmppath}/{runid}_{bentoservice_name}.model.zip"
    bentoservice_zipfile_crc = f"{tmppath}/.{runid}_{bentoservice_name}.model.zip.crc"
    
    fs = SinaraFileSystem.FileSystem()
    if not fs.exists(f"{bentoservice_path}/_SUCCESS"):
        raise Exception("There is no _SUCCESS file for '{path}'")
    
    fs.get(f"{bentoservice_path}/model.zip", bentoservice_zipfile)
    
    # unpack zip archive   
    bentoservice_dir = f"{tmppath}/{runid}/{bentoservice_name}"
    unpack_dest_folder = dest_folder if dest_folder else bentoservice_dir
    unpack_dest_folder_tmp = Path(unpack_dest_folder) / '_tmp'
    
    shutil.unpack_archive(bentoservice_zipfile, unpack_dest_folder_tmp )
    
    bentoml_yaml = Path(unpack_dest_folder_tmp) / "bentoml.yml"
    with open(bentoml_yaml, 'r') as f:
        bentoml_info = yaml.safe_load(f)
        
    artifacts_folder = Path(unpack_dest_folder_tmp) / bentoml_info['metadata']['service_name'] / 'artifacts'

    shutil.move(src=artifacts_folder, dst=unpack_dest_folder)
    shutil.rmtree(unpack_dest_folder_tmp)

    # remove zip file from tmp
    os.remove(bentoservice_zipfile)
    try:
        os.remove(bentoservice_zipfile_crc)
    except:
        pass #crc file doesn't exisis
    
    return unpack_dest_folder
