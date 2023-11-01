from .fs import SinaraFileSystem
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

def get_curr_run_id():
    if "DSML_CURR_RUN_ID" not in os.environ:
        run_id = reset_curr_run_id()
    else:
        run_id = os.environ["DSML_CURR_RUN_ID"]
    return run_id

def get_sinara_step_tmp_path():
    return f"{os.getcwd()}/tmp"

def save_bentoservice( bentoservice, *, path, service_version ):
    
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
    
    if service_version:
        save_info = [f'BENTO_SERVICE={service_version}']
    else:
        raise Exception("Service version could not be empty!")
    
    #write bento service to tmp dir
    runid = get_curr_run_id()
    
    bentoservice_name = os.path.basename(fspath)
    tmppath = get_sinara_step_tmp_path()
    bentoservice_dir = f"{tmppath}/{runid}/{bentoservice_name}"
    
    shutil.rmtree(bentoservice_dir, ignore_errors=True)
    os.makedirs(bentoservice_dir, exist_ok=True)
    
    bentoservice.set_version(service_version)
    bentoservice.save_to_dir(bentoservice_dir)
    
    SERVICE_VERSION_TEXT = '''
    @bentoml.api()
    def service_version(self):
        return self.version
    '''
    if not hasattr(bentoservice, 'service_version'):
        bentoservice_file = os.path.join(bentoservice_dir, bentoservice.__class__.__name__, 'bentoservice.py')
        with open(bentoservice_file, 'a') as f:
            f.write(SERVICE_VERSION_TEXT)
        
    fix_bentoml_013_2(f'{bentoservice_dir}/bentoml-init.sh')

    save_info_file = os.path.join(bentoservice_dir, 'save_info.txt')

    with open(save_info_file, 'w+') as f:
        f.writelines(line + '\n' for line in save_info)
    
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
    
def load_bentoservice(path):

    # read zip file from dir
    runid = get_curr_run_id()
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

def start_dev_bentoservice( bentoservice, use_popen = False, debug = False ):
   #fix of bentoservice import bug
    __import__(bentoservice.__class__.__module__)

    if use_popen:
        bentoservice_dir = bentoservice._bento_service_bundle_path
        bentoservice_cmd = ["python", "-m", "bentoml", "serve", "--port", "5000", bentoservice_dir]
        if debug:
            bentoservice_cmd.insert(-1, "--debug")
        bentoservice.process = Popen(bentoservice_cmd)
    else:
        bentoservice.start_dev_server(debug=debug)
    
    #wait 30 sec for bentoservice is really started
    ex = None
    for i in range(30):
        try:
            healthz = requests.get("http://127.0.0.1:5000/healthz")
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
