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

def get_curr_run_id():
    if "DSML_CURR_RUN_ID" not in os.environ:
        run_id = reset_curr_run_id()
    else:
        run_id = os.environ["DSML_CURR_RUN_ID"]
    return run_id

def get_sinara_step_tmp_path():
    return f"{os.getcwd()}/tmp"

def save_bentoservice( bentoservice, fspath ):
    
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

    #write bento service to tmp dir
    runid = get_curr_run_id()
    
    bentoservice_name = os.path.basename(fspath)
    tmppath = get_sinara_step_tmp_path()
    bentoservice_dir = f"{tmppath}/{runid}/{bentoservice_name}"
    
    shutil.rmtree(bentoservice_dir, ignore_errors=True)
    os.makedirs(bentoservice_dir, exist_ok=True) 
    
    bentoservice.save_to_dir(bentoservice_dir)
    fix_bentoml_013_2(f'{bentoservice_dir}/bentoml-init.sh')
    
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

def start_dev_bentoservice( bentoservice ):
   #fix of bentoservice import bug
    __import__(bentoservice.__class__.__module__) 
    bentoservice.start_dev_server()
    
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
            time.sleep(1) #sometime healthz is up, but other methods in intermediate state
            break
    if ex:
        stop_dev_bentoservice( bentoservice )
        raise ex
    

def stop_dev_bentoservice( bentoservice ):
    bentoservice.stop_dev_server()

