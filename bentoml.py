from ..env import get_curr_run_id, get_dsml_component_tmp_path
from ..fs import FileSystem
import os
import shutil
from pathlib import Path
import logging
import time
import requests
import bentoml
from enum import Enum
import json
import yaml
from subprocess import STDOUT, PIPE, run, Popen
from .utils import *

def get_curr_run_id():
    if "DSML_CURR_RUN_ID" not in os.environ:
        run_id = reset_curr_run_id()
    else:
        run_id = os.environ["DSML_CURR_RUN_ID"]
    return run_id

def save_bentoservice( bentoservice, fspath ):
    offline_build=True
    ''' save to fs model packed as a BentoService Python object '''

    #write bento service to tmp dir
    runid = get_curr_run_id()
    
    bentoservice_name = os.path.basename(fspath)
    tmppath = get_dsml_component_tmp_path()
    bentoservice_dir = f"{tmppath}/{runid}/{bentoservice_name}"
    
    shutil.rmtree(bentoservice_dir, ignore_errors=True)
    os.makedirs(bentoservice_dir, exist_ok=True)
    
    bentoservice.save_to_dir(bentoservice_dir)
    
    #make zip file for bento service
    bentoservice_zipfile =  f"{tmppath}/{runid}_{bentoservice_name}.model" 
    shutil.make_archive(bentoservice_zipfile, 'zip', bentoservice_dir)

    #write zip file to fs
    
    fs = FileSystem.FileSystem()
    fs.makedirs(fspath)
    fs.put(f"{bentoservice_zipfile}.zip", f"{fspath}/model.zip")
    fs.touch(f"{fspath}/_SUCCESS")
    
#remove zip file from tmp
    os.remove(f"{bentoservice_zipfile}.zip")
