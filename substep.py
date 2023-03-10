import os
from datetime import datetime
from pathlib import Path

import glob
import shutil
import dataclasses
import inspect
import pprint
import json
import atexit

from IPython.core.display import Markdown, display

from .fs import SinaraFileSystem
import logging

def get_sinara_version():
    return "0.0.1"

def isnotebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter

    

def set_curr_notebook_name(nb_name):
    os.environ["DSML_CURR_NOTEBOOK_NAME"] = nb_name
    
    
def get_curr_notebook_name():
    return os.getenv('DSML_CURR_NOTEBOOK_NAME') or "standalone"


def set_curr_notebook_output_name(nb_name):
    os.environ["DSML_CURR_NOTEBOOK_OUTPUT_NAME"] = nb_name

def get_curr_notebook_output_name():
    return os.getenv('DSML_CURR_NOTEBOOK_OUTPUT_NAME') or "standalone"

def get_sinara_user_work_dir():
    return os.getenv("JUPYTER_SERVER_ROOT") or '/home/jovyan/work'


def get_tmp_prepared():
    if "DSML_CURR_RUN_ID_FROM_PLINE" not in os.environ:
        valid_tmp_target_path = f'/data/tmp{os.getcwd().replace(get_sinara_user_work_dir(),"")}'
        os.makedirs(valid_tmp_target_path, exist_ok=True)
        tmp_path = Path('./tmp')
        if tmp_path.is_symlink():
            tmp_link = tmp_path.readlink()
            if tmp_link.as_posix() != valid_tmp_target_path:
                print("'tmp' dir is not valid, creating valid tmp dir...")
                tmp_path.unlink()                
                os.symlink(valid_tmp_target_path, './tmp', target_is_directory=True)
        else:
            if tmp_path.exists():
                print('\033[1m' + 'Current \'tmp\' folder inside your component is going to be deleted. It\'s safe, as \'tmp\' is moving to cache and will be recreated again.' + '\033[0m')
                shutil.rmtree(tmp_path)

            os.symlink(valid_tmp_target_path, './tmp', target_is_directory=True)
    else:
        tmp_path = Path('./tmp')
        if tmp_path.is_symlink():
            tmp_path.unlink()
        else:
            if tmp_path.exists():
                shutil.rmtree(tmp_path)
        
        os.makedirs(get_sinara_component_tmp_path())
   

def print_line_as_bold(str):
    
    if isnotebook():
        display(Markdown(f"**{str}**\n"))
    else:
        print(str)

        
    
def ipynb_to_html(src_path, dst_path):
    
    import nbformat
    from nbconvert import HTMLExporter

    # read source notebook
    with open(src_path) as f:
        nb = nbformat.read(f, as_version=4)
    
    # export to html
    html_exporter = HTMLExporter()
    html_data, resources = html_exporter.from_notebook_node(nb)

    # write to output file
    with open(dst_path, "w") as f:
        f.write(html_data)

        
start_time = datetime.now()
pp = pprint.PrettyPrinter()

def get_curr_run_id():
    if "DSML_CURR_RUN_ID" not in os.environ:
        run_id = reset_curr_run_id()
    else:
        run_id = os.environ["DSML_CURR_RUN_ID"]
    return run_id

def get_sinara_step_tmp_path():
    return f"{os.getcwd()}/tmp"

def reset_curr_run_id():
    from datetime import datetime
    run_id = f"run-{datetime.now().strftime('%y-%m-%d-%H%M%S')}"
    # When sinara_component is running in Kubernetes CronJob
    if "DSML_CURR_RUN_ID_FROM_PLINE" in os.environ:
        run_id = os.environ["DSML_CURR_RUN_ID_FROM_PLINE"]
    os.environ["DSML_CURR_RUN_ID"] = run_id
    return run_id

def get_user():
    # TODO
    # single use 
    if True:
        return os.getenv("DSML_USER") or 'buslovaev_ma'
    if is_local_host():
        return os.getenv("DSML_USER") or get_local_var("hdfs_user", "Enter your NLMK user name:")
    return  os.getenv("JUPYTERHUB_USER") or os.getenv("DSML_USER") or getpass.getuser()

def get_data_paths():
    data_paths = {
        "test": "/data/products",
        "prod": "/data/production",
        "user": f"/data/home/{get_user()}"
    }
    return data_paths

def get_env_path(env_name):

    env_paths = get_data_paths()
    if env_name not in env_paths:
        raise Exception("Unexpected env_name value:" + env_name)

    return env_paths[env_name]





ENV_NAME = 'ENV_NAME'
PIPELINE_NAME = 'PIPELINE_NAME'
ZONE_NAME = 'ZONE_NAME'
STEP_NAME = 'STEP_NAME'
RUN_ID = 'RUN_ID'
ENTITY_NAME = 'ENTITY_NAME'
ENTITY_PATH = 'ENTITY_PATH'
SUBSTEP_NAME = 'SUBSTEP_NAME'

class NotebookSubstepRunResult:
    """DSML module must set DSMLModule.run_result property
    on the completion of work using DSMLModuleRunResult value.
    By defaul DSMLModule.run_result is set into UNKNOWN value"""
    
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


@dataclasses.dataclass(frozen=True)
class DSMLUrls:
    """
    DSMLUrls is base class for objects that contain URLs of Data Entities
    DSMLUrls objects are created via methods of DSMLModule 
    """
    pass


class NotebookSubstep:
    """
    NotebookSubstep instance must be created inside any NotebookSubstep notebook
    NotebookSubstep provides: 
     - functions to define interface of NotebookSubstep
     - functions to work with data cache
     - properties to access NotebookSubstep execution context
    Single instance only of NotebookSubstep allowed inside python kernel
    """
    # single instance only of NotebookSubstep allowed inside python kernel
    _current_module = None
    
    def __init__(self,
                 pipeline_params,
                 step_params,
                 substeps_params):
        """NotebookSubstep constructor"""
        
        # TODO
        # Give permissions to /data
        get_tmp_prepared()
        
        self._pipeline_params = pipeline_params
        self._step_params = step_params
        self._substeps_params = substeps_params
        
        self._run_id = get_curr_run_id()
        self._env_name = pipeline_params["env_name"] or "user"
        self._pipeline_name = pipeline_params["pipeline_name"]
        if not self._pipeline_name:
            raise Exception("'pipeline_name' must be specified within pipeline_params")
        self._zone_name = pipeline_params["zone_name"] or "main_stand"
        
        # TODO
        # step_name could be defined at step_params level?
        self._step_name = pipeline_params.get("step_name") or Path(os.getcwd()).name
        
        self._registered_inputs = []
        self._registered_outputs = []
        self._registered_custom_inputs = []
        self._registered_custom_outputs = []
        self._registered_tmp_inputs = []
        self._registered_tmp_outputs = []
        
        
        
        self.registered_inputs = {}
        
        self.registered_outputs = {}
        
        self.registered_tmp_io = {}


        #self._commit = str(git.Repo().head.commit)[:8]  # TODO what if git isn't presented
        #self._origin = next(git.Repo().remotes.origin.urls)
        #git.Repo().git.clear_cache()

        self._run_result = NotebookSubstepRunResult.UNKNOWN


        NotebookSubstep._current_module = self
        
        self._serialize_run(
            get_curr_notebook_name(),
            get_curr_notebook_output_name(),
            f"{datetime.now()}",
            pipeline_params,
            step_params,
            substeps_params,
            True)

# The idea of a tool which creates structure of steps in Git. The tool will be in a separate repository

    def exit_in_visualize_mode(self):
        # TODO
        # Stop the notebook for visualizing a pipeline
        pass
    def _artifacts_url(self):
        """Returns hdfs path where all artifact entities are stored"""
        env_path = get_env_path(self._env_name)
        artifacts_url = f"{env_path}/{self._pipeline_name}/{self._zone_name}/{self._step_name}/{self._run_id}"
        return artifacts_url

    
    def _serialize_run(
            self,
            input_nb_name,
            output_nb_name,
            start_time,
            pipeline_params,
            step_params,
            substeps_params,
            intermediate_runinfo = False):
        """Serializes run_info by DSMLComponentJob
           'intermediate_runinfo' parameter is for an intermediate run_info serialization
           Run result for intermediate_runinfo can be not set as 'SUCCESS'
        """
        #self._register_artifact_url(f"reports.{get_curr_notebook_name()}")

        start_time = datetime.fromisoformat(start_time)
        stop_time = datetime.now()
        if not intermediate_runinfo:
            self._run_result = NotebookSubstepRunResult.SUCCESS
            
        run_info = self._get_runinfo()
        run_info["input_nb_name"] = input_nb_name
        run_info["output_nb_name"] = output_nb_name

        # you can check if paremeters/run_paremeters were changed via sinara module run
        # by comparison run_info saved using serialize run and run_info saved via _save_runinfo
        # TODO: it makes sence to raise exception if parameters or run_parameters were changed
        run_info["pipeline_params"] = pipeline_params
        run_info["step_params"] = step_params
        run_info["substeps_params"] = substeps_params
        run_info["sinara_version"] = get_sinara_version()  

        run_info["start_time"] = f"{start_time}"
        run_info["stop_time"] = f"{stop_time}"
        run_info["status"] = 'COMPLETE'
        run_info["duration"] = f"{stop_time - start_time}"

        # TODO
        #run_info["commit"] = self._commit
        
        run_info["artifacts_url"] = self._artifacts_url()
        run_info["notebook_report_url"] = self.make_data_url(self._step_name, self._env_name, self._pipeline_name, self._zone_name, f"reports.{get_curr_notebook_name()}", self._run_id, 'outputs')
        
        # Image and Nexus Build Snapshot coordinates
        #run_info["image_info"] = {
        #    "hdfs_shapshot_runid": os.getenv("HDFS_SNAPSHOT_RUNID"),
        #    "hdfs_snapshot_root_path": os.getenv("HDFS_SNAPSHOT_ROOT_PATH"),
        #    "image_git_repo": os.getenv("IMAGE_GIT_REPO"),
        #    "image_git_repo_commit": os.getenv("IMAGE_GIT_REPO_COMMIT")
        #}

        #pp.pprint(run_info)

        otput_nb_stem = Path(output_nb_name).stem
        run_info_file_name = f"tmp/{otput_nb_stem}.runinfo.json"

        with open(run_info_file_name, 'w') as outfile:
            json.dump(run_info, outfile)


    def _get_runinfo(self):
        """Returns current run_info"""
        global start_time
        run_info = {}
        stop_time = datetime.now()
        run_info["start_time"] = f"{start_time}"
        run_info["stop_time"] = f"{stop_time}"
        run_info["result"] = self._run_result
        run_info["pipeline_params"] = self._pipeline_params
        run_info["step_params"] = self._step_params
        run_info["substeps_params"] = self._substeps_params
        run_info["resources"] = self.registered_inputs
        run_info["artifacts"] = self.registered_outputs
        run_info["cache"] = self.registered_tmp_io
        run_info["status"] = 'RUNNING'
        run_info["duration"] = f"{stop_time - start_time}"
        # TODO
        #run_info["origin"] = self._origin
        run_info["step_name"] = self._step_name

        return run_info

    
    def interface(self, *,
                  inputs=[],
                  outputs=[],
                  custom_inputs=[],
                  custom_outputs=[],
                  tmp_inputs=[],
                  tmp_outputs=[]
                 ):
        
        self._registered_inputs = self._get_validated_interface_data(inputs,
                                                                     required_keys=[STEP_NAME,
                                                                                    ENTITY_NAME],
                                                                     available_keys=[STEP_NAME,
                                                                                     ENTITY_NAME,
                                                                                     ENV_NAME,
                                                                                     PIPELINE_NAME,
                                                                                     ZONE_NAME,
                                                                                     RUN_ID],
                                                                     )
        
        self._inputs_for_print = [self.get_entity_for_print(entity, 'inputs') for entity in self._registered_inputs]
        
        #pp.pprint(self._inputs_for_print)
        
        self._registered_outputs = self._get_validated_interface_data(outputs,
                                                                     required_keys=[ENTITY_NAME],
                                                                     available_keys=[ENTITY_NAME])
        
        self._outputs_for_print = [self.get_entity_for_print(entity, 'outputs') for entity in self._registered_outputs]
        
        self._registered_custom_inputs = self._get_validated_interface_data(custom_inputs,
                                                                     required_keys=[ENTITY_NAME,
                                                                                    ENTITY_PATH],
                                                                     available_keys=[ENTITY_NAME,
                                                                                     ENTITY_PATH])
        
        self._custom_inputs_for_print = [self.get_entity_for_print(entity, 'custom_inputs') for entity in self._registered_custom_inputs]
        
        self._registered_custom_outputs = self._get_validated_interface_data(custom_outputs,
                                                                             required_keys=[ENTITY_NAME,
                                                                                            ENTITY_PATH],
                                                                             available_keys=[ENTITY_NAME,
                                                                                            ENTITY_PATH])
        
        self._custom_outputs_for_print = [self.get_entity_for_print(entity, 'custom_outputs') for entity in self._registered_custom_outputs]
        
        self._registered_tmp_inputs = self._get_validated_interface_data(tmp_inputs,
                                                                         required_keys=[ENTITY_NAME],
                                                                         available_keys=[ENTITY_NAME])
        
        self._tmp_inputs_for_print = [self.get_entity_for_print(entity,'tmp_inputs') for entity in self._registered_tmp_inputs]
        
        
        self._registered_tmp_outputs = self._get_validated_interface_data(tmp_outputs,
                                                                          required_keys=[ENTITY_NAME],
                                                                          available_keys=[ENTITY_NAME])
        
        self._tmp_outputs_for_print = [self.get_entity_for_print(entity, 'tmp_inputs') for entity in self._registered_tmp_outputs]
        
        
        
    def print_interface_info(self):
        """Prints artifacts, resources, cache urls and cache usage info"""
        
        if not self._inputs_for_print:
            return
        
        print_line_as_bold("INPUTS:")
        pp.pprint(self._inputs_for_print)
        print("\n")
        
        if not self._outputs_for_print:
            return
        print_line_as_bold("OUTPUTS:")
        pp.pprint(self._outputs_for_print)
        print("\n")
        
        if not self._custom_inputs_for_print:
            return
        print_line_as_bold("CUSTOM INPUTS:")
        pp.pprint(self._custom_inputs_for_print)
        print("\n")
        
        if not self._custom_outputs_for_print:
            return
        print_line_as_bold("CUSTOM OUTPUTS:")
        pp.pprint(self._custom_outputs_for_print)
        print("\n")
        
        if not self._tmp_inputs_for_print:
            return
        print_line_as_bold("TMP INPUTS:")
        pp.pprint(self._tmp_inputs_for_print)
        print("\n")
   
        if not self._tmp_outputs_for_print:
            return
        print_line_as_bold("TMP OUTPUTS:")
        pp.pprint(self._tmp_outputs_for_print)
        print("\n")

        #if not self._registered_last_cache_entities and not self._registered_new_cache_entities:
        #    return

        #print_line_as_bold("CACHE URLS:")
        #pp.pprint(self._registered_new_cache_entities)
        #pp.pprint(self._registered_last_cache_entities)
        #print("\n")

        #print_line_as_bold("CACHE USAGE:")
        #self.print_cache_usage()    
    
    @property
    def env_name(self):
        return self._env_name

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def zone_name(self):
        return self._zone_name

    @property
    def step_name(self):
        return self._step_name

    @property
    def run_id(self):
        return self._run_id
        
    @property
    def substep_name(self):
        return self._substep_name
    
    def _last_run_id_from_fs(self, entity_name, step_path):

        fs = SinaraFileSystem.FileSystem()

        entity_paths = fs.glob(f"{step_path}/*/{entity_name}/_SUCCESS")

        run_ids = sorted([entity_path.split("/")[-3] for entity_path in entity_paths])

        if len(run_ids) > 0:
            return run_ids[-1]
        else:
            return None

    def last_run_id(self, step_name, env_name, pipeline_name, zone_name, entity_name):
        
        env_path = get_env_path(env_name)
        
        print(env_path)
        
        step_path = f"{env_path}/{pipeline_name}/{zone_name}/{step_name}"
        print(step_path)

        last_run_id_from_fs = self._last_run_id_from_fs(entity_name, step_path)

        if last_run_id_from_fs is None:
            raise Exception(
                f"There is no successfully created entity for path: '{step_path}/*/{entity_name}' ")
        return last_run_id_from_fs

    def make_data_url(self, step_name, env_name, pipeline_name, zone_name, entity_name, run_id, data_type):
        self._validate_entity_name(entity_name)
        
        entity_name = entity_name.replace(".", "_")

        entity_full_name = self._get_entity_full_name(step_name, env_name, pipeline_name, zone_name, entity_name)

     
        env_path = get_env_path(env_name)
        entity_url = f"{env_path}/{pipeline_name}/{zone_name}/{step_name}/{run_id}/{entity_name}"
        
        if data_type == 'inputs':

            self.registered_inputs[entity_full_name] = entity_url
            
        elif data_type == 'outputs':
            
            self.registered_outputs[entity_full_name] = entity_url
        
        elif data_type == 'tmp_inputs' or data_type == 'tmp_outputs':
            
            self.registered_tmp_io[f'cache:{entity_full_name}'] = entity_url

        return entity_url

    def _get_entity_full_name(self, step_name, env_name, pipeline_name, zone_name, entity_name):
        return f"{env_name}.{pipeline_name}.{zone_name}.{step_name}.{entity_name}"

    def get_entity_for_print(self, entity, data_type):
        
        entity_name = entity.get(ENTITY_NAME)
        step_name = self._step_name
        env_name = self._env_name
        pipeline_name = self._pipeline_name
        zone_name = self._zone_name
        run_id = self._run_id
        
        self._validate_entity_name(entity_name)
        
        entity_name = entity_name.replace(".", "_")

        entity_full_name = self._get_entity_full_name(step_name, env_name, pipeline_name, zone_name, entity_name)

     
        env_path = get_env_path(env_name)
        entity_url = f"{env_path}/{pipeline_name}/{zone_name}/{step_name}/{run_id}/{entity_name}"
        
        if data_type == 'inputs':

            return {entity_full_name: entity_url}
            
        elif data_type == 'outputs':
            
            return {entity_full_name: entity_url }
        
        elif data_type == 'custom_inputs':
            entity_url = entity.get(ENTITY_PATH, None)

            return {entity_name: entity_url}
            
        elif data_type == 'custom_outputs':
            
            entity_url = entity.get(ENTITY_PATH, None)

            return {entity_name: entity_url}

        
        elif data_type == 'tmp_inputs' or data_type == 'tmp_outputs':
            
            return {f'cache:{entity_full_name}': entity_url }


    
    def _validate_entity_name(self, entity_name):
        """Checks allowed symbols of entity name and raise exeption for non-valid name"""
        if entity_name.find('/') != -1:
            raise Exception("Entity name can't contain '/' character")
    
    def _get_validated_interface_data(self, data, *, required_keys, available_keys):
        """
        ???????????? ???????? required && available
        ???????????????? ???????????????? ????????????????????
        ?? ???????????? ???? full step notebook
        
        """
        if data:
            for required_key in required_keys:
                for data_item in data:
                    if required_key not in data_item.keys():
                        raise Exception(f"Mandatory key '{required_key}' isn't defined in {data_item}. Required keys are {available_keys}")

            for data_item in data:
                for key, value in data_item.items():
                    if key not in available_keys:
                        raise Exception(f"This key '{key}' in '{data_item}' isn't permitted. Available keys are {available_keys}")
        return data

    def inputs(self, *, step_name, env_name="curr_env_name", pipeline_name="curr_pipeline_name", zone_name="curr_zone_name", run_id="last_run_id"):
        
        registered_inputs_info = []
        
        for _input in self._registered_inputs:
            entity_name = _input.get(ENTITY_NAME, None)
            pipeline_name = pipeline_name if pipeline_name != "curr_pipeline_name" else self.pipeline_name
            env_name = env_name if env_name != "curr_env_name" else self.env_name
            zone_name = zone_name if zone_name != "curr_zone_name" else self.zone_name
            run_id = run_id if run_id != "last_run_id" else self.last_run_id(step_name, env_name, pipeline_name, zone_name, entity_name)
           
            
            entity_url = self.make_data_url(step_name, env_name, pipeline_name, zone_name, entity_name, run_id, 'inputs')
            

            #self.requested_resources = {}

            #component_run_id = f"{env_name}.{pipeline_name}.{zone_name}.{step_name}"

            #module._register_resource_component(component_run_id, self)

            registered_inputs_info.append((entity_name, str, dataclasses.field(default=entity_url)))
            
            

        # python allows different classes with the same name
        registered_inputs = dataclasses.make_dataclass('InputUrls',
                                                        registered_inputs_info,
                                                    bases=(DSMLUrls,),
                                                    frozen=True)()
        pp.pprint(registered_inputs)
        print("\n")
        return registered_inputs
            
    def outputs(self, *, env_name="curr_env_name", pipeline_name="curr_pipeline_name", zone_name="curr_zone_name"):     

        registered_outputs_info = []
        
        for _output in self._registered_outputs:
            step_name = self._step_name
            entity_name = _output.get(ENTITY_NAME, None)
            pipeline_name = pipeline_name if pipeline_name != "curr_pipeline_name" else self.pipeline_name
            env_name = env_name if env_name != "curr_env_name" else self.env_name
            zone_name = zone_name if zone_name != "curr_zone_name" else self.zone_name
            run_id = self._run_id
           
            
            entity_url = self.make_data_url(step_name, env_name, pipeline_name, zone_name, entity_name, run_id, 'outputs')


            registered_outputs_info.append((entity_name, str, dataclasses.field(default=entity_url)))
            
            

        # python allows different classes with the same name
        registered_outputs = dataclasses.make_dataclass('OutputUrls',
                                                        registered_outputs_info,
                                                    bases=(DSMLUrls,),
                                                    frozen=True)()
        pp.pprint(registered_outputs)
        print("\n")
        return registered_outputs

            

    def custom_inputs(self):
        
        registered_inputs_info = []
        
        for _input in self._registered_custom_inputs:
            entity_name = _input.get(ENTITY_NAME, None)
            
            entity_url = _input.get(ENTITY_PATH, None)

            registered_inputs_info.append((entity_name, str, dataclasses.field(default=entity_url)))
            
            

        # python allows different classes with the same name
        registered_inputs = dataclasses.make_dataclass('CustomInputUrls',
                                                        registered_inputs_info,
                                                    bases=(DSMLUrls,),
                                                    frozen=True)()
        pp.pprint(registered_inputs)
        print("\n")
        return registered_inputs

            
    def custom_outputs(self):     

        registered_outputs_info = []
        
        for _output in self._registered_custom_outputs:
            entity_name = _output.get(ENTITY_NAME, None)
            entity_url = _output.get(ENTITY_PATH, None)


            registered_outputs_info.append((entity_name, str, dataclasses.field(default=entity_url)))
            
            

        # python allows different classes with the same name
        registered_outputs = dataclasses.make_dataclass('CustomOutputUrls',
                                                        registered_outputs_info,
                                                    bases=(DSMLUrls,),
                                                    frozen=True)()
        pp.pprint(registered_outputs)
        print("\n")
        return registered_outputs
            

    def tmp_inputs(self, *, env_name="curr_env_name", pipeline_name="curr_pipeline_name", zone_name="curr_zone_name", run_id="last_run_id"):
        
        registered_inputs_info = []
        
        for _input in self._registered_tmp_inputs:
            step_name = self._step_name
            entity_name = _input.get(ENTITY_NAME, None)
            pipeline_name = pipeline_name if pipeline_name != "curr_pipeline_name" else self.pipeline_name
            env_name = env_name if env_name != "curr_env_name" else self.env_name
            zone_name = zone_name if zone_name != "curr_zone_name" else self.zone_name
            run_id = run_id if run_id != "last_run_id" else self.last_run_id(step_name, env_name, pipeline_name, zone_name, entity_name)
           
            
            entity_url = self.make_data_url(step_name, env_name, pipeline_name, zone_name, entity_name, run_id, 'tmp_inputs')

            registered_inputs_info.append((entity_name, str, dataclasses.field(default=entity_url)))
            
            

        # python allows different classes with the same name
        registered_inputs = dataclasses.make_dataclass('TmpInputUrls',
                                                        registered_inputs_info,
                                                    bases=(DSMLUrls,),
                                                    frozen=True)()
        pp.pprint(registered_inputs)
        print("\n")
        return registered_inputs
            
    def tmp_outputs(self, *, env_name="curr_env_name", pipeline_name="curr_pipeline_name", zone_name="curr_zone_name"):     

        registered_outputs_info = []
        
        for _output in self._registered_tmp_outputs:
            step_name = self._step_name
            entity_name = _output.get(ENTITY_NAME, None)
            pipeline_name = pipeline_name if pipeline_name != "curr_pipeline_name" else self.pipeline_name
            env_name = env_name if env_name != "curr_env_name" else self.env_name
            zone_name = zone_name if zone_name != "curr_zone_name" else self.zone_name
            run_id = self._run_id
           
            
            entity_url = self.make_data_url(step_name, env_name, pipeline_name, zone_name, entity_name, run_id, 'tmp_outputs')


            registered_outputs_info.append((entity_name, str, dataclasses.field(default=entity_url)))
            
            

        # python allows different classes with the same name
        registered_outputs = dataclasses.make_dataclass('TmpOutputUrls',
                                                        registered_outputs_info,
                                                    bases=(DSMLUrls,),
                                                    frozen=True)()
        pp.pprint(registered_outputs)
        print("\n")
        return registered_outputs