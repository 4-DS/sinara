import json
from pathlib import Path, PurePosixPath
from datetime import datetime
import os
import shutil
import sys
import logging
from .substep import get_curr_run_id, reset_curr_run_id, set_curr_notebook_name, set_curr_notebook_output_name, get_curr_notebook_name,\
    get_sinara_step_tmp_path, get_tmp_prepared, StopExecution
#from .report_publisher import ReportPublisher
from .substep import print_line_as_bold, ipynb_to_html
import fnmatch
import git
import glob
from IPython.core.display import display
import pandas as pd
import pprint
import copy
import re

from subprocess import STDOUT, PIPE, Popen, run, CalledProcessError

import nbformat
import jupyter_client

from sinara.common import importSinaraModuleClass
from .fs import SinaraFileSystem

class Step:
# Here we use 'reset_curr_run_id' to ensure an unique run_id every time we are running Sinara Step interactively 
    def __init__(self, 
                step_params_file_globs,
                pipeline_params_file_globs = None,
                env_name = None):

        os.environ["SNR_IS_JOB_RUN"] = "True"
        get_tmp_prepared()
        
        self.notebooks = []
        self.exit_code = 0
        self._curr_exception = None
        step_params_file_path = self._get_run_params_file(step_params_file_globs)
        
        pipeline_params_file_path = None
        
        if pipeline_params_file_globs:
            pipeline_params_file_path = self._get_run_params_file(pipeline_params_file_globs)
            
        set_run_papermill_params(step_params_file_path, pipeline_params_file_path)
        substeps_params = get_run_papermill_params()["substeps_params"]

        print_line_as_bold(f"SINARA Step params:")
        run_parameters_to_print = copy.deepcopy(get_run_papermill_params()["pipeline_params"])

        if env_name is not None:
            run_parameters_to_print["env_name"] = env_name
            os.environ["SINARA_STEP_ENV_NAME"] = env_name
        pprint.pprint(run_parameters_to_print, compact=True)
        print("\n")
        
        try:
            if type(substeps_params) is list:
                for nb in substeps_params:
                    if type(nb) is str:
                        if '.py' in nb:
                            self.notebooks.append(SinaraStepPythonModule(nb, env_name = env_name))
                        else:
                            self.notebooks.append(SinaraStepNotebook(nb, env_name = env_name))
                    elif type(nb) is dict:
                        name = nb["substep_name"]
                        params = nb["substep_params"]
                        if '.py' in name:
                            self.notebooks.append(SinaraStepPythonModule(name, 
                                                                    sent_params = params,
                                                                    replace_params_file = True,
                                                                    env_name = env_name))
                        else:
                            self.notebooks.append(SinaraStepNotebook(name, 
                                                                    sent_params = params,
                                                                    replace_params_file = True,
                                                                    env_name = env_name))
                    else:
                        raise Exception("Incorrect format of substeps_params")
            elif type(substeps_params) is str:
                self.notebooks.append(SinaraStepNotebook(substeps_params,"ci_params.json", env_name))
                # ci_params.json is legacy file name for backward compatibility 
            else:
                raise Exception("Incorrect format of substeps_params")
        except Exception as e:
            raise Exception(f"Mandatory parameter 'substeps_params' has incorrect format. Must be either list of strings or params")
        finally:
            reset_curr_run_id()
            
    def _get_run_params_file(self, run_params_file_path):
        
        if isinstance(run_params_file_path, str):
            run_params_file_path = [run_params_file_path]
            
        filenames = []
        for gl in run_params_file_path:
            filenames += glob.glob(gl)
        if len(filenames) == 1:
            return filenames[0]
        elif len(filenames) == 0:
            raise Exception("run_params_file_path doesn't match any file")
    
        df = pd.DataFrame(filenames, columns =['run params file'])    
        display(df)
    
        while True:
            print("Select an index of run params file from the table above:")
            index = input("")
            if not index.isnumeric() or not 0 <= int(index) < len(filenames):
                print(f"Specify integer index value between 0 and {len(filenames)-1}, please!")
                continue
            else:
                return filenames[int(index)]
    
    def handle_exception(self, e):
        if isinstance(e, StopExecution):
            self.exit_code = 254
        else:
            self.exit_code = 1
            print(e)
        self._curr_exception = e
    
    def handle_exit(self):
        ''' Sets correct exit code '''
        # Suppress exception on sys.exit(0) - happens only in ipython
        if not interpreter_is_ipython():
            sys.exit(self.exit_code)
        
    @staticmethod
    def clear_cache():
        #clear_cache removes files created inside current run
        run_id = get_curr_run_id()
        for run_info_file_name in glob.glob(f"tmp/{run_id}*.runinfo.json"):
            with open(run_info_file_name) as json_file:
                cache_urls = json.load(json_file)["cache"]
                for cache_url in cache_urls.values():
                    try:
                        shutil.rmtree(cache_url)
                    except:
                        pass
                    try:
                        os.remove(cache_url)
                    except:
                        pass
                        
        
#SinaraStepJob = SinaraStepCICD # SinaraStepCICD name for backward compatibility

#import atexit
#_=atexit.register(SinaraStepJob.clear_cache)


from abc import ABC,abstractmethod
 
class SinaraStepModule(ABC):
    @abstractmethod
    def parse(self):
        pass
    
    @abstractmethod
    def run(self):
        pass
    
    @abstractmethod
    def publish(self, git_repository=None):
        pass
    
    @abstractmethod
    def reproduce(self, runinfo_path):
        pass
    
    @abstractmethod
    def print_Step_requirements(self):
        pass

class SinaraStepNotebook(SinaraStepModule):

    def __init__(self, nb_name, 
                 params_file_name=None, #parameters file name or by default will be  f'{Path(nb_name).stem}.params.json'
                 sent_params = None, #explicit parameters will replace parameters in the params_file_name
                 replace_params_file = False, #params inside params_file_name must be either replaced by sent_params or joined with sent_params
                 external_entity_catalogue = None, # resourses catalogue will replace resources than defined in the notebook's code
                 env_name = None, #  redefine env where notebook will be run
                 stand_name = None, #  redefine zone name in which notebook will be run
                 standalone_run = False): # notebook will be executed in individual run (separately fron other notebooks in the step)
        self.input_nb_name = nb_name
        if params_file_name is None:
            substeps_params = get_run_papermill_params()["substeps_params"]
            if type(substeps_params) is list:
                self.nb_params_name = f'{Path(nb_name).stem}.params.json'
            elif type(substeps_params) is str:
                self.nb_params_name = "ci_params.json"
            else:
                raise Exception(f"Mandatory parameter 'substeps_params' has incorrect type")
        else:
            self.nb_params_name = params_file_name
        self.replace_params_file = replace_params_file
        self.sent_params = sent_params
        self.external_entity_catalogue = external_entity_catalogue
        self.stand_name = stand_name
        self.env_name = env_name
        if standalone_run:
            reset_curr_run_id()
        self.standalone_run = standalone_run
        self.run_report_paths = {
            "commit_report":None,
            "business_report":None,
            "run_info": None,
            "run_id": None
        }

    def parse(self):
        from nbconvert import NotebookExporter
        self.nb_body, self.inputs = NotebookExporter().from_filename(self.input_nb_name)
        self.input_nb_dict = json.loads(self.nb_body)
        self.tagged_known_cells = {}
        self.output_nb_dict = self.input_nb_dict.copy()

        known_tags = ["parameters"]
        musthave_tags = ["parameters"]

        # fill in the tagged_known_cells
        for cell in self.input_nb_dict["cells"]:
            metadata = cell["metadata"]
            if "tags" in metadata:
                tags = metadata["tags"]
                for tag in tags:
                    if tag in known_tags:
                        if tag in self.tagged_known_cells:
                            self.print_Step_requirements()
                            raise Exception(f"Tag '{tag}' is applied more than once")
                        self.tagged_known_cells[tag] = cell

        # check if all musthave_tags exist
        for tag in musthave_tags:
            if tag not in self.tagged_known_cells:
                self.print_Step_requirements()
                raise Exception(f"SINARA notebook requires a cell tagged by tag:'{tag}'")

    def run(self):
        """
        Run all substeps using papermill engine
        """
        def _get_jupyter_kernel_name():
            desired_kernel = None
            if "kernel_name" in params["step_params"]:
                desired_kernel = params["step_params"]["kernel_name"]
            elif "kernel_name" in params["pipeline_params"]:
                desired_kernel = params["pipeline_params"]["kernel_name"]
                
            import jupyter_client
            available_kernels = jupyter_client.kernelspec.find_kernel_specs()
            if available_kernels:
                default_kernel = next(iter(available_kernels))
                if desired_kernel and desired_kernel in available_kernels:
                    return desired_kernel
                elif desired_kernel and desired_kernel not in available_kernels:
                    print_line_as_bold(f"WARNING: kernel '{desired_kernel}' is not installed, using defaut kernel '{default_kernel}'")
                return default_kernel
            else:
                print_line_as_bold(f"WARNING: no kernels are installed")
                return None
        
        self.parse()
        
        self._clear_source_by_tag("parameters")

        self.output_nb_name = self._get_output_notebook_name()
        self._append_serialize_run_cell()

        import tempfile
        temp_nb_name = tempfile.gettempdir() + "/" + self.input_nb_name

        import nbformat
        nbformat.write(nbformat.from_dict(self.output_nb_dict), temp_nb_name, 4)

        
        params = get_papermill_params(self.nb_params_name, 
                                      self.sent_params,
                                      self.replace_params_file,
                                      self.external_entity_catalogue,
                                      self.stand_name,
                                      self.env_name)
        
        import papermill

        
        commit_report_path = f"tmp/{self.output_nb_name}"
        set_curr_notebook_name(self.input_nb_name)
        set_curr_notebook_output_name(self.output_nb_name)
        
        print_line_as_bold(f"SINARA Notebook params for {self.input_nb_name}:")
        pprint.pprint(params, compact=True)
        pprint.pprint(params["params"], compact=True)
        
        #try:
        #kernel_name_param = params["run_params"]["conda_env"]
            
        #ksm = jupyter_client.kernelspec.KernelSpecManager()
        #kernel_specs = ksm.find_kernel_specs()
        #kernel_specs.pop('python3', None)
        #kernel_name = next(iter(kernel_specs))
        #if len(kernel_specs) == 1 and kernel_name != kernel_name_param:
        #    print_line_as_bold('\033[1m' + f"WARNING: conda_env '{kernel_name_param}' not exists in kernels, running defaut kernel '{kernel_name}'" + '\033[0m')
        #    kernel_name_param = kernel_name
        _SinaraSettings = importSinaraModuleClass(module_name = "settings", class_name = "_SinaraSettings")
        if hasattr(_SinaraSettings, '_lazy_init_settings'):
            _SinaraSettings._lazy_init_settings()
        
        try:
            jupyter_kernel_name = _get_jupyter_kernel_name()

            stdout = None
            stderr = None
            step_verbose = None
            substep_verbose = None
            
            if "verbose_output" in params["step_params"]:
                step_verbose = params["step_params"]["verbose_output"]

            if "verbose_output" in params["substep_params"]:
                substep_verbose = params["substep_params"]["verbose_output"]

            if substep_verbose is None:
                verbose_output = step_verbose if step_verbose else False
            else:
                verbose_output = substep_verbose

            if verbose_output:
                stdout = sys.stdout
                stderr = sys.stderr

            nn = papermill.execute.execute_notebook(temp_nb_name,
                                                        commit_report_path,
                                                        kernel_name=jupyter_kernel_name,
                                                        # actual pipeline and step parameters are loaded in the notebook
                                                        parameters={"substep_params": params["substep_params"]},
                                                        log_output=verbose_output,
                                                        autosave_cell_every=10,
                                                        stdout_file=stdout,
                                                        stderr_file=stderr)
        except Exception as e:
            if hasattr(e, 'ename') and e.ename == 'StopExecution':
                raise StopExecution
            raise e

        #finally:
        import pathlib
        commit_report_stem = Path(commit_report_path).stem
        commit_report_dir = str(Path(commit_report_path).parent)
        business_report_path = f"{commit_report_dir}/{commit_report_stem}.business_report.ipynb"
        runinfo_path = f"{commit_report_dir}/{commit_report_stem}.runinfo.json"

        if os.path.exists(runinfo_path):
            write_business_report(commit_report_path, business_report_path,runinfo_path)

        self.run_report_paths["commit_report"] = str(pathlib.Path(commit_report_path).absolute())
        self.run_report_paths["business_report"] = str(pathlib.Path(business_report_path).absolute())
        self.run_report_paths["run_info"] = str(pathlib.Path(runinfo_path).absolute())
        self.run_report_paths["run_id"] = get_curr_run_id()
            
        return self.run_report_paths

    def publish(self, git_repository=None):
        """
        This is for back compatibility only. The method is deprecated.
        """
        return ''

    def reproduce(self, runinfo_path):
        # TODO:
        pass

    def print_Step_requirements(self):
        
        print_line_as_bold("SINARA Notebook requirements:")
        print( '''\
        1. Notebook must contain separated cell tagged as "parameters"
        2. Notebook must contain the initialized variables:
            - substep_params(dict) and pipeline_params(dict) inside a cell tagged as "parameters" 
            - inputs(dict)
            - outputs (dict)
        3. substep_params, pipeline_params, inputs, outputs must not be changed after the initilization
        ''')

    def _get_output_notebook_name(self):

        run_id = get_curr_run_id()
        input_nb_stem = Path(self.input_nb_name).stem

        return f"{run_id}_{input_nb_stem}.ipynb"

    def _clear_source_by_tag(self, tag):
        for cell in self.output_nb_dict["cells"]:
            metadata = cell["metadata"]
            if "tags" in metadata:
                tags = metadata["tags"]
                if tag in tags:
                    cell["source"] = []

    def _remove_cell_by_tag(self, tag):
        for cell in list(self.output_nb_dict["cells"]):
            metadata = cell["metadata"]
            if "tags" in metadata:
                tags = metadata["tags"]
                if tag in tags:
                    self.output_nb_dict["cells"].remove(cell)

    def _append_serialize_run_cell(self):

        start_time = datetime.now()
        serialize_run_cell = {'cell_type': 'code',
                              'execution_count': None,
                              'metadata': {'tags': ['serialize_run']},
                              'outputs': [],
                              'source': ['if "substep" in globals():\n',
                                            f'    substep._serialize_run(\n\
                                            "{self.input_nb_name}",\n\
                                            "{self.output_nb_name}",\n\
                                            "{start_time}",\n\
                                            pipeline_params,\n\
                                            step_params,\n\
                                            substep_params)\n'
                                            'else:\n'
                                            "    raise Exception('SINARA module must have defined module variable')"]}
        
        self.output_nb_dict["cells"].append(serialize_run_cell)
        
        #notebook_name_cell = {'cell_type': 'code',
        #                      'execution_count': None,
        #                      'metadata': {},
        #                      'outputs': [],
        #                     'source': ['import os\n',
        #                                   f'os.environ["SINARA_NOTEBOOK_NAME"] = "{self.input_nb_name}"']}
        
        
        
        #self.output_nb_dict["cells"].insert(1, notebook_name_cell)

class SinaraStepPythonModule(SinaraStepModule):

    def __init__(self, nb_name, 
                 params_file_name=None, #parameters file name or by default will be  f'{Path(nb_name).stem}.params.json'
                 sent_params = None, #explicit parameters will replace parameters in the params_file_name
                 replace_params_file = False, #params inside params_file_name must be either replaced by sent_params or joined with sent_params
                 external_entity_catalogue = None, # resourses catalogue will replace resources than defined in the notebook's code
                 env_name = None, #  redefine env where notebook will be run
                 stand_name = None, #  redefine zone name in which notebook will be run
                 standalone_run = False): # notebook will be executed in individual run (separately fron other notebooks in the step)
        
        self.input_nb_name = nb_name
        if params_file_name is None:
            substeps_params = get_run_papermill_params()["substeps_params"]
            if type(substeps_params) is list:
                self.nb_params_name = f'{Path(nb_name).stem}.params.json'
            elif type(substeps_params) is str:
                self.nb_params_name = "ci_params.json"
            else:
                raise Exception(f"Mandatory parameter 'substeps_params' has incorrect type")
        else:
            self.nb_params_name = params_file_name
        self.replace_params_file = replace_params_file
        self.sent_params = sent_params
        self.external_entity_catalogue = external_entity_catalogue
        self.stand_name = stand_name
        self.env_name = env_name
        if standalone_run:
            reset_curr_run_id()
        self.standalone_run = standalone_run
        self.run_report_paths = {
            "commit_report":None,
            "business_report":None,
            "run_info": None,
            "run_id": None
        }

    def parse(self):
        pass

    def run(self):
        self.output_nb_name = self._get_output_notebook_name()

        params = get_papermill_params(self.nb_params_name, 
                                              self.sent_params,
                                              self.replace_params_file,
                                              self.external_entity_catalogue,
                                              self.stand_name,
                                              self.env_name)
        
        commit_report_path = f"tmp/{self.output_nb_name}"
        shutil.copy(self.input_nb_name, commit_report_path)
        
        set_curr_notebook_name(self.input_nb_name)
        set_curr_notebook_output_name(self.output_nb_name)
        
        print_line_as_bold(f"SINARA Module params for {self.input_nb_name}:")
        pprint.pprint(params["params"], compact=True)
        
        try:
            notebook_params = json.dumps(params["params"])
            notebook_run_params = json.dumps(params["run_params"])
                #run_result = run(f"python {self.input_nb_name} --params '{notebook_params}' --run_params '{notebook_run_params}' | tee {commit_report_path}.log", 
                #                 shell=True, cwd=None, check=True)
                
            with Popen(f"python {self.input_nb_name} --params '{notebook_params}' --run_params '{notebook_run_params}'", 
                                 shell=True, stdout=PIPE, stderr=STDOUT, cwd=None) as child_process, open(f'{commit_report_path}.log', 'w') as logfile:

                for line in child_process.stdout:
                    decoded_line = line.decode("utf-8")
                    sys.stdout.write(decoded_line)
                    logfile.write(decoded_line)
                child_process.communicate()
                
                if child_process.returncode != 0:
                    raise Exception(f"SINARA Python module '{self.input_nb_name}' is failed!")

        finally:
            import pathlib
            commit_report_stem = Path(commit_report_path).stem
            commit_report_dir = str(Path(commit_report_path).parent)
            #business_report_path = f"{commit_report_dir}/{commit_report_stem}.business_report.ipynb"
            #runinfo_path = f"{commit_report_dir}/{commit_report_stem}.runinfo.json"

            #write_business_report(commit_report_path, business_report_path,runinfo_path)

            self.run_report_paths["commit_report"] = str(pathlib.Path(commit_report_path).absolute())
            #self.run_report_paths["business_report"] = str(pathlib.Path(business_report_path).absolute())
            #self.run_report_paths["run_info"] = str(pathlib.Path(runinfo_path).absolute())
            self.run_report_paths["run_id"] = get_curr_run_id()

        return self.run_report_paths

    def publish(self, git_repository=None):
        """
        This is for back compatibility only. The method is deprecated.
        """
        return ''

    def reproduce(self, runinfo_path):
        # TODO:
        pass

    def print_Step_requirements(self):
        
        print_line_as_bold("SINARA Python Module requirements:")
        print( '''\
        1. Module must contain the initialized variables:
            - params(dict) and run_params(dict)
        2. params, run_params, inputs, outputs and result values must not be changed after the initilization
        ''')

    def _get_output_notebook_name(self):

        run_id = get_curr_run_id()
        input_nb_stem = Path(self.input_nb_name).stem

        return f"{run_id}_{input_nb_stem}.py"
        
def get_papermill_params(
                ci_params_file_name, 
                sent_params = None,
                replace_params_file = False,
                external_entity_catalogue = None,
                stand_name = None,
                env_name = None
                ):
    papermill_params = get_run_papermill_params()
    
    if replace_params_file:
        # full params file format support
        papermill_params["substep_params"] = sent_params
    else:
        #for backward compatibility when separate file is defined for each notebook
        with open(ci_params_file_name) as json_file:
            papermill_params["params"] = json.load(json_file)
        #for backward compatiblity when an system test partially replaces params in notebokk paramer file     
        if sent_params is not None:
            papermill_params["params"] = {**papermill_params["params"],**sent_params}
    
        
    if stand_name is not None:
        papermill_params["pipeline_params"]["zone_name"] = stand_name
    
    if env_name is not None:
        papermill_params["pipeline_params"]["env_name"] = env_name
        
    del papermill_params["substeps_params"]
    
    return papermill_params

    
def set_run_papermill_params(step_params_file_path, pipeline_params_file_path=None):
    os.environ["SINARA_STEP_PARAMS_FILE_PATH"] = step_params_file_path
    os.environ["SINARA_PIPELINE_PARAMS_FILE_PATH"] = '' if not pipeline_params_file_path else pipeline_params_file_path

def get_run_papermill_params():
    step_params_file_path = os.environ["SINARA_STEP_PARAMS_FILE_PATH"]
    pipeline_params_file_path = os.environ["SINARA_PIPELINE_PARAMS_FILE_PATH"]
    
    papermill_params = {}
    with open(step_params_file_path) as json_file:
        papermill_params["params"] = json.load(json_file)

    required_pipeline_params = ["env_name",
                               "pipeline_name",
                               #"zone_name",
                               #"docker_image",
                               #"conda_env",
                          ]
    
    papermill_params["pipeline_params"] = papermill_params["params"]["pipeline_params"]
    
    if pipeline_params_file_path:
        with open(pipeline_params_file_path) as json_file:
            papermill_params["pipeline_params"] = json.load(json_file)["pipeline_params"]
            
    papermill_params["step_params"] = papermill_params["params"]["step_params"]
    papermill_params["substeps_params"] = papermill_params["params"]["substeps_params"]
    
    for param in required_pipeline_params:
        if param not in papermill_params["pipeline_params"]:
            raise Exception(f"Mandatory parameter '{param}' isn't defined in {step_params_file_path}")
    return papermill_params


def write_business_report(nb_commit_report, 
                          nb_business_report_file_name, 
                          runinfo_file_name):
    print(f"RUN INFO FILE NAME:{runinfo_file_name}")
    from nbconvert import NotebookExporter
    import json

    (body, inputs) = NotebookExporter().from_filename(nb_commit_report)

    body_dict = json.loads(body)
    
    # remove non business_report cell
    for cell in list(body_dict["cells"]):
        if cell["cell_type"] != "code":
            continue
        is_business_report_cell = "business_report" in cell["metadata"]["tags"]

        if not cell["outputs"] or not is_business_report_cell:
            body_dict["cells"].remove(cell)
        else:
            cell["source"] = []

    with open(runinfo_file_name) as json_file:
        runinfo_dict = json.load(json_file)
        body_dict["cells"].insert(0, create_business_report_summary(runinfo_dict))

    import nbformat
    nbformat.write(nbformat.from_dict(body_dict), nb_business_report_file_name, 4)


def create_business_report_summary(runinfo_dict):
    # define business report temaple
    import uuid
    business_report_cell = {
        "cell_type": "markdown",
        "id": str(uuid.uuid4()),
        "metadata": {},
        "source": [
            "#### Step {product_name}/{Step_name}/{commit} has been built and checked \n",
            "\n",
            "**Inputs**:\n",
            "- {input_id} : {input_url}\n",
            "    \n",
            "**Outputs**:\n",
            "- {output_id} : {output_url}\n",
            "    \n",
            "**Details**:\n",
            "- Start time (UTC): {start_time}\n",
            "- Duration (UTC): {duration}"
        ]
    }
    HEADER_INDEX = 0
    INPUT_INDEX = 3
    OUTPUT_INDEX = 6
    START_TIME_INDEX = 9
    DURATION_INDEX = 10

    # specify header
    header_params = {
        "product_name": runinfo_dict["pipeline_params"]["pipeline_name"],
        "Step_name": runinfo_dict["step_name"],
        #TODO: enable commit saving
        "commit": 'None'
    }
    header_template = business_report_cell["source"][HEADER_INDEX]
    header = header_template.format(**header_params)
    business_report_cell["source"][HEADER_INDEX] = header

    # specify inputs
    input_template = business_report_cell["source"][INPUT_INDEX]
    inputs = ""
    for res_key, res_value in runinfo_dict["inputs"].items():
        inputs = inputs + input_template.format(input_id=res_key, input_url=res_value) + "\n"
    business_report_cell["source"][INPUT_INDEX] = inputs

    # specify inputs
    output_template = business_report_cell["source"][OUTPUT_INDEX]
    outputs = ""
    for artf_key, artf_value in runinfo_dict["outputs"].items():
        outputs = outputs + output_template.format(output_id=artf_key, output_url=artf_value) + "\n"
    business_report_cell["source"][OUTPUT_INDEX] = outputs

    # specify start_time
    start_time_template = business_report_cell["source"][START_TIME_INDEX]
    start_time = start_time_template.format(start_time=runinfo_dict["start_time"])
    business_report_cell["source"][START_TIME_INDEX] = start_time

    # specify duration
    duration_template = business_report_cell["source"][DURATION_INDEX]
    duration = duration_template.format(duration=runinfo_dict["duration"])
    business_report_cell["source"][DURATION_INDEX] = duration
    
    return business_report_cell        

def interpreter_is_ipython():
    try:
        return bool(__IPYTHON__)
    except NameError:
        return False

class StepSafeguard:
    
    @staticmethod
    def step_is_in_dir(globs):
        if isinstance(globs, str):
            globs = [globs]
        for glob in globs:  
            if fnmatch.fnmatch(sys.path[0].lower(), glob):
                return True
        raise Exception(f"This can be run for steps only located in a directory matches '{globs}' pattern")
    
    
    @staticmethod
    def step_is_in_branch(globs):
        if isinstance(globs, str):
            globs = [globs]
        for glob in globs:  
            if fnmatch.fnmatch(git.Repo().active_branch.name.lower(), glob):
                return True
        raise Exception(f"This can be run for steps only located in a git branch matches '{globs}' pattern")
            
    @staticmethod
    def git_reset(branch=None):
        curr_branch = git.Repo().active_branch.name
        if not branch or curr_branch == branch:
            git.Repo().head.reset(index=True,working_tree=True)
        else:
            git.Repo().git.checkout(branch)
            git.Repo().head.reset(index=True,working_tree=True)
            git.Repo().git.checkout(curr_branch)

class StepReport:
   
    @staticmethod
    def tag_commit_by_run(run_id=None, run_fs_path=None):
        run_id = run_id or StepReport._get_last_run_id()
        run_fs_path = run_fs_path or StepReport._get_run_fs_path(run_id)
        StepReport._fetch_all_tags()
        tag_ref = git.Repo().create_tag(run_id, message=run_fs_path)
        git.Repo().remote().push(tag_ref)
        
    @staticmethod
    def _fetch_all_tags():
        #TODO: git fetch --all --tags
        #https://linuxtut.com/en/76a3fb171e9143ff695e/
        
        origin = git.Repo().remote()
        tags = origin.fetch(**{"tags":True})
       
    @staticmethod
    def _get_run_fs_path(run_id = None):
        run_id = run_id or StepReport._get_last_run_id()
        run_info_file_names = glob.glob(f"tmp/{run_id}*.runinfo.json")
        if len(run_info_file_names) == 0:
            raise Exception(f"tmp folder doesn't contain '{run_id}*.runinfo.json' file ")
        run_info_file_name = run_info_file_names[0]
        
        with open(run_info_file_name) as json_file:       
            outputs_urls = json.load(json_file)["outputs"]
            if len(outputs_urls) == 0:
                raise Exception(f"Sinara step outputs inside '{run_info_file_name}' file must contain at least one output ")
            outputs_url = list(outputs_urls.values())[0]
            return re.match(f"(.*?{run_id})",outputs_url).group()
        
    @staticmethod
    def _get_last_run_id():
        run_info_file_names = glob.glob(f"tmp/run*.runinfo.json")
        if len(run_info_file_names) == 0:
            raise Exception("there is not *.runinfo.json files inside tmp folder")
        run_info_file_name = sorted(run_info_file_names)[-1]
        return re.match(f".*(run-.*?)_", run_info_file_name).group(1)

    @staticmethod
    def save(run_id=None):
        run_id = run_id or get_curr_run_id()
        
        for run_info_file_name in glob.glob(f"tmp/{run_id}*.runinfo.json"):
            
            nb_name = re.search(f"{run_id}_(.+?).runinfo.json", run_info_file_name).group(1)
            local_runinfo_path = f"tmp/{run_id}_{nb_name}.runinfo.json"
            
            local_metrics_path = f"tmp/{run_id}_{nb_name}.metrics.json"
            
            nb_module_detected = None
            local_report_path = ''
            _local_report_path = f"tmp/{run_id}_{nb_name}.ipynb" 
            if Path(_local_report_path).is_file():
                local_report_path = _local_report_path
                nb_module_detected = True
            else:
                _local_report_path = f"tmp/{run_id}_{nb_name}.py.log"
                if Path(_local_report_path).is_file():
                    local_report_path = _local_report_path
                    nb_module_detected = False
            
            fs = SinaraFileSystem.FileSystem()
                
            if nb_module_detected:
                local_business_report_path = f"tmp/{run_id}_{nb_name}.business_report.ipynb"
            
                hmtl_local_report_path = f"tmp/{run_id}_{nb_name}.html"
                hmtl_local_business_report_path = f"tmp/{run_id}_{nb_name}.business_report.html"
            
                with open(run_info_file_name) as json_file:

                    outputs_urls = json.load(json_file)["outputs"]
                    reports_urls = [x for x in outputs_urls if re.search(f"(.+?).reports.{nb_name}.ipynb", x)]
                    if len(reports_urls) != 1:
                        raise Exception(f"Sinara step outputs must contain exactly one output url for reports like '*.reports.{nb_name}.ipynb' ")
                    target_reports_dir_path = outputs_urls[reports_urls[0]]
      
                    base_output_url = Path(target_reports_dir_path).parent
                    tensorboard_output_dir = base_output_url / "tensorboard"

                    tmp_tensorboard_log_dir = f"tmp/tensorboard"
                    log_events = fs.glob(f"{tmp_tensorboard_log_dir}/**/{run_id}/events.out*")
                    for log_path in log_events:
                        p = Path(log_path)
                        events_file = p.name
                        log_name = p.parts[-3]
                        output_dir = tensorboard_output_dir / log_name
                        output_path = output_dir / events_file
                        fs.makedirs(output_dir)
                        print(f"Saving tensorboard logs {log_path} to {output_path}")
                        fs.put(log_path, output_path)

                target_runinfo_path = f"{target_reports_dir_path}/runinfo.json"
                target_report_path = f"{target_reports_dir_path}/report.html"
                target_report_path_ipynb =  f"{target_reports_dir_path}/report.ipynb"
                target_business_report_path = f"{target_reports_dir_path}/business_report.html"
                target_metrics_path = f"{target_reports_dir_path}/metrics.json"

                ipynb_to_html(local_report_path, hmtl_local_report_path)
                ipynb_to_html(local_business_report_path, hmtl_local_business_report_path)

                fs.makedirs(target_reports_dir_path)
                fs.put(local_runinfo_path,target_runinfo_path)
                fs.put(hmtl_local_report_path,target_report_path)
                fs.put(hmtl_local_business_report_path,target_business_report_path)
                fs.put(local_report_path,target_report_path_ipynb)
                fs.put(local_metrics_path,target_metrics_path)
                fs.touch(f"{target_reports_dir_path}/_SUCCESS")

                os.remove(hmtl_local_report_path)
                os.remove(hmtl_local_business_report_path)
            else:
                
                local_report_path_py = f"tmp/{run_id}_{nb_name}.py"
                
                with open(run_info_file_name) as json_file:

                    outputs_urls = json.load(json_file)["outputs"]
                    reports_urls = [x for x in outputs_urls if re.search(f"(.+?).reports.{nb_name}.py", x)]
                    if len(reports_urls) != 1:
                        raise Exception(f"Sinara step outputs must contain exactly one output url for reports like '*.reports.{nb_name}.py' ")
                    target_reports_dir_path = outputs_urls[reports_urls[0]]

                    base_output_url = Path(target_reports_dir_path).parent
                    tensorboard_output_dir = base_output_url / "tensorboard"
 
                    tmp_tensorboard_log_dir = f"tmp/tensorboard"
                    log_events = fs.glob(f"{tmp_tensorboard_log_dir}/**/{run_id}/events.out*")
                    for log_path in log_events:
                        p = Path(log_path)
                        events_file = p.name
                        log_name = p.parts[-3]
                        output_dir = tensorboard_output_dir / log_name
                        output_path = output_dir / events_file
                        fs.makedirs(output_dir)
                        print(f"Saving tensorboard logs {log_path} to {output_path}")
                        fs.put(log_path, output_path)

                # Set SUCCESS if there had not been no exceptions before
                
                with open(local_runinfo_path, 'r+') as f:
                    local_runinfo_json = json.load(f)
                    local_runinfo_json["result"] = 'SUCCESS'
                    f.seek(0)        # <--- should reset file position to the beginning.
                    json.dump(local_runinfo_json, f, indent=4)
                    f.truncate()     # remove remaining part

                target_runinfo_path = f"{target_reports_dir_path}/runinfo.json"
                target_report_path = f"{target_reports_dir_path}/report.txt"
                target_report_path_py =  f"{target_reports_dir_path}/report.py"
                target_metrics_path = f"{target_reports_dir_path}/metrics.json"

                fs.makedirs(target_reports_dir_path)
                fs.put(local_runinfo_path,target_runinfo_path)
                fs.put(local_report_path,target_report_path)
                fs.put(local_report_path_py,target_report_path_py)
                fs.put(local_metrics_path,target_metrics_path)
                fs.touch(f"{target_reports_dir_path}/_SUCCESS")

class SinaraDiffReport:
    
    _curr_branch = None
    _target_branch = None
    
    _curr_run = None 
    _target_run = None
    
    _curr_run_path = None 
    _target_run_path = None
    
    _curr_commit = None 
    _target_commit = None
    
    _dsml_module_names_added = None
    _dsml_module_names_removed = None
    _dsml_module_names_changed = None
    
    _hdfs_module_added_paths = None
    _hdfs_module_target_paths = None
    _hdfs_module_curr_paths = None
    _hdfs_module_removed_paths = None
    
    _hdfs_module_added_paths_html = None
    _hdfs_module_target_paths_html = None
    _hdfs_module_curr_paths_html = None
    _hdfs_module_removed_paths_html = None
    
    _local_module_target_paths = None
    _local_module_curr_paths = None
    _local_module_removed_paths = None
    _local_module_added_paths = None
    _local_module_diff_paths = None
    _local_diff_info_path = None
    _local_success_file_path = None
    _local_diff_report_dir = None
    
    _local_module_target_paths_html = None
    _local_module_curr_paths_html = None
    _local_module_removed_paths_html = None
    _local_module_added_paths_html = None
    
    _hdfs_diff_report_dir = None
    
    _diff_module_target_paths = None
    _diff_module_curr_paths = None
    _diff_module_diff_paths = None
    _diff_module_removed_paths = None
    _diff_module_added_paths = None

    _diff_diff_info_path = None
    _diff_success_file_path = None
    _diff_diff_report_dir = None
    
    
    @staticmethod
    def save(curr_branch, target_branch):
        me = SinaraDiffReport
        me._fetch_all_tags()
        
        print("Diff report creating:\n")
        
        me._curr_branch = curr_branch
        curr_top_tag = me._get_top_tag(curr_branch); print(f"\tCURRENT BRANCH: {me._curr_branch}")
        me._curr_commit = curr_top_tag[2] if curr_top_tag else None; print(f"\tCURRENT COMMIT: {me._curr_commit}")
        me._curr_run = curr_top_tag[0] if curr_top_tag else None; print(f"\tCURRENT RUN: {me._curr_run}")
        me._curr_run_path = curr_top_tag[1] if curr_top_tag else None; print(f"\tCURRENT RUN PATH: {me._curr_run_path}\n")
        
        me._target_branch = target_branch
        target_top_tag = me._get_top_tag(target_branch); print(f"\tTARGET BRANCH: {me._target_branch}")
        me._target_commit = target_top_tag[2] if target_top_tag else None; print(f"\tTARGET COMMIT: {me._target_commit}")
        me._target_run = target_top_tag[0] if target_top_tag else None; print(f"\tTARGET RUN: {me._target_run}")
        me._target_run_path = target_top_tag[1] if target_top_tag else None; print(f"\tTARGET RUN PATH: {me._target_run_path}\n")
        
        if not me._curr_run or not me._target_run:
            print(f"WARNING: diff report wasn't created due to the absence of commits tagged by runs either on current branch or on target branch")
            print(f"WARNING: current branch: '{curr_branch}' ")
            print(f"WARNING: target branch: '{target_branch}' ")
            return
        
        SinaraDiffReport._get_module_names()
        SinaraDiffReport._get_fs_paths()
        SinaraDiffReport._make_local_paths()
        SinaraDiffReport._make_diff_paths()
        
        print(f"\tPreparing tmp diff report inside of '{me._local_diff_report_dir}':")
        
        if not SinaraDiffReport._copy_modules_to_local():
            return
        SinaraDiffReport._make_module_diffs()
        SinaraDiffReport._make_diff_info()
        SinaraDiffReport._make_success_file()
        
        print(f"\tCopying tmp diff report to 'hdfs://{me._diff_diff_report_dir}'")
        SinaraDiffReport._copy_diff_report_to_fs()
        print(f"\tTagging current commit by DIFF TAG...")
        diff_id = SinaraDiffReport._tag_commit_by_diff()
        print(f"\tDIFF TAG: {diff_id}")
        print("\tDiff report created")
    
    @staticmethod
    def _get_module_names():
        me = SinaraDiffReport
        fs = SinaraFileSystem.FileSystem()
        
        curr_report_paths = fs.glob(f"{me._curr_run_path}/reports.*.ipynb") if me._curr_run_path else []
        curr_report_names = {PurePosixPath(report_path).name for report_path in curr_report_paths }
        
        target_report_paths = fs.glob(f"{me._target_run_path}/reports.*.ipynb") if me._target_run_path else []
        target_report_names = {PurePosixPath(report_path).name for report_path in target_report_paths }

        me._dsml_module_names_added = curr_report_names - target_report_names
        me._dsml_module_names_removed = target_report_names - curr_report_names
        me._dsml_module_names_changed = target_report_names & curr_report_names
    
    @staticmethod
    def _get_fs_paths():
        me = SinaraDiffReport
        
        me._hdfs_module_added_paths = [ f"{me._curr_run_path}/{report_added}/report.ipynb" for report_added in me._dsml_module_names_added ]
        me._hdfs_module_target_paths = [ f"{me._target_run_path}/{report_changed}/report.ipynb" for report_changed in me._dsml_module_names_changed ]
        me._hdfs_module_curr_paths = [ f"{me._curr_run_path}/{report_changed}/report.ipynb" for report_changed in me._dsml_module_names_changed ]
        me._hdfs_module_removed_paths = [ f"{me._target_run_path}/{report_changed}/report.ipynb" for report_changed in me._dsml_module_names_removed ]
        
        me._hdfs_module_added_paths_html = [ f"{me._curr_run_path}/{report_added}/report.html" for report_added in me._dsml_module_names_added ]
        me._hdfs_module_target_paths_html = [ f"{me._target_run_path}/{report_changed}/report.html" for report_changed in me._dsml_module_names_changed ]
        me._hdfs_module_curr_paths_html = [ f"{me._curr_run_path}/{report_changed}/report.html" for report_changed in me._dsml_module_names_changed ]
        me._hdfs_module_removed_paths_html = [ f"{me._target_run_path}/{report_changed}/report.html" for report_changed in me._dsml_module_names_removed ]
        
    @staticmethod
    def _make_diff_paths():
        me = SinaraDiffReport
        diff_dir = f"{me._curr_run_path}/diff.{me._curr_run}.{me._target_run}"
        
        me._diff_module_target_paths = [f"{diff_dir}/{module_name.split('.')[1]}.target.html" for module_name in me._dsml_module_names_changed]
        me._diff_module_curr_paths = [f"{diff_dir}/{module_name.split('.')[1]}.curr.html" for module_name in me._dsml_module_names_changed]
        me._diff_module_diff_paths = [f"{diff_dir}/{module_name.split('.')[1]}.diff.txt" for module_name in me._dsml_module_names_changed]
        me._diff_module_removed_paths = [f"{diff_dir}/{module_name.split('.')[1]}.removed.html" for module_name in me._dsml_module_names_removed]
        me._diff_module_added_paths = [f"{diff_dir}/{module_name.split('.')[1]}.added.html" for module_name in me._dsml_module_names_added]
        
        me._diff_diff_info_path = f"{diff_dir}/diff_info.json"
        me._diff_success_file_path = f"{diff_dir}/_SUCCESS"
        me._diff_diff_report_dir = diff_dir
        
    @staticmethod
    def _make_local_paths():
        me = SinaraDiffReport
        diff_dir = f"{get_sinara_step_tmp_path()}/diff.{me._curr_run}.{me._target_run}"
        
        me._local_module_target_paths = [f"{diff_dir}/{module_name.split('.')[1]}.target.ipynb" for module_name in me._dsml_module_names_changed]
        me._local_module_curr_paths = [f"{diff_dir}/{module_name.split('.')[1]}.curr.ipynb" for module_name in me._dsml_module_names_changed]
        me._local_module_diff_paths = [f"{diff_dir}/{module_name.split('.')[1]}.diff.txt" for module_name in me._dsml_module_names_changed]
        me._local_module_removed_paths = [f"{diff_dir}/{module_name.split('.')[1]}.removed.ipynb" for module_name in me._dsml_module_names_removed]
        me._local_module_added_paths = [f"{diff_dir}/{module_name.split('.')[1]}.added.ipynb" for module_name in me._dsml_module_names_added]
        
        me._local_module_target_paths_html = [f"{diff_dir}/{module_name.split('.')[1]}.target.html" for module_name in me._dsml_module_names_changed]
        me._local_module_curr_paths_html = [f"{diff_dir}/{module_name.split('.')[1]}.curr.html" for module_name in me._dsml_module_names_changed]
        me._local_module_removed_paths_html = [f"{diff_dir}/{module_name.split('.')[1]}.removed.html" for module_name in me._dsml_module_names_removed]
        me._local_module_added_paths_html = [f"{diff_dir}/{module_name.split('.')[1]}.added.html" for module_name in me._dsml_module_names_added]
        
        me._local_diff_info_path = f"{diff_dir}/diff_info.json"
        me._local_success_file_path = f"{diff_dir}/_SUCCESS"
        me._local_diff_report_dir = diff_dir
        
    @staticmethod
    def _copy_modules_to_local():
        
        me = SinaraDiffReport
        if os.path.exists(me._local_success_file_path):
            print("local diff report wasn't created due to it already exists")
            return False
        
        shutil.rmtree(me._local_diff_report_dir, ignore_errors=True)
        os.makedirs(me._local_diff_report_dir, exist_ok=False)
        
        fs = SinaraFileSystem.FileSystem()
        list(map(fs.get, me._hdfs_module_added_paths, me._local_module_added_paths))
        list(map(fs.get, me._hdfs_module_removed_paths, me._local_module_removed_paths))
        list(map(fs.get, me._hdfs_module_curr_paths, me._local_module_curr_paths))
        list(map(fs.get, me._hdfs_module_target_paths, me._local_module_target_paths))
        list(map(fs.get, me._hdfs_module_added_paths_html, me._local_module_added_paths_html))
        list(map(fs.get, me._hdfs_module_removed_paths_html, me._local_module_removed_paths_html))
        list(map(fs.get, me._hdfs_module_curr_paths_html, me._local_module_curr_paths_html))
        list(map(fs.get, me._hdfs_module_target_paths_html, me._local_module_target_paths_html))
        return True
    
    @staticmethod 
    def _make_module_diffs():
        me = SinaraDiffReport
        list(map(me._make_ipynb_diff_file, me._local_module_target_paths, me._local_module_curr_paths, me._local_module_diff_paths))
          
    @staticmethod 
    def _make_ipynb_diff_file( base, remote, output ):
        from nbdime.utils import read_notebook
        from nbdime.diffing.notebooks import diff_notebooks
        from nbdime.args import prettyprint_config_from_args, process_diff_flags
        from nbdime.prettyprint import pretty_print_notebook_diff
        from nbdime import nbdiffapp

        #https://nbdime.readthedocs.io/en/latest/cli.html?highlight=ignore-metadata#common-diff-options
        args= [base, remote, "--ignore-metadata", "--ignore-id", "--ignore-attachments", "--ignore-outputs", "--ignore-details"]

        arguments = nbdiffapp._build_arg_parser().parse_args(args)
        arguments.use_color = False
        process_diff_flags(arguments)

        a = read_notebook(base, on_null='empty')
        a.cells.pop(-1)
        b = read_notebook(remote, on_null='empty')
        b.cells.pop(-1)
        d = diff_notebooks(a, b)

        class Printer:
            def __init__( self, file):
                self._output_file = file
            def write(self, text):
                self._output_file.write(text)
                
        if os.path.exists(output):
            os.remove(output)
        with open(output, 'a') as f:
            config = prettyprint_config_from_args(arguments, out=Printer(f))
            pretty_print_notebook_diff(base, remote, a, d, config)
            f.flush()
    
    @staticmethod
    def _make_diff_info():
        me = SinaraDiffReport
        
        diff_info = {
            "curr_run_path": me._curr_run_path,
            "target_run_path": me._target_run_path
        }
        with open(me._local_diff_info_path, 'w') as outfile:
            json.dump(diff_info, outfile)
    
    @staticmethod
    def _make_success_file():
        me = SinaraDiffReport
        Path(me._local_success_file_path).touch()
   
    @staticmethod
    def _copy_diff_report_to_fs():
        me = SinaraDiffReport
        fs = SinaraFileSystem.FileSystem()
        
        fs.makedirs(me._diff_diff_report_dir)
        
        list(map(fs.put, me._local_module_added_paths_html, me._diff_module_added_paths))
        list(map(fs.put, me._local_module_removed_paths_html, me._diff_module_removed_paths))
        list(map(fs.put, me._local_module_curr_paths_html, me._diff_module_curr_paths))
        list(map(fs.put, me._local_module_target_paths_html, me._diff_module_target_paths))
        list(map(fs.put, me._local_module_diff_paths, me._diff_module_diff_paths))
        
        fs.put(me._local_diff_info_path,  me._diff_diff_info_path )
        fs.put(me._local_success_file_path, me._diff_success_file_path )
    
    @staticmethod
    def _tag_commit_by_diff():
        me = SinaraDiffReport
        diff_id = f"diff.{me._curr_run}.{me._target_run}"
        diff_fs_path = me._diff_diff_report_dir
        StepReport._fetch_all_tags()
        tag_ref = git.Repo().create_tag(diff_id, message=diff_fs_path)
        git.Repo().remote().push(tag_ref)
        return diff_id
    
    @staticmethod 
    def _get_top_tag(branch_name, traverse_last_commits_count=100):
        #it is straighforward initial method implementation. 
        #Performance can degrade with an increase in the number of tags in the repository
        #Algorithm complexity is O(n) where n is number of tags in the git repo
        all_tags = git.Repo().tags
        ordered_branch_commits = list(git.Repo().iter_commits(branch_name, max_count=traverse_last_commits_count))
        tagged_commits = { tag.commit for tag in all_tags if tag.name.startswith("run-")}
        ordered_tagged_branch_commits = [ commit for commit in ordered_branch_commits if commit in tagged_commits ]
        if len(ordered_tagged_branch_commits) == 0:
            return []
        
        last_tagged_branch_commit = ordered_tagged_branch_commits[0]

        tags_on_last_commit = [(tag.name,tag.tag.message,tag.commit) for tag in all_tags if tag.commit == last_tagged_branch_commit ]
        return sorted(tags_on_last_commit, key=lambda x: x[0])[-1]
    
    @staticmethod
    def _fetch_all_tags():
        #TODO: git fetch --all --tags
        #https://linuxtut.com/en/76a3fb171e9143ff695e/
        me = SinaraDiffReport

        repo = git.Repo()
        original_branch = repo.active_branch
        
        origin = repo.remote()
        tags = origin.fetch(**{"tags":True})
        git.Repo().git.clear_cache()
        try:
            for branch in repo.branches:
                if branch == repo.active_branch:
                    continue
                branch.checkout()
                git.Repo().git.clear_cache()
                repo.git.pull()
                git.Repo().git.clear_cache()
                origin = repo.remote()
                tags = origin.fetch(**{"tags":True})
        finally:
            git.Repo().git.clear_cache()
            original_branch.checkout()
