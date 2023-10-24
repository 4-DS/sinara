import importlib
import os

def importSinaraModuleClass(module_name, class_name):
    #module = importlib.import_module('sinara.infra.local_filesystem.spark')
    if "INFRA_NAME" in os.environ:
        module_package = f"sinara.infra.{os.environ['INFRA_NAME']}.{module_name}"
        #print (module_package)
        module = importlib.import_module(module_package)
        module_class = getattr(module, class_name)
        
    else:
        raise Exception("Please, set Sinara infra: os.environ[\"INFRA_NAME\"] ")
    return module_class
