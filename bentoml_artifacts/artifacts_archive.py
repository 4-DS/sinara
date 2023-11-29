from bentoml import env, artifacts, api, BentoService
from bentoml.adapters import DataframeInput, JsonInput
from bentoml.service.artifacts.common import TextFileArtifact, JSONArtifact
from bentoml.utils.hybridmethod import hybridmethod
from bentoml.exceptions import BentoMLException
from .binary_artifact import BinaryFileArtifact
    
@env(infer_pip_packages=True)
@artifacts([
###artifacts_placeholder###
])
class ArtifactsArchive(BentoService):
    """
    ArchiveBentoService used to create archive file with artifacts
    """
    
    @hybridmethod
    def pack(self, name, *args, **kwargs):
        """
        BentoService#pack method is used for packing trained model instances with a
        BentoService instance and make it ready for BentoService#save.

        pack(name, *args, **kwargs):

        :param name: name of the declared model artifact
        :param args: args passing to the target model artifact to be packed
        :param kwargs: kwargs passing to the target model artifact to be packed
        :return: this BentoService instance
        """

        
        if 'artifact_type' in kwargs:
            self.artifacts[name] = kwargs['artifact_type']
            del kwargs['artifact_type']
        elif len(args) > 0 and isinstance(args[0], str):
            self.artifacts[name] = TextFileArtifact(name)
        elif len(args) > 0 and isinstance(args[0], dict):
            self.artifacts[name] = JSONArtifact(name)
        else:
            file_extension = ''
            if 'file_extension' in kwargs:
                file_extension = kwargs['file_extension']
                del kwargs['file_extension']
            self.artifacts[name] = BinaryFileArtifact(name, file_extension = file_extension)
        
        self.artifacts.get(name).pack(*args, **kwargs)
        return self

    @pack.classmethod
    def pack(cls, *args, **kwargs):  # pylint: disable=no-self-argument
        """
        **Deprecated**: Legacy `BentoService#pack` class method, no longer supported
        """
        raise BentoMLException(
            "BentoService#pack class method is deprecated, use instance method `pack` "
            "instead. e.g.: svc = MyBentoService(); svc.pack('model', model_object)"
        )
        
    def postprocess(self, bentoservice_dir):
        
        from pathlib import Path, PurePath
        
        import os, shutil
        def copytree(src, dst, symlinks=False, ignore=None):
            for item in os.listdir(src):
                s = src / item
                d = dst / item
                if os.path.isdir(str(s)):
                    shutil.copytree(s, d, symlinks, ignore)
                else:
                    shutil.copy2(s, d)
        
        import sys, os, shutil
        import yaml
        
        bentoml_yaml = Path(bentoservice_dir) / "bentoml.yml"
        
        with open(bentoml_yaml, 'r') as f:
            bentoml_info = yaml.safe_load(f)
        
        service_name = bentoml_info['metadata']['service_name']
        py_path = Path(bentoservice_dir) / service_name / bentoml_info['metadata']['module_file']        
        
        source_path = Path(sys.modules[BinaryFileArtifact.__module__].__file__).resolve()
        copytree(source_path.parent.absolute(), py_path.parent.absolute())
        
        artifact_list = []
        for artifact_name in self.artifacts:
            artifact = self.artifacts.get(artifact_name)
            args = ','.join([ f'"{x}"' for x in artifact._BentoServiceArtifact__args])
            kwargs = ','.join([ f'{k}="{v}"' for k, v in artifact._BentoServiceArtifact__kwargs.items()])
            delim = ',' if len(kwargs) > 0 else ''
            artifact_ctor = f'    {artifact.__class__.__name__}({args}{delim}{kwargs})'
            artifact_list.append(artifact_ctor)
        artifacts_decorator = ',\n'.join(artifact_list)             
        
        with open(py_path, 'r') as file:
            py_text = file.read()
            py_text = py_text.replace("###artifacts_placeholder###", f'{artifacts_decorator}', 1)
                
        if py_text:
            with open(py_path, 'w') as file:
                file.write(py_text)
