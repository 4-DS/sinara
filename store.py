from .fs import SinaraFileSystem
from os import path, makedirs
from pathlib import Path
import tarfile

import glob


class SinaraStore:
    """
    SinaraStore is to work with data storage. The current Sinara store depends on a context.
    When you run components within cloud, store is S3.
    When you run components locally, Sinara store is local disk or sometimes S3, depending on the options.
    
    SinaraStore provides:
     - functions to download a file from store to temporary data dir
     - functions to upload a file from temporary data dir to store
    """
        
    @staticmethod
    def copy_tmp_files_to_store(tmp_dir=str, store_dir=str, file_globs=["*"]):
        SinaraStore.archive_tmp_files_to_store(tmp_dir, store_dir, file_globs)

    @staticmethod
    def copy_store_files_to_tmp(store_dir=str, tmp_dir=str, file_globs=["*"]):
        SinaraStore.dearchive_store_files_to_tmp(store_dir, tmp_dir, file_globs)
        
    @staticmethod
    def archive_tmp_files_to_store(tmp_dir=str, store_path=str, file_globs=["*"]):
        """
            upload list of files from data temporary directory to store
            subfolders are not supported
        """        
        if isinstance(file_globs, str):
            file_globs = [file_globs]
            
        filenames = []
        for gl in file_globs:
            filenames += glob.glob(str(Path(tmp_dir, gl)))
        if len(filenames) == 0:
            raise Exception("file_globs doesn't match any file")
        
        fs = SinaraFileSystem.FileSystem()
        fs.makedirs(store_path)
        # for tmp_file_path in filenames:
        #     store_file_path = str(Path(store_path, Path(tmp_file_path).name))
        #     fs.put(tmp_file_path, store_file_path)
        tar_file_path = f'{tmp_dir}/files.tar'
        with tarfile.open(tar_file_path, 'w') as tar:
            for tmp_file_path in filenames:
                tar.add(tmp_file_path, arcname=tmp_file_path.replace(tmp_dir, ''))

        store_file_path = str(Path(store_path, Path(tar_file_path).name))
        fs.put(tar_file_path, store_file_path)
        fs.touch(Path(store_path, '_SUCCESS'))
        Path(tar_file_path).unlink()
        
        
    @staticmethod
    def dearchive_store_files_to_tmp(store_path=str, tmp_dir=str, file_globs=["*"]):
        """
            download list of files from data temporary directory to store
            subfolders are not supported
        """ 
        if isinstance(file_globs, str):
            file_globs = [file_globs]
        
        fs = SinaraFileSystem.FileSystem()
        
        filenames = []
        for gl in file_globs:
            filenames += fs.glob(str(Path(store_path, gl)))
        if len(filenames) == 0:
            raise Exception("file_globs doesn't match any file")
        
        # store_globs = fs.glob(store_path)
        # makedirs(tmp_dir, exist_ok=True)
        fs.makedirs(tmp_dir)
        # for store_file_path in filenames:
        #     tmp_file_path = str(Path(tmp_dir, Path(store_file_path).name))
        #     fs.get(store_file_path, tmp_file_path)
        store_file_path = str(Path(store_path, 'files.tar'))
        tar_file_path = str(Path(tmp_dir, Path(store_file_path).name))
        fs.get(store_file_path, tar_file_path)
        with tarfile.open(tar_file_path) as tar:
            tar.extractall(tmp_dir)
        Path(tar_file_path).unlink()
