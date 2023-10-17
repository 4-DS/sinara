from .fs import SinaraFileSystem

from os import path, makedirs
from pathlib import Path

import glob


class SinaraStore:
    """
    SinaraStore is to work with data storage. The current Sinara store depends on a context.
    When you run components within cloud, store is S3.
    When you run components locally, Sinara store is local disk or sometimes S3, depending on the options.
    
    SinaraStore provides:
     - functions to download a file from store to temporary data
     - functions to upload a file from temporary data to store
    """

    @staticmethod
    def write_entity(file_path=str, store_file_path=str):
        """
            upload an entity to store
        """
        
        fs = SinaraFileSystem.FileSystem()
        fs_folder_path = path.dirname(store_file_path)
        fs.makedirs(fs_folder_path)
        
        fs.put(file_path, store_file_path)
        
        # create SUCCESS file        
        fs.touch(fs_folder_path + '/' + '_SUCCESS')
    
    @staticmethod
    def copy_tmp_files_to_store(tmp_dir=str, store_dir=str, file_globs=[]):
        """
            upload list of files from data temporary directory to store
            subfolders are not supported
        """        
        if isinstance(file_globs, str):
            file_globs = [file_globs]
            
        filenames = []
        for gl in file_globs:
            filenames += glob.glob(str(Path(cache_dir, gl)))
        if len(filenames) == 0:
            raise Exception("file_globs doesn't match any file")
        
        fs = SinaraFileSystem.FileSystem()
        fs.makedirs(store_dir)
        for cache_file_path in filenames:
            store_file_path = str(Path(store_dir, Path(cache_file_path).name))
            fs.put(cache_file_path, store_file_path)
        
        fs.touch(store_dir + '/' + '_SUCCESS')
        
    @staticmethod
    def copy_store_dir_to_tmp(store_dir=str, tmp_dir=str):
        """ 
            download file from directory on store to temporary local dir
        """
        fs = SinaraFileSystem.FileSystem()
        
        store_globs = fs.glob(store_dir)
        makedirs(tmp_dir, exist_ok=True)
        for store_file_path in store_globs:
            tmp_file_path = str(Path(tmp_dir, Path(store_file_path).name))
            fs.get(store_file_path, tmp_file_path)
