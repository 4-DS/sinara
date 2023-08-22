from .fs import SinaraFileSystem
from .env import get_cache_paths, get_cache_path

from os import path, makedirs
from pathlib import Path

import glob


class SinaraStore:
    """
    SinaraStore is to work with data storage. The current Sinara store depends on a context.
    When you run components within cloud, store is S3.
    When you run components locally, Sinara store is local disk or sometimes S3, depending on the options.
    
    SinaraStore provides:
     - functions to download a file from store to data cache
     - functions to upload a file from data cache to store
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
