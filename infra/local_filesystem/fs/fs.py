import os
import shutil
from pathlib import Path
import sys

class SinaraLocalFileSystem(object):

    @staticmethod
    def FileSystem():
        return _SinaraLocalFileSystem

# setting Sinara abstract class
sys.path.append('../../sinara')

# importing
from sinara.fs.fs import _SinaraFileSystem

class _SinaraLocalFileSystem(_SinaraFileSystem):
    
    @staticmethod
    def glob(path):
        import glob
        return glob.glob(path)
    
    @staticmethod
    def exists(path):
        return os.path.exists(path)

    @staticmethod
    def get(srcpath, dstpath):
        shutil.copyfile(srcpath, dstpath)
    
    @staticmethod
    def makedirs(path):
        if not os.path.isdir(path):
            os.makedirs(path)
    
    @staticmethod
    def put(srcpath, dstpath):
        shutil.copyfile(srcpath, dstpath)
        
    @staticmethod
    def get(srcpath, dstpath):
        shutil.copyfile(srcpath, dstpath)
    
    @staticmethod
    def touch(path):
        Path(path).touch()


