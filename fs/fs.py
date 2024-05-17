from abc import (
  ABC,
  abstractmethod,
)

class _SinaraFileSystem(ABC):

    @staticmethod
    @abstractmethod
    def glob(path):
        pass
    
    @staticmethod
    @abstractmethod
    def exists(path):
        pass

    @staticmethod
    @abstractmethod
    def get(srcpath, dstpath):
        pass
    
    @staticmethod
    @abstractmethod
    def makedirs(path):
        pass
    
    @staticmethod
    @abstractmethod
    def put(srcpath, dstpath):
        pass
    
    @staticmethod
    @abstractmethod
    def touch(path):
        pass
