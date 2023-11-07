import atexit

from abc import (
  ABC,
  abstractmethod,
)
class _SinaraSpark(ABC):    
    @staticmethod
    @abstractmethod
    def session_is_stopped():
        pass

    
    @staticmethod
    @abstractmethod
    def run_session(clusterSize=0 , app="my app", conf=None, reuse_session=True):
        pass

    
    @staticmethod
    @abstractmethod
    def ui_url():
        pass
    
    
    @staticmethod
    @abstractmethod
    def stop_session():
        pass
    
    
    @staticmethod
    @abstractmethod
    def _conf(conf = None, driver_mem = "4g", driver_max_result = "4g" ):
        pass

_=atexit.register(_SinaraSpark.stop_session)