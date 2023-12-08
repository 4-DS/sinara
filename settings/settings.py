from abc import (
  ABC,
  abstractmethod,
)
class __SinaraSettings(ABC):
    
    @staticmethod
    @abstractmethod
    def get_tmp_paths():
        pass
    
    @staticmethod
    @abstractmethod
    def get_tmp_path(env_name):
        pass

    @staticmethod
    @abstractmethod
    def get_user():
        pass
    
    @staticmethod
    @abstractmethod
    def get_data_paths():
        pass
    
    @staticmethod
    @abstractmethod
    def get_env_path(env_name):
        pass