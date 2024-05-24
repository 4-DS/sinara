import os

from abc import (
  ABC,
  abstractmethod,
)

#def read_cgroup_setting(cgroup_setting_file_path):
#    cgroup_setting_value = None
#    if os.path.isfile(cgroup_setting_file_path):
#        with open(cgroup_setting_file_path) as cgroup_setting:
#            cgroup_setting_value = cgroup_setting.read().strip()
            
#    if cgroup_setting_value is None or not cgroup_setting_value.isdigit():
#        raise Exception("Can't detect values from cgroups")
#        
#    return int(cgroup_setting_value)

#MEM_LIMIT_FPATH = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
#CPU_SHARES_FPATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
#CPU_PERIOD_FPATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"

def get_spark_driver_memory(snr_server_memory):
    """
    Get memory limit for Spark driver
    """
    #recommendations: https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    # spark.driver.memory (in Gb) DRIVER_MEM_LIMIT * 0.9 ? 0.75(spark.driver.memoryOverhead is typically 0.1)
    return int(snr_server_memory * 0.75)

def get_spark_driver_cores(snr_server_cpu):
    """
    Get number of cores for Spark driver
    """
    #considered having correct values on Sinara Server start
    return int(snr_server_cpu)

def get_snv_server_mem_limit():
    """
    Get memory limit for Sinara Server
    """
    if "SINARA_SERVER_MEMORY_LIMIT" in os.environ:
        return int(int(os.environ["SINARA_SERVER_MEMORY_LIMIT"])/1024/1024/1024)
    else:
        # For back compatibility
        # TODO: check if this is correct
        # print -> warnings.warn("....", ....)
        print("WARNING: Using default 4 Gb memory for Spark. Please, follow https://github.com/4-DS/sinara-tutorials/wiki/SinaraML-How-To") 
        return 8
        
def get_snv_server_cores():
    """
    Get number of cores for Sinara Server
    """
    if "SINARA_SERVER_CORES" in os.environ:
        return int(os.environ["SINARA_SERVER_CORES"])
    else:
        # For back compatibility
        print("WARNING: Using default 4 cores for Spark. Please, follow https://github.com/4-DS/sinara-tutorials/wiki/SinaraML-How-To") 
        return 4

class __SinaraSettings(ABC):
    """
    Sinara settings
    """
    SNR_SERVER_MEMORY_LIMIT = get_snv_server_mem_limit()
    SNR_SERVER_CORES = get_snv_server_cores()

    SNR_SPARK_DRIVER_MEM_LIMIT = get_spark_driver_memory(SNR_SERVER_MEMORY_LIMIT)
    SNR_SPARK_DRIVER_CORES = get_spark_driver_cores(SNR_SERVER_CORES)
    
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