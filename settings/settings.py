import os

from abc import (
  ABC,
  abstractmethod,
)

def read_cgroup_setting(cgroup_setting_file_path):
    cgroup_setting_value = None
    if os.path.isfile(cgroup_setting_file_path):
        with open(cgroup_setting_file_path) as cgroup_setting:
            cgroup_setting_value = cgroup_setting.read().strip()
            if cgroup_setting_value is None or not cgroup_setting_value.isdigit():
                raise Exception("Can't detect values from cgroups")
    return int(cgroup_setting_value)

MEM_LIMIT_FPATH = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
CPU_SHARES_FPATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
CPU_PERIOD_FPATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"

def get_spark_driver_memory(snr_server_memory):
    #recommendations: https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    # spark.driver.memory (in Gb) DRIVER_MEM_LIMIT * 0.9 ? 0.75(spark.driver.memoryOverhead is typically 0.1)
    return int(snr_server_memory * 0.75)

def get_spark_driver_cores(snr_server_cpu):
    #considered having correct values on Sinara Server start
    return int(snr_server_cpu)

class __SinaraSettings(ABC):

    SNR_SERVER_MEMORY_LIMIT = int(read_cgroup_setting(MEM_LIMIT_FPATH)/1024/1024/1024)
    SNR_SERVER_CORES = int(read_cgroup_setting(CPU_SHARES_FPATH)/read_cgroup_setting(CPU_PERIOD_FPATH))

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