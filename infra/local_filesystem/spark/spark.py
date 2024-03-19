from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

import socket
import sys
import math
import os

# setting Sinara abstract class
sys.path.append('../../sinara')

# importing
from sinara.spark.spark import _SinaraSpark
from sinara.settings import _SinaraSettings

def get_spark_driver_cores():
    return _SinaraSettings.SNR_SPARK_DRIVER_CORES

def get_spark_driver_memory():
    return _SinaraSettings.SNR_SPARK_DRIVER_MEM_LIMIT

class SinaraSpark(_SinaraSpark):
    
    _clustersize = 0
    _config = None
    _spark = None

    @staticmethod
    def session_is_stopped():
        if SinaraSpark._spark is None:
            return True
        if SinaraSpark._spark._sc._jsc is None:
            return True
        return SinaraSpark._spark._sc._jsc.sc().isStopped()

    @staticmethod
    def _master(clusterSize=0, debug=False):
        SinaraSpark._clustersize = clusterSize
        if SinaraSpark._clustersize >= 1:
            raise Exception("Spark cluster size >=1 is not supported") 
        elif SinaraSpark._clustersize == 0:
            vcores = str(get_spark_driver_cores())
            return  "local["+vcores+"]"
        else:
            vcores = str(max(1, int(get_spark_driver_cores() * SinaraSpark._clustersize )))
            return  "local["+vcores+"]"
    
    @staticmethod
    def run_session(clusterSize=0 , app="SinaraML Spark App", conf=None, reuse_session=True, debug=False):
        SinaraSpark.stop_session()
        print("Session is run")
        
        if not SinaraSpark.session_is_stopped() and not reuse_session:
            raise Exception("Spark session is already running. You should stop it first before new Spark session can be run")
           
        master = SinaraSpark._master(clusterSize, debug)
        config = SinaraSpark._conf(conf)
        SinaraSpark._spark = SparkSession \
            .builder \
            .appName(app) \
            .master(master) \
            .config(conf=config)\
            .getOrCreate()
            
        return SinaraSpark._spark

    @staticmethod
    def is_job_run():
        if "SNR_IS_JOB_RUN" in os.environ and os.environ["SNR_IS_JOB_RUN"] == 'True':
            return True   # Jupyter notebook or qtconsole
        else:
            return False  # Other type (?)
    
    @staticmethod
    def ui_url():
        port = 4040
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1',port))
            if result == 0:
                port += 1
                sock.close()
            else:
                port -= 1
                sock.close()
                break

        from IPython.core.display import display, HTML
        display(HTML(f"<a href='/proxy/{port}/jobs/' target='blank'>Open Spark UI</a>"))
        if SinaraSpark.is_job_run():
            try:
                import subprocess
                from jupyter_server import serverapp
                server_info = next(serverapp.list_running_servers())
                url = f"http://{server_info['hostname']}:{server_info['port']}/proxy/{port}/jobs/"
                formatted_url = f'print("Spark UI: \\033]8;;{url}\\033\\\\{url}\\033]8;;\\033\\\\")'
                subprocess.call(['python', '-c', formatted_url])
            except:
                pass
            
    
    @staticmethod
    def stop_session():
        try:
            SinaraSpark._spark.stop()
        except:
            pass

    
    @staticmethod
    def _conf(conf = None):  
        
        if conf is None:
            conf = SparkConf(False)

        spark_driver_memory = get_spark_driver_memory()

        if SinaraSpark._clustersize >= 1:
            raise Exception("Spark cluster size >=1 is not supported") 
        elif SinaraSpark._clustersize == 0:       
            conf.set("spark.driver.memory", str(spark_driver_memory) + "g")
            conf.set("spark.driver.maxResultSize", str(math.ceil(spark_driver_memory/3)) + "g")
        else:
            #TODO Check if 32 GB works
            part_of_spark_driver_memory = max( 1, int (spark_driver_memory * SinaraSpark._clustersize))
            conf.set("spark.driver.memory", str(part_of_spark_driver_memory) + "g")
            conf.set("spark.driver.maxResultSize", str(math.ceil(part_of_spark_driver_memory/3)) + "g")

        SinaraSpark._config = conf
        return conf
