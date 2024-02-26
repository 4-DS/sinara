from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
#from pyspark import SparkConf

import socket
import sys
 
# setting Sinara abstract class
sys.path.append('../../sinara')

# importing
from sinara.spark.spark import _SinaraSpark

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
    def run_session(clusterSize=0 , app="my app", conf=None, reuse_session=True, debug=True):
        SinaraSpark.stop_session()
        print("Session is run")
        
        if not SinaraSpark.session_is_stopped() and not reuse_session:
            raise Exception("Spark session is already running. You should stop it first before new Spark session can be run")
        
        master = "local[*]"
        config = SinaraSpark._conf(conf)
        SinaraSpark._spark = SparkSession \
            .builder \
            .appName(app) \
            .master(master) \
            .config(conf=config)\
            .getOrCreate()
            
        return SinaraSpark._spark

    
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

    
    @staticmethod
    def stop_session():
        try:
            SinaraSpark._spark.stop()
        except:
            pass

    
    @staticmethod
    def _conf(conf = None, driver_mem = "4g", driver_max_result = "4g" ):  
        
        if conf is None:
            conf = SparkConf(False)
     
        conf.set("spark.sql.parquet.enableVectorizedReader", "false" )
        #conf.set("spark.executor.memory",SPARK_EXECUTOR_MEMORY)
        #conf.set("spark.executor.cores",SPARK_EXECUTOR_CORES)
        conf.set("spark.driver.memory", driver_mem)
        conf.set("spark.driver.maxResultSize", driver_max_result)

        SinaraSpark._config = conf
        return conf
