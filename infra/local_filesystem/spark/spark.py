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

        if debug:
            SinaraSpark._spark._sc.setLogLevel("DEBUG")
            
        return SinaraSpark._spark


    @staticmethod
    def stop_session():
        try:
            SinaraSpark._spark.stop()
        except:
            pass

    
    @staticmethod
    def _conf(conf = None, debug=False):  
        
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

        if debug:
            conf.set("spark.log.level", "DEBUG")

        conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')

        SinaraSpark._config = conf
        return conf
