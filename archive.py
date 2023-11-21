import os
from os import path
from functools import partial
from pyspark.sql.functions import regexp_replace

def SinaraArchive_save_file(file_col, tmp_dir):
    '''
    _save_file defined as function since runtime error when declared as method:
    It appears that you are attempting to reference SparkContext from a broadcast variable, action, or transformation.
    SparkContext can only be used on the driver, not in code that it run on workers. For more information, see SPARK-5063.
    '''
    file_name = path.join(tmp_dir, file_col.relPath.strip('/'))
    file_binary = file_col.content

    os.makedirs(path.dirname(file_name), exist_ok=True)
    with open(file_name, 'wb') as f_id:
        f_id.write(file_binary)
            
class SinaraArchive:

    BLOCK_SIZE = 60 * 1024 * 1024
    
    def __init__(self, spark):
        self._spark = spark;
        
    def pack_files_form_tmp_to_spark_df(self, tmp_dir):
        #.option("recursiveFileLookup", "true")
        df = self._spark.read.format("binaryFile").option("pathGlobFilter", "*.*").load(tmp_dir) \
                .withColumn("relPath", regexp_replace('path', 'file:' + tmp_dir, '')) \
                .drop("path")
        return df
    
    def pack_files_from_tmp_to_store(self, tmp_dir, store_path):
        df = self.pack_files_form_tmp_to_spark_df(tmp_dir)
        df.write.option("parquet.block.size", self.BLOCK_SIZE).mode("overwrite").parquet(store_path)
    
    def unpack_files_from_spark_df_to_tmp(self, df_archive, tmp_dir):
        df_archive.foreach(partial(SinaraArchive_save_file, tmp_dir=tmp_dir))
    
    def unpack_files_from_store_to_tmp(self, store_path, tmp_dir):
        df = self._spark.read.parquet(store_path)
        self.unpack_files_from_spark_df_to_tmp(df, tmp_dir)