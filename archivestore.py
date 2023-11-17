# arhivestore(spark).pack_files_from_tmp_to_store
# archivestore(spark).unpack_files_from_store_to_tmp
# archivestore(spark).unpack_files_from_dataframe_to_tmp_files(df_archive)
# df_archive = archivestore(spark).pack_files_form_tmp_to_dataframe

# spark._jsc.hadoopConfiguration().set("dfs.block.size", "62914560")

# blockSize= 1024*1024*60
# spark._jsc.hadoopConfiguration().setInt("dfs.blocksize", blockSize)
# spark._jsc.hadoopConfiguration().setInt("dfs.block.size", blockSize)
# spark._jsc.hadoopConfiguration().setInt("parquet.block.size",blockSize) 

# df.write.option("parquet.block.size", 60 * 1024 * 1024).mode("overwrite").parquet(tmp_dir + "/output")
# #df.write.parquet(custom_inputs.tmp_dir + "/output")


import os
from os import path
from functools import partial
from pyspark.sql.functions import regexp_replace

def SinaraArchiveStore_save_file(file_col, tmp_dir):
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
            
class SinaraArchiveStore:

    BLOCK_SIZE = 60 * 1024 * 1024
    
    def __init__(self, spark):
        self._spark = spark;
        
    def pack_files_form_tmp_to_dataframe(self, tmp_dir):
        #.option("recursiveFileLookup", "true")
        df = self._spark.read.format("binaryFile").option("pathGlobFilter", "*.*").load(tmp_dir) \
                .withColumn("relPath", regexp_replace('path', 'file:' + tmp_dir, ''))
        return df
    
    def pack_files_from_tmp_to_store(self, tmp_dir, store_path):
        df = self.pack_files_form_tmp_to_dataframe(tmp_dir)
        df.write.option("parquet.block.size", self.BLOCK_SIZE).mode("overwrite").parquet(store_path)
    
    def unpack_files_from_dataframe_to_tmp_files(self, df_archive, tmp_dir):
        df_archive.foreach(partial(SinaraArchiveStore_save_file, tmp_dir=tmp_dir))
    
    def unpack_files_from_store_to_tmp(self, store_path, tmp_dir):
        df = self._spark.read.parquet(store_path)
        self.unpack_files_from_dataframe_to_tmp_files(df, tmp_dir)