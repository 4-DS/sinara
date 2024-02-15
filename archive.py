import os
import shutil
from os import path
from pathlib import Path
from functools import partial
from pyspark.sql.functions import regexp_replace,col,lit
from urllib.parse import urlsplit
 
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
    ROW_SIZE = 600 * 1024
    
    def __init__(self, spark):
        self._spark = spark;
        
    def pack_files_form_tmp_to_spark_df(self, tmp_dir):
        #.option("recursiveFileLookup", "true")
        tmp_url = tmp_dir
        url = urlsplit(tmp_dir)
        if not url.scheme:
            tmp_url = f'file://{url.path}'

        pathlist = [x for x in Path(tmp_dir).glob(f'**/*') if not str(x.name).endswith(".parts") and not str(x.parent).endswith(".parts")]
        for path in pathlist:
            if  self.ROW_SIZE < Path(path).stat().st_size:
                self._split_file(path, self.ROW_SIZE)
            
        df = self._spark.read.format("binaryFile").option("pathGlobFilter", "*").option("recursiveFileLookup", "true") \
                .load(tmp_url) \
                .filter(col('length') <= self.ROW_SIZE) \
                .withColumn("relPath", regexp_replace('path', 'file:' + tmp_dir, '')) \
                .drop("path")
        return df
    
    def pack_files_from_tmp_to_store(self, tmp_dir, store_path):
        df = self.pack_files_form_tmp_to_spark_df(tmp_dir)
        df.write.option("parquet.block.size", self.BLOCK_SIZE).mode("overwrite").parquet(store_path)
    
    def unpack_files_from_spark_df_to_tmp(self, df_archive, tmp_dir):
        df_archive.foreach(partial(SinaraArchive_save_file, tmp_dir=tmp_dir))
        self._join_parts(tmp_dir)
    
    def unpack_files_from_store_to_tmp(self, store_path, tmp_dir):
        df = self._spark.read.parquet(store_path)
        self.unpack_files_from_spark_df_to_tmp(df, tmp_dir)
        
    def _split_file(self, path, chunk_size):
        parts_path = Path(f"{path.parent}/{path.name}.parts")
        parts_path.mkdir(parents=False, exist_ok=True)
        part_num = 0
        with open(path, 'rb') as fin:
            while True:
                chunk = fin.read(chunk_size)
                if not chunk: break
                chunk_filename = f"{parts_path}/part-{part_num:04d}"
                with open(chunk_filename, 'wb') as fout: 
                    fout.write(chunk)
                    part_num = part_num + 1
        Path(f"{parts_path}/_PARTS").touch()
        
    def _join_parts(self, path):
        parts_dirlist = [x for x in Path(path).glob('**/*.parts') if x.is_dir()]
        for parts_dir in parts_dirlist:
            file_name = str(parts_dir.parent.joinpath(parts_dir.stem))
            with open(file_name, 'wb') as fout:
                for part_file in sorted(Path(parts_dir).glob('part-*')):
                    with open(str(part_file), "rb") as infile:
                        fout.write(infile.read())
            shutil.rmtree(parts_dir)