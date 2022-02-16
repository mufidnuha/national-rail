import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql.functions import *
from pyspark.sql.types import *
#from etl.extract.spark_conn import _init_spark, _conf
from pyspark.sql import SparkSession
import pyspark
import os

if __name__ == "__main__":
    
    spark = SparkSession \
        .builder \
        .appName("NationalRail") \
        .getOrCreate()
    
    date= '20220206'
    root_dir = os.getcwd()
    src_path = '{root_dir}/mnt/data_lake/landing/PPTimetable/{date}/*_ref_v*.xml'.format(root_dir=root_dir, date=date)
    dest_path = '{root_dir}/mnt/data_lake/clean/PPTimetable/{date}/'.format(root_dir=root_dir, date=date)
    
    row_tag = 'LocationRef'
    df = spark.read.format('xml').options(rowTag=row_tag, header=True).load(src_path)
    df.toPandas().to_csv('{dest_path}/coba.csv'.format(dest_path=dest_path))
    spark.stop()

