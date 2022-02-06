import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
import os
import re

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.12.0 pyspark-shell'
def _conf():
    number_cores = 8
    memory_gb = 8
    conf = (
        pyspark.SparkConf()
            .setMaster('local[{}]'.format(number_cores))
            .set('spark.driver.memory', '{}g'.format(memory_gb))
    )
    return conf

def _init_spark(conf):
    return SparkSession \
        .builder \
        .appName("NationalRail") \
        .config(conf=conf) \
        .getOrCreate()

spark = _init_spark(_conf())

def extract(file, row_tag):
    df = spark.read.format('xml').options(rowTag=row_tag, header=True).load(file)
    return df
    
def create_locations(file):
    row_tag = 'LocationRef'
    locations = extract(file, row_tag)
    locations = locations.select(col('_tpl').alias('loc_id'), col('_locname').alias('loc_name')).distinct()
    return locations

def create_reason(expr, id, reason_text):
    row_tag = 'Reason'
    df = extract(file, row_tag).distinct()
    df = df.filter(col('_reasontext').rlike(expr)) \
            .select(col('_code'), col('_reasontext').substr(lit(len(expr)), length(col('_reasontext'))).alias('_reasontext')) \
            .select(col('_code').alias(id), col('_reasontext').alias(reason_text))
    return df

def create_toc(file):
    row_tag = 'TocRef'
    df = extract(file, row_tag)
    toc = df.select(col('_toc').alias('code'), col('_tocname').alias('toc_name')).distinct()
    return toc

file = '/Users/mufidnuha/Desktop/national_rail/PPTimetable/20220206/*_ref_v*.xml'
locations = create_locations(file)
cancel_reason = create_reason('This train has been cancelled because of ', 'cancel_id', 'cancel_text')
late_reason = create_reason('This train has been delayed by ', 'late_id', 'late_text')
toc = create_toc(file)