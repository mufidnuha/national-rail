from pyspark.sql import SparkSession
import pyspark

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