import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_conn import _init_spark, _conf
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.12.0 pyspark-shell'
spark = _init_spark(_conf())

def extract(file, row_tag):
    df = spark.read.format('xml').options(rowTag=row_tag, header=True).load(file)
    return df

def create_journeys(df):
    journeys = df.select(col('_rid').cast(StringType()).alias('rid'),
                            col('_uid').alias('uid'),
                            col('_ssd').alias('ssd'),
                            col('_status').alias('status'),
                            col('_toc').alias('toc'),
                            col('_trainId').alias('trainId'),
                            col('_trainCat').alias('trainCat'),
                            col('_qtrain').alias('qtrain'),
                            col('_can').alias('cancel'),
                            col('_isPassengerSvc').alias('is_passenger_svc'),
                            col('_isCharter').alias('is_charter'),
                            col('cancelReason').alias('cancel_reason')) \
                .withColumn('cancel', when(col('cancel').isNull(), False)
                                    .otherwise(col('cancel'))) \
                .withColumn('is_charter', when(col('is_charter').isNull(), False)
                                            .otherwise(col('is_charter'))) \
                .withColumn('is_passenger_svc', when(col('is_passenger_svc').isNull(), False)
                                                .otherwise(col('is_passenger_svc'))) \
                .withColumn('qtrain', when(col('qtrain').isNull(), False)
                                        .otherwise(col('qtrain')))
    #journeys = transform_toc(journeys)
    #journeys = transform_cancel(journeys)
    return journeys