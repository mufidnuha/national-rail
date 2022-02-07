import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

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

def main():
    spark = SparkSession \
        .builder \
        .appName("NationalRail") \
        .getOrCreate()

    #change automate
    date = '20220206'
    root_dir = os.getcwd()
    src_path = '{root_dir}/mnt/data_lake/landing/PPTimetable/{date}/*_v8.xml'.format(root_dir=root_dir, date=date)
    dest_path = '{root_dir}/mnt/data_lake/clean/PPTimetable/{date}/'.format(root_dir=root_dir, date=date)
    
    df = spark.read.format('xml').options(rowTag='Journey', header=True).load(src_path)
    journeys = create_journeys(df)
    journeys.toPandas().to_csv('{dest_path}/{date}_journeys.csv'.format(dest_path=dest_path, date=date))

    spark.stop()

if __name__=="__main__":
    main()