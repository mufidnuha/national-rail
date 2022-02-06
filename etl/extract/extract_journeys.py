import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql.functions import *
from pyspark.sql.types import *

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