import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import os
import re

class JourneysTransformer:
    def __init__(self, df) -> None:
        self.df = df

    def create_journeys(self):
        journeys = self.df.select(col('_rid').cast(StringType()).alias('rid'),
                                col('_uid').alias('uid'),
                                col('_ssd').alias('date'),
                                col('_status').alias('status'),
                                col('_toc').alias('toc'),
                                col('_trainId').alias('train_id'),
                                col('_trainCat').alias('train_cat'),
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
    
class RoutesTransformer:
    def __init__(self, df) -> None:
        self.df = df

    def select_route(self, loc):
        dtype_column = self.df.select(loc).dtypes[0][1]
        match_array = re.search("^array", dtype_column)
        if match_array:
            temp = self.df.select(col('_uid'), explode(col(loc)).alias('route'))
        else:
            temp = self.df.select(col('_uid'), col(loc).alias('route'))
        #temp = temp.withColumn('tag_loc', lit(loc))
        return temp

    def create_tag(self, temp, route):
        if route == 'OPOR':
            temp = temp.withColumn('tag_route', lit('OR'))
        elif route == 'OPIP':
            temp = temp.withColumn('tag_route', lit('IP'))
        elif route == 'OPDT':
            temp = temp.withColumn('tag_route', lit('DT'))
        else:
            temp = temp.withColumn('tag_route', lit(route))
        return temp

    def select_col(self, temp, route):
        if route == 'PP':
            temp = temp.select(col('_uid'), col('tag_route'), col('route._tpl'), col('route._act'), col('route._wtp').alias('_wta'), col('route._wtp').alias('_wtd'))
        else:
            route_attb = temp.schema['route'].dataType.names
            temp = temp.select(col('_uid'), col('tag_route'), *([col('route')[c].alias(c) for c in route_attb]))
        return temp

    def create_routes(self, spark):
        routes_list = ['OR','IP','DT', 'OPOR', 'OPIP', 'OPDT', 'PP']
        temp_routes = []

        for route in self.df.columns:
            if route in routes_list:
                temp = self.select_route(route)
                temp = self.create_tag(temp, route)
                temp = self.select_col(temp, route)
                temp_routes.append(temp)
        
        routes = spark.createDataFrame([], StructType([]))
        for i in range(len(temp_routes)):
            routes = routes.unionByName(temp_routes[i], allowMissingColumns=True)

        routes = routes.select(col('_uid').alias('uid'),
                                col('tag_route'),
                                col('_tpl').alias('tpl'),
                                col('_act').alias('act'),
                                col('_pta').alias('pta'),
                                col('_wta').alias('wta'),
                                col('_ptd').alias('ptd'),
                                col('_wtd').alias('wtd'))
        #routes = transform_cleansing(routes)
        return routes

def main():
    spark = SparkSession \
        .builder \
        .appName("NationalRail") \
        .getOrCreate()

    #change automate
    date = '20220206'#sys.argv[1]
    root_dir = os.getcwd()
    src_path = '{root_dir}/mnt/data_lake/landing/PPTimetable/{date}/*_v8.xml'.format(root_dir=root_dir, date=date)
    dest_path = '{root_dir}/mnt/data_lake/clean/PPTimetable/{date}/'.format(root_dir=root_dir, date=date)
    filter_date = date[:4] + '-' + date[4:6] + '-' + date[6:]

    df = spark.read.format('xml').options(rowTag='Journey', header=True).load(src_path)
    df = df.filter(col('_ssd') == filter_date)

    #extract-transform journey
    journeys_transformer = JourneysTransformer(df)
    journeys = journeys_transformer.create_journeys()

    #extract-transform routes
    routes_transformer = RoutesTransformer(df)
    routes = routes_transformer.create_routes(spark)

    #load
    journeys.toPandas().to_csv('{dest_path}/{date}_journeys.csv'.format(dest_path=dest_path, date=date))
    routes.toPandas().to_csv('{dest_path}/{date}_routes.csv'.format(dest_path=dest_path, date=date))

    spark.stop()

if __name__=="__main__":
    main()