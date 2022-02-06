import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_conn import _init_spark, _conf
import os
import re

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.12.0 pyspark-shell'
spark = _init_spark(_conf())

def select_route(df, loc):
    dtype_column = df.select(loc).dtypes[0][1]
    match_array = re.search("^array", dtype_column)
    if match_array:
        temp = df.select(col('_uid'), explode(col(loc)).alias('route'))
    else:
        temp = df.select(col('_uid'), col(loc).alias('route'))
    #temp = temp.withColumn('tag_loc', lit(loc))
    return temp

def create_tag(temp, route):
    if route == 'OPOR':
        temp = temp.withColumn('tag_route', lit('OR'))
    elif route == 'OPIP':
        temp = temp.withColumn('tag_route', lit('IP'))
    elif route == 'OPDT':
        temp = temp.withColumn('tag_route', lit('DT'))
    else:
        temp = temp.withColumn('tag_route', lit(route))
    return temp

def select_col(temp, route):
    if route == 'PP':
        temp = temp.select(col('_uid'), col('tag_route'), col('route._tpl'), col('route._act'), col('route._wtp').alias('_wta'), col('route._wtp').alias('_wtd'))
    else:
        route_attb = temp.schema['route'].dataType.names
        temp = temp.select(col('_uid'), col('tag_route'), *([col('route')[c].alias(c) for c in route_attb]))
    return temp

def create_routes(df):
    routes_list = ['OR','IP','DT', 'OPOR', 'OPIP', 'OPDT', 'PP']
    temp_routes = []

    for route in df.columns:
        if route in routes_list:
            temp = select_route(df, route)
            temp = create_tag(temp, route)
            temp = select_col(temp, route)
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