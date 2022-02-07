import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.12.0 pyspark-shell'
#spark = _init_spark(_conf())
def extract_xml(spark, file, row_tag):
    df = spark.read.format('xml').options(rowTag=row_tag, header=True).load(file)
        
    return df
    
def create_locations(df):
    return df.select(col('_tpl').alias('loc_id'), col('_locname').alias('loc_name')).distinct()

def create_reason(df, expr, id, reason_text):
    df = df.filter(col('_reasontext').rlike(expr)) \
            .select(col('_code'), col('_reasontext').substr(lit(len(expr)), length(col('_reasontext'))).alias('_reasontext')) \
            .select(col('_code').alias(id), col('_reasontext').alias(reason_text)).distinct()
    
    return df

def create_toc(df):
    return df.select(col('_toc').alias('code'), col('_tocname').alias('toc_name')).distinct()

def main():
    spark = SparkSession \
        .builder \
        .appName("NationalRail") \
        .getOrCreate()

    date = '20220206'

    root_dir = os.getcwd()
    src_path = '{root_dir}/mnt/data_lake/landing/PPTimetable/{date}/*_ref_v*.xml'.format(root_dir=root_dir, date=date)
    dest_path = '{root_dir}/mnt/data_lake/clean/PPTimetable/{date}/'.format(root_dir=root_dir, date=date)

    locations = extract_xml(spark, src_path, 'LocationRef')
    reason = extract_xml(spark, src_path, 'Reason')
    toc = extract_xml(spark, src_path, 'TocRef')

    locations = create_locations(locations)
    cancel_reason = create_reason(reason, 'This train has been cancelled because of ', 'cancel_id', 'cancel_text')
    late_reason = create_reason(reason, 'This train has been delayed by ', 'late_id', 'late_text')
    toc = create_toc(toc)

    locations.toPandas().to_csv('{dest_path}/{date}_locations.csv'.format(dest_path=dest_path, date=date))
    cancel_reason.toPandas().to_csv('{dest_path}/{date}_cancel_reason.csv'.format(dest_path=dest_path, date=date))
    late_reason.toPandas().to_csv('{dest_path}/{date}_late_reason.csv'.format(dest_path=dest_path, date=date))
    toc.toPandas().to_csv('{dest_path}/{date}_toc_reason.csv'.format(dest_path=dest_path, date=date))

    spark.stop()

if __name__ == "__main__":
    main()

