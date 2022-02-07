import findspark
findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
from pyspark.sql.functions import *
from pyspark.sql.types import *
from spark_conn import _init_spark, _conf
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.12.0 pyspark-shell'
spark = _init_spark(_conf())

def extract_xml(file, row_tag):
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

def extract_ref(date):
    root_dir = os.getcwd()
    ref_file = '{root_dir}/PPTimetable/{date}/*_ref_v*.xml'.format(root_dir=root_dir, date=date)

    locations = extract_xml(ref_file, 'LocationRef')
    reason = extract_xml(ref_file, 'Reason')
    toc = extract_xml(ref_file, 'TocRef')

    locations = create_locations(locations)
    cancel_reason = create_reason(reason, 'This train has been cancelled because of ', 'cancel_id', 'cancel_text')
    late_reason = create_reason(reason, 'This train has been delayed by ', 'late_id', 'late_text')
    toc = create_toc(toc)