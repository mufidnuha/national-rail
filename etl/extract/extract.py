#import findspark
#findspark.init('/Users/mufidnuha/server/spark-3.2.0-bin-hadoop3.2')
#from pyspark.sql.functions import *
#from pyspark.sql.types import *
from spark_conn import _init_spark, _conf
import os
from extract_ref import create_locations, create_reason, create_toc, extract_xml
#from extract_journeys import create_journeys
#from extract_routes import create_routes

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.12:0.12.0 pyspark-shell'
spark = _init_spark(_conf())

def extract_xml(file, row_tag):
    df = spark.read.format('xml').options(rowTag=row_tag, header=True).load(file)
    
    return df

root_dir = os.getcwd()
ref_file = '{root_dir}/PPTimetable/20220206/*_ref_v*.xml'.format(root_dir=root_dir)

locations = extract_xml(ref_file, 'LocationRef')
reason = extract_xml(ref_file, 'Reason')
toc = extract_xml(ref_file, 'TocRef')

locations = create_locations(locations)
cancel_reason = create_reason(reason, 'This train has been cancelled because of ', 'cancel_id', 'cancel_text')
late_reason = create_reason(reason, 'This train has been delayed by ', 'late_id', 'late_text')
toc = create_toc(toc)

#date = '2021-12-20'
file = '{root_dir}/PPTimetable/20220206/*_v8.xml'.format(root_dir=root_dir)
#row_tag_journey = 'Journey'
#df = extract_xml(file, row_tag_journey)

#journeys = create_journeys(df)
#routes = create_routes(df)