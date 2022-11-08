import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
####Creating data souce after crawling tables
tab1 = glueContext.create_dynamic_frame.from_catalog(
    database = "db-sumrit",
    table_name = "inputs",transformation_ctx = "tab1"
 , additional_options = {"jobBookmarkKeys":["serial"],"jobBookmarkKeysSortOrder":"asc"})
# Dynamic Frame to Spark DataFrame 
sparkDf_tab1 = tab1.toDF()
# Import Dyanmic DataFrame class
from awsglue.dynamicframe import DynamicFrame

#Convert from Spark Data Frame to Glue Dynamic Frame
tab1_conv = DynamicFrame.fromDF(sparkDf_tab1, glueContext, "convert")
#transform for a single file
retransform = tab1_conv.repartition(1)
glueContext.write_dynamic_frame.from_options(
frame =retransform,
connection_type = "s3",
connection_options = {"path":"s3://sumrit-code/output/"},
format = "csv",
format_options={
"separator": ","
},
transformation_ctx = "datasink01"
)
###################################################
import boto3
from datetime import datetime

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
BUCKET_NAME = "sumrit-code"
PREFIX = "output/"

s3_resource = boto3.resource('s3')
client = boto3.client('s3')

objects = list(s3_resource.Bucket(BUCKET_NAME).objects.filter(Prefix=PREFIX))
objects.sort(key=lambda o: o.last_modified)
latest_object = objects[-1].key
client.copy_object(Bucket=BUCKET_NAME, CopySource=BUCKET_NAME+'/'+latest_object, Key=PREFIX + "Output" + str(now) + ".csv")

run_objects = list(s3_resource.Bucket(BUCKET_NAME).objects.filter(Prefix=PREFIX))
run_objects.sort(key=lambda o: o.last_modified)
run_object = run_objects[-2].key
client.delete_object(Bucket=BUCKET_NAME, Key=run_object)

job.commit()