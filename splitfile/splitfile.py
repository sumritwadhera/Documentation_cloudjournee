import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
####Creating data souce after crawling tables
tab1 = glueContext.create_dynamic_frame.from_catalog(
    database = "db-sumrit",
    table_name = "inputs"
)
# Dynamic Frame to Spark DataFrame 
sparkDf_tab1 = tab1.toDF()
sparkDf_tab1_up = sparkDf_tab1.filter("serial<=5")
sparkDf_tab1_down = sparkDf_tab1.filter("serial>=6")


# Import Dyanmic DataFrame class
from awsglue.dynamicframe import DynamicFrame

#Convert from Spark Data Frame to Glue Dynamic Frame
tab1_conv1 = DynamicFrame.fromDF(sparkDf_tab1_up, glueContext, "convert")
tab1_conv2 = DynamicFrame.fromDF(sparkDf_tab1_down, glueContext, "convert")
#transform for a single file
retransform1 = tab1_conv1.repartition(1)
retransform2 = tab1_conv2.repartition(1)

glueContext.write_dynamic_frame.from_options(
frame =retransform1,
connection_type = "s3",
connection_options = {"path":"s3://sumrit-code/output1/"},
format = "csv",
format_options={
"separator": ","
},
transformation_ctx = "datasink01"
)
glueContext.write_dynamic_frame.from_options(
frame =retransform2,
connection_type = "s3",
connection_options = {"path":"s3://sumrit-code/output2/"},
format = "csv",
format_options={
"separator": ","
},
transformation_ctx = "datasink02"
)
#############################
import boto3
client = boto3.client('s3')
BUCKET_NAME= "sumrit-code"
#PREFIX="output/"
folders = ["output1","output2"]
for PREFIX in folders:
    # print(PREFIX)

    response = client.list_objects(
        Bucket=BUCKET_NAME,
        Prefix=PREFIX,
    )
    # print(response)
    name = response["Contents"][0]["Key"]
    # print(name)
    client.copy_object(Bucket=BUCKET_NAME, CopySource=BUCKET_NAME+'/'+name, Key=PREFIX+'/'+"RESULT.csv")
    client.delete_object(Bucket=BUCKET_NAME, Key=name)
    job.commit()
