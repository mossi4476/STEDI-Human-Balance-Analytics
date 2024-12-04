import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1733244933527 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="customer_trusted2", transformation_ctx="AmazonS3_node1733244933527")

# Script generated for node Amazon S3
AmazonS3_node1733244934050 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="accelerometer_to_trusted", transformation_ctx="AmazonS3_node1733244934050")

# Script generated for node Join
Join_node1733244939734 = Join.apply(frame1=AmazonS3_node1733244934050, frame2=AmazonS3_node1733244933527, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1733244939734")

# Script generated for node Drop Fields
DropFields_node1733244950890 = DropFields.apply(frame=Join_node1733244939734, paths=["z", "y", "x", "timestamp"], transformation_ctx="DropFields_node1733244950890")

# Script generated for node Drop Duplicates
DropDuplicates_node1733244966588 =  DynamicFrame.fromDF(DropFields_node1733244950890.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1733244966588")

# Script generated for node Drop Fields
DropFields_node1733244998392 = DropFields.apply(frame=DropDuplicates_node1733244966588, paths=["email", "phone", "customername", "birthday"], transformation_ctx="DropFields_node1733244998392")

# Script generated for node Amazon S3
AmazonS3_node1733245012938 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1733244998392, connection_type="s3", format="json", connection_options={"path": "s3://stedi-data-lake/customer/curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1733245012938")

job.commit()