import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1733242360664 = glueContext.create_dynamic_frame.from_catalog(database="steadi", table_name="customer_trusted2", transformation_ctx="AmazonS3_node1733242360664")

# Script generated for node Amazon S3
AmazonS3_node1733242359848 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1733242359848")

# Script generated for node Join
Join_node1733242376540 = Join.apply(frame1=AmazonS3_node1733242360664, frame2=AmazonS3_node1733242359848, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1733242376540")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT *
FROM myDataSource
WHERE FROM_UNIXTIME(timeStamp / 1000) > FROM_UNIXTIME(shareWithResearchAsOfDate / 1000);


'''
SQLQuery_node1733242397911 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1733242376540}, transformation_ctx = "SQLQuery_node1733242397911")

# Script generated for node Drop Fields
DropFields_node1733242412167 = DropFields.apply(frame=SQLQuery_node1733242397911, paths=["shareWithFriendsAsOfDate", "phone", "lastUpdateDate", "customerName", "serialNumber", "birthDay", "shareWithResearchAsOfDate", "registrationDate", "email", "shareWithPublicAsOfDate"], transformation_ctx="DropFields_node1733242412167")

# Script generated for node Drop Duplicates
DropDuplicates_node1733242421701 =  DynamicFrame.fromDF(DropFields_node1733242412167.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1733242421701")

# Script generated for node Amazon S3
AmazonS3_node1733242437646 = glueContext.getSink(path="s3://stedi-data-lake/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1733242437646")
AmazonS3_node1733242437646.setCatalogInfo(catalogDatabase="steadi",catalogTableName="accelerometer_to_trusted")
AmazonS3_node1733242437646.setFormat("json")
AmazonS3_node1733242437646.writeFrame(DropDuplicates_node1733242421701)
job.commit()