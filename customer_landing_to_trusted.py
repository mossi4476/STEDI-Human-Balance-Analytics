import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

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
AmazonS3_node1733237760261 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://dung4476-cd0030bucket-customers/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1733237760261")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT * 
FROM myDataSource
WHERE shareWithResearchAsOfDate IS NULL 
AND shareWithResearchAsOfDate = 0;
'''
SQLQuery_node1733237764033 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1733237760261}, transformation_ctx = "SQLQuery_node1733237764033")

# Script generated for node Amazon S3
AmazonS3_node1733237767869 = glueContext.getSink(path="s3://dung4476-cd0030bucket-customers/customer/landing/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1733237767869")
AmazonS3_node1733237767869.setCatalogInfo(catalogDatabase="steadi",catalogTableName="customer_trusted2")
AmazonS3_node1733237767869.setFormat("json")
AmazonS3_node1733237767869.writeFrame(SQLQuery_node1733237764033)
job.commit()