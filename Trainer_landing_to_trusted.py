import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Landing
step_trainer_landing = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing",
)

# Script generated for node Customer Curated
customer_curated = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated",
)

# Script generated for node Filter - Privacy Filter
privacy_filtered_customer = ApplyMapping.apply(
    frame=customer_curated,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="privacy_filtered_customer",
)

# Script generated for node Join - Privacy Join
privacy_filtered_data = Join.apply(
    frame1=step_trainer_landing,
    frame2=privacy_filtered_customer,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="privacy_filtered_data",
)

# Script generated for node Drop Fields
cleaned_customer_data = DropFields.apply(
    frame=privacy_filtered_data,
    paths=[
        "right_serialNumber",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="cleaned_customer_data",
)
# Script generated for node Drop Duplicates
deduplicated_data = DynamicFrame.fromDF(
    cleaned_customer_data.toDF().dropDuplicates(),
    glueContext,
    "deduplicated_data",
)

# Script generated for node Step Trainer Trusted
trusted_step_trainer = glueContext.write_dynamic_frame.from_options(
    frame=deduplicated_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="trusted_step_trainer",
)

job.commit()
