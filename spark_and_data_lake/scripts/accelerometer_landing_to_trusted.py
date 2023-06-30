import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing Bucket
AccelerometerLandingBucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://duybucketproject/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingBucket_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1688110227401 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://duybucketproject/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1688110227401",
)

# Script generated for node Join
Join_node1688110283513 = Join.apply(
    frame1=AccelerometerLandingBucket_node1,
    frame2=CustomerTrusted_node1688110227401,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1688110283513",
)

# Script generated for node Drop Fields
DropFields_node1688110333418 = DropFields.apply(
    frame=Join_node1688110283513,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1688110333418",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1688110413635 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688110333418,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://duybucketproject/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1688110413635",
)

job.commit()
