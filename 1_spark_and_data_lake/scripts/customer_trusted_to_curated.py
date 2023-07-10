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

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://duybucketproject/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1688112942236 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://duybucketproject/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1688112942236",
)

# Script generated for node Join
Join_node1688113072397 = Join.apply(
    frame1=AccelerometerLanding_node1688112942236,
    frame2=CustomerTrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1688113072397",
)

# Script generated for node Drop Fields
DropFields_node1688113095784 = DropFields.apply(
    frame=Join_node1688113072397,
    paths=["x", "y", "z", "timeStamp", "user"],
    transformation_ctx="DropFields_node1688113095784",
)

# Script generated for node Customer Curated
CustomerCurated_node1688113143477 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688113095784,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://duybucketproject/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node1688113143477",
)

job.commit()
