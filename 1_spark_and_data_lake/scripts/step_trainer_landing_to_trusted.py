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

# Script generated for node Customer Curated
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://duybucketproject/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1688113836282 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://duybucketproject/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1688113836282",
)

# Script generated for node Join
Join_node1688113877495 = Join.apply(
    frame1=StepTrainerLanding_node1688113836282,
    frame2=CustomerCurated_node1,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1688113877495",
)

# Script generated for node Drop Fields
DropFields_node1688113933042 = DropFields.apply(
    frame=Join_node1688113877495,
    paths=[
        "customerName",
        "phone",
        "email",
        "serialNumber",
        "birthDay",
        "registrationDate",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1688113933042",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1688114004345 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688113933042,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://duybucketproject/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1688114004345",
)

job.commit()
