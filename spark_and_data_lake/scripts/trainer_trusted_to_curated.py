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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://duybucketproject/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1688116693225 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://duybucketproject/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1688116693225",
)

# Script generated for node Join
Join_node1688116721376 = Join.apply(
    frame1=AccelerometerTrusted_node1688116693225,
    frame2=StepTrainerTrusted_node1,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="Join_node1688116721376",
)

# Script generated for node Drop Fields
DropFields_node1688116757837 = DropFields.apply(
    frame=Join_node1688116721376,
    paths=["user"],
    transformation_ctx="DropFields_node1688116757837",
)

# Script generated for node Trainer Curated
TrainerCurated_node1688116767938 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1688116757837,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://duybucketproject/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="TrainerCurated_node1688116767938",
)

job.commit()
