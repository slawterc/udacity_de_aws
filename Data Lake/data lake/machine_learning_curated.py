import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1722517707876 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1722517707876")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722517705419 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1722517705419")

# Script generated for node Join
Join_node1722517710480 = Join.apply(frame1=StepTrainerTrusted_node1722517707876, frame2=AccelerometerTrusted_node1722517705419, keys1=["sensorReadingTime"], keys2=["timestamp"], transformation_ctx="Join_node1722517710480")

# Script generated for node Drop Fields
DropFields_node1722518243598 = DropFields.apply(frame=Join_node1722517710480, paths=["`.serialNumber`"], transformation_ctx="DropFields_node1722518243598")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1722517713236 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1722518243598, connection_type="s3", format="json", connection_options={"path": "s3://stedi-project/machine_learning/curated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1722517713236")

job.commit()