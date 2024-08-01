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

# Script generated for node Customer Curated
CustomerCurated_node1722516658477 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1722516658477")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1722516659979 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1722516659979")

# Script generated for node Join
Join_node1722516662459 = Join.apply(frame1=CustomerCurated_node1722516658477, frame2=StepTrainerLanding_node1722516659979, keys1=["serialnumber"], keys2=["serialNumber"], transformation_ctx="Join_node1722516662459")

# Script generated for node Drop Fields
DropFields_node1722516664252 = DropFields.apply(frame=Join_node1722516662459, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1722516664252")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1722516667312 = glueContext.getSink(path="s3://stedi-project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1722516667312")
StepTrainerTrusted_node1722516667312.setCatalogInfo(catalogDatabase="stedi-project",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1722516667312.setFormat("json")
StepTrainerTrusted_node1722516667312.writeFrame(DropFields_node1722516664252)
job.commit()