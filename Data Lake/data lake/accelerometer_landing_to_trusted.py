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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1722511208408 = glueContext.create_dynamic_frame.from_catalog(database="stedi-project", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1722511208408")

# Script generated for node Customer Trusted
CustomerTrusted_node1722511207375 = glueContext.create_dynamic_frame.from_catalog(database="stedi-project", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1722511207375")

# Script generated for node Privacy Filter
PrivacyFilter_node1722511211133 = Join.apply(frame1=AccelerometerLanding_node1722511208408, frame2=CustomerTrusted_node1722511207375, keys1=["user"], keys2=["email"], transformation_ctx="PrivacyFilter_node1722511211133")

# Script generated for node Drop Fields
DropFields_node1722511213039 = DropFields.apply(frame=PrivacyFilter_node1722511211133, paths=["email", "phone", "customername", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1722511213039")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722511215779 = glueContext.getSink(path="s3://stedi-project/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1722511215779")
AccelerometerTrusted_node1722511215779.setCatalogInfo(catalogDatabase="stedi-project",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1722511215779.setFormat("json")
AccelerometerTrusted_node1722511215779.writeFrame(DropFields_node1722511213039)
job.commit()