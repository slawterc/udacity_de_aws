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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1722511208408 = glueContext.create_dynamic_frame.from_catalog(database="stedi-project", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1722511215779")

# Script generated for node Customer Trusted
CustomerTrusted_node1722511207375 = glueContext.create_dynamic_frame.from_catalog(database="stedi-project", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1722511207375")

# Script generated for node Privacy Filter
PrivacyFilter_node1722511211133 = Join.apply(frame1=AccelerometerLanding_node1722511208408, frame2=CustomerTrusted_node1722511207375, keys1=["user"], keys2=["email"], transformation_ctx="PrivacyFilter_node1722511211133")

# Script generated for node Drop Fields and Duplicates
SqlQuery2489 = '''
select distinct customername, email, phone, birthday, serialnumber, 
registrationdate, lastupdatedate, sharewithresearchasofdate, 
sharewithpublicasofdate, sharewithfriendsasofdate from myDataSource
'''
DropFieldsandDuplicates_node1722512911660 = sparkSqlQuery(glueContext, query = SqlQuery2489, mapping = {"myDataSource":PrivacyFilter_node1722511211133}, transformation_ctx = "DropFieldsandDuplicates_node1722512911660")

# Script generated for node Customer Curated
CustomerCurated_node1722511215779 = glueContext.getSink(path="s3://stedi-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1722511215779")
CustomerCurated_node1722511215779.setCatalogInfo(catalogDatabase="stedi-project",catalogTableName="customer_curated")
CustomerCurated_node1722511215779.setFormat("json")
CustomerCurated_node1722511215779.writeFrame(DropFieldsandDuplicates_node1722512911660)
job.commit()