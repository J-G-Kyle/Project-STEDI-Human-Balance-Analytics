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

# Script generated for node Customer Trusted Node
CustomerTrustedNode_node1720625124202 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrustedNode_node1720625124202")

# Script generated for node Accelerometer Landing Node
AccelerometerLandingNode_node1720625074204 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingNode_node1720625074204")

# Script generated for node join customer_trusted and accelerometer keep only accelerometer fields
SqlQuery3377 = '''
select user, timestamp, x, y, z
from accelerometer_landing as a
    inner join customer_trusted as c
    where c.email = a.user
'''
joincustomer_trustedandaccelerometerkeeponlyaccelerometerfields_node1720625531494 = sparkSqlQuery(glueContext, query = SqlQuery3377, mapping = {"customer_trusted":CustomerTrustedNode_node1720625124202, "accelerometer_landing":AccelerometerLandingNode_node1720625074204}, transformation_ctx = "joincustomer_trustedandaccelerometerkeeponlyaccelerometerfields_node1720625531494")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1720696873367 = glueContext.write_dynamic_frame.from_catalog(frame=joincustomer_trustedandaccelerometerkeeponlyaccelerometerfields_node1720625531494, database="stedi", table_name="accelerometer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="accelerometer_trusted_node1720696873367")

job.commit()