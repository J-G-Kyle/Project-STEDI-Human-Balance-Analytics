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

# Script generated for node customer_trusted
customer_trusted_node1720699905836 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1720699905836")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1720699907216 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1720699907216")

# Script generated for node select distinct customers who have accelerometer data for research
SqlQuery2942 = '''
select distinct customerName, email, phone, birthDay,
serialnumber, registrationDate, lastupdatedate,
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate
from customer_trusted as ct
inner join accelerometer_trusted as act
where act.user = ct.email
'''
selectdistinctcustomerswhohaveaccelerometerdataforresearch_node1720700131477 = sparkSqlQuery(glueContext, query = SqlQuery2942, mapping = {"accelerometer_trusted":accelerometer_trusted_node1720699907216, "customer_trusted":customer_trusted_node1720699905836}, transformation_ctx = "selectdistinctcustomerswhohaveaccelerometerdataforresearch_node1720700131477")

# Script generated for node Customer curated
Customercurated_node1720700996031 = glueContext.write_dynamic_frame.from_catalog(frame=selectdistinctcustomerswhohaveaccelerometerdataforresearch_node1720700131477, database="stedi", table_name="customer_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="Customercurated_node1720700996031")

job.commit()