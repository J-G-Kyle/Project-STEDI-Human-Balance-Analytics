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

# Script generated for node Customer Curated Node
CustomerCuratedNode_node1720625124202 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCuratedNode_node1720625124202")

# Script generated for node Step_trainer Landing Node
Step_trainerLandingNode_node1720625074204 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="Step_trainerLandingNode_node1720625074204")

# Script generated for node join accelerometr_trusted and step_trainer_landing keep only step_trainer fields
SqlQuery3756 = '''
select sensorreadingtime, stl.serialnumber, distancefromobject
from step_trainer_landing as stl
    inner join customer_curated as cc
    where cc.serialnumber = stl.serialnumber
'''
joinaccelerometr_trustedandstep_trainer_landingkeeponlystep_trainerfields_node1720625531494 = sparkSqlQuery(glueContext, query = SqlQuery3756, mapping = {"customer_curated":CustomerCuratedNode_node1720625124202, "step_trainer_landing":Step_trainerLandingNode_node1720625074204}, transformation_ctx = "joinaccelerometr_trustedandstep_trainer_landingkeeponlystep_trainerfields_node1720625531494")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1720696873367 = glueContext.write_dynamic_frame.from_catalog(frame=joinaccelerometr_trustedandstep_trainer_landingkeeponlystep_trainerfields_node1720625531494, database="stedi", table_name="step_trainer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="step_trainer_trusted_node1720696873367")

job.commit()