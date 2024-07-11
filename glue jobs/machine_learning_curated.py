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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1720709327852 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1720709327852")

# Script generated for node step trainer trusted
steptrainertrusted_node1720709310962 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1720709310962")

# Script generated for node inner join on timestamp and remove pii
SqlQuery3257 = '''
select sensorreadingtime, serialnumber, distancefromobject,
x, y, z
from step_trainer_trusted as stt
    inner join accelerometer_trusted as act
    where stt.sensorreadingtime = act.timestamp
'''
innerjoinontimestampandremovepii_node1720709558668 = sparkSqlQuery(glueContext, query = SqlQuery3257, mapping = {"step_trainer_trusted":steptrainertrusted_node1720709310962, "accelerometer_trusted":accelerometer_trusted_node1720709327852}, transformation_ctx = "innerjoinontimestampandremovepii_node1720709558668")

# Script generated for node machine_learning_curated
machine_learning_curated_node1720710195879 = glueContext.write_dynamic_frame.from_catalog(frame=innerjoinontimestampandremovepii_node1720709558668, database="stedi", table_name="machine_learning_curated", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="machine_learning_curated_node1720710195879")

job.commit()