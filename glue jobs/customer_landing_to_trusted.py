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

# Script generated for node stedi.customer_landing
stedicustomer_landing_node1720692263544 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="stedicustomer_landing_node1720692263544")

# Script generated for node SQL - filter sharewithreasearchasofdate not null
SqlQuery3174 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SQLfiltersharewithreasearchasofdatenotnull_node1720388659327 = sparkSqlQuery(glueContext, query = SqlQuery3174, mapping = {"myDataSource":stedicustomer_landing_node1720692263544}, transformation_ctx = "SQLfiltersharewithreasearchasofdatenotnull_node1720388659327")

# Script generated for node stedi.customer_trusted
stedicustomer_trusted_node1720695618830 = glueContext.write_dynamic_frame.from_catalog(frame=SQLfiltersharewithreasearchasofdatenotnull_node1720388659327, database="stedi", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="stedicustomer_trusted_node1720695618830")

job.commit()