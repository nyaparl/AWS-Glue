import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = (spark.read.format('csv').option("escapeQuotes", "true") \
      .option("header","ture") \
      .load("s3://cg-pr-glue-bucket/input/OfficeData.csv"))
df.write.format("csv").mode("overwrite").save("s3://cg-pr-glue-bucket/output/")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()