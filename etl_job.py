import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue catalog (raw CSV)
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="kuda_ecommerce_db",
    table_name="orders",
    transformation_ctx="datasource"
)

# Convert to Spark DataFrame for transformations
df = datasource.toDF()

# Cast order_date from string to proper date
df = df.withColumn("order_date", to_date(df["order_date"], "yyyy-MM-dd"))

# Convert back to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

# Write to curated zone as Parquet, partitioned by year and month
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": "s3://kuda-ecommerce-datalake/curated/orders/",
        "partitionKeys": ["year", "month"]
    },
    format="parquet",
    transformation_ctx="datasink"
)

job.commit()
print("ETL job complete.")
