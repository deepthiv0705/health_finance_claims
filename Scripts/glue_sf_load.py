import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T


def get_param(name):
    """Fetch SSM secure string (KMS decrypted)"""
    return boto3.client('ssm').get_parameter(
        Name=name,
        WithDecryption=True
    )["Parameter"]["Value"]
    
# --------- Glue Job Args ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job_name = args["JOB_NAME"]

# --------- Spark + Glue -----------
#sc = SparkContext()
#glueContext = GlueContext(sc)
#spark = glueContext.spark_session
#logger = glueContext.get_logger()
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

logger.info(f"===== STARTING GLUE JOB: {job_name} =====")

# --------- 1. Load Snowflake Secrets ---------
logger.info("Fetching Snowflake credentials from SSM")

account   = get_param("/snowflake/account")
user      = get_param("/snowflake/user")
password  = get_param("/snowflake/password")
warehouse = get_param("/snowflake/warehouse")
database  = get_param("/snowflake/database")
schema    = get_param("/snowflake/schema")

sfOptions = {
    "sfURL": f"{account}.snowflakecomputing.com",
    "sfUser": user,
    "sfPassword": password,
    "sfWarehouse": warehouse,
    "sfDatabase": database,
    "sfSchema": schema,
}

# --------- 2. List of datasets ----------
datasets = [
    {"path": "s3://projinsuranceclaim/clean/members/",   "table": "MEMBERS"},
    {"path": "s3://projinsuranceclaim/clean/claims/",    "table": "CLAIMS"},
    {"path": "s3://projinsuranceclaim/clean/providers/", "table": "PROVIDERS"},
]

# --------- 3. Load each into Snowflake ----------
for d in datasets:
    try:
        logger.info(f"Reading Parquet: {d['path']}")
        df = spark.read.parquet(d["path"])
        
        print("Columns:", df.columns)
        logger.info(f"Columns: {df.columns}")

        # Clean column names
        for c in df.columns:
            new_c = c.replace(".", "_").upper()
            df = df.withColumnRenamed(c, new_c)
            
         # Convert NullType columns
        for c in df.schema:
            if isinstance(c.dataType, T.NullType):
                df = df.withColumn(c.name, F.lit(None).cast(T.IntegerType()))

        logger.info(f"Writing to Snowflake table: {d['table']}")

        (
            df.write
              .format("snowflake")
              .options(**sfOptions)
              .option("dbtable", d["table"])
              .mode("overwrite")  # or append
              .save()
        )

        logger.info(f"SUCCESS → {d['table']}")
        print(f"SUCCESS: {d['table']}")

    except Exception as e:
        logger.error(f"FAILURE → {d['table']}: {str(e)}")
        print(f"FAILED: {d['table']} → {e}")

logger.info("===== JOB COMPLETE =====")
print("===== JOB COMPLETE =====")

spark.stop()
