# Glue Job A: scripts/glue_clean_insclaims.py
# Use Glue 3.0/4.0 environment (PySpark). Requires pyarrow for parquet in local, Glue's Spark supports writing parquet.

import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from datetime import datetime

# ----- Glue job args -----
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'RAW_S3_PATH',        # s3://bucket/raw/claims/
                           'CLEAN_S3_PATH'])       # Optional for job B but keep
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_S3_PATH = args['RAW_S3_PATH']
CLEAN_S3_PATH = args['CLEAN_S3_PATH']
#SECRETS_ARN = args['SECRETS_ARN']

# --------- 1. Read raw CSV (coalesce small files if needed) ----------
# Provide explicit schema to avoid type inference issues
claims_schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("provider_id", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("claim_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("paid_amt", DoubleType(), True),
    StructField("status", StringType(), True)
    # additional raw columns if present will be ignored
])

df_raw = (spark.read.option("header", "true")
                    .schema(claims_schema)
                    .csv(f"{args['RAW_S3_PATH']}/claims/*.csv"))  # e.g. s3://de-claims-analytics-bucket/raw/claims/*.csv

# --------- 2. Basic cleaning & type conversions ----------
# Trim strings, uppercase status, parse timestamp
df = (df_raw
      .withColumn("claim_id", F.trim(F.col("claim_id")))
      .withColumn("member_id", F.trim(F.col("member_id")).cast("int"))
      .withColumn("provider_id", F.trim(F.col("provider_id")).cast("int"))
      .withColumn("diagnosis", F.upper(F.trim(F.col("diagnosis"))))
      .withColumn("status", F.upper(F.trim(F.col("status"))))
      .withColumn("amount", F.round(F.col("amount").cast("double"), 2))
      .withColumn("paid_amt", F.round(F.col("paid_amt").cast("double"), 2))
)

# Parse claim_date with fallback
df = df.withColumn("claim_ts",
                   F.to_timestamp(F.col("claim_date"), "yyyy-MM-dd HH:mm:ss"))
# If parsing fails, try another format (just example)
df = df.withColumn("claim_ts",
                   F.coalesce(F.col("claim_ts"),
                              F.to_timestamp(F.col("claim_date"), "yyyy-MM-dd")))

# Drop rows missing critical IDs
df = df.filter(F.col("claim_id").isNotNull() & F.col("member_id").isNotNull())

# --------- 3. Derived columns & enrichment ----------
# ingestion time
df = df.withColumn("ingestion_ts", F.current_timestamp())

# year/month partition
df = df.withColumn("year", F.year(F.col("claim_ts"))) \
       .withColumn("month", F.date_format(F.col("claim_ts"), "MM").cast("int"))

# reimbursement ratio
df = df.withColumn("reimbursement_ratio",
                   F.when(F.col("amount") > 0, F.col("paid_amt") / F.col("amount")).otherwise(None))

# high value flag
df = df.withColumn("high_value_flag", F.when(F.col("amount") >= 50000.0, F.lit(1)).otherwise(F.lit(0)))

# attempt to compute adjudicated days if adjudicated_date exists in raw; if not, we leave NULL
# (if you have adjudicated_date column, parse and compute difference)
if 'adjudicated_date' in df.columns:
    df = df.withColumn("adjudicated_ts",
                       F.to_timestamp(F.col("adjudicated_date"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("turnaround_days", F.datediff(F.col("adjudicated_ts"), F.col("claim_ts")))
else:
    df = df.withColumn("turnaround_days", F.lit(None).cast(IntegerType()))

# --------- 4. Deduplicate & Data Quality ----------
# simple dedupe by claim_id keeping latest ingestion (if duplicates)
w = Window.partitionBy("claim_id").orderBy(F.col("ingestion_ts").desc())
df = df.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") == 1).drop("rank")

# Data quality: invalid amounts
bad_amounts = df.filter(F.col("amount") <= 0).select("claim_id").limit(10)
if bad_amounts.count() > 0:
    print("Found claims with non-positive amounts (sample):")
    bad_amounts.show(5, truncate=False)


# --------- 6. Final column selection & ordering ----------
final_cols = [
    "claim_id", "member_id", "provider_id", "diagnosis", "claim_ts",
    "amount", "paid_amt", "status",
    "reimbursement_ratio", "turnaround_days", "high_value_flag",
    "ingestion_ts", "year", "month"
]
# include member/provider enrichment columns if present
for c in df.columns:
    if c not in final_cols:
        final_cols.append(c)

df_final = df.select(*final_cols)

print("PATH:", f"{args['RAW_S3_PATH']}/claims/")
print("Claims count =", df_final.count())

# --------- 7. Write partitioned parquet (partitioned by year/month) ----------
# Overwrite per run or write mode=append depending on your design. We do partitioned write for analytics.
(df_final.repartition(200)  # tune based on dataset size; 200 is example
         .write
         .mode("overwrite")   # use append for incremental loads
         .partitionBy("year", "month")
         .parquet(f"{args['CLEAN_S3_PATH']}/claims/"))   # e.g. s3://de-claims-analytics-bucket/clean/claims/

# Also write a small snapshot for quick queries: latest_run
(df_final.coalesce(1)
         .write.mode("overwrite")
         .parquet(f"{args['CLEAN_S3_PATH']}/claims/".rstrip("/") + "/_latest/"))

print("CLEAN WRITE complete. Rows:", df_final.count())

# ==========================================
# Members CSV -> CLEAN Parquet (partitioned)
# ==========================================

try:
    print("Reading MEMBERS CSV ...")
    
    members_schema = StructType([
        StructField("member_id", StringType(), True),
        StructField("member_name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("state", StringType(), True),
        StructField("join_date", StringType(), True)
    ])
    
    members_raw = (spark.read
                        .option("header", "true")
                        .schema(members_schema)
                        .csv(f"{args['RAW_S3_PATH']}/members/*.csv"))
    
    members_df = (members_raw
                    .withColumn("member_id", F.trim(F.col("member_id")).cast("int"))
                    .withColumn("member_name", F.initcap(F.trim(F.col("member_name"))))
                    .withColumn("age", F.col("age").cast("int"))
                    .withColumn("gender", F.upper(F.trim(F.col("gender"))))
                    .withColumn("state", F.upper(F.trim(F.col("state"))))
                    .withColumn("join_ts", F.to_timestamp(F.col("join_date"), "yyyy-MM-dd"))
                    .withColumn("ingestion_ts", F.current_timestamp())
                 )
    
    # partitions — year & month from join date
    members_df = members_df\
        .withColumn("join_year", F.year(F.col("join_ts")))\
        .withColumn("join_month", F.month(F.col("join_ts")))
    
    print("Writing MEMBERS Parquet to CLEAN zone ...")
    
    (members_df.repartition(30)
        .write
        .mode("overwrite")
        .partitionBy("join_year", "join_month")
        .parquet(f"{args['CLEAN_S3_PATH']}/members/"))
    
    (members_df.coalesce(1)
        .write.mode("overwrite")
        .parquet(f"{args['CLEAN_S3_PATH']}/members/".rstrip("/") + "/_latest/"))
    
    print("MEMBERS WRITE Complete. Rows:", members_df.count())
    print("Members count =", members_df.count())

except Exception as e:
    print("Members processing failed:", e)
	
# ==========================================
# Providers CSV -> CLEAN Parquet (partitioned)
# ==========================================

try:
    print("Reading PROVIDERS CSV ...")
    
    providers_schema = StructType([
        StructField("provider_id", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("speciality", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("join_date", StringType(), True)
    ])
    
    providers_raw = (spark.read
                        .option("header", "true")
                        .schema(providers_schema)
                        .csv(f"{args['RAW_S3_PATH']}/providers/*.csv"))
    
    providers_df = (providers_raw
                        .withColumn("provider_id", F.trim(F.col("provider_id")).cast("int"))
                        .withColumn("provider_name", F.initcap(F.trim(F.col("provider_name"))))
                        .withColumn("speciality", F.initcap(F.trim(F.col("speciality"))))
                        .withColumn("city", F.upper(F.trim(F.col("city"))))
                        .withColumn("state", F.upper(F.trim(F.col("state"))))
                        .withColumn("join_ts", F.to_timestamp(F.col("join_date"), "yyyy-MM-dd"))
                        .withColumn("ingestion_ts", F.current_timestamp())
                   )
    
    # partitions — year & month from join date
    providers_df = providers_df\
        .withColumn("join_year", F.year(F.col("join_ts")))\
        .withColumn("join_month", F.month(F.col("join_ts")))
    
    print("Writing PROVIDERS Parquet to CLEAN zone ...")
    
    
    print("Providers count =", providers_df.count())
    
    (providers_df.repartition(20)
        .write
        .mode("overwrite")
        .partitionBy("join_year", "join_month")
        .parquet(f"{args['CLEAN_S3_PATH']}/providers/"))
    
    (providers_df.coalesce(1)
        .write.mode("overwrite")
        .parquet(f"{args['CLEAN_S3_PATH']}/providers/".rstrip("/") + "/_latest/"))
    
    print("PROVIDERS WRITE Complete. Rows:", providers_df.count())

except Exception as e:
    print("Providers processing failed:", e)

# job commit if using glueContext job
if job:
    job.commit()
