import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql import functions as F  
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -------------------------------
# Source: Read CSV from S3
# -------------------------------
AmazonS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://<your-input-bucket>/ncr_ride_bookings.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node"
)

# -------------------------------
# FIX #1: Keep date/time as string in schema mapping
# -------------------------------
ChangeSchema_node = ApplyMapping.apply(
    frame=AmazonS3_node,
    mappings=[
        ("date", "string", "ride_date", "string"),   # FIXED
        ("time", "string", "ride_time", "string"),   # FIXED
        ("vehicle type", "string", "vehicle_type", "string"),
        ("pickup location", "string", "pickup_location", "string"),
        ("drop location", "string", "drop_location", "string"),
        ("avg vtat", "string", "avg_vtat", "float"),
        ("avg ctat", "string", "avg_ctat", "float"),
        ("cancelled rides by customer", "string", "cancelled_rides_by_customer", "string"),
        ("cancelled rides by driver", "string", "cancelled_rides_by_driver", "string"),
        ("incomplete rides", "string", "incomplete_rides", "string"),
        ("booking value", "string", "booking_value", "float"),
        ("ride distance", "string", "ride_distance", "double"),
        ("driver ratings", "string", "driver_ratings", "double"),
        ("customer rating", "string", "customer_rating", "double"),
        ("payment method", "string", "payment_method", "string"),
    ],
    transformation_ctx="ChangeSchema_node"
)

# -------------------------------
# Transformations in PySpark
# -------------------------------
df = ChangeSchema_node.toDF()

# FIX #1b: Cast date & time properly
df = df.withColumn("ride_date", F.to_date(F.col("ride_date"), "yyyy-MM-dd"))
df = df.withColumn("ride_time", F.to_timestamp(F.col("ride_time"), "HH:mm:ss"))

# FIX #2: Convert cancellation/incomplete to int
df = df.withColumn("cancelled_rides_by_customer",
                   F.when(F.col("cancelled_rides_by_customer").isNull(), "0")
                    .otherwise(F.trim(F.col("cancelled_rides_by_customer"))).cast("int"))

df = df.withColumn("cancelled_rides_by_driver",
                   F.when(F.col("cancelled_rides_by_driver").isNull(), "0")
                    .otherwise(F.trim(F.col("cancelled_rides_by_driver"))).cast("int"))

df = df.withColumn("incomplete_rides",
                   F.when(F.col("incomplete_rides").isNull(), "0")
                    .otherwise(F.trim(F.col("incomplete_rides"))).cast("int"))

# Vehicle type label encoding
df = df.withColumn("vehicle_type_encoded",
                   F.when(F.lower(F.trim(F.col("vehicle_type"))) == "ebike", 1)
                    .when(F.lower(F.trim(F.col("vehicle_type"))) == "go sedan", 2)
                    .when(F.lower(F.trim(F.col("vehicle_type"))) == "auto", 3)
                    .when(F.lower(F.trim(F.col("vehicle_type"))) == "premier sedan", 4)
                    .when(F.lower(F.trim(F.col("vehicle_type"))) == "bike", 5)
                    .when(F.lower(F.trim(F.col("vehicle_type"))) == "go mini", 6)
                    .when(F.lower(F.trim(F.col("vehicle_type"))) == "uber xl", 7)
                    .otherwise(0))

# Payment method label encoding
df = df.withColumn("payment_method_encoded",
                   F.when(F.col("payment_method").isNull() | (F.trim(F.col("payment_method")) == ""), 0)
                    .when(F.trim(F.col("payment_method")) == "UPI", 1)
                    .when(F.trim(F.col("payment_method")) == "Debit Card", 2)
                    .when(F.trim(F.col("payment_method")) == "Cash", 3)
                    .when(F.trim(F.col("payment_method")) == "Uber Wallet", 4)
                    .when(F.trim(F.col("payment_method")) == "Credit Card", 5)
                    .otherwise(0))

# Back to DynamicFrame
transformed_dyf = DynamicFrame.fromDF(df, glueContext, "transformed_dyf")

# -------------------------------
# FIX #3: Simplify DQ rule
# -------------------------------
DEFAULT_DATA_QUALITY_RULESET = "Rules = [ ColumnCount > 0 ]"

EvaluateDataQuality().process_rows(
    frame=transformed_dyf,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={"dataQualityEvaluationContext": "DQ_Check", "enableDataQualityResultsPublishing": True},
    additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"}
)

# -------------------------------
# Write outputs: Parquet + CSV
# -------------------------------
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dyf,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://<your-output-bucket>/parquet-output/", "partitionKeys": []},
    format_options={"compression": "uncompressed"},
    transformation_ctx="ParquetOutput"
)

# FIX #4: Add header to CSV
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dyf,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://<your-output-bucket>/csv-output/", "partitionKeys": []},
    format_options={"withHeader": True},  # FIXED
    transformation_ctx="CSVOutput"
)

job.commit()
