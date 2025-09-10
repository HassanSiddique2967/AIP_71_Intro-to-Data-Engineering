import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, to_timestamp, to_date, trim

spark = SparkSession.builder.appName("GlueLogParser").getOrCreate()

# -------- CONFIG --------
SOURCE_PATH = "s3://your-source/log-events-viewer-result.csv"   # <-- source log file
DEST_PATH   = "s3://your-destination/cleaned-logs/"         # <-- destination folder
DEBUG_PATH  = "s3://your-debug-folder/debug/"                        # <-- debug folder
# ------------------------

# Read CSV with header
df = spark.read.csv(SOURCE_PATH, header=True, inferSchema=True)

# Debug: write schema + sample to S3
schema_str = "\n".join([f"{f.name}: {f.dataType}" for f in df.schema.fields])
spark.sparkContext.parallelize([schema_str]).coalesce(1).saveAsTextFile(DEBUG_PATH + "schema/")
df.limit(20).coalesce(1).write.mode("overwrite").option("header", "true").csv(DEBUG_PATH + "sample/")

# Detect correct column name
log_col = "message" if "message" in df.columns else "_c1"

# Regex patterns for parsing
patterns = {
    "timestamp_raw": r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2},\d+)",
    "log_level": r"^(TRACE|INFO|DEBUG|WARN|ERROR)",
    "thread": r"\[(.*?)\]",
    "class": r"\s([\w.$]+)\s+\[",
    "line": r"\s(\d+)\s+",
    "message_clean": r"\]\s\d+\s+(.*)"
}

# Apply regex extraction
for col_name, pattern in patterns.items():
    df = df.withColumn(col_name, regexp_extract(col(log_col), pattern, 1))

# Convert timestamp + extract date
df = df.withColumn("timestamp", to_timestamp(col("timestamp_raw"), "yyyy-MM-dd'T'HH:mm:ss,SSS"))
df = df.withColumn("date", to_date(col("timestamp")))

# Select clean columns
parsed_df = df.select(
    "timestamp",
    "log_level",
    "thread",
    "class",
    "line",
    "message_clean",
    "date"
)

# Remove empty rows (where message_clean is null or empty string)
parsed_df = parsed_df.filter(trim(col("message_clean")) != "")

# Debug: write 20 parsed rows
parsed_df.limit(20).coalesce(1).write.mode("overwrite").option("header", "true").csv(DEBUG_PATH + "parsed_sample/")

# Final output: single CSV file (will be part-0000*.csv inside cleaned-logs/)
parsed_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(DEST_PATH)

print("Debug outputs written to:", DEBUG_PATH)
print("Final output written to:", DEST_PATH)
