import os
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

# Create a Spark session with connection parameters for Redis
spark = SparkSession.builder.appName("incremental-aggregation").getOrCreate()

# Connect to Redis
r = redis.Redis(
    host='redis',
    port=6379,
    decode_responses=True
)

# Specify non-category column names
EXCLUDED_COLUMNS = ['order_id', 'order_dow', 'order_hour_of_day', 'days_since_prior_order']

# Save redis key as a constant for easy manipulation
REDIS_KEY = "last_processed_day"

# Define constants for the directory to read from and file to write to
RAW_DIR = "/opt/data/incremental/raw"
OUTPUT_FILE = "/opt/data/processed/orders.csv"

# Define the file path to store the aggregated data
output_file = OUTPUT_FILE

# Get the day of the week that was last processed 
last_dow_processed = None
val = r.get(REDIS_KEY)

# Debugging purposes
print(f"Last day processed from Redis: {val if val else 'None'}")

# Set last_dow_processed to 0 if it does not exist; else convert to an int
last_dow_processed = int(val) if val else -1

# Get the folders that are currently in opt/data/incremental/raw
raw_dir = RAW_DIR

# Sort the folder numbers in ascending order
raw_folders = sorted(os.listdir(raw_dir), key=lambda x: int(x))

# Get the unprocessed folders that need to be processed
unprocessed_folders = [f"{raw_dir}/{d}/orders_{d}.csv" for d in raw_folders if int(d) > last_dow_processed]

if unprocessed_folders:
    print(f"Unprocessed folders: {unprocessed_folders}")
    
    # Read the unprocessed CSV files in the data/incremental/raw folder
    df = spark.read.csv(unprocessed_folders, header=True, sep=",")

    # Get the category names
    df_categories = [c for c in df.columns if c not in EXCLUDED_COLUMNS]

    # Extract each category and its count
    stack_expr = "stack({0}, {1}) as (category, items_count)".format(
        len(df_categories),
        ", ".join(["'{0}', `{0}`".format(c) for c in df_categories])
    )

    # Transform the wide data into long data that with the following columns : day_of_the_week, hour_of_day, category, items_count
    long_df = df.select(
        col("order_dow").alias("day_of_week"),
        col("order_hour_of_day").alias("hour_of_day"),
        expr(stack_expr)
    )

    # Convert the items_count from string to int in order to sum up the items_count
    long_df = long_df.withColumn("items_count", col("items_count").cast("int"))

    # Group the long data into (day_of_week, hour_of_day, category)
    aggregated_df = long_df.groupBy('day_of_week', 'hour_of_day', 'category').sum('items_count')
    aggregated_df = aggregated_df.withColumnRenamed("sum(items_count)", "items_count")

    # Store aggregated dataframe in orders.csv file
    aggregated_df.write.mode("append").csv(output_file, header=True)

    # Update the value of the dow that was last processed
    r.set(REDIS_KEY, int(raw_folders[-1]))