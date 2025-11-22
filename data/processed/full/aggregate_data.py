import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col

# Runs a Spark session in local mode and uses all CPU cores
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Define the file path to store the aggregated data
output_file = "/opt/data/processed/orders.csv"

# Specify non-category column names
EXCLUDED_COLUMNS = ['order_id', 'order_dow', 'order_hour_of_day', 'days_since_prior_order']

# Read all the 7 CSV files and convert it into a single Dataframe
df = spark.read.csv("/opt/data/raw/*/*.csv", header=True, sep=",")

# Get the category names
df_categories = [col for col in df.columns if col not in EXCLUDED_COLUMNS]

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
aggregated_df.write.mode("overwrite").csv(output_file, header=True)