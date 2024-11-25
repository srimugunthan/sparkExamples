from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, window
from pyspark.sql.types import TimestampType

# Create a SparkSession
spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()

# Sample DataFrame with timestamp column
data = [
    ("Alice", 10, "2023-11-20 12:00:00"),
    ("Bob", 20, "2023-11-21 13:00:00"),
    ("Charlie", 15, "2023-11-22 14:00:00"),
    ("Alice", 12, "2023-11-23 15:00:00"),
    ("Bob", 22, "2023-11-24 16:00:00"),
    ("Charlie", 18, "2023-11-25 17:00:00"),
]

df = spark.createDataFrame(data, ["name", "value", "timestamp"])
df = df.withColumn("timestamp", df["timestamp"].cast(TimestampType()))

# Define a window specification
windowSpec = Window.partitionBy("name").orderBy("timestamp").rangeBetween(-7*24*60*60, 0)

# Use lag to get the previous record within the 7-day window
df = df.withColumn("previous_value", lag("value", 1).over(windowSpec))

df.show()
