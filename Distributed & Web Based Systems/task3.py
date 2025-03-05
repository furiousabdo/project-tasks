from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, array_sort, expr

# Initialize Spark session
spark = SparkSession.builder.appName("Top3GradesPerYear").getOrCreate()

# Load dataset
df = spark.read.option("header", "false").csv("coursegrades.txt")

# Rename relevant columns (assuming _c0 = Year, _c2 = Grade)
df = df.select(
    col("_c0").alias("Year"),
    col("_c2").cast("int").alias("Grade")
)

# Group by Year and collect all grades in a list
grouped_df = df.groupBy("Year").agg(collect_list("Grade").alias("Grades"))

# Sort the grades in descending order and extract the top 3
sorted_df = grouped_df.withColumn("SortedGrades", array_sort(col("Grades")))
top_3_df = sorted_df.withColumn("Top3Grades", expr("slice(SortedGrades, -3, 3)"))

# Select relevant columns
result_df = top_3_df.select("Year", "Top3Grades")

# Show results
result_df.show(truncate=False)

# Stop Spark session
spark.stop()

