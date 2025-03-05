from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, array_sort, expr, size


spark = SparkSession.builder.appName("Top3GradesPerYear").getOrCreate()

df = spark.read.option("header", "false").csv("coursegrades.txt")

# Rename columns
df = df.select(
    col("_c0").alias("Year"),
    col("_c2").cast("int").alias("Grade")
)

# Group by Year and collect all grades
grouped_df = df.groupBy("Year").agg(collect_list("Grade").alias("Grades"))

# Sort the grades in descending order
sorted_df = grouped_df.withColumn("SortedGrades", expr("reverse(array_sort(Grades))"))

# Ensure at least 3 grades exist, or take what's available
top_3_df = sorted_df.withColumn(
    "Top3Grades",
    expr("IF(size(SortedGrades) >= 3, slice(SortedGrades, 1, 3), SortedGrades)")
)

result_df = top_3_df.select("Year", "Top3Grades")

result_df.show()

spark.stop()