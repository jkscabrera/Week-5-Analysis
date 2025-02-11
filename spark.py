from pyspark.sql import SparkSession
import time

# start spark
spark = SparkSession.builder.appName("Red Pixel Analysis").getOrCreate()

# parquet file
parquet_file = "final.parquet"
df = spark.read.parquet(parquet_file)

# start
start_time = time.perf_counter()

# query
df.createOrReplaceTempView("pixels")
query_most_used_red_pixel = """
    SELECT x, y, COUNT(*) AS usage_count
    FROM pixels
    WHERE pixel_color = '#FF4500'
    GROUP BY x, y
    ORDER BY usage_count DESC
    LIMIT 3
"""

# run query
most_used_red_pixel = spark.sql(query_most_used_red_pixel)
most_used_red_pixel = most_used_red_pixel.collect()

# end time
end_time = time.perf_counter()

# result
print("Pixel with the most red placements:")
for row in most_used_red_pixel:
    print(row)

# execution time
execution_time = end_time - start_time
print(f"Query executed in {execution_time:.4f} seconds")

# stop spark
spark.stop()
