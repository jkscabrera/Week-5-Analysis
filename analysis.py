import duckdb
import time

# parquet file
parquet_file = "final.parquet"

# query to find the pixel that used red the most
query_most_used_red_pixel = f"""
    SELECT x, y, COUNT(*) AS usage_count
    FROM '{parquet_file}'
    WHERE pixel_color = '#FF4500'
    GROUP BY x, y
    ORDER BY usage_count DESC
    LIMIT 3;
"""

# start time
start_time = time.perf_counter()

# query to get the pixel where red was used the most
most_used_red_pixel = duckdb.query(query_most_used_red_pixel).to_df()

# end time
end_time = time.perf_counter()

# result
print("Pixel with the most red placements:")
print(most_used_red_pixel)

# execution time
execution_time = end_time - start_time
print(f"Query executed in {execution_time:.4f} seconds")
