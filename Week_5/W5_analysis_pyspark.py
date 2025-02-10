from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, lag, unix_timestamp, avg, expr
from pyspark.sql.window import Window
from time import perf_counter_ns

def get_total_users(spark, file_path):
    df = spark.read.parquet(file_path)
    total_users = df.select(countDistinct("user_id")).collect()[0][0]
    return total_users

def find_most_active_users(spark, file_path, top_percent=1):
    df = spark.read.parquet(file_path).filter(col("user_id").isNotNull())

    total_users = get_total_users(spark, file_path)
    top_n = max(1, int(total_users * (top_percent / 100)))

    df_users = (df.groupBy("user_id")
                .agg(count("*").alias("pixel_placements"))
                .orderBy(col("pixel_placements").desc())
                .limit(top_n))
    
    return df_users.collect()

def find_sus_users_by_time_intervals(spark, file_path, top_users):
    df = spark.read.parquet(file_path).filter(
        (col("user_id").isNotNull()) & (col("timestamp").isNotNull()) & (col("user_id").isin(top_users))
    )

    window_spec = Window.partitionBy("user_id").orderBy(col("timestamp"))

    df_intervals = df.withColumn("prev_timestamp", lag("timestamp").over(window_spec)) \
                     .filter(col("prev_timestamp").isNotNull()) \
                     .withColumn("interval", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))

    df_sus = df_intervals.groupBy("user_id").agg(avg("interval").alias("avg_interval")) \
                         .filter(col("avg_interval") < 420) \
                         .orderBy("avg_interval")
    
    return df_sus.collect()

def find_most_painted_coordinates_by_bots(spark, file_path, suspicious_users):
    df = spark.read.parquet(file_path).filter(
        (col("user_id").isin(suspicious_users)) & (col("timestamp").isNotNull())
    )

    df_coords = (df.groupBy("coordinate")
                 .agg(count("*").alias("placements"))
                 .orderBy(col("placements").desc())
                 .limit(20))

    return df_coords.collect()

def track_hourly_changes_by_bots(spark, file_path, suspicious_users):
    df = spark.read.parquet(file_path).filter(
        (col("user_id").isin(suspicious_users)) & (col("timestamp").isNotNull())
    )

    df_filtered = df.filter(
        (expr("CAST(SPLIT_PART(coordinate, ',', 1) AS INT) BETWEEN 892 AND 961 AND CAST(SPLIT_PART(coordinate, ',', 2) AS INT) BETWEEN 1830 AND 1886")) |
        (expr("CAST(SPLIT_PART(coordinate, ',', 1) AS INT) BETWEEN 1611 AND 1691 AND CAST(SPLIT_PART(coordinate, ',', 2) AS INT) BETWEEN 212 AND 277"))
    )

    df_hourly = (df_filtered.groupBy(expr("date_trunc('hour', timestamp)").alias("hour"))
                 .agg(count("*").alias("bot_changes"))
                 .orderBy(col("bot_changes").desc()))

    return df_hourly.collect()

def main():
    start_timer = perf_counter_ns()
    file_path = "merged_canvas_history.parquet"
    spark = SparkSession.builder \
    .appName("CanvasAnalysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()


    top_users = find_most_active_users(spark, file_path, top_percent=1)
    top_users_list = [row["user_id"] for row in top_users]

    print(f"Total users analyzed: {len(top_users_list)}")

    sus_users = find_sus_users_by_time_intervals(spark, file_path, top_users_list)
    suspicious_users_list = [row["user_id"] for row in sus_users]

    print(f"Amount of suspected bots: {len(suspicious_users_list)}")

    bot_coordinates = find_most_painted_coordinates_by_bots(spark, file_path, suspicious_users_list)
    print("Most painted coordinates by suspected bots:")
    for row in bot_coordinates:
        print(row)

    bot_hourly_changes = track_hourly_changes_by_bots(spark, file_path, suspicious_users_list)
    print("Hourly bot changes:")
    for row in bot_hourly_changes:
        print(row)

    spark.stop()
    end_timer = perf_counter_ns()
    exe_time = end_timer - start_timer
    print(f"Execution Time: {exe_time / 1_000_000} ms")

if __name__ == "__main__":
    main()
