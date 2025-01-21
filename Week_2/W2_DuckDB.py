import duckdb
from datetime import datetime
from time import perf_counter_ns
import os
import gzip
import pyarrow.csv as pv
import pyarrow.parquet as pq

def parse_timestamp(timestamp_str):
    formats = ["%Y-%m-%d %H:%M:%S.%f UTC", "%Y-%m-%d %H:%M:%S UTC"]
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Invalid format: {timestamp_str}")

def check_time_format(time_str):
    try:
        return datetime.strptime(time_str, "%Y-%m-%d %H")
    except ValueError:
        raise ValueError(f"Invalid format: {time_str}")

def check_time_range(start_time, end_time):
    if end_time <= start_time:
        raise ValueError("End time should be after start time.")
    return True

def convert_gzip_to_parquet(gzip_path, parquet_path, batch_size=512 * 1024**2):
    with gzip.open(gzip_path, mode='rb') as file:
        csv_reader = pv.open_csv(
            file,
            parse_options=pv.ParseOptions(delimiter=","),
            convert_options=pv.ConvertOptions(include_columns=["timestamp", "pixel_color", "coordinate"]),
            read_options=pv.ReadOptions(block_size=batch_size)
        )

        for i, batch in enumerate(csv_reader):
            table = batch.to_table()
            batch_file = f"{parquet_path}_part_{i}.parquet"
            pq.write_table(table, batch_file, compression="snappy")

        batch_files = [f"{parquet_path}_part_{i}.parquet" for i in range(i + 1)]
        final_table = pq.concat_tables([pq.read_table(f) for f in batch_files])
        pq.write_table(final_table, parquet_path, compression="snappy")

def process_parquet_with_duckdb(file_path, start_time, end_time):
    query_pixel_color = f"""
    SELECT 
        LOWER(TRIM(pixel_color)) AS pixel_color,
        COUNT(*) AS color_count
    FROM read_parquet('{file_path}')
    WHERE 
        timestamp IS NOT NULL
        AND CAST(timestamp AS TIMESTAMP) >= '{start_time}' 
        AND CAST(timestamp AS TIMESTAMP) < '{end_time}'
        AND pixel_color IS NOT NULL
    GROUP BY LOWER(TRIM(pixel_color))
    ORDER BY color_count DESC
    """

    query_coordinate = f"""
        SELECT 
            LOWER(TRIM(coordinate)) AS coordinate,
            COUNT(*) AS coordinate_count
        FROM read_parquet('{file_path}')
        WHERE 
            timestamp IS NOT NULL
            AND CAST(timestamp AS TIMESTAMP) >= '{start_time}' 
            AND CAST(timestamp AS TIMESTAMP) < '{end_time}'
            AND coordinate IS NOT NULL
        GROUP BY LOWER(TRIM(coordinate))
        ORDER BY coordinate_count DESC
    """

    result_pixel_color = duckdb.query(query_pixel_color).to_df()

    result_coordinate = duckdb.query(query_coordinate).to_df()

    most_place_pixel_color = result_pixel_color.iloc[0]['pixel_color'] if not result_pixel_color.empty else "None"
    most_placed_pixel = result_coordinate.iloc[0]['coordinate'] if not result_coordinate.empty else "None"

    return most_place_pixel_color, most_placed_pixel

def main():
    start_timer = perf_counter_ns()

    try:
        start_hour = input("Start time (YYYY-MM-DD HH): ")
        end_hour = input("End time (YYYY-MM-DD HH): ")

        start_time = check_time_format(start_hour)
        end_time = check_time_format(end_hour)
        check_time_range(start_time, end_time)

        gzip_path = '2022_place_canvas_history.csv.gzip'
        parquet_path = '2022_place_canvas_history.parquet'

        if not os.path.exists(parquet_path):
            print("Converting gzip to parquet...")
            convert_gzip_to_parquet(gzip_path, parquet_path)
        else:
            print("Parquet file already exists. Skipping conversion.")

        common_pixel_color, common_coordinate = process_parquet_with_duckdb(parquet_path, start_time, end_time)
        print(f"Most Placed pixel_color: {common_pixel_color}")
        print(f"Most Placed Pixel Location: ({common_coordinate})")

    except ValueError as e:
        print(f"Error: {e}")

    end_timer = perf_counter_ns()
    exe_time = end_timer - start_timer
    print(f"Execution Time: {exe_time / 1_000_000} ms")

if __name__ == "__main__":
    main()
