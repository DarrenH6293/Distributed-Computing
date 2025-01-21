import pandas as pd
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count
from time import perf_counter_ns
import os
import gzip
import pyarrow
import pyarrow.csv as pv
import pyarrow.parquet as pq

def parse_timestamp(timestamp_str):
    formats = ["%Y-%m-%d %H:%M:%S.%f UTC",
               "%Y-%m-%d %H:%M:%S UTC"]
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
            table = pyarrow.Table.from_batches([batch])

            batch_file = f"{parquet_path}_part_{i}.parquet"
            pq.write_table(table, batch_file, compression="snappy")

        batch_files = [f"{parquet_path}_part_{i}.parquet" for i in range(i + 1)]
        final_table = pyarrow.concat_tables([pq.read_table(f) for f in batch_files])
        pq.write_table(final_table, parquet_path, compression="snappy")


def process_single_chunk(chunk):
    pixel_color_count = chunk['pixel_color'].value_counts().to_dict()
    coordinate_count = chunk['coordinate'].value_counts().to_dict()
    return pixel_color_count, coordinate_count

def merge_results(results):
    pixel_color_count = {}
    coordinate_count = {}

    for pixel_color_result, pixel_result in results:
        for pixel_color, count in pixel_color_result.items():
            pixel_color_count[pixel_color] = pixel_color_count.get(pixel_color, 0) + count
        for pixel, count in pixel_result.items():
            coordinate_count[pixel] = coordinate_count.get(pixel, 0) + count

    return pixel_color_count, coordinate_count

def read_and_process_chunk(row_group_idx, file_path, start_time, end_time):

    parquet_file = pq.ParquetFile(file_path)
    chunk = parquet_file.read_row_group(row_group_idx, columns=["timestamp", "pixel_color", "coordinate"]).to_pandas()

    chunk['timestamp'] = chunk['timestamp'].apply(parse_timestamp)

    filtered_chunk = chunk[(chunk['timestamp'] >= start_time) & (chunk['timestamp'] < end_time)]

    return filtered_chunk

def process_parquet(file_path, start_time, end_time, chunk_size=100000):
    pixel_color_count = {}
    coordinate_count = {}

    parquet_file = pq.ParquetFile(file_path)
    num_row_groups = parquet_file.num_row_groups

    with Pool(cpu_count()) as pool:
        row_group_chunks = pool.starmap(
            read_and_process_chunk,
            [(i, file_path, start_time, end_time) for i in range(num_row_groups)]
        )

    filtered_data = pd.concat(row_group_chunks, ignore_index=True)


    smaller_chunks = np.array_split(filtered_data, cpu_count())

    with Pool(cpu_count()) as pool:
        chunk_results = pool.map(process_single_chunk, smaller_chunks)

    chunk_pixel_color_count, chunk_coordinate_count = merge_results(chunk_results)
    for pixel_color, count in chunk_pixel_color_count.items():
        pixel_color_count[pixel_color] = pixel_color_count.get(pixel_color, 0) + count
    for coordinate, count in chunk_coordinate_count.items():
        coordinate_count[coordinate] = coordinate_count.get(coordinate, 0) + count

    most_place_pixel_color = max(pixel_color_count, key=pixel_color_count.get, default="None")
    most_placed_pixel = max(coordinate_count, key=coordinate_count.get, default="None")

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
            convert_gzip_to_parquet(gzip_path, parquet_path)
        else:
            print("Parquet file already exists. Skipping conversion.")

        common_pixel_color, common_coordinate = process_parquet(parquet_path, start_time, end_time)
        print(f"Most Placed pixel_color: {common_pixel_color}")
        print(f"Most Placed Pixel Location: ({common_coordinate})")

    except ValueError as e:
        print(f"Error: {e}")

    end_timer = perf_counter_ns()
    exe_time = end_timer - start_timer
    print(f"Execution Time: {exe_time / 1_000_000} ms")

if __name__ == "__main__":
    main()
