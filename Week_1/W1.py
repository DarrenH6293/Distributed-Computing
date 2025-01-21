import csv
import gzip
from datetime import datetime
from multiprocessing import Pool
from time import perf_counter_ns
import os
import numpy as np

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

def process_chunk(chunk, start_time, end_time):
    color_count = {}
    pixel_coordinate_count = {}

    for row in chunk:
        try:
            timestamp = parse_timestamp(row[0])
        except ValueError:
            continue

        if start_time <= timestamp < end_time:
            color = row[2]
            pixel_coordinate = row[3]

            color_count[color] = color_count.get(color, 0) + 1
            pixel_coordinate_count[pixel_coordinate] = pixel_coordinate_count.get(pixel_coordinate, 0) + 1

    return color_count, pixel_coordinate_count

# Merge results from chunks
def merge_results(results):
    color_count = {}
    pixel_coordinate_count = {}

    for color_result, pixel_result in results:
        for color, count in color_result.items():
            color_count[color] = color_count.get(color, 0) + count
        for pixel, count in pixel_result.items():
            pixel_coordinate_count[pixel] = pixel_coordinate_count.get(pixel, 0) + count

    return color_count, pixel_coordinate_count

def process_csv(file_path, start_time, end_time, chunk_size=100000):
    color_count = {}
    pixel_coordinate_count = {}

    with gzip.open(file_path, mode='rt', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader) # Skip header

        while True:
            # Read a chunk
            chunk = [row for _, row in zip(range(chunk_size), reader)]
            if not chunk:
                break # No more rows
            
            chunk_parts = np.array_split(chunk, os.cpu_count())

            # Multiprocessing
            with Pool() as pool:
                chunk_results = pool.starmap(
                    process_chunk, [(chunk_part, start_time, end_time) for chunk_part in chunk_parts]
)
            # Merge chunk results
            chunk_color_count, chunk_pixel_count = merge_results(chunk_results)

            # Overall results
            for color, count in chunk_color_count.items():
                color_count[color] = color_count.get(color, 0) + count
            for pixel, count in chunk_pixel_count.items():
                pixel_coordinate_count[pixel] = pixel_coordinate_count.get(pixel, 0) + count

    # Results
    most_place_color = max(color_count, key=color_count.get, default="None")
    most_placed_pixel = max(pixel_coordinate_count, key=pixel_coordinate_count.get, default="None")

    return most_place_color, most_placed_pixel

def main():
    start_timer = perf_counter_ns()

    try:
        start_hour = input("Start time (YYYY-MM-DD HH): ")
        end_hour = input("End time (YYYY-MM-DD HH): ")

        start_time = check_time_format(start_hour)
        end_time = check_time_format(end_hour)
        check_time_range(start_time, end_time)

        file_path = '2022_place_canvas_history.csv.gzip'

        common_color, common_coordinate = process_csv(file_path, start_time, end_time)
        print(f"Most Placed Color: {common_color}")
        print(f"Most Placed Pixel Location: ({common_coordinate})")

    except ValueError as e:
        print(f"Error: {e}")

    end_timer = perf_counter_ns()
    exe_time = end_timer - start_timer
    print(f"Execution Time: {exe_time / 1_000_000} ms")

if __name__ == "__main__":
    main()
