import duckdb
from datetime import datetime
from time import perf_counter_ns
import os
import gzip
import pyarrow.csv as pv
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

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
    LIMIT 3
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
    LIMIT 3
    """

    result_pixel_color = duckdb.query(query_pixel_color).to_df()
    result_coordinate = duckdb.query(query_coordinate).to_df()

    return result_pixel_color, result_coordinate

def process_hourly_changes_for_top_coordinates(file_path, start_time, end_time, top_coordinates):
    top_coords_str = ",".join([f"'{coord}'" for coord in top_coordinates])
    
    query_hourly = f"""
        SELECT 
            DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
            LOWER(TRIM(coordinate)) AS coordinate,
            COUNT(*) AS changes
        FROM read_parquet('{file_path}')
        WHERE 
            timestamp IS NOT NULL
            AND CAST(timestamp AS TIMESTAMP) >= '{start_time}'
            AND CAST(timestamp AS TIMESTAMP) < '{end_time}'
            AND LOWER(TRIM(coordinate)) IN ({top_coords_str})
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

    df_hourly = duckdb.query(query_hourly).to_df()
    return df_hourly

def process_hourly_median_changes_for_all_coordinates(file_path, start_time, end_time):
    query_hourly_median = f"""
        WITH changes_per_coord_per_hour AS (
            SELECT 
                DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
                LOWER(TRIM(coordinate)) AS coordinate,
                COUNT(*) AS changes
            FROM read_parquet('{file_path}')
            WHERE 
                timestamp IS NOT NULL
                AND CAST(timestamp AS TIMESTAMP) >= '{start_time}'
                AND CAST(timestamp AS TIMESTAMP) < '{end_time}'
                AND coordinate IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT 
            hour,
            MEDIAN(changes) AS median_changes
        FROM changes_per_coord_per_hour
        GROUP BY hour
        ORDER BY hour
    """

    df_hourly_median = duckdb.query(query_hourly_median).to_df()
    return df_hourly_median

def process_distribution_changes_per_coord_per_hour(file_path, start_time, end_time):
    query = f"""
        WITH changes_per_coord_per_hour AS (
            SELECT 
                DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
                LOWER(TRIM(coordinate)) AS coordinate,
                COUNT(*) AS changes
            FROM read_parquet('{file_path}')
            WHERE 
                timestamp IS NOT NULL
                AND CAST(timestamp AS TIMESTAMP) >= '{start_time}'
                AND CAST(timestamp AS TIMESTAMP) < '{end_time}'
                AND coordinate IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT changes
        FROM changes_per_coord_per_hour
    """
    df_dist = duckdb.query(query).to_df()
    return df_dist

def get_top_colors_for_top_coordinates(file_path, start_time, end_time, top_coordinates):
    top_coords_str = ",".join([f"'{coord}'" for coord in top_coordinates])
    
    query = f"""
        SELECT 
            LOWER(TRIM(coordinate)) AS coordinate,
            LOWER(TRIM(pixel_color)) AS pixel_color,
            COUNT(*) AS color_count
        FROM read_parquet('{file_path}')
        WHERE 
            timestamp IS NOT NULL
            AND CAST(timestamp AS TIMESTAMP) >= '{start_time}'
            AND CAST(timestamp AS TIMESTAMP) < '{end_time}'
            AND LOWER(TRIM(coordinate)) IN ({top_coords_str})
            AND pixel_color IS NOT NULL
        GROUP BY coordinate, pixel_color
        ORDER BY coordinate, color_count DESC
    """

    df_colors = duckdb.query(query).to_df()

    top_colors_per_coordinate = {}
    for coord in top_coordinates:
        filtered_df = df_colors[df_colors['coordinate'] == coord].head(2)
        top_colors_per_coordinate[coord] = filtered_df[['pixel_color', 'color_count']].values.tolist()

    return top_colors_per_coordinate

def plot_hourly_changes(hourly_changes_df, hourly_median_df, top_3_coords):
    plt.figure(figsize=(10, 6))
    
    for coord in top_3_coords:
        coord_data = hourly_changes_df[hourly_changes_df['coordinate'] == coord]
        plt.plot(coord_data['hour'], coord_data['changes'], marker='o', label=f'Coord {coord}')
    
    plt.plot(hourly_median_df['hour'], hourly_median_df['median_changes'], linestyle='--', marker='s', color='black', label='Median of all coords')
    
    plt.xlabel('Hour')
    plt.ylabel('Number of Changes')
    plt.title('Hourly Changes for Top 3 Coordinates and Median')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()


def main():
    start_timer = perf_counter_ns()

    try:
        start_time = check_time_format("2022-04-01 00")
        end_time = check_time_format("2022-04-06 00")
        check_time_range(start_time, end_time)

        gzip_path = '2022_place_canvas_history.csv.gzip'
        parquet_path = '2022_place_canvas_history.parquet'

        if not os.path.exists(parquet_path):
            print("Converting gzip to parquet...")
            convert_gzip_to_parquet(gzip_path, parquet_path)
        else:
            print("Parquet file already exists. Skipping conversion.")

        result_pixel_color, result_coordinate = process_parquet_with_duckdb(parquet_path, start_time, end_time)
        
        print("\nTop 3 coordinates and their counts:")
        if not result_coordinate.empty:
            for i, row in result_coordinate.iterrows():
                print(f"{i+1}. {row['coordinate']}: {row['coordinate_count']} hits")

            top_3_coords = [row['coordinate'] for _, row in result_coordinate.iterrows()]

            hourly_changes_df = process_hourly_changes_for_top_coordinates(parquet_path, start_time, end_time, top_3_coords)
            hourly_median_df = process_hourly_median_changes_for_all_coordinates(parquet_path, start_time, end_time)
            plot_hourly_changes(hourly_changes_df, hourly_median_df, top_3_coords)

            top_colors = get_top_colors_for_top_coordinates(parquet_path, start_time, end_time, top_3_coords)

            print("\nTop 2 colors for each of the top 3 coordinates:")
            for coord, colors in top_colors.items():
                print(f"\nCoordinate: {coord}")
                for color, count in colors:
                    print(f"  Color: {color} => {count} times")
        
        print("\nGenerating histogram of changes per coordinate-hour...")
        df_dist = process_distribution_changes_per_coord_per_hour(
            parquet_path,
            start_time,
            end_time
        )

        if not df_dist.empty:
            plt.figure(figsize=(8,6))
            plt.hist(df_dist['changes'], bins=50, log=True, edgecolor='black')
            plt.xlabel('Changes per Coordinate-Hour')
            plt.ylabel('Frequency (log scale)')
            plt.title('r/place Changes Distribution (Coordinate-Hour Aggregation)')
            plt.tight_layout()
            plt.show()
        else:
            print("No data found for histogram.")

    except ValueError as e:
        print(f"Error: {e}")

    end_timer = perf_counter_ns()
    exe_time = end_timer - start_timer
    print(f"\nExecution Time: {exe_time / 1_000_000:.2f} ms")

if __name__ == "__main__":
    main()
