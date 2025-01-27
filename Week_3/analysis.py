import duckdb
from datetime import datetime
from time import perf_counter_ns

def users_color_rank(parquet_path, start_time, end_time):
    query = f"""
        SELECT pixel_color, COUNT(DISTINCT user_id) AS distinct_users
        FROM parquet_scan('{parquet_path}')
        WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'
        GROUP BY pixel_color
        ORDER BY distinct_users DESC
    """
    result = duckdb.query(query).to_df()
    return result

def find_avg_sess_len(parquet_path, start_time, end_time):
    query = f"""
        WITH user_sessions AS (
            SELECT
                user_id,
                CAST(timestamp AS TIMESTAMP) AS timestamp,
                CASE
                    WHEN LAG(CAST(timestamp AS TIMESTAMP)) OVER (PARTITION BY user_id ORDER BY CAST(timestamp AS TIMESTAMP)) IS NULL THEN 0
                    WHEN CAST(timestamp AS TIMESTAMP) - LAG(CAST(timestamp AS TIMESTAMP)) OVER (PARTITION BY user_id ORDER BY CAST(timestamp AS TIMESTAMP)) > INTERVAL '15 minutes' THEN 1
                    ELSE 0
                END AS is_new_session
            FROM parquet_scan('{parquet_path}')
            WHERE CAST(timestamp AS TIMESTAMP) BETWEEN '{start_time}' AND '{end_time}'
        ),
        sessions AS (
            SELECT
                user_id,
                SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id,
                timestamp
            FROM user_sessions
        ),
        session_durations AS (
            SELECT
                user_id,
                session_id,
                MAX(timestamp) - MIN(timestamp) AS session_duration
            FROM sessions
            GROUP BY user_id, session_id
            HAVING COUNT(*) > 1
        )
        SELECT AVG(EXTRACT(epoch FROM session_duration)) AS avg_session_length
        FROM session_durations
    """
    result = duckdb.query(query).fetchone()[0]
    return result



def find_pxl_percentiles(parquet_path, start_time, end_time):
    query = f"""
        SELECT
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pixel_count) AS p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pixel_count) AS p75,
            PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY pixel_count) AS p90,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pixel_count) AS p99
        FROM (
            SELECT user_id, COUNT(*) AS pixel_count
            FROM parquet_scan('{parquet_path}')
            WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'
            GROUP BY user_id
        )
    """
    result = duckdb.query(query).to_df()
    return result

def find_frst_time_usrs(parquet_path, start_time, end_time):
    query = f"""
        WITH filtered_data AS (
            SELECT
                user_id,
                CAST(timestamp AS TIMESTAMP) AS timestamp
            FROM parquet_scan('{parquet_path}')
            WHERE CAST(timestamp AS TIMESTAMP) <= '{end_time}'
        ),
        first_time_users AS (
            SELECT
                user_id,
                MIN(timestamp) AS first_pixel_time
            FROM filtered_data
            GROUP BY user_id
        )
        SELECT COUNT(*) AS first_time_user_count
        FROM first_time_users
        WHERE first_pixel_time BETWEEN '{start_time}' AND '{end_time}'
    """
    result = duckdb.query(query).fetchone()[0]
    return result


def main():
    start_timer = perf_counter_ns()
    start_hour = input("Start time (YYYY-MM-DD HH): ")
    end_hour = input("End time (YYYY-MM-DD HH): ")

    try:
        start_time = datetime.strptime(start_hour, "%Y-%m-%d %H")
        end_time = datetime.strptime(end_hour, "%Y-%m-%d %H")

        if end_time <= start_time:
            raise ValueError("End time must be after start time.")

        parquet_path = 'output_file.parquet'


        colors_ranking = users_color_rank(parquet_path, start_time, end_time)
        print("Colors Ranking by Distinct Users:")
        print(colors_ranking)

        avg_session_length = find_avg_sess_len(parquet_path, start_time, end_time)
        print(f"\nAverage Session Length: {avg_session_length} seconds")

        percentiles = find_pxl_percentiles(parquet_path, start_time, end_time)
        print("\nPixel Placement Percentiles:")
        print(percentiles)

        first_time_users = find_frst_time_usrs(parquet_path, start_time, end_time)
        print(f"\nFirst-Time Users: {first_time_users}")

        end_timer = perf_counter_ns()
        exe_time = end_timer - start_timer
        print(f"Execution Time: {exe_time / 1_000_000} ms")

    except ValueError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
