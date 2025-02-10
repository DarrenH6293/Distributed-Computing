import duckdb
from time import perf_counter_ns

def get_total_users(file_path):
    query = f"""
        SELECT COUNT(DISTINCT user_id) AS total_users
        FROM read_parquet('{file_path}')
        WHERE user_id IS NOT NULL
    """
    result = duckdb.query(query).fetchone()
    return result[0] if result else 0


def find_most_active_users(file_path, top_percent=1):
    total_users = get_total_users(file_path)
    top_n = max(1, int(total_users * (top_percent / 100)))

    query_users = f"""
        SELECT 
            user_id AS user,
            COUNT(*) AS pixel_placements
        FROM read_parquet('{file_path}')
        WHERE 
            timestamp IS NOT NULL
            AND user_id IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT {top_n}
    """
    df_users = duckdb.query(query_users).to_df()
    return df_users


def find_sus_users_by_time_intervals(file_path, top_users):
    top_users_str = ",".join([f"'{user}'" for user in top_users])

    query = f"""
        WITH user_intervals AS (
            SELECT 
                user_id, 
                CAST(timestamp AS TIMESTAMP) AS timestamp,
                LAG(CAST(timestamp AS TIMESTAMP)) OVER (PARTITION BY user_id ORDER BY CAST(timestamp AS TIMESTAMP)) AS prev_timestamp
            FROM read_parquet('{file_path}')
            WHERE 
                timestamp IS NOT NULL 
                AND user_id IS NOT NULL
                AND user_id IN ({top_users_str})
        )
        SELECT 
            user_id AS user, 
            AVG(EXTRACT(EPOCH FROM timestamp) - EXTRACT(EPOCH FROM prev_timestamp)) AS avg_interval
        FROM user_intervals
        WHERE prev_timestamp IS NOT NULL
        GROUP BY 1
        HAVING avg_interval < 420
        ORDER BY 2
    """
    df = duckdb.query(query).to_df()
    return df

def find_most_painted_coordinates_by_bots(file_path, suspicious_users):
    bot_users_str = ",".join([f"'{user}'" for user in suspicious_users])

    query = f"""
        SELECT 
            coordinate, 
            COUNT(*) AS placements
        FROM read_parquet('{file_path}')
        WHERE 
            timestamp IS NOT NULL 
            AND user_id IS NOT NULL
            AND user_id IN ({bot_users_str})
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 20
    """
    df = duckdb.query(query).to_df()
    return df


def track_hourly_changes_by_bots(file_path, suspicious_users):
    bot_users_str = ",".join([f"'{user}'" for user in suspicious_users])

    query = f"""
        SELECT 
            DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour,
            COUNT(*) AS bot_changes
        FROM read_parquet('{file_path}')
        WHERE 
            timestamp IS NOT NULL
            AND user_id IN ({bot_users_str})
            AND (
                (CAST(SPLIT_PART(coordinate, ',', 1) AS INTEGER) BETWEEN 892 AND 961 
                 AND CAST(SPLIT_PART(coordinate, ',', 2) AS INTEGER) BETWEEN 1830 AND 1886)
                OR
                (CAST(SPLIT_PART(coordinate, ',', 1) AS INTEGER) BETWEEN 1611 AND 1691 
                 AND CAST(SPLIT_PART(coordinate, ',', 2) AS INTEGER) BETWEEN 212 AND 277)
            )
        GROUP BY 1
        ORDER BY 2 DESC
    """
    
    df = duckdb.query(query).to_df()
    return df


def main():
    start_timer = perf_counter_ns()
    file_path = "merged_canvas_history.parquet"

    top_users_df = find_most_active_users(file_path, top_percent=1)
    print(f"Total users analyzed: {len(top_users_df)}")

    top_users = top_users_df["user"].tolist()

    sus_users_df = find_sus_users_by_time_intervals(file_path, top_users)
    print(f"Amount of suspected bots: {len(sus_users_df)}")

    suspicious_users = sus_users_df["user"].tolist()

    bot_coordinates_df = find_most_painted_coordinates_by_bots(file_path, suspicious_users)
    print("Most painted coordinates by suspected bots:")
    print(bot_coordinates_df)

    bot_hourly_changes_df = track_hourly_changes_by_bots(file_path, suspicious_users)
    print(bot_hourly_changes_df)
    end_timer = perf_counter_ns()
    exe_time = end_timer - start_timer
    print(f"Execution Time: {exe_time / 1_000_000} ms")

if __name__ == "__main__":
    main()
