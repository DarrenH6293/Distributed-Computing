import duckdb

def exploratory_analysis(file_path):
    query_schema = f"DESCRIBE SELECT * FROM read_parquet('{file_path}')"
    schema_info = duckdb.query(query_schema).to_df()
    print("\nDataset:\n", schema_info)

    query_total_rows = f"SELECT COUNT(*) AS total_rows FROM read_parquet('{file_path}')"
    total_rows = duckdb.query(query_total_rows).to_df()
    print("\nTotal Rows in Dataset:", total_rows.iloc[0]['total_rows'])

    query_outliers = f"""
        SELECT state, precinct, year, office, party, votes
        FROM read_parquet('{file_path}')
        WHERE votes IS NOT NULL AND TRIM(precinct) <> '' AND TRIM(party) <> '' AND office = 'President'
        ORDER BY votes DESC
        LIMIT 5
        """
    outliers = duckdb.query(query_outliers).to_df()
    print("\nHighest Votes:\n", outliers)

    query_missing_values = f"""
    SELECT 
        'office' AS column_name, COUNT(*) AS missing_count FROM read_parquet('{file_path}') WHERE office IS NULL
    UNION ALL
    SELECT 
        'precinct' AS column_name, COUNT(*) AS missing_count FROM read_parquet('{file_path}') WHERE precinct IS NULL
    UNION ALL
    SELECT 
        'party' AS column_name, COUNT(*) AS missing_count FROM read_parquet('{file_path}') WHERE party IS NULL
    UNION ALL
    SELECT 
        'votes' AS column_name, COUNT(*) AS missing_count FROM read_parquet('{file_path}') WHERE votes IS NULL
    UNION ALL
    SELECT 
        'state' AS column_name, COUNT(*) AS missing_count FROM read_parquet('{file_path}') WHERE state IS NULL
    UNION ALL
    SELECT 
        'year' AS column_name, COUNT(*) AS missing_count FROM read_parquet('{file_path}') WHERE year IS NULL
    """
    missing_values = duckdb.query(query_missing_values).to_df()
    print("\nMissing Values:\n", missing_values)


    query_duplicates = f"""
    SELECT COUNT(*) AS duplicate_count
    FROM (
        SELECT *, COUNT(*) OVER (PARTITION BY office, precinct, party, votes, state, year) AS row_count
        FROM read_parquet('{file_path}')
    ) WHERE row_count > 1
    """
    duplicates = duckdb.query(query_duplicates).to_df()
    print("\nDuplicates Found:", duplicates.iloc[0]['duplicate_count'])

    query_summary = f"""
    SELECT 
        MIN(votes) AS min_votes,
        MAX(votes) AS max_votes,
        AVG(votes) AS avg_votes
    FROM read_parquet('{file_path}')
    WHERE votes IS NOT NULL
    """
    summary_stats = duckdb.query(query_summary).to_df()
    print("\nVote Statistics:\n", summary_stats)

    query_party_distribution = f"""
    SELECT party, COUNT(*) AS total_votes
    FROM read_parquet('{file_path}')
    WHERE party IS NOT NULL
    GROUP BY party
    ORDER BY total_votes DESC
    """
    party_distribution = duckdb.query(query_party_distribution).to_df()
    print("\nParty Distribution:\n", party_distribution)

    query_state_turnout = f"""
    SELECT state, SUM(votes) AS total_votes
    FROM read_parquet('{file_path}')
    WHERE votes IS NOT NULL
    GROUP BY state
    ORDER BY total_votes DESC
    LIMIT 5
    """
    state_turnout = duckdb.query(query_state_turnout).to_df()
    print("\nTotal Votes by State:\n", state_turnout)

parquet_file = "election_results_cleaned.parquet"
exploratory_analysis(parquet_file)
