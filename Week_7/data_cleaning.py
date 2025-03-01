import os
import glob
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import duckdb

def convert_to_parquet(input_folder, output_file, batch_size=512 * 1024**2):
    all_files = glob.glob(os.path.join(input_folder, "**", "*.csv"), recursive=True)
    print(all_files)
    temp_parquet_files = []

    required_columns = ["office", "precinct", "party", "votes"]
    schema = pa.schema([
        ("office", pa.string()),
        ("precinct", pa.string()),
        ("party", pa.string()),
        ("votes", pa.int64()), 
        ("state", pa.string()),
        ("year", pa.string())
    ])

    for file_index, file_path in enumerate(all_files):
        try:
            parts = file_path.replace("\\", "/").split("/")
            state = parts[-3]
            year = parts[-2] 


            with open(file_path, mode='rb') as file:
                csv_reader = pv.open_csv(
                    file,
                    parse_options=pv.ParseOptions(delimiter=","),
                    read_options=pv.ReadOptions(block_size=batch_size)
                )

                for batch_index, batch in enumerate(csv_reader):
                    batch_columns = batch.schema.names

                    batch_dict = {name: batch.column(name) for name in batch_columns}

                    for col in required_columns:
                        if col not in batch_columns:
                            if col == "votes":
                                batch_dict[col] = pa.array([0] * len(batch), type=pa.int64())
                            else:
                                batch_dict[col] = pa.array([""] * len(batch), type=pa.string())

                    batch_dict["state"] = pa.array([state] * len(batch), type=pa.string())
                    batch_dict["year"] = pa.array([year] * len(batch), type=pa.string())

                    table = pa.Table.from_pydict(batch_dict, schema=schema)

                    temp_file = f"{output_file}_temp_{file_index}_{batch_index}.parquet"
                    pq.write_table(table, temp_file, compression="snappy")
                    temp_parquet_files.append(temp_file)

            print(f"Processed: {file_path}")

        except Exception as e:
            print(f"Skipping {file_path}. Error: {e}")

    if temp_parquet_files:
        combined_table = pa.concat_tables([pq.read_table(f) for f in temp_parquet_files], promote_options="string")
        pq.write_table(combined_table, output_file, compression="snappy")

        for temp_file in temp_parquet_files:
            os.remove(temp_file)

        print(f"Saved to {output_file}")
    else:
        print("No files found.")

def clean_final_parquet(final_parquet_file):
    conn = duckdb.connect()
    conn.execute(f"""
    CREATE TABLE election_data AS 
    SELECT DISTINCT * FROM read_parquet('{final_parquet_file}')
    WHERE votes >= 0
    AND LOWER(TRIM(precinct)) NOT LIKE '%total votes%'
    AND LOWER(TRIM(precinct)) NOT LIKE '%total%'
    """)

    cleaned_file = final_parquet_file.replace(".parquet", "_cleaned.parquet")
    conn.execute(f"COPY election_data TO '{cleaned_file}' (FORMAT 'parquet', COMPRESSION 'SNAPPY')")

    conn.execute("DROP TABLE election_data")
    conn.close()

    print(f"Saved to {cleaned_file}")

input_folder = "open-elections-data-by-state-and-precinct"
output_file = "election_results.parquet"
convert_to_parquet(input_folder, output_file)
clean_final_parquet(output_file)
