from time import perf_counter_ns
import os
import gzip
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

def convert_gzip_to_parquet(gzip_path, parquet_path, batch_size=512 * 1024**2):
    with gzip.open(gzip_path, mode='rb') as file:
        csv_reader = pv.open_csv(
            file,
            parse_options=pv.ParseOptions(delimiter=","),
            convert_options=pv.ConvertOptions(include_columns=["timestamp", "pixel_color", "user_id"]),
            read_options=pv.ReadOptions(block_size=batch_size) 
        )

        for i, batch in enumerate(csv_reader):
            table = pa.Table.from_batches([batch])

            batch_file = f"{parquet_path}_part_{i}.parquet"
            pq.write_table(table, batch_file, compression="snappy")

        batch_files = [f"{parquet_path}_part_{i}.parquet" for i in range(i + 1)]
        final_table = pa.concat_tables([pq.read_table(f) for f in batch_files])
        pq.write_table(final_table, parquet_path, compression="snappy")


def update_user_id(parquet_path, output_path):
    parquet_file = pq.ParquetFile(parquet_path)

    user_mapping = {}
    user_counter = 0

    updated_row_groups = []

    for i in range(parquet_file.num_row_groups):
        row_group = parquet_file.read_row_group(i)

        user_ids = row_group.column('user_id').to_pylist()

        new_user_ids = []
        for user_id in user_ids:
            if user_id not in user_mapping:
                user_mapping[user_id] = user_counter
                user_counter += 1
            new_user_ids.append(user_mapping[user_id])


        new_user_id_column = pa.array(new_user_ids)

        updated_row_group = row_group.remove_column(row_group.schema.get_field_index('user_id'))
        updated_row_group = updated_row_group.append_column('user_id', new_user_id_column)


        updated_row_groups.append(updated_row_group)

    final_table = pa.concat_tables(updated_row_groups)

    pq.write_table(final_table, output_path, compression='snappy')


def main():
    start_timer = perf_counter_ns()

    try:
        gzip_path = '2022_place_canvas_history.csv.gzip'
        parquet_path = '2022_place_canvas_history_userid.parquet'

        if not os.path.exists(parquet_path):
            print("Converting gzip to parquet...")
            convert_gzip_to_parquet(gzip_path, parquet_path)
            update_user_id('2022_place_canvas_history_userid.parquet', 'output_file.parquet')
        else:
            print("Parquet file already exists. Skipping conversion.")

    except ValueError as e:
        print(f"Error: {e}")

    end_timer = perf_counter_ns()
    exe_time = end_timer - start_timer
    print(f"Execution Time: {exe_time / 1_000_000} ms")

if __name__ == "__main__":
    main()