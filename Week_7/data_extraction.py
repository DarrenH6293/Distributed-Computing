import os
import zipfile
import shutil
import re

def extract_zip(zip_path, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as z:
        all_files = z.namelist()

        for file in all_files:
            parts = file.split('/')

            if len(parts) >= 3 and "__general__" in parts[-1] and "__precinct.csv" in parts[-1] and "openelections-data" in parts[-3] and "special" not in file:
                state_folder = parts[-3]
                year_folder = parts[-2]
                csv_filename = parts[-1]

                state_path = os.path.join(output_folder, state_folder)
                year_path = os.path.join(state_path, year_folder)
                os.makedirs(year_path, exist_ok=True)

                if csv_filename.endswith("__general__precinct.csv"):
                    target_path = os.path.join(year_path, csv_filename)
                    with z.open(file) as src, open(target_path, 'wb') as dst:
                        shutil.copyfileobj(src, dst)
                    print(f"Extracted: {file} → {target_path}")

                elif re.search(r'__general__.+__precinct\.csv$', csv_filename):
                    target_path = os.path.join(year_path, csv_filename)
                    with z.open(file) as src, open(target_path, 'wb') as dst:
                        shutil.copyfileobj(src, dst)
                    print(f"Extracted: {file} → {target_path}")

zip_path = "archive.zip" 
output_folder = "open-elections-data-by-state-and-precinct"
extract_zip(zip_path, output_folder)
