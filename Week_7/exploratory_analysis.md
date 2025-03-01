# Data Extraction
The structure and content of the election data files vary significantly between states, and even within the same state, formatting differences exist across different years due to factors such as legal changes. In this dataset, some states had vote totals at both the county and statewide levels, while others didn't have specific vote counts entirely. Additionally, candidate and party names are not standardized across states, particularly for minor-party candidates. These inconsistencies made simple extraction from the ZIP file difficult, requiring me to selectively exclude files that lacked essential information, such as vote counts. To ensure accurate extraction, I had to manually look at each state's folder to understand the file naming conventions and data structure before modifying my script accordingly.

# Data Cleaning
Once I extracted the necessary data, I had to create a uniform dataset for analysis. Some states provided a single file containing all counties, while others split precinct data into multiple files. To standardize the data, I first merged these precinct-level files. Then I decided which columns would be needed and which would not be needed to answer my question. Finally, I converted the processed data into a Parquet format for more efficient analysis. I continued to add more to my script to make it as good as possible after performing some exploratory data analysis to correct some issues that came up that I didn't see before.

# Exploratory Data Analysis 
I used DuckDB to explore and validate the dataset.

## Schema Validation
- Ensure that all necessary fields were present.
- Expected columns were properly formatted.

## Row Count Validation
- Helped check that all extracted data was correctly written to the Parquet file.
- No major discrepancies existed in the number of records.
- Removed duplicate records.

## Outlier Detection
- See potential data anomalies by getting the highest vote counts.
- Helped identify any unreasonable vote counts that could show incorrect aggregation or errors in extraction. 
- If outliers were present, I cross-checked them with original sources or processed them for further investigation.

## Missing Value Analysis
- Missing values could significantly impact analysis. 
- Gathered the number of null or empty values across the columns.
- Check whether exclusion or data correction was necessary.

## Aggregation Check
- Calculate the minimum, maximum, and average vote counts to highlight potential data entry errors.
- Find party distribution for inconsistencies in party name.