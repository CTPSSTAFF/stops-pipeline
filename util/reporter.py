import pandas as pd
import re
from pathlib import Path

def run_reporting(config_manager):
    """
    Generates reports by executing configured SQL-like queries against CSV files.
    """
    print("\n--- 🚀 Starting Report Generation ---")

    reporting_config = config_manager.reporting_config
    if not reporting_config:
        print("INFO: Reporting config not found. Skipping.")
        return

    # Get configuration details
    input_base_path = Path(reporting_config.get("csv_input_folderpath", "extracted_csv_tables"))
    output_base_path = Path(reporting_config.get("report_output_folderpath", "reporting_data"))
    aliases = reporting_config.get("aliases_to_include_in_report", [])
    reports_to_run = reporting_config.get("data_query_reports", [])

    if not reports_to_run:
        print("INFO: No data queries found in the report config.")
        return

    for report in reports_to_run:
        output_filename = report.get("output_filename")
        query_parts = report.get("sql_query", [])

        # --- THIS IS THE CRITICAL CHANGE ---
        # Join the potentially multi-line array from JSON into a single, clean string.
        # The .strip() for each line handles leading/trailing whitespace.
        full_sql_string = " ".join(line.strip() for line in query_parts)
        # ------------------------------------

        print(f"\nProcessing report: '{output_filename}'")
        print(f"  SQL: {full_sql_string}")

        # Use regex to parse the table name and the WHERE clause from the query
        match = re.search(r"FROM\s+\[(.*?)\](?:_?(WHERE\s+.*))?", full_sql_string, re.IGNORECASE)
        if not match:
            print(f"  ❌ ERROR: Could not parse table name from query: {full_sql_string}")
            continue

        table_id = match.group(1).replace('.', '_')
        filter_condition = match.group(2)
        print(f"  -> Parsed Table: '{table_id}', Columns: ['*'], Filter: '{filter_condition}'")

        combined_df = pd.DataFrame()

        # Gather data from each alias's corresponding CSV file
        for alias in aliases:
            # Construct the path to the source CSV file
            # e.g., extracted_csv_tables/Table_10_01/[2024]__10_01.csv
            table_subfolder = f"Table_{table_id}"
            csv_filename = f"[{alias}]__{table_id}.csv"
            csv_path = input_base_path / table_subfolder / csv_filename

            if not csv_path.is_file():
                print(f"  ⚠️ WARNING: Input file not found for alias '{alias}': {csv_path}")
                continue

            try:
                df = pd.read_csv(csv_path)
                
                # Apply the filter if one exists
                if filter_condition:
                    filtered_df = df.query(filter_condition.replace("WHERE ", "", 1))
                else:
                    filtered_df = df

                # Add the 'Alias' column to track the source
                filtered_df.insert(0, 'Alias', alias)
                combined_df = pd.concat([combined_df, filtered_df], ignore_index=True)

            except Exception as e:
                print(f"  ❌ ERROR: Failed to process alias '{alias}' for table '{table_id}'. Reason: {e}")

        # Save the combined data to the final report file
        if not combined_df.empty:
            output_filepath = output_base_path / output_filename
            combined_df.to_csv(output_filepath, index=False)
            print(f"  ✅ Successfully created report: '{output_filepath}' with {len(combined_df)} rows.")
        else:
            print(f"  - No data generated for report '{output_filename}'. File not created.")

    print("\n--- ✅ Report Generation Finished ---")