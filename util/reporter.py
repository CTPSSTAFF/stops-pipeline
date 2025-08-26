# util/reporter.py

import pandas as pd
import re
from pathlib import Path
from pandasql import sqldf

def run_reporting(config_manager):
    """
    Generates filtered CSV reports using pandasql to execute queries.
    """
    print("\n--- 🚀 Starting Report Generation ---")
    
    reporting_config = config_manager.reporting_config
    base_input_path = Path(reporting_config["csv_input_folderpath"])
    output_path = Path(reporting_config["report_output_folderpath"])
    aliases = reporting_config["aliases_to_include_in_report"]
    reports_to_generate = reporting_config["data_query_reports"]

    source_dataframes = {}

    for report in reports_to_generate:
        output_filename = report["output_filename"]
        sql_string = " ".join(line.strip() for line in report["sql_query"] if line.strip())
        
        print(f"\nProcessing report: '{output_filename}'")
        print(f"  SQL: {sql_string}")

        match = re.search(r"FROM\s+(\[.*?\])", sql_string, re.IGNORECASE)
        
        if not match:
            print(f"  ❌ ERROR: Could not parse table ID from query: {sql_string}")
            continue
            
        original_table_specifier = match.group(1)
        table_id = original_table_specifier.strip('[]').replace('.', '_')

        if table_id not in source_dataframes:
            all_alias_dfs = []
            for alias in aliases:
                filename = f"[{alias}]__{table_id}.csv"
                table_folder = f"Table_{table_id}"
                file_path = base_input_path / table_folder / filename

                if file_path.exists():
                    df = pd.read_csv(file_path)
                    df.insert(0, 'Alias', alias)
                    all_alias_dfs.append(df)
                else:
                    print(f"  ⚠️ WARNING: Source file not found at '{file_path}'")
            
            if not all_alias_dfs:
                print(f"  ❌ ERROR: No source data for table '{table_id}'. Skipping report.")
                continue
            
            source_dataframes[table_id] = pd.concat(all_alias_dfs, ignore_index=True)

        dataframe_variable_name = f"Table_{table_id}"
        globals()[dataframe_variable_name] = source_dataframes[table_id]

        query_to_run = sql_string.replace(original_table_specifier, dataframe_variable_name)

        query_to_run = re.sub(r'(\sIN\s*)\[(.*?)\]', r'\1(\2)', query_to_run, flags=re.IGNORECASE)

        try:
            filtered_df = sqldf(query_to_run, globals())
            
            output_filepath = output_path / output_filename
            filtered_df.to_csv(output_filepath, index=False)
            print(f"  ✅ Successfully created report: '{output_filepath}' with {len(filtered_df)} rows.")

        except Exception as e:
            print(f"  ❌ ERROR: SQL execution failed for '{output_filename}'. Error: {e}")
            continue

    print("\n--- ✅ Report Generation Finished ---")