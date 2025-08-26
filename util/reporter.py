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

    source_dataframes = {} # Cache for loaded data

    for report in reports_to_generate:
        output_filename = report["output_filename"]
        sql_string = " ".join(line.strip() for line in report["sql_query"] if line.strip())
        
        print(f"\nProcessing report: '{output_filename}'")
        print(f"  SQL: {sql_string}")

        # MODIFICATION: Find all table specifiers (e.g., [11.01]) to support UNIONs.
        table_specifiers = re.findall(r"\[\d+\.\d+\]", sql_string)
        
        if not table_specifiers:
            print(f"  ❌ ERROR: Could not parse any table ID like '[X.XX]' from query: {sql_string}")
            continue
        
        # Load all required DataFrames for the current query
        query_to_run = sql_string
        all_tables_found = True
        for specifier in set(table_specifiers): # Use set to avoid redundant loads
            table_id = specifier.strip('[]').replace('.', '_')
            
            # Load from files if not already in our cache
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
                    print(f"  ❌ ERROR: No source data found for table '{table_id}'.")
                    all_tables_found = False
                    break 
                
                source_dataframes[table_id] = pd.concat(all_alias_dfs, ignore_index=True)

            if not all_tables_found:
                break

            # Make the DataFrame available in the global scope for sqldf
            dataframe_variable_name = f"Table_{table_id}"
            globals()[dataframe_variable_name] = source_dataframes[table_id]
            
            # Replace the specifier (e.g., [11.01]) with the variable name (e.g., Table_11_01)
            query_to_run = query_to_run.replace(specifier, dataframe_variable_name)

        if not all_tables_found:
            print(f"  Skipping report '{output_filename}' due to missing data.")
            continue

        # This part remains the same
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