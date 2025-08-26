# util/reporter.py

import pandas as pd
import re
from pathlib import Path
import ast # Import the Abstract Syntax Tree module

def _parse_simple_sql(sql_lines):
    """
    Parses the simple SQL-like query from the config file.
    Extracts table ID and the WHERE clause.
    """
    full_query = " ".join(line.strip() for line in sql_lines if line.strip()).strip()

    table_match = re.search(r"FROM\s+\[(.*?)\]", full_query, re.IGNORECASE)
    if not table_match:
        return None, None
    table_id = table_match.group(1).replace('.', '_')

    where_match = re.search(r"WHERE\s+(.*)", full_query, re.IGNORECASE)
    where_clause = where_match.group(1).strip() if where_match else None
    
    print(f"  -> Parsed Table: '{table_id}', Filter: '{where_clause}'")
    return table_id, where_clause


def run_reporting(config_manager):
    """
    Generates filtered CSV reports based on data_query_reports config.
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
        sql_query = report["sql_query"]
        
        print(f"\nProcessing report: '{output_filename}'")
        print(f"  SQL: {' '.join(sql_query)}")

        table_id, where_clause = _parse_simple_sql(sql_query)

        if not table_id:
            print(f"  ❌ ERROR: Could not parse table ID from query.")
            continue

        if table_id not in source_dataframes:
            all_alias_dfs = []
            for alias in aliases:
                filename_template = f"[{alias}]__{table_id}.csv"
                table_folder = f"Table_{table_id}"
                file_path = base_input_path / table_folder / filename_template

                if file_path.exists():
                    df = pd.read_csv(file_path)
                    df.insert(0, 'Alias', alias)
                    all_alias_dfs.append(df)
                else:
                    print(f"  ⚠️ WARNING: Source file not found at '{file_path}'")
            
            if not all_alias_dfs:
                print(f"  ❌ ERROR: No source data found for table '{table_id}'. Skipping report.")
                continue
            
            source_dataframes[table_id] = pd.concat(all_alias_dfs, ignore_index=True)

        full_df = source_dataframes[table_id]
        
        # Apply the filter (WHERE clause)
        if where_clause:
            try:
                # =================== FIX STARTS HERE ===================
                # The .query() engine struggles with list literals inside the string.
                # We will detect 'IN' clauses and handle them with the more robust .isin() method.
                if ' in ' in where_clause.lower():
                    # Split clause into column name and the list part
                    # e.g., "Route_ID IN ['val1', 'val2']"
                    col_name, values_str = re.split(r'\s+IN\s+', where_clause, maxsplit=1, flags=re.IGNORECASE)
                    col_name = col_name.strip()
                    
                    # Safely evaluate the string representation of the list into a real Python list
                    values_list = ast.literal_eval(values_str.strip())
                    
                    # Use the robust pandas isin() method for filtering
                    filtered_df = full_df[full_df[col_name].isin(values_list)]
                else:
                    # For simple queries (like ==, >, <), .query() works fine.
                    filtered_df = full_df.query(where_clause)
                # =================== FIX ENDS HERE =====================

            except Exception as e:
                print(f"  ❌ ERROR: Could not apply filter to DataFrame. Error: {e}")
                continue
        else:
            filtered_df = full_df

        output_filepath = output_path / output_filename
        filtered_df.to_csv(output_filepath, index=False)
        print(f"  ✅ Successfully created report: '{output_filepath}' with {len(filtered_df)} rows.")

    print("\n--- ✅ Report Generation Finished ---")