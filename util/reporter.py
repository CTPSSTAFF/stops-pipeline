import pandas as pd
from pathlib import Path
import sys

def _get_table_config_by_id(table_id, data_tables_config):
    """
    Finds the configuration for a specific table ID from the list of table configs.
    
    Args:
        table_id (str): The ID of the table to find (e.g., "10.01").
        data_tables_config (list): The list of table configuration dictionaries.

    Returns:
        dict: The matching table configuration dictionary, or None if not found.
    """
    for table_config in data_tables_config:
        if table_config.get("table_id") == table_id:
            return table_config
    return None

def run_reporting(config_manager):
    """
    Generates summary reports by querying and combining data from multiple CSV files.

    This function reads the reporting configuration to understand which reports to
    create. For each report, it iterates through specified data aliases (e.g., "2024",
    "2045"), reads the corresponding input CSV, filters it based on query
    parameters, and combines the results into a single output CSV file.
    """
    print("\n--- 📊 Starting Report Generation ---")

    # Get necessary configuration dictionaries from the manager
    reporting_config = config_manager.reporting_config
    if not reporting_config:
        print("INFO: Reporting config not found. Skipping report generation.")
        return

    extraction_config = config_manager.extraction_config
    if not extraction_config:
        print("FATAL: Extraction config not found, cannot locate input CSVs for reporting.")
        sys.exit(1)

    data_tables_config = extraction_config.get("tables_to_extract", [])
    if not data_tables_config:
        print("FATAL: Data tables configuration not found in extraction_config. Cannot proceed.")
        sys.exit(1)
        
    # Get base paths and list of aliases to include
    csv_input_base_path = Path(reporting_config.get("csv_input_folderpath", "extracted_csv_tables"))
    report_output_base_path = Path(reporting_config.get("report_output_folderpath", "summary_reports"))
    aliases_to_process = reporting_config.get("aliases_to_include_in_report", [])
    reports_to_generate = reporting_config.get("data_query_reports", [])

    if not aliases_to_process:
        print("WARNING: 'aliases_to_include_in_report' is empty. No reports will be generated.")
        return
        
    if not reports_to_generate:
        print("INFO: 'data_query_reports' is empty or not defined. Nothing to report.")
        return

    # Process each report defined in the configuration
    for report_query in reports_to_generate:
        table_to_compare = report_query.get("table_to_compare")
        output_filename = report_query.get("output_filename")
        columns_to_query = report_query.get("column_to_query", [])
        values_to_find = report_query.get("values", [])

        if not all([table_to_compare, output_filename, columns_to_query, values_to_find]):
            print("WARNING: Skipping a report due to missing configuration keys (e.g., table, output, column, or values).")
            continue
            
        print(f"\nProcessing report for table '{table_to_compare}' -> '{output_filename}'")
        
        # Find the configuration details for the table being processed
        table_config = _get_table_config_by_id(table_to_compare, data_tables_config)
        if not table_config:
            print(f"  - ❌ ERROR: No configuration found for table_id '{table_to_compare}' in config_data_tables.json. Skipping.")
            continue
            
        # Get the subfolder and filename template for the input CSVs
        csv_subfolder = table_config.get("output_subfolder")
        csv_filename_template = table_config.get("output_filename_template")
        
        if not all([csv_subfolder, csv_filename_template]):
             print(f"  - ❌ ERROR: Table config for '{table_to_compare}' is missing 'output_subfolder' or 'output_filename_template'. Skipping.")
             continue

        all_data_for_report = []

        # Iterate through each specified alias (e.g., "2024", "2045", "example")
        for alias in aliases_to_process:
            filename = csv_filename_template.format(alias=alias)
            input_csv_path = csv_input_base_path / csv_subfolder / filename
            
            print(f"  - Reading and filtering '{input_csv_path}'...")

            if not input_csv_path.is_file():
                print("    - WARNING: File not found. Skipping this alias for the report.")
                continue

            try:
                # Load the source data
                source_df = pd.read_csv(input_csv_path)
                
                # Apply the filter based on the query configuration
                filtered_df = source_df.copy()
                final_mask = pd.Series(True, index=filtered_df.index)
                
                for i, column in enumerate(columns_to_query):
                    query_values = values_to_find[i]
                    if column not in filtered_df.columns:
                        print(f"    - WARNING: Column '{column}' not found in '{input_csv_path}'. Skipping this filter condition.")
                        continue
                    
                    # Create a mask for the current condition using 'startswith' for flexibility
                    # e.g., 'Green' will match 'Green-B', 'Green-C', etc.
                    condition_mask = filtered_df[column].astype(str).str.startswith(tuple(query_values), na=False)
                    final_mask &= condition_mask
                    
                filtered_df = filtered_df[final_mask]

                if filtered_df.empty:
                    print("    - INFO: No rows matched the query criteria in this file.")
                    continue
                
                # Add an 'Alias' column to identify the data source in the final report
                filtered_df.insert(0, 'Alias', alias)
                
                all_data_for_report.append(filtered_df)

            except Exception as e:
                print(f"    - ❌ ERROR: Failed to process file '{input_csv_path}'. Reason: {e}")

        # Combine data from all aliases into one report file
        if all_data_for_report:
            final_report_df = pd.concat(all_data_for_report, ignore_index=True)
            
            # Construct the full output path for the report
            output_path = report_output_base_path / output_filename
            
            # Ensure the parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save the combined DataFrame to a new CSV
            final_report_df.to_csv(output_path, index=False)
            print(f"  - ✅ Report successfully generated at '{output_path}'")
        else:
            print(f"  - INFO: No data found across all specified aliases for this report. No output file was created.")

    print("\n--- ✅ Report Generation Complete ---")