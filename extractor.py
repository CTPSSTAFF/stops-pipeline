import pandas as pd
import io
import re
import os
from pathlib import Path

class StopsPRNExtractor:
    """
    A class to systematically extract tables from STOPS program .PRN output files.
    This class contains multiple static methods, each designed to parse a specific
    table format from the text-based .PRN files.
    """
    # NOTE: All your original extraction methods are included here.
    @staticmethod
    def _extract_metadata_from_prn(lines, start_index):
        metadata = {}
        for meta_line_offset in range(1, 10):
            meta_line_num = start_index - meta_line_offset
            if meta_line_num >= 0:
                meta_line = lines[meta_line_num].strip()
                if "Program STOPS" in meta_line:
                    # ... (rest of your metadata extraction logic) ...
                    pass
        return metadata

    @staticmethod
    def _extract_table_9_01_from_prn(file_path, table_id="9.01"):
        # ... (full method from your original code) ...
        # This is a placeholder for your complete function.
        # For brevity, I am not repeating the entire large function here,
        # but you should paste your complete _extract_table_9_01_from_prn here.
        df = pd.DataFrame() # Replace with your actual extraction logic
        metadata = {}
        return df, metadata

    @staticmethod
    def _extract_table_10_01_from_prn(file_path, table_id="10.01"):
        # ... (full method from your original code) ...
        df = pd.DataFrame() # Replace with your actual extraction logic
        metadata = {}
        return df, metadata

    @staticmethod
    def _extract_table_10_02_from_prn(file_path, table_id="10.02"):
        # ... (full method from your original code) ...
        df = pd.DataFrame() # Replace with your actual extraction logic
        metadata = {}
        return df, metadata

    @staticmethod
    def _extract_table_11_from_prn(file_path, table_id):
        # ... (full method from your original code) ...
        df = pd.DataFrame() # Replace with your actual extraction logic
        metadata = {}
        return df, metadata


def get_extraction_method(table_id_str):
    """Dynamically gets the correct extraction method based on the table ID."""
    if table_id_str.startswith("11"):
        # Special case for all 11.xx tables using the same function
        return StopsPRNExtractor._extract_table_11_from_prn

    method_name = f"_extract_table_{table_id_str.replace('.', '_')}_from_prn"
    return getattr(StopsPRNExtractor, method_name, None)


def run_extraction(config):
    """Main function to run the data extraction process from config."""
    print("--- 🎬 Starting Data Extraction ---")
    base_prn_dir = Path(config["prn_files_folderpath"])
    csv_output_dir = Path(config["csv_output_folderpath"])
    extraction_config = config["prn_files_data_extraction_config"]
    files_to_process = extraction_config["files_to_process"]
    tables_to_extract = extraction_config["tables_to_extract"]

    for file_info in files_to_process:
        alias = file_info["alias"]
        filename = file_info["filename"]
        
        if file_info.get("is_full_folderpath", False):
            file_path = Path(file_info["folderpath"]) / filename
        else:
            file_path = base_prn_dir / filename

        if not file_path.exists():
            print(f"❗️ WARNING: File not found for alias '{alias}': {file_path}. Skipping.")
            continue
            
        print(f"\nProcessing File: '{filename}' (Alias: '{alias}')")

        for table_id in tables_to_extract:
            print(f"  -> Attempting to extract Table {table_id}...")
            extraction_func = get_extraction_method(table_id)
            if not extraction_func:
                print(f"     - No extraction method found for Table {table_id}. Skipping.")
                continue

            df, metadata = extraction_func(str(file_path), table_id)
            
            if df.empty:
                print(f"     - No data found for Table {table_id} in this file.")
                continue

            table_folder_name = f"Table_{table_id.replace('.', '_')}"
            table_output_dir = csv_output_dir / table_folder_name
            table_output_dir.mkdir(parents=True, exist_ok=True)
            output_filename = f"[{alias}]__Table_{table_id.replace('.', '_')}.csv"
            output_path = table_output_dir / output_filename

            df.to_csv(output_path, index=False)
            print(f"     ✅ Successfully saved to: {output_path}")

    print("\n--- ✅ Data Extraction Complete ---")