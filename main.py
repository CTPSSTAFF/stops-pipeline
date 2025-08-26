import json
import sys
from pathlib import Path
import shutil
from extractor import run_extraction
from reporter import run_reporting

# UPDATED: Define paths to the two new config files
EXTRACTION_CONFIG_FILE = Path("config_extraction.json")
REPORTING_CONFIG_FILE = Path("config_reporting.json")

def initialize_folders(ext_config, rpt_config):
    """
    Deletes and recreates specified output folders to ensure a clean run.
    """
    print("Initializing output folders...")
    
    folders_to_clean = []
    
    # Get extraction output folder if initialization is enabled
    ext_run_flags = ext_config.get("run_flags", {})
    if ext_run_flags.get("INITIALIZE_FOLDERS", False):
        extraction_settings = ext_config.get("prn_files_data_extraction_config", {})
        csv_output_folder = Path(extraction_settings.get("output_base_folder", "extracted_csv_tables"))
        folders_to_clean.append(csv_output_folder)

    # Get report output folder if initialization is enabled (can be controlled by extraction flag)
    if ext_run_flags.get("INITIALIZE_FOLDERS", False):
        report_output_folder = Path(rpt_config.get("report_output_folderpath", "summary_report"))
        folders_to_clean.append(report_output_folder)

    for folder in folders_to_clean:
        if folder.exists() and folder.is_dir():
            print(f"  - Deleting existing folder: '{folder}'")
            shutil.rmtree(folder)
        
        print(f"  - Creating new folder: '{folder}'")
        folder.mkdir(parents=True, exist_ok=True)
    
    if folders_to_clean:
        print("Folder initialization complete.")

def load_config(file_path, description):
    """Helper function to load a JSON config file."""
    if not file_path.is_file():
        print(f"INFO: {description} file not found at '{file_path}'. Skipping related steps.")
        return None
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"❌ FATAL: '{file_path}' is not a valid JSON file.")
        sys.exit(1)

def main():
    """
    Main pipeline wrapper. Reads configs and runs steps based on flags.
    """
    print("Starting the data processing pipeline...")
    print(f"Running {Path(__file__).name}")
    print("--------------------------------------------------")

    # UPDATED: Load both configuration files
    extraction_config = load_config(EXTRACTION_CONFIG_FILE, "Extraction config")
    reporting_config = load_config(REPORTING_CONFIG_FILE, "Reporting config")
    
    # --- Step 0: Initialization ---
    if extraction_config and reporting_config:
        try:
            initialize_folders(extraction_config, reporting_config)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Initialization: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

    print("--------------------------------------------------")
    
    # --- Step 1: Data Extraction ---
    if extraction_config and extraction_config.get("run_flags", {}).get("CONDUCT_DATA_EXTRACTION", False):
        try:
            run_extraction(extraction_config)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Data Extraction: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("Skipping Data Extraction as per config.")

    # --- Step 2: Report Generation ---
    if reporting_config and reporting_config.get("run_flags", {}).get("CONDUCT_REPORT_GENERATION", False):
        try:
            run_reporting(reporting_config)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Report Generation: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("Skipping Report Generation as per config.")

    print("\n🎉 Pipeline finished successfully!")
    print("--------------------------------------------------")


if __name__ == "__main__":
    main()