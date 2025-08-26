import sys
from pathlib import Path
import shutil
from config_manager import ConfigManager
from extractor import run_extraction
from reporter import run_reporting

# Define paths to the primary configuration files
EXTRACTION_CONFIG_FILE = "config_data_extraction.json"
REPORTING_CONFIG_FILE = "config_data_report.json"

def initialize_folders(ext_config, rpt_config):
    """
    Deletes and recreates specified output folders to ensure a clean run.
    This is called only when a new data extraction is being performed.
    """
    print("Initializing output folders for a new extraction run...")
    
    folders_to_clean = []
    
    # Get extraction output folder path
    csv_output_folder = Path(ext_config.get("output_base_folder", "extracted_csv_tables"))
    folders_to_clean.append(csv_output_folder)

    # Get report output folder path
    report_output_folder = Path(rpt_config.get("report_output_folderpath", "summary_reports"))
    folders_to_clean.append(report_output_folder)

    for folder in folders_to_clean:
        if folder.exists() and folder.is_dir():
            print(f"  - Deleting existing folder: '{folder}'")
            shutil.rmtree(folder)
        
        print(f"  - Creating new folder: '{folder}'")
        folder.mkdir(parents=True, exist_ok=True)
    
    if folders_to_clean:
        print("Folder initialization complete.")

def main():
    """
    Main pipeline wrapper. Instantiates a ConfigManager and runs steps based on flags.
    """
    print("Starting the data processing pipeline...")
    print(f"Running {Path(__file__).name}")
    print("--------------------------------------------------")

    # Instantiate the manager and load all configurations
    config_manager = ConfigManager(
        extraction_path=EXTRACTION_CONFIG_FILE,
        reporting_path=REPORTING_CONFIG_FILE
    )
    config_manager.load_all()

    # Get the fully loaded and hydrated configuration objects
    extraction_config = config_manager.extraction_config
    reporting_config = config_manager.reporting_config

    # --- Step 0: Initialization ---
    if extraction_config and extraction_config.get("run_flags", {}).get("CONDUCT_DATA_EXTRACTION", False):
        print("\nData extraction is enabled. Initializing folders for a clean run.")
        if reporting_config:
            try:
                initialize_folders(extraction_config, reporting_config)
            except Exception as e:
                print(f"\n❌ FATAL ERROR during Initialization: {e}")
                import traceback
                traceback.print_exc()
                sys.exit(1)
        else:
            print("WARNING: Reporting config not found, cannot determine all paths. Skipping folder initialization.")
    else:
        print("\nFolder initialization is skipped when not conducting a new data extraction.")

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