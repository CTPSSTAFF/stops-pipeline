import json
import sys
from pathlib import Path
import shutil
from extractor import run_extraction
from reporter import run_reporting

# Use Path for better cross-platform compatibility
CONFIG_FILE = Path("[UPDATE ME] STOPS_Report Config.json")

def initialize_folders(config):
    """
    Deletes and recreates specified output folders to ensure a clean run.
    """
    print("Initializing output folders...")
    csv_output_folder = Path(config.get("csv_output_folderpath", "extracted_csv_tables"))
    report_output_folder = Path(config.get("report_output_folderpath", "summary_report"))
    
    folders_to_clean = [csv_output_folder, report_output_folder]
    
    for folder in folders_to_clean:
        if folder.exists() and folder.is_dir():
            print(f"  - Deleting existing folder: '{folder}'")
            shutil.rmtree(folder)
        
        print(f"  - Creating new folder: '{folder}'")
        folder.mkdir(parents=True, exist_ok=True)
    print("Folder initialization complete.")

def main():
    """
    Main pipeline wrapper. Reads config and runs steps based on flags.
    """
    print("Starting the data processing pipeline...")
    print(f"Running {Path(__file__).name}")
    print("--------------------------------------------------")

    if not CONFIG_FILE.is_file():
        print(f"❌ FATAL: Configuration file not found at '{CONFIG_FILE}'.")
        sys.exit(1)

    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
    except json.JSONDecodeError:
        print(f"❌ FATAL: '{CONFIG_FILE}' is not a valid JSON file.")
        sys.exit(1)

    run_flags = config.get("run_flags", {})

    # --- Step 0: Initialization ---
    if run_flags.get("INITIALIZE_FOLDERS", False):
        try:
            initialize_folders(config)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Initialization: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("Skipping Folder Initialization as per config.")

    print("--------------------------------------------------")
    
    # --- Step 1: Data Extraction ---
    if run_flags.get("CONDUCT_DATA_EXTRACTION", False):
        try:
            run_extraction(config)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Data Extraction: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("Skipping Data Extraction as per config.")

    # --- Step 2: Report Generation ---
    if run_flags.get("CONDUCT_REPORT_GENERATION", False):
        try:
            run_reporting(config)
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