import sys
from pathlib import Path
import shutil
from configurations.config_manager import ConfigManager
from util.extractor import run_extraction
from util.reporter import run_reporting


def clear_and_create_folder(folder_path: Path):
    """
    Deletes a folder if it exists, then recreates it to ensure a clean state.
    """
    print(f"Initializing folder: '{folder_path}'")
    if folder_path.exists() and folder_path.is_dir():
        print(f"  - Deleting existing folder...")
        shutil.rmtree(folder_path)
    
    print(f"  - Creating new folder...")
    folder_path.mkdir(parents=True, exist_ok=True)
    print("  - Folder ready.")


def main():
    """
    Main pipeline wrapper. Instantiates a ConfigManager and runs steps based on flags.
    """
    print("Starting the data processing pipeline...")
    print(f"Running {Path(__file__).name}")
    print("--------------------------------------------------")

    # Instantiate the manager and load all configurations
    config_manager = ConfigManager()
    config_manager.load_all()

    # Get the fully loaded configuration objects
    extraction_config = config_manager.extraction_config
    reporting_config = config_manager.reporting_config

    print("--------------------------------------------------")
    
    # --- Step 1: Data Extraction ---
    if extraction_config and extraction_config.get("run_flags", {}).get("CONDUCT_DATA_EXTRACTION", False):
        try:
            # Initialize folder for a clean extraction run
            extraction_output_folder = Path(extraction_config.get("output_base_folder", "extracted_csv_tables"))
            clear_and_create_folder(extraction_output_folder)
            
            # Run the extraction process
            run_extraction(extraction_config)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Data Extraction: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("\nSkipping Data Extraction as per config.")

    # --- Step 2: Report Generation ---
    if reporting_config and reporting_config.get("run_flags", {}).get("CONDUCT_REPORT_GENERATION", False):
        try:
            # Initialize folder for a clean reporting run
            report_output_folder = Path(reporting_config.get("report_output_folderpath", "reporting_data"))
            clear_and_create_folder(report_output_folder)

            # Run the reporting process
            run_reporting(config_manager)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Report Generation: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print("\nSkipping Report Generation as per config.")

    print("\n🎉 Pipeline finished successfully!")
    print("--------------------------------------------------")

if __name__ == "__main__":
    main()