import json
import sys
from extractor import run_extraction
from reporter import run_reporting

CONFIG_FILE = '[UPDATE ME] STOPS_Report Config.json'

def main():
    """
    Main pipeline wrapper. Reads config and runs steps based on flags.
    """
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"❌ FATAL: Configuration file not found at '{CONFIG_FILE}'.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"❌ FATAL: '{CONFIG_FILE}' is not a valid JSON file.")
        sys.exit(1)

    run_flags = config.get("run_flags", {})

    # --- Step 1: Data Extraction ---
    if run_flags.get("CONDUCT_DATA_EXTRACTION", False):
        try:
            run_extraction(config)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Data Extraction: {e}")
            sys.exit(1)
    else:
        print("Skipping Data Extraction as per config.")

    # --- Step 2: Report Generation ---
    if run_flags.get("CONDUCT_REPORT_GENERATION", False):
        try:
            run_reporting(config)
        except Exception as e:
            print(f"\n❌ FATAL ERROR during Report Generation: {e}")
            sys.exit(1)
    else:
        print("Skipping Report Generation as per config.")

    print("\n🎉 Pipeline finished successfully!")

if __name__ == "__main__":
    main()