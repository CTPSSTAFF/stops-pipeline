import json
import sys
from pathlib import Path

class ConfigManager:
    """
    A master class to find, load, and manage all configuration files for the pipeline.
    It centralizes the logic for reading JSON files and hydrating the main
    configuration with its sub-configurations.
    """
    def __init__(self, extraction_path, reporting_path):
        """
        Initializes the manager with the paths to the primary config files.

        Args:
            extraction_path (str): Path to the data extraction config file.
            reporting_path (str): Path to the report generation config file.
        """
        self.extraction_config_path = Path(extraction_path)
        self.reporting_config_path = Path(reporting_path)
        self.extraction_config = None
        self.reporting_config = None

    def _load_json_file(self, file_path, description):
        """
        Safely loads a single JSON file.

        Args:
            file_path (Path): The path object for the file to load.
            description (str): A human-readable description for logging.

        Returns:
            dict or list: The loaded JSON data, or None if the file doesn't exist.
        """
        if not file_path.is_file():
            print(f"INFO: {description} file not found at '{file_path}'.")
            return None
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"❌ FATAL: '{file_path}' is not a valid JSON file.")
            sys.exit(1)
        except Exception as e:
            print(f"❌ FATAL: Could not read {file_path}. Reason: {e}")
            sys.exit(1)

    def _hydrate_extraction_config(self):
        """
        Loads secondary configs (aliases, tables) and injects them into the
        main extraction configuration dictionary.
        """
        if not self.extraction_config:
            return

        print("Hydrating extraction configuration from linked files...")

        # Load Aliases
        aliases_path_str = self.extraction_config.get("data_aliases_config_filepath")
        if aliases_path_str:
            aliases_config = self._load_json_file(Path(aliases_path_str), "Data Aliases")
            if aliases_config is not None:
                self.extraction_config["files_to_process"] = aliases_config
                print(f"  - Loaded {len(aliases_config)} file aliases from '{aliases_path_str}'")
        else:
            print("  - WARNING: 'data_aliases_config_filepath' not found in extraction config.")

        # Load Tables
        tables_path_str = self.extraction_config.get("data_tables_config_filepath")
        if tables_path_str:
            tables_config = self._load_json_file(Path(tables_path_str), "Data Tables")
            if tables_config is not None:
                self.extraction_config["tables_to_extract"] = tables_config
                print(f"  - Loaded {len(tables_config)} table definitions from '{tables_path_str}'")
        else:
            print("  - WARNING: 'data_tables_config_filepath' not found in extraction config.")

    def load_all(self):
        """Loads all primary and secondary configuration files."""
        print("--- ⚙️ Loading Configurations ---")
        self.extraction_config = self._load_json_file(self.extraction_config_path, "Extraction config")
        self.reporting_config = self._load_json_file(self.reporting_config_path, "Reporting config")
        
        # Hydrate the extraction config with its linked sub-configs
        self._hydrate_extraction_config()
        print("--- ✅ Configurations Loaded ---")