import pandas as pd
import glob
from pathlib import Path

class ReportGenerator:
    def __init__(self, config):
        """Initializes the generator with settings from the config file."""
        self.config = config
        self.csv_input_dir = Path(config["csv_output_folderpath"])
        self.report_output_dir = Path(config["report_output_folderpath"])
        self.report_output_dir.mkdir(parents=True, exist_ok=True)

    def generate_reports_from_config(self):
        """Generates all reports defined in the config file."""
        report_configs = self.config.get("report_generation_data_config", {})
        route_comparisons = report_configs.get("route_level_comparison", [])

        if not route_comparisons:
            print("No 'route_level_comparison' reports found in config.")
            return

        print(f"Found {len(route_comparisons)} route-level comparison reports to generate.")
        
        for i, report_def in enumerate(route_comparisons):
            print(f"\n--- Generating Report {i+1}: '{report_def['output_filename']}' ---")
            self.create_comparison_report(report_def)

    def create_comparison_report(self, report_def):
        """Creates a single filtered comparison report."""
        source_table_id = report_def["source_table_id"]
        filter_col = report_def["row_filter_column"]
        rows_to_include = report_def["rows_to_include"]
        output_filename = report_def["output_filename"]

        table_folder_name = f"Table_{source_table_id.replace('.', '_')}"
        search_pattern = str(self.csv_input_dir / table_folder_name / "*.csv")
        source_files = glob.glob(search_pattern)

        if not source_files:
            print(f"❗️ WARNING: No source CSVs found for table '{source_table_id}'.")
            return
        
        print(f"Found {len(source_files)} source files for Table {source_table_id}.")

        all_data = []
        for f_path in source_files:
            alias = Path(f_path).stem.split(']')[0][1:]
            df = pd.read_csv(f_path)
            df['SourceAlias'] = alias
            all_data.append(df)

        if not all_data:
            return
            
        combined_df = pd.concat(all_data, ignore_index=True)
        filtered_df = combined_df[combined_df[filter_col].isin(rows_to_include)]
        
        if filtered_df.empty:
            print("WARNING: The filter resulted in an empty DataFrame. No output saved.")
            return

        output_path = self.report_output_dir / output_filename
        filtered_df.to_csv(output_path, index=False)
        print(f"✅ Report saved successfully to: {output_path}")

def run_reporting(config):
    """Main function to run the report generation process."""
    print("--- 📊 Starting Report Generation ---")
    generator = ReportGenerator(config)
    generator.generate_reports_from_config()
    print("\n--- ✅ Report Generation Complete ---")