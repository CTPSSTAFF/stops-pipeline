import pandas as pd
from pathlib import Path

class ReportGenerator:
    def __init__(self, config):
        """Initializes the generator with settings from the config file."""
        self.config = config
        self.csv_input_dir = Path(config["csv_output_folderpath"])
        self.report_output_dir = Path(config["report_output_folderpath"])
        self.report_output_dir.mkdir(parents=True, exist_ok=True)
        self.files_info = config.get("prn_files_data_extraction_config", {}).get("files_to_process", [])

    def generate_reports_from_config(self):
        """Generates all reports defined in the config file."""
        report_definitions = self.config.get("route_level_comparison_reports", [])

        if not report_definitions:
            print("No 'route_level_comparison_reports' reports found in config.")
            return

        print(f"Found {len(report_definitions)} route-level comparison reports to generate.")
        
        for i, report_def in enumerate(report_definitions):
            print(f"\n--- Generating Report {i+1}: '{report_def.get('output_filename', 'N/A')}' ---")
            self.create_comparison_report(report_def)

    def create_comparison_report(self, report_def):
        """Creates a single filtered and pivoted comparison report."""
        table_to_compare = report_def["table_to_compare"]
        route_ids = report_def["route_ids"]
        output_filename = report_def["output_filename"]
        
        all_data = []
        
        for file_info in self.files_info:
            alias = file_info["alias"]
            table_folder_name = f"Table_{table_to_compare.replace('.', '_')}"
            input_filename = f"[{alias}]__Table_{table_to_compare.replace('.', '_')}.csv"
            csv_path = self.csv_input_dir / table_folder_name / input_filename
            
            if not csv_path.exists():
                print(f"  - WARNING: Source CSV not found for alias '{alias}': {csv_path}. Skipping.")
                continue
            
            try:
                df = pd.read_csv(csv_path)
                if "Route_ID" not in df.columns:
                    print(f"  - ERROR: 'Route_ID' column not found in {csv_path}. Skipping.")
                    continue
                
                filtered_df = df[df['Route_ID'].isin(route_ids)].copy()
                filtered_df['alias'] = alias
                all_data.append(filtered_df)
            except Exception as e:
                print(f"  - ERROR: Could not process {csv_path}: {e}")

        if not all_data:
            print(f"  - No data found across all sources to generate report '{output_filename}'.")
            return
            
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Define index and value columns for pivoting
        index_cols = ['Route_ID', 'Route_Name']
        valid_index_cols = [col for col in index_cols if col in combined_df.columns]
        
        if not valid_index_cols:
            print(f"  - ERROR: Index columns {index_cols} not found in the data. Cannot pivot.")
            return

        value_cols = [col for col in combined_df.columns if col not in valid_index_cols + ['alias']]
        
        try:
            pivot_df = combined_df.pivot_table(index=valid_index_cols, columns='alias', values=value_cols)
            
            # Flatten the MultiIndex columns for a clean CSV format
            pivot_df.columns = [f"{val}_{col}" for val, col in pivot_df.columns]
            pivot_df.reset_index(inplace=True)
            
            output_path = self.report_output_dir / output_filename
            pivot_df.to_csv(output_path, index=False)
            print(f"✅ Report saved successfully to: {output_path}")
        except Exception as e:
            print(f"  - ERROR: Failed to pivot and save report '{output_filename}'. Reason: {e}")


def run_reporting(config):
    """Main function to run the report generation process."""
    print("--- 📊 Starting Report Generation ---")
    generator = ReportGenerator(config)
    generator.generate_reports_from_config()
    print("\n--- ✅ Report Generation Complete ---")