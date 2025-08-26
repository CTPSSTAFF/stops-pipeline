import pandas as pd
from pathlib import Path
import re

class ReportGenerator:
    def __init__(self, config):
        """Initializes the generator with settings from the config file."""
        self.config = config
        # UPDATED: Use the new, explicit input path key
        self.csv_input_dir = Path(config["csv_input_folderpath"])
        self.report_output_dir = Path(config["report_output_folderpath"])
        self.report_output_dir.mkdir(parents=True, exist_ok=True)

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
        """
        Creates a single comparison report by filtering and pivoting data.
        
        This method reads data from multiple scenario CSVs, filters for specific
        route_ids, and then transforms the data into a summary table where each
        row is a metric and each column is a scenario, making for easy comparison.
        """
        table_to_compare = report_def["table_to_compare"]
        route_ids = report_def["route_ids"]
        output_filename = report_def["output_filename"]
        
        all_data = []
        
        # Scan for input files instead of using a predefined list
        table_folder_name = f"Table_{table_to_compare.replace('.', '_')}"
        table_folder_path = self.csv_input_dir / table_folder_name
        
        if not table_folder_path.exists():
            print(f"  - WARNING: Input folder not found: {table_folder_path}. Skipping report.")
            return

        # Use glob to find all CSVs and extract the alias from the filename
        for csv_path in table_folder_path.glob('[*]__*.csv'):
            match = re.search(r"\[(.*?)\]", csv_path.name)
            if not match:
                print(f"  - WARNING: Could not extract alias from filename '{csv_path.name}'. Skipping.")
                continue
            alias = match.group(1)
            
            try:
                df = pd.read_csv(csv_path)
                if "Route_ID" not in df.columns:
                    print(f"  - ERROR: 'Route_ID' column not found in {csv_path}. Skipping.")
                    continue
                
                filtered_df = df[df['Route_ID'].isin(route_ids)].copy()
                filtered_df['alias'] = alias
                all_data.append(filtered_df)
                print(f"  - Processing data from alias '{alias}'...")
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
            # 1. Melt the DataFrame to convert metrics from columns to rows (long format)
            melted_df = combined_df.melt(
                id_vars=valid_index_cols + ['alias'],
                value_vars=value_cols,
                var_name='Metric',
                value_name='Value'
            )

            # 2. Pivot the melted data to create the final comparison table
            #    Rows: Route ID, Route Name, Metric
            #    Columns: Each scenario (alias)
            final_report_df = melted_df.pivot_table(
                index=valid_index_cols + ['Metric'],
                columns='alias',
                values='Value'
            )

            # 3. Clean up the DataFrame for a tidy CSV output
            final_report_df.reset_index(inplace=True)
            final_report_df.rename_axis(None, axis=1, inplace=True)

            # 4. Save the final report
            output_path = self.report_output_dir / output_filename
            final_report_df.to_csv(output_path, index=False)
            print(f"✅ Report saved successfully to: {output_path}")

        except Exception as e:
            print(f"  - ERROR: Failed to pivot and save report '{output_filename}'. Reason: {e}")


def run_reporting(config):
    """Main function to run the report generation process."""
    print("--- 📊 Starting Report Generation ---")
    generator = ReportGenerator(config)
    generator.generate_reports_from_config()
    print("\n--- ✅ Report Generation Complete ---")