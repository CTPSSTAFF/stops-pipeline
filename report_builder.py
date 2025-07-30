import pandas as pd
import glob
import re
import os
import matplotlib.pyplot as plt
from pathlib import Path
import shutil # Import shutil for rmtree

class ReportGenerator:
    """
    A class to systematically read, process, and generate a summary report from transit data files.
    This version generates separate wide-format CSVs for each scenario and a new set of PNGs.
    """
    def __init__(self, table_9_01_pattern: str, table_10_01_pattern: str, output_dir: str = 'summary_report'):
        self.table_9_01_pattern = table_9_01_pattern
        self.table_10_01_pattern = table_10_01_pattern
        self.output_dir = Path(output_dir)
        self.system_9_01_df = pd.DataFrame()
        self.per_stop_9_01_df = pd.DataFrame()
        self.system_10_01_df = pd.DataFrame()
        self.per_route_10_01_df = pd.DataFrame()

        self.scenarios = ['EXISTING', 'NO-BUILD', 'BUILD']
        self.modes_9_01 = ['WLK', 'KNR', 'PNR', 'XFR', 'ALL']
        self.modes_10_01 = ['WLK', 'KNR', 'PNR', 'ALL']

        # Define specific route categories
        self.rapid_transit_routes = [
            'Blue&T', 'Green-B&T', 'Green-C&T', 'Green-D&T', 'Green-E&T',
            'Mattapan&T', 'Orange&T', 'Red&T'
        ]
        self.commuter_rail_routes = [
            'CR-Fairmount&T', 'CR-Fitchburg&T', 'CR-Franklin&T', 'CR-Greenbush&T',
            'CR-Haverhill&T', 'CR-Kingston&T', 'CR-Lowell&T', 'CR-Middleborough&T',
            'CR-Needham&T', 'CR-Newburyport&T', 'CR-Providence&T', 'CR-Worcester&T',
            'CR: FALL River&T'
        ]
        self.ferry_routes = [
            'Boat-EastBoston&T', 'Boat-F1&T', 'Boat-F4&T', 'Boat-F6&T', 'Boat-Lynn&T'
        ]

        # --- Clear existing output directory contents ---
        if self.output_dir.exists() and self.output_dir.is_dir():
            print(f"Clearing existing files in {self.output_dir}...")
            for item in self.output_dir.iterdir():
                if item.is_file():
                    item.unlink() # Remove files
                elif item.is_dir():
                    shutil.rmtree(item) # Remove subdirectories and their contents
        # --- End clearing section ---

        self.output_dir.mkdir(parents=True, exist_ok=True)
        plt.style.use('ggplot')

    def _process_table(self, file_pattern: str, name_col: str, total_value: str, modes: list) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Generic method to process a set of table files."""
        system_level_data = []
        item_data = []
        file_list = glob.glob(file_pattern, recursive=True)

        for file_path in file_list:
            try:
                df = pd.read_csv(file_path)
                match = re.search(r'\[(.*?)\]', os.path.basename(file_path))
                year_or_scenario = match.group(1) if match else 'unknown'
                
                system_df_raw = df[df[name_col].str.lower().str.strip() == total_value.lower()].copy()
                item_df_raw = df[df[name_col].str.lower().str.strip() != total_value.lower()].copy()

                for scenario in self.scenarios:
                    scenario_prefix = f'Y{year_or_scenario}_{scenario.replace("-", "")}'
                    
                    if not system_df_raw.empty:
                        for mode in modes:
                            col_name = f'{scenario_prefix}_{mode}'
                            if col_name in system_df_raw.columns:
                                system_level_data.append({
                                    'Year': year_or_scenario,
                                    'Scenario': scenario,
                                    'Mode': mode,
                                    'Boardings': system_df_raw[col_name].iloc[0]
                                })

                    if not item_df_raw.empty:
                        for mode in modes:
                            col_name = f'{scenario_prefix}_{mode}'
                            if col_name in item_df_raw.columns:
                                item_df_temp = item_df_raw[[name_col, col_name]].copy()
                                item_df_temp.rename(columns={col_name: 'Boardings'}, inplace=True)
                                item_df_temp['Year'] = year_or_scenario
                                item_df_temp['Scenario'] = scenario
                                item_df_temp['Mode'] = mode
                                item_data.append(item_df_temp)

            except Exception as e:
                print(f"Could not process file {file_path}: {e}")

        system_level_df = pd.DataFrame(system_level_data)
        item_df = pd.concat(item_data, ignore_index=True) if item_data else pd.DataFrame()
        return system_level_df, item_df

    def process_files(self):
        """Calls the processing methods for both Table 9.01 and 10.01 files."""
        self.system_9_01_df, self.per_stop_9_01_df = self._process_table(
            self.table_9_01_pattern, 'Station_Name', 'Total', self.modes_9_01
        )
        self.system_10_01_df, self.per_route_10_01_df = self._process_table(
            self.table_10_01_pattern, 'Route_ID', 'Total', self.modes_10_01
        )

    def _save_scenario_dataframes(self):
        """
        Saves separate wide-format CSVs for each scenario with updated filenames.
        This method handles general system and per-stop/per-route data.
        """
        for scenario in self.scenarios:
            # Table 9.01 - System Level
            if not self.system_9_01_df.empty:
                df_scenario = self.system_9_01_df[self.system_9_01_df['Scenario'] == scenario]
                if not df_scenario.empty:
                    df_wide = df_scenario.pivot_table(index='Year', columns='Mode', values='Boardings')
                    df_wide.to_csv(self.output_dir / f'9_01_system_level_boardings_{scenario.lower().replace("-", "_")}.csv')

            # Table 9.01 - Per-stop
            if not self.per_stop_9_01_df.empty:
                df_scenario = self.per_stop_9_01_df[self.per_stop_9_01_df['Scenario'] == scenario]
                if not df_scenario.empty:
                    df_wide = df_scenario.pivot_table(index='Station_Name', columns=['Year', 'Mode'], values='Boardings')
                    df_wide.to_csv(self.output_dir / f'9_01_per_stop_boardings_{scenario.lower().replace("-", "_")}.csv')

            # Table 10.01 - System Level
            if not self.system_10_01_df.empty:
                df_scenario = self.system_10_01_df[self.system_10_01_df['Scenario'] == scenario]
                if not df_scenario.empty:
                    df_wide = df_scenario.pivot_table(index='Year', columns='Mode', values='Boardings')
                    df_wide.to_csv(self.output_dir / f'10_01_system_level_boardings_{scenario.lower().replace("-", "_")}.csv')
            
            # Table 10.01 - Per-route (general, not specific categories)
            if not self.per_route_10_01_df.empty:
                df_scenario = self.per_route_10_01_df[self.per_route_10_01_df['Scenario'] == scenario]
                if not df_scenario.empty:
                    df_wide = df_scenario.pivot_table(index='Route_ID', columns=['Year', 'Mode'], values='Boardings')
                    df_wide.to_csv(self.output_dir / f'10_01_per_route_boardings_{scenario.lower().replace("-", "_")}.csv')

    def _save_category_dataframes(self, category_name: str, route_list: list):
        """
        Saves separate wide-format CSVs for each scenario for a specific route category.
        """
        if self.per_route_10_01_df.empty:
            return

        df_category = self.per_route_10_01_df[self.per_route_10_01_df['Route_ID'].isin(route_list)].copy()

        if not df_category.empty:
            for scenario in self.scenarios:
                df_scenario = df_category[df_category['Scenario'] == scenario]
                if not df_scenario.empty:
                    df_wide = df_scenario.pivot_table(index='Route_ID', columns=['Year', 'Mode'], values='Boardings')
                    df_wide.to_csv(self.output_dir / f'10_01_per_route_{category_name.lower().replace(" ", "_")}_{scenario.lower().replace("-", "_")}.csv')


    def _plot_10_01_category_comparison(self, category_name: str, route_list: list):
        """
        Generates grouped bar charts for Table 10.01 per-route boardings for a specific category.
        The x-axis is the modes, and the bars are the years/scenarios.
        """
        if self.per_route_10_01_df.empty:
            return

        df_category = self.per_route_10_01_df[self.per_route_10_01_df['Route_ID'].isin(route_list)].copy()
        
        if not df_category.empty:
            for mode in self.modes_10_01: # Iterate through modes for this category
                df_mode = df_category[df_category['Mode'] == mode]
                
                if not df_mode.empty:
                    pivot_df = df_mode.pivot_table(index='Route_ID', columns='Year', values='Boardings')
                    # Exclude the 'Total' row from the chart if it somehow appears in filtered data
                    pivot_df = pivot_df[pivot_df.index.str.lower() != 'total']
                    
                    if not pivot_df.empty:
                        pivot_df.plot(
                            kind='bar',
                            figsize=(15, 8),
                            title=f'10.01 {category_name} Boardings for Mode: {mode}',
                            ylabel='Total Boardings',
                            xlabel='Route ID',
                            rot=45
                        )
                        plt.legend(title='Scenario')
                        plt.tight_layout()
                        plt.savefig(self.output_dir / f'10_01_{category_name.lower().replace(" ", "_")}_{mode.lower()}_comparison.png')
                        plt.close()

    def _plot_9_01_system_level_comparison(self):
        """
        Generates a grouped bar chart for system-level boardings,
        with modes on the x-axis and scenarios as the bars.
        """
        if self.system_9_01_df.empty:
            return

        modes_to_plot = ['ALL', 'KNR', 'PNR', 'WLK', 'XFR']
        df_modes = self.system_9_01_df[self.system_9_01_df['Mode'].isin(modes_to_plot)]
        
        if not df_modes.empty:
            # Pivot for plotting: modes as index, scenarios as columns
            pivot_df = df_modes.pivot_table(index='Mode', columns='Year', values='Boardings')
            
            if not pivot_df.empty:
                # Create the grouped bar chart
                pivot_df.plot(
                    kind='bar',
                    figsize=(15, 8),
                    title='9.01 System-Level Boardings Comparison Across Scenarios',
                    ylabel='Total Boardings',
                    xlabel='Access Mode',
                    rot=0
                )
                plt.legend(title='Scenario')
                plt.tight_layout()
                plt.savefig(self.output_dir / '9_01_system_level_comparison.png')
                plt.close()

    def generate_report(self):
        """
        Generates the full report, including CSVs and the newly requested plots.
        """
        print("Processing files and extracting data...")
        self.process_files()
        print("Saving processed data to separate CSVs for each scenario...")
        self._save_scenario_dataframes()
        
        # Save and plot data for specific categories
        print("Saving and generating visualizations for Rapid Transit, Commuter Rail, and Ferry...")
        self._save_category_dataframes('Rapid Transit', self.rapid_transit_routes)
        self._plot_10_01_category_comparison('Rapid Transit', self.rapid_transit_routes)

        self._save_category_dataframes('Commuter Rail', self.commuter_rail_routes)
        self._plot_10_01_category_comparison('Commuter Rail', self.commuter_rail_routes)

        self._save_category_dataframes('Ferry', self.ferry_routes)
        self._plot_10_01_category_comparison('Ferry', self.ferry_routes)

        print("Generating general system-level visualizations...")
        self._plot_9_01_system_level_comparison()
        
        print("Report generation complete.")

if __name__ == "__main__":
    base_dir = None
    for root, dirs, files in os.walk('.'):
        if 'extracted_csv_tables' in dirs:
            base_dir = Path(root) / 'extracted_csv_tables'
            break
    
    if base_dir:
        table_9_01_files = str(base_dir / "Table_9_01" / "[*]__Table_9_01.csv")
        table_10_01_files = str(base_dir / "Table_10_01" / "[*]__Table_10_01.csv")

        generator = ReportGenerator(table_9_01_files, table_10_01_files)
        generator.generate_report()
    else:
        print("Error: 'extracted_csv_tables' directory not found.")
