import pandas as pd
import glob
import re
import os
import matplotlib.pyplot as plt

class ReportGenerator:
    """
    A class to systematically read, process, and generate a summary report from transit data files.
    This version generates both 'long' (original) and 'wide' format CSVs for comparison.
    """
    def __init__(self, table_9_01_pattern, table_10_01_pattern, output_dir='summary_report'):
        self.table_9_01_pattern = table_9_01_pattern
        self.table_10_01_pattern = table_10_01_pattern
        self.output_dir = output_dir
        self.system_9_01_df = pd.DataFrame()
        self.per_stop_9_01_df = pd.DataFrame()
        self.system_10_01_df = pd.DataFrame()
        self.per_route_10_01_df = pd.DataFrame()

        self.modes_9_01 = ['WLK', 'KNR', 'PNR', 'XFR', 'ALL']
        self.modes_10_01 = ['WLK', 'KNR', 'PNR', 'ALL']

        os.makedirs(self.output_dir, exist_ok=True)
        plt.style.use('ggplot')

    def _process_table(self, file_pattern, name_col, total_value, modes):
        """Generic method to process a set of table files."""
        system_level_data = []
        item_data = []
        file_list = glob.glob(file_pattern, recursive=True)

        for file_path in file_list:
            try:
                df = pd.read_csv(file_path)
                match = re.search(r'\[(.*?)\]', os.path.basename(file_path))
                year_or_scenario = match.group(1) if match else 'unknown'
                
                system_df = df[df[name_col].str.contains(total_value, na=False)].copy()
                item_df = df[~df[name_col].str.contains(total_value, na=False)].copy()

                if not system_df.empty:
                    for scenario in ['NO-BUILD', 'BUILD']:
                        for mode in modes:
                            col_name = f'Y{year_or_scenario}_{scenario}_{mode}'
                            if col_name in system_df.columns:
                                value = system_df[col_name].iloc[0]
                                system_level_data.append({
                                    'Year': year_or_scenario,
                                    'Scenario': scenario,
                                    'Mode': mode,
                                    'Boardings': value
                                })

                if not item_df.empty:
                    for scenario in ['NO-BUILD', 'BUILD']:
                        for mode in modes:
                            col_name = f'Y{year_or_scenario}_{scenario}_{mode}'
                            if col_name in item_df.columns:
                                item_df_temp = item_df[[name_col, col_name]].copy()
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
            self.table_10_01_pattern, 'Route_Name', 'Total', self.modes_10_01
        )

    def _save_long_dataframes(self):
        """Saves the processed dataframes to long-format CSV files."""
        if not self.system_9_01_df.empty:
            self.system_9_01_df.to_csv(os.path.join(self.output_dir, 'system_level_boardings_long_9_01.csv'), index=False)
        if not self.per_stop_9_01_df.empty:
            self.per_stop_9_01_df.to_csv(os.path.join(self.output_dir, 'per_stop_boardings_long_9_01.csv'), index=False)
        if not self.system_10_01_df.empty:
            self.system_10_01_df.to_csv(os.path.join(self.output_dir, 'system_level_boardings_long_10_01.csv'), index=False)
        if not self.per_route_10_01_df.empty:
            self.per_route_10_01_df.to_csv(os.path.join(self.output_dir, 'per_route_boardings_long_10_01.csv'), index=False)

    def _save_wide_dataframes(self):
        """Pivots and saves the dataframes to wide-format CSV files with multi-level headers."""
        if not self.system_9_01_df.empty:
            system_wide_df = self.system_9_01_df.pivot_table(
                index='Year', columns=['Scenario', 'Mode'], values='Boardings'
            )
            system_wide_df.to_csv(os.path.join(self.output_dir, 'system_level_boardings_wide_9_01.csv'))
        
        if not self.per_stop_9_01_df.empty:
            per_stop_wide_df = self.per_stop_9_01_df.pivot_table(
                index='Station_Name', columns=['Year', 'Scenario', 'Mode'], values='Boardings'
            )
            per_stop_wide_df.to_csv(os.path.join(self.output_dir, 'per_stop_boardings_wide_9_01.csv'))
        
        if not self.system_10_01_df.empty:
            system_wide_df = self.system_10_01_df.pivot_table(
                index='Year', columns=['Scenario', 'Mode'], values='Boardings'
            )
            system_wide_df.to_csv(os.path.join(self.output_dir, 'system_level_boardings_wide_10_01.csv'))
        
        if not self.per_route_10_01_df.empty:
            per_route_wide_df = self.per_route_10_01_df.pivot_table(
                index='Route_Name', columns=['Year', 'Scenario', 'Mode'], values='Boardings'
            )
            per_route_wide_df.to_csv(os.path.join(self.output_dir, 'per_route_boardings_wide_10_01.csv'))

    def _plot_system_level(self, df, table_name, modes):
        """Generates system-level boarding plots."""
        if df.empty:
            return

        fig, axes = plt.subplots(1, 2, figsize=(15, 6), sharey=True)
        fig.suptitle(f'System-Level Boardings by Mode (Table {table_name})', fontsize=16)

        df_no_build = df[df['Scenario'] == 'NO-BUILD'].pivot(index='Year', columns='Mode', values='Boardings').reset_index()
        if not df_no_build.empty:
            df_no_build.set_index('Year')[modes].plot(kind='bar', ax=axes[0])
            axes[0].set_title('NO-BUILD Scenario')
            axes[0].set_ylabel('Number of Boardings')
            axes[0].set_xlabel('Year')
            axes[0].tick_params(axis='x', rotation=45)

        df_build = df[df['Scenario'] == 'BUILD'].pivot(index='Year', columns='Mode', values='Boardings').reset_index()
        if not df_build.empty:
            df_build.set_index('Year')[modes].plot(kind='bar', ax=axes[1])
            axes[1].set_title('BUILD Scenario')
            axes[1].set_ylabel('Number of Boardings')
            axes[1].set_xlabel('Year')
            axes[1].tick_params(axis='x', rotation=45)

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(os.path.join(self.output_dir, f'system_level_boardings_{table_name}.png'))
        plt.close()

    def _plot_per_item_data(self, df, item_name, plot_name):
        """Generates plots for top per-stop or per-route boardings."""
        if df.empty:
            return

        for year in df['Year'].unique():
            for scenario in df['Scenario'].unique():
                df_plot = df[(df['Year'] == year) & (df['Scenario'] == scenario) & (df['Mode'] == 'ALL')]
                if not df_plot.empty:
                    top_10 = df_plot.nlargest(10, 'Boardings')
                    plt.figure(figsize=(12, 8))
                    
                    plt.bar(top_10[item_name].astype(str), top_10['Boardings'])
                    
                    plt.title(f'Top 10 {plot_name} by Total Boardings ({year} {scenario})')
                    plt.xlabel(item_name)
                    plt.ylabel('Total Boardings')
                    plt.xticks(rotation=45, ha='right')
                    plt.tight_layout()
                    plt.savefig(os.path.join(self.output_dir, f'top_10_{plot_name.lower().replace(" ", "_")}_{year}_{scenario}.png'))
                    plt.close()

    def generate_report(self):
        """Generates the full report, including CSVs and plots."""
        print("Processing files and extracting data...")
        self.process_files()
        print("Saving processed data to both long and wide format CSVs...")
        self._save_long_dataframes()
        self._save_wide_dataframes()
        print("Generating visualizations...")
        self._plot_system_level(self.system_9_01_df, '9.01', self.modes_9_01[:-1])
        self._plot_system_level(self.system_10_01_df, '10.01', self.modes_10_01[:-1])
        self._plot_per_item_data(self.per_stop_9_01_df, 'Station_Name', 'Stations')
        self._plot_per_item_data(self.per_route_10_01_df, 'Route_Name', 'Routes')
        print("Report generation complete.")