import pandas as pd
import io
import re
import os

class StopsPRNExtractor:
    def __init__(self):
        self.files = {}  # {alias: file_path}
        self.tables = {} # {alias: DataFrame}
        self.metadata = {} # {alias: metadata dict}

    def add_files(self, file_info_list):
        """
        Add multiple files to the extractor.
        file_info_list: list of tuples [(alias, file_path), ...]
        """
        for alias, file_path in file_info_list:
            self.files[alias] = file_path

    def extract_all_tables(self):
        """
        Extracts all specified tables from all loaded files.
        Populates self.tables and self.metadata.
        """
        for alias, file_path in self.files.items():
            # Extract Table 9.01
            df_9_01, meta_9_01 = self._extract_table_9_01_from_prn(file_path)
            if not df_9_01.empty:
                self.tables[f"{alias}_table_9_01"] = df_9_01
                self.metadata[f"{alias}_table_9_01"] = meta_9_01
            else:
                print(f"Warning: Table 9.01 could not be extracted from {file_path}")

            # Extract Table 10.01
            df_10_01, meta_10_01 = self._extract_table_10_01_from_prn(file_path)
            if not df_10_01.empty:
                self.tables[f"{alias}_table_10_01"] = df_10_01
                self.metadata[f"{alias}_table_10_01"] = meta_10_01
            else:
                print(f"Warning: Table 10.01 could not be extracted from {file_path}")
            
            # Extract Table 10.02
            df_10_02, meta_10_02 = self._extract_table_10_02_from_prn(file_path)
            if not df_10_02.empty:
                self.tables[f"{alias}_table_10_02"] = df_10_02
                self.metadata[f"{alias}_table_10_02"] = meta_10_02
            else:
                print(f"Warning: Table 10.02 could not be extracted from {file_path}")

            # Extract Table 11.01
            df_11_01, meta_11_01 = self._extract_table_11_from_prn(file_path, "11.01")
            if not df_11_01.empty:
                self.tables[f"{alias}_table_11_01"] = df_11_01
                self.metadata[f"{alias}_table_11_01"] = meta_11_01
            else:
                print(f"Warning: Table 11.01 could not be extracted from {file_path}")

            # Extract Table 11.02
            df_11_02, meta_11_02 = self._extract_table_11_from_prn(file_path, "11.02")
            if not df_11_02.empty:
                self.tables[f"{alias}_table_11_02"] = df_11_02
                self.metadata[f"{alias}_table_11_02"] = meta_11_02
            else:
                print(f"Warning: Table 11.02 could not be extracted from {file_path}")

            # Extract Table 11.03
            df_11_03, meta_11_03 = self._extract_table_11_from_prn(file_path, "11.03")
            if not df_11_03.empty:
                self.tables[f"{alias}_table_11_03"] = df_11_03
                self.metadata[f"{alias}_table_11_03"] = meta_11_03
            else:
                print(f"Warning: Table 11.03 could not be extracted from {file_path}")

            # Extract Table 11.04
            df_11_04, meta_11_04 = self._extract_table_11_from_prn(file_path, "11.04")
            if not df_11_04.empty:
                self.tables[f"{alias}_table_11_04"] = df_11_04
                self.metadata[f"{alias}_table_11_04"] = meta_11_04
            else:
                print(f"Warning: Table 11.04 could not be extracted from {file_path}")

    def export_to_csv(self, output_dir="extracted_tables"):
        """
        Exports all extracted tables to individual CSV files within a folder for each table type.
        The filename will be based on the original alias and the table number.
        output_dir: directory where CSV files will be saved.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")

        for alias_key, dataframe in self.tables.items():
            # Determine table type and original alias
            table_type = ""
            original_alias = alias_key
            if "_table_9_01" in alias_key:
                table_type = "Table_9_01"
                original_alias = alias_key.replace("_table_9_01", "")
            elif "_table_10_01" in alias_key:
                table_type = "Table_10_01"
                original_alias = alias_key.replace("_table_10_01", "")
            elif "_table_10_02" in alias_key:
                table_type = "Table_10_02"
                original_alias = alias_key.replace("_table_10_02", "")
            elif "_table_11_01" in alias_key:
                table_type = "Table_11_01"
                original_alias = alias_key.replace("_table_11_01", "")
            elif "_table_11_02" in alias_key:
                table_type = "Table_11_02"
                original_alias = alias_key.replace("_table_11_02", "")
            elif "_table_11_03" in alias_key:
                table_type = "Table_11_03"
                original_alias = alias_key.replace("_table_11_03", "")
            elif "_table_11_04" in alias_key:
                table_type = "Table_11_04"
                original_alias = alias_key.replace("_table_11_04", "")
            else:
                # Skip if table type is not recognized
                continue

            # Create the table-specific folder
            table_folder = os.path.join(output_dir, table_type)
            if not os.path.exists(table_folder):
                os.makedirs(table_folder)
                print(f"Created directory: {table_folder}")

            # Create the final file path using the original alias
            file_name = f"[{original_alias}]__{table_type}.csv"
            output_path = os.path.join(table_folder, file_name)

            try:
                dataframe.to_csv(output_path, index=False)
                print(f"Exported '{alias_key}' to '{output_path}' successfully.")
            except Exception as e:
                print(f"Error exporting '{alias_key}' to CSV: {e}")

    @staticmethod
    def _extract_metadata_from_prn(lines, start_index):
        metadata = {}
        for meta_line_offset in range(1, 10):
            meta_line_num = start_index - meta_line_offset
            if meta_line_num >= 0:
                meta_line = lines[meta_line_num].strip()
                if "Program STOPS" in meta_line:
                    program_version_parts = meta_line.split(" - ", 1)
                    if len(program_version_parts) > 0:
                        metadata["Program"] = program_version_parts[0].replace("Program ", "").strip()
                    if len(program_version_parts) > 1 and "Version:" in program_version_parts[1]:
                        version_match = re.search(r'Version:\s*(\S+)\s*-\s*(\d{2}/\d{2}/\d{4})', program_version_parts[1])
                        if version_match:
                            metadata["Version"] = f"{version_match.group(1)} - {version_match.group(2)}"
                        else:
                            metadata["Version"] = program_version_parts[1].split("Version: ")[1].split(" - ")[0].strip()
                    elif "Version:" in meta_line:
                        version_match = re.search(r'Version:\s*(\S+)\s*-\s*(\d{2}/\d{2}/\d{4})', meta_line)
                        if version_match:
                            metadata["Version"] = f"{version_match.group(1)} - {version_match.group(2)}"
                elif "Run:" in meta_line:
                    parts = meta_line.split("Run:")
                    if len(parts) > 1:
                        run_system_part = parts[1].strip()
                        run_match = re.search(r'^(.*?)(?:\s+System:\s*(.*))?$', run_system_part)
                        if run_match:
                            metadata["Run"] = run_match.group(1).strip()
                            if run_match.group(2):
                                metadata["System"] = run_match.group(2).strip()
                        else:
                            metadata["Run"] = run_system_part
                elif "Page" in meta_line:
                    page_match = re.search(r'Page\s+(\d+)', meta_line)
                    if page_match:
                        metadata["Page"] = page_match.group(1).strip()
        return metadata

    @staticmethod
    def _extract_table_10_01_from_prn(file_path):
        metadata = {}
        actual_data_lines = []
        in_table_10_01_section = False
        start_of_table_data = -1
        colspecs = []
        names = []
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+10\.01", line):
                in_table_10_01_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            if in_table_10_01_section and start_of_table_data == -1:
                header_line = None
                sub_header_line = None
                
                if re.search(r"Y20\d\d", line) and i + 2 < len(lines):
                    header_line = line
                    if re.search(r"Route_ID.*WLK.*KNR", lines[i+2]):
                        sub_header_line = lines[i+2]
                        if i + 3 < len(lines) and re.search(r"^=+", lines[i+3]):
                            start_of_table_data = i + 4
                            break

        if start_of_table_data == -1:
            return pd.DataFrame(), metadata
        
        main_headers = re.finditer(r"(Y20\d\d\s+[\w-]+)", header_line)
        header_info = []
        for m in main_headers:
            header_info.append({'name': m.group(1).strip().replace(' ', '_'), 'start': m.start()})
        
        colspecs = [(0, 20), (20, 56), (56, 65)]
        names = ["Route_ID", "Route_Name", "Count"]

        start_of_dynamic_columns = 65
        for i, main_header in enumerate(header_info):
            for j, sub_header in enumerate(["WLK", "KNR", "PNR", "ALL"]):
                col_start = start_of_dynamic_columns + (i * 40) + (j * 10)
                col_end = col_start + 10
                colspecs.append((col_start, col_end))
                names.append(f"{main_header['name']}_{sub_header}")
        
        for line_to_collect in lines[start_of_table_data:]:
            if "Total" in line_to_collect:
                actual_data_lines.append(line_to_collect.rstrip())
                break
            if re.search(r"Table\s+\d+\.\d+", line_to_collect) or (line_to_collect.strip() and "Program STOPS" in line_to_collect):
                break
            if line_to_collect.strip() and not re.fullmatch(r"={2,}", line_to_collect.strip()) and not re.fullmatch(r"-{2,}", line_to_collect.strip()):
                actual_data_lines.append(line_to_collect.rstrip())
        
        if not actual_data_lines or len(colspecs) != len(names):
            return pd.DataFrame(), metadata
        
        data_for_df = io.StringIO('\n'.join(actual_data_lines))
        
        dtypes_dict = {col: str for col in names}
        
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names,
                         dtype=dtypes_dict)

        for col in df.columns:
            df[col] = df[col].str.strip()
            if col not in ["Route_ID", "Route_Name"]:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)

        if not df.empty:
            last_row = df.iloc[-1]
            route_name_val = str(last_row["Route_Name"]).strip().lower()
            if route_name_val == "total":
                df.at[df.index[-1], "Route_Name"] = "Total"
                df.at[df.index[-1], "Route_ID"] = "Total"
                df.iloc[-1] = df.iloc[-1].fillna(0)

        return df, metadata

    @staticmethod
    def _extract_table_9_01_from_prn(file_path):
        metadata = {}
        actual_data_lines = []
        in_table_9_01_section = False
        start_of_table_data = -1
        colspecs = []
        names = []
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+9\.01", line):
                in_table_9_01_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)

            if in_table_9_01_section and start_of_table_data == -1:
                header_line = None
                sub_header_line = None
                
                if re.search(r"Y20\d\d", line) and i + 2 < len(lines):
                    header_line = line
                    if re.search(r"Stop_id1.*WLK.*KNR", lines[i+2]):
                        sub_header_line = lines[i+2]
                        if i + 3 < len(lines) and re.search(r"^=+", lines[i+3]):
                            start_of_table_data = i + 4
                            break

        if start_of_table_data == -1:
            return pd.DataFrame(), metadata
        
        main_headers = re.finditer(r"(Y20\d\d\s+[\w-]+)", header_line)
        header_info = []
        for m in main_headers:
            header_info.append({'name': m.group(1).strip().replace(' ', '_'), 'start': m.start()})
        
        colspecs = [(0, 26), (26, 47)]
        names = ["Stop_id1", "Station_Name"]

        start_of_dynamic_columns = 47
        for i, main_header in enumerate(header_info):
            for j, sub_header in enumerate(["WLK", "KNR", "PNR", "XFR", "ALL"]):
                if j == 0:
                    col_start = start_of_dynamic_columns + (i * 51)
                    col_end = col_start + 11
                else:
                    col_start = start_of_dynamic_columns + (i * 51) + 11 + ((j - 1) * 10)
                    col_end = col_start + 10
                
                colspecs.append((col_start, col_end))
                names.append(f"{main_header['name']}_{sub_header}")
        
        for line_to_collect in lines[start_of_table_data:]:
            if "Total" in line_to_collect:
                actual_data_lines.append(line_to_collect.rstrip())
                break
            if re.search(r"Table\s+\d+\.\d+", line_to_collect) or (line_to_collect.strip() and "Program STOPS" in line_to_collect):
                break
            if line_to_collect.strip() and not re.fullmatch(r"={2,}", line_to_collect.strip()) and not re.fullmatch(r"-{2,}", line_to_collect.strip()):
                actual_data_lines.append(line_to_collect.rstrip())
        
        if not actual_data_lines or len(colspecs) != len(names):
            return pd.DataFrame(), metadata

        data_for_df = io.StringIO('\n'.join(actual_data_lines))
        
        dtypes_dict = {col: str for col in names}
        
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names,
                         dtype=dtypes_dict)

        for col in df.columns:
            df[col] = df[col].str.strip()
            if col not in ["Stop_id1", "Station_Name"]:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)

        if not df.empty:
            last_row = df.iloc[-1]
            station_name_val = str(last_row["Station_Name"]).strip().lower()
            if station_name_val == "total":
                df.at[df.index[-1], "Station_Name"] = "Total"
                df.at[df.index[-1], "Stop_id1"] = "Total"
                df.iloc[-1] = df.iloc[-1].fillna(0)

        return df, metadata
        
    @staticmethod
    def _extract_table_10_02_from_prn(file_path):
        metadata = {}
        all_data_text = []
        in_table_10_02_section = False
        start_of_data = -1
        header_line = None
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}
        
        for i, line in enumerate(lines):
            if re.search(r"Table\s+10\.02", line):
                in_table_10_02_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            if in_table_10_02_section and start_of_data == -1:
                if re.search(r"Y20\d\d", line):
                    header_line = line
                if header_line and re.search(r"Route_ID.*Count", line):
                    if i + 1 < len(lines) and re.search(r"^=+", lines[i + 1]):
                        start_of_data = i + 2
                        break
        
        if start_of_data == -1 or header_line is None:
            return pd.DataFrame(), metadata

        main_headers = re.finditer(r"(Y20\d\d\s+[\w-]+)", header_line)
        header_info = [{'name': re.sub(r'\s+', '_', m.group(1).strip()), 'start': m.start()} for m in main_headers]
        
        colspecs = [(0, 20), (20, 56), (56, 65)]
        names = ["Route_ID", "Group_Name", "Count"]

        start_of_dynamic_columns = 65
        for i, main_header in enumerate(header_info):
            for j, sub_header in enumerate(["WLK", "KNR", "PNR", "ALL"]):
                col_start = start_of_dynamic_columns + (i * 40) + (j * 10)
                col_end = col_start + 10
                colspecs.append((col_start, col_end))
                names.append(f"{main_header['name']}_{sub_header}")
        
        for line in lines[start_of_data:]:
            if re.search(r"Table\s+\d+\.\d+", line) or re.search(r"Program STOPS", line):
                break
            if line.strip() and not re.fullmatch(r"={2,}", line.strip()):
                all_data_text.append(line)
        
        if not all_data_text:
            return pd.DataFrame(), metadata
        
        data_io = io.StringIO('\n'.join(all_data_text))
        
        df = pd.read_fwf(data_io, colspecs=colspecs, header=None, names=names, dtype=str)
        
        df["Route_ID"] = df["Route_ID"].str.strip().replace('', pd.NA)
        df["Group_Name"] = df["Group_Name"].str.strip().replace('', pd.NA)
        df["Count"] = df["Count"].str.strip().replace('', pd.NA)
        
        df['Route_Name'] = df['Group_Name'].apply(lambda x: x if str(x).startswith('--') else pd.NA)
        
        # Propagate Route_ID and Route_Name down to sub-group rows
        df['Route_ID'] = df['Route_ID'].ffill()
        df['Route_Name'] = df['Route_Name'].ffill()
        
        # Clean up the Group_Name column for the main route header rows
        df.loc[df['Group_Name'].str.startswith('--', na=False), 'Group_Name'] = pd.NA
        
        # Correctly handle the special case for the 'Total' rows
        is_total_header = df['Route_ID'].str.lower().str.strip().eq('total')
        df.loc[is_total_header, 'Route_Name'] = 'Total'
        
        is_total_group_name = df['Group_Name'].str.lower().str.strip().eq('total')
        df.loc[is_total_group_name, 'Group_Name'] = 'Total'
        df.loc[is_total_group_name, 'Route_Name'] = 'Total'
        
        # Reorder columns to the desired output format
        final_names = ["Route_ID", "Route_Name", "Group_Name", "Count"] + names[3:]
        df = df[final_names]
        
        for col in names[3:]:
            df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
        
        df["Count"] = pd.to_numeric(df["Count"], errors='coerce')
        
        return df, metadata

    @staticmethod
    def _extract_table_11_from_prn(file_path, table_id):
        metadata = {}
        data_text = []
        in_table_section = False
        start_of_data = -1
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}
        
        header_line = None
        for i, line in enumerate(lines):
            if re.search(r"Table\s+" + re.escape(table_id), line):
                in_table_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            if in_table_section and start_of_data == -1:
                if re.search(r"Y20\d\d", line) and re.search(r"EXISTING", line):
                    header_line = line
                    sub_header_line = lines[i + 1]
                    separator_line = lines[i + 2]
                    if re.search(r"=", separator_line):
                        start_of_data = i + 3
                        break
        
        if start_of_data == -1 or header_line is None:
            return pd.DataFrame(), metadata

        # Define fixed-width column specifications and names
        colspecs = [(0, 9), (9, 29), (29, 43), 
                    (44, 52), (52, 61), 
                    (61, 70), (70, 79), 
                    (79, 88), (88, 97), 
                    (97, 106), (106, 115)]
        
        names = ["HH_Cars", "Sub_mode", "Access_mode", 
                 "Y2024_EXISTING_Model", "Y2024_EXISTING_Survey",
                 "Y2050_NO_BUILD_Model", "Y2050_NO_BUILD_Survey",
                 "Y2050_BUILD_Model", "Y2050_BUILD_Survey",
                 "Y2050_BUILD_Project_Model", "Y2050_BUILD_Project_Survey"]
        
        # Collect data lines and skip separator lines
        for line in lines[start_of_data:]:
            if re.search(r"Table\s+\d+\.\d+", line) or re.search(r"Program STOPS", line):
                break
            if re.fullmatch(r"[-=]+\s+.*", line.strip()) or not line.strip():
                continue
            data_text.append(line.rstrip())
        
        if not data_text or len(colspecs) != len(names):
            print(f"Warning: Data lines or column specification mismatch for Table {table_id}")
            return pd.DataFrame(), metadata
        
        data_io = io.StringIO('\n'.join(data_text))
        
        df = pd.read_fwf(data_io, colspecs=colspecs, header=None, names=names, dtype=str)

        # Clean and convert data types
        for col in df.columns:
            # First, clean the specific columns as requested
            if col == "Access_mode":
                df[col] = df[col].str.replace('|', '', regex=False).str.strip()
            
            # Then, perform the general stripping
            df[col] = df[col].str.strip()
            
            if col not in ["HH_Cars", "Sub_mode", "Access_mode"]:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)
        
        # Forward fill the main grouping columns
        df['HH_Cars'] = df['HH_Cars'].mask(df['HH_Cars'].eq('')).ffill()
        df['Sub_mode'] = df['Sub_mode'].mask(df['Sub_mode'].eq('')).ffill()
        
        return df, metadata