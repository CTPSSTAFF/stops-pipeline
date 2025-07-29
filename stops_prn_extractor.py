# Re-importing necessary libraries as a good practice
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

    def extract_table_10_01(self):
        """
        Extract Table 10.01 and metadata from all loaded files.
        Populates self.tables and self.metadata.
        """
        for alias, file_path in self.files.items():
            df, meta = self._extract_table_10_01_from_prn(file_path)
            if not df.empty:
                self.tables[alias] = df
                self.metadata[alias] = meta
            else:
                print(f"Warning: Table 10.01 could not be extracted from {file_path}")

    def extract_table_9_01(self):
        """
        Extract Table 9.01 and metadata from all loaded files.
        Populates self.tables and self.metadata.
        """
        for alias, file_path in self.files.items():
            df, meta = self._extract_table_9_01_from_prn(file_path)
            if not df.empty:
                self.tables[f"{alias}_table_9_01"] = df
                self.metadata[f"{alias}_table_9_01"] = meta
            else:
                print(f"Warning: Table 9.01 could not be extracted from {file_path}")

    def extract_table_10_02(self):
        """
        Extract Table 10.02 and metadata from all loaded files.
        Populates self.tables and self.metadata.
        """
        for alias, file_path in self.files.items():
            df, meta = self._extract_table_10_02_from_prn(file_path)
            if not df.empty:
                self.tables[f"{alias}_table_10_02"] = df
                self.metadata[f"{alias}_table_10_02"] = meta
            else:
                print(f"Warning: Table 10.02 could not be extracted from {file_path}")

    def export_to_csv(self, output_dir="extracted_tables"):
        """
        Exports all extracted tables to individual CSV files.
        Each file will be named according to its extracted alias and table type.
        output_dir: directory where CSV files will be saved.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")

        for alias_key, dataframe in self.tables.items():
            file_name = ""
            if "_table_9_01" in alias_key:
                original_alias = alias_key.replace("_table_9_01", "")
                file_name = f"{original_alias}_Table_9_01.csv"
            elif "_table_10_02" in alias_key:
                original_alias = alias_key.replace("_table_10_02", "")
                file_name = f"{original_alias}_Table_10_02.csv"
            else:
                file_name = f"{alias_key}_Table_10_01.csv"
            
            output_path = os.path.join(output_dir, file_name)
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
        all_data_rows = []
        in_table_10_02_section = False
        start_of_table_data = -1
        colspecs_main = [(0, 20), (20, 56), (56, 65)]
        names_main = ["Route_ID", "Group_Name", "Count"]

        colspecs_sub = [(20, 56)]
        names_sub = ["Group_Name"]
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+10\.02", line):
                in_table_10_02_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)

            if in_table_10_02_section and start_of_table_data == -1:
                header_line = None
                sub_header_line = None
                
                if re.search(r"Y20\d\d", line) and i + 2 < len(lines):
                    header_line = line
                    if re.search(r"Route_ID.*Count.*WLK", lines[i+2]):
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
        
        # Build dynamic colspecs for sub-lines based on header info
        colspecs_sub_dynamic = []
        names_sub_dynamic = []
        start_of_dynamic_columns = 65
        for i, main_header in enumerate(header_info):
            for j, sub_header in enumerate(["WLK", "KNR", "PNR", "ALL"]):
                col_start = start_of_dynamic_columns + (i * 40) + (j * 10)
                col_end = col_start + 10
                colspecs_sub_dynamic.append((col_start, col_end))
                names_sub_dynamic.append(f"{main_header['name']}_{sub_header}")
        
        # Combine the lists for sub-lines
        colspecs_sub = colspecs_sub + colspecs_sub_dynamic
        names_sub = names_sub + names_sub_dynamic
        
        # Master list of all column names for the final DataFrame
        all_col_names = names_main + names_sub_dynamic
        
        current_route_id = None
        current_route_name = None
        
        for line_to_collect in lines[start_of_table_data:]:
            if re.search(r"Table\s+\d+\.\d+", line_to_collect) or (line_to_collect.strip() and "Program STOPS" in line_to_collect):
                break
            
            line_data = line_to_collect.rstrip()
            
            # Check for main Route_ID line
            if re.match(r"^\S", line_data) and line_data.strip():
                df_line = pd.read_fwf(io.StringIO(line_data), colspecs=colspecs_main, header=None, names=names_main, dtype=str)
                row_dict = df_line.iloc[0].to_dict()
                
                # Update current route info
                current_route_id = row_dict["Route_ID"]
                current_route_name = row_dict["Group_Name"]
                
                # Add None for dynamic columns
                for col in names_sub_dynamic:
                    row_dict[col] = None
                all_data_rows.append(row_dict)
            
            # Check for indented sub-group lines or total group lines
            elif re.match(r"^\s{2,}\S", line_data) and line_data.strip():
                df_line = pd.read_fwf(io.StringIO(line_data), colspecs=colspecs_sub, header=None, names=names_sub, dtype=str)
                row_dict = df_line.iloc[0].to_dict()
                
                # Prepend the current route information
                row_dict["Route_ID"] = current_route_id
                row_dict["Count"] = None
                
                # The 'Group_Name' is correctly parsed from this line
                all_data_rows.append(row_dict)

        if not all_data_rows:
            return pd.DataFrame(), metadata
        
        # Create the DataFrame from the list of dictionaries
        df = pd.DataFrame(all_data_rows, columns=all_col_names)
        
        # Final cleanup and type conversion
        for col in df.columns:
            if col not in ["Route_ID", "Group_Name"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)
        
        return df, metadata