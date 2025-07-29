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
        header_line_index = -1
        sub_header_line_index = -1
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+10\.01", line):
                in_table_10_01_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            if in_table_10_01_section:
                # Find the main header line with the years
                if re.search(r"Y20\d\d", line) and header_line_index == -1:
                    header_line_index = i
                
                # Find the sub-header line with WLK, KNR, PNR, etc.
                if re.search(r"Route_ID\s+.*Route Name.*Count.*(WLK|KNR|PNR|XFR|ALL)", line) and sub_header_line_index == -1:
                    if i + 1 < len(lines) and re.search(r"^=+", lines[i+1]):
                        sub_header_line_index = i
                        
                        # We've found the headers, now parse them to build colspecs and names
                        main_header_line = lines[header_line_index]
                        sub_header_line = lines[sub_header_line_index + 2] if sub_header_line_index + 2 < len(lines) else ""
                        
                        # Find main headers and their start positions
                        main_headers = re.finditer(r"(Y20\d\d\s+[\w-]+)", main_header_line)
                        header_info = []
                        for m in main_headers:
                            header_info.append({'name': m.group(1).strip().replace(' ', '_'), 'start': m.start()})
                        
                        # Find sub-headers and their start/end positions
                        sub_headers = re.finditer(r"\b(WLK|KNR|PNR|XFR|ALL)\b", sub_header_line)
                        sub_header_info = []
                        for m in sub_headers:
                            sub_header_info.append({'name': m.group(1), 'start': m.start(), 'end': m.end()})

                        # Build a combined list of column headers and their positions
                        colspecs = [(0, 20), (20, 56), (56, 65)] # Fixed columns
                        names = ["Route_ID", "Route_Name", "Count"]

                        # Determine the start position of the first dynamic column block
                        start_of_dynamic_columns = 65

                        for i, main_header in enumerate(header_info):
                            # The start position for sub-columns is the start of the main header
                            # adjusted for padding
                            year_start_pos = main_header['start']
                            
                            # Find the sub-headers that fall within this main header's span
                            for j, sub_header in enumerate(["WLK", "KNR", "PNR", "ALL"]):
                                # Calculate the start and end of the column based on fixed widths
                                col_start = start_of_dynamic_columns + (i * 40) + (j * 10)
                                col_end = col_start + 10
                                colspecs.append((col_start, col_end))
                                names.append(f"{main_header['name']}_{sub_header}")
                        
                        if sub_header_line_index != -1:
                            # Start collecting data after the separator line
                            for line_to_collect in lines[sub_header_line_index + 3:]:
                                if "Total" in line_to_collect and re.search(r"={20,}", lines[lines.index(line_to_collect) + 1]):
                                    actual_data_lines.append(line_to_collect.rstrip())
                                    in_table_10_01_section = False
                                    break
                                
                                if re.search(r"Table\s+\d+\.\d+", line_to_collect) or (line_to_collect.strip() and "Program STOPS" in line_to_collect):
                                    in_table_10_01_section = False
                                    break

                                if line_to_collect.strip() and not re.fullmatch(r"={2,}", line_to_collect.strip()) and not re.fullmatch(r"-{2,}", line_to_collect.strip()):
                                    actual_data_lines.append(line_to_collect.rstrip())
                            
                            # Break the outer loop once data collection is complete
                            break

        if not actual_data_lines or not colspecs or len(colspecs) != len(names):
            return pd.DataFrame(), metadata
        
        data_for_df = io.StringIO('\n'.join(actual_data_lines))
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names,
                         dtype={col: str for col in names})

        for col in df.columns:
            df[col] = df[col].str.strip()
            if col not in ["Route_ID", "Route_Name"]:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)

        return df, metadata

    @staticmethod
    def _extract_table_9_01_from_prn(file_path):
        metadata = {}
        actual_data_lines = []
        in_table_9_01_section = False
        header_line_index = -1
        sub_header_line_index = -1
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+9\.01", line):
                in_table_9_01_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)

            if in_table_9_01_section:
                # Find the main header line with the years
                if re.search(r"Y20\d\d", line) and header_line_index == -1:
                    header_line_index = i
                
                # Find the sub-header line with WLK, KNR, PNR, etc.
                if re.search(r"Stop_id1\s+.*Station Name.*WLK.*KNR.*PNR.*XFR.*ALL", line) and sub_header_line_index == -1:
                    if i + 1 < len(lines) and re.search(r"^=+", lines[i+1]):
                        sub_header_line_index = i

                        # Parse headers to build colspecs and names
                        main_header_line = lines[header_line_index]
                        sub_header_line = lines[sub_header_line_index + 2] if sub_header_line_index + 2 < len(lines) else ""
                        
                        # Find main headers and their start positions
                        main_headers = re.finditer(r"(Y20\d\d\s+[\w-]+)", main_header_line)
                        header_info = []
                        for m in main_headers:
                            header_info.append({'name': m.group(1).strip().replace(' ', '_'), 'start': m.start()})
                        
                        # Find sub-headers and their start/end positions
                        sub_headers = re.finditer(r"\b(WLK|KNR|PNR|XFR|ALL)\b", sub_header_line)
                        sub_header_info = []
                        for m in sub_headers:
                            sub_header_info.append({'name': m.group(1), 'start': m.start()})

                        # Build a combined list of column headers and their positions
                        colspecs = [(0, 26), (26, 47)] # Fixed columns
                        names = ["Stop_id1", "Station_Name"]

                        # Determine the start position of the first dynamic column block
                        start_of_dynamic_columns = 47

                        for i, main_header in enumerate(header_info):
                            # The start position for sub-columns is the start of the main header
                            # adjusted for padding
                            year_start_pos = main_header['start']
                            
                            # Find the sub-headers that fall within this main header's span
                            for j, sub_header in enumerate(["WLK", "KNR", "PNR", "XFR", "ALL"]):
                                # Calculate the start and end of the column based on fixed widths
                                # The first column 'WLK' is 11 wide, the rest are 10
                                if j == 0:
                                    col_start = start_of_dynamic_columns + (i * 51)
                                    col_end = col_start + 11
                                else:
                                    col_start = start_of_dynamic_columns + (i * 51) + 11 + ((j - 1) * 10)
                                    col_end = col_start + 10
                                
                                colspecs.append((col_start, col_end))
                                names.append(f"{main_header['name']}_{sub_header}")
                        
                        if sub_header_line_index != -1:
                            # Start collecting data after the separator line
                            for line_to_collect in lines[sub_header_line_index + 3:]:
                                if "Total" in line_to_collect and re.search(r"={20,}", lines[lines.index(line_to_collect) + 1]):
                                    actual_data_lines.append(line_to_collect.rstrip())
                                    in_table_9_01_section = False
                                    break
                                
                                if re.search(r"Table\s+\d+\.\d+", line_to_collect) or (line_to_collect.strip() and "Program STOPS" in line_to_collect):
                                    in_table_9_01_section = False
                                    break

                                if line_to_collect.strip() and not re.fullmatch(r"={2,}", line_to_collect.strip()) and not re.fullmatch(r"-{2,}", line_to_collect.strip()):
                                    actual_data_lines.append(line_to_collect.rstrip())
                            
                            # Break the outer loop once data collection is complete
                            break


        if not actual_data_lines or not colspecs or len(colspecs) != len(names):
            return pd.DataFrame(), metadata

        data_for_df = io.StringIO('\n'.join(actual_data_lines))
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names,
                         dtype={col: str for col in names})

        for col in df.columns:
            df[col] = df[col].str.strip()
            if col not in ["Stop_id1", "Station_Name"]:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)

        return df, metadata