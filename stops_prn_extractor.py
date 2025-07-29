import pandas as pd
import io
import re


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
                # For Table 10.01, store directly under the alias
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
                # For Table 9.01, store under alias_table_9_01
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
            # Check if it's a Table 9.01 entry (indicated by the suffix in the key)
            if "_table_9_01" in alias_key:
                original_alias = alias_key.replace("_table_9_01", "")
                file_name = f"{original_alias}_Table_9_01.csv"
            else:
                # Assume it's a Table 10.01 entry (or other default extraction)
                # The alias_key itself is the original alias for Table 10.01
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
        started_collecting_data = False

        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+10\.01", line):
                in_table_10_01_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
                continue

            if in_table_10_01_section:
                if i + 1 < len(lines):
                    header_line_check = lines[i].strip()
                    if "Route_ID" in header_line_check and "Route Name" in header_line_check and \
                       "Count" in header_line_check and "ALL" in header_line_check and \
                       re.search(r"^=+\s+=+\s+=+.*", lines[i+1]):
                        started_collecting_data = True
                        continue 

                if started_collecting_data:
                    if "Total" in line and re.search(r"={20,}", lines[i+1] if i + 1 < len(lines) else ""):
                        actual_data_lines.append(line.rstrip()) 
                        in_table_10_01_section = False
                        break

                    if re.search(r"Table\s+\d+\.\d+", line) or (line.strip() and "Program STOPS" in line):
                        in_table_10_01_section = False
                        break
                    
                    if line.strip() and not re.fullmatch(r"={2,}", line.strip()) and not re.fullmatch(r"-{2,}", line.strip()):
                        actual_data_lines.append(line.rstrip())

        if not actual_data_lines:
            return pd.DataFrame(), metadata

        colspecs = [
            (0, 20),   # Route_ID
            (20, 56),  # --Route Name
            (56, 65),  # Count
            (65, 75),  # Y2024_EXISTING_WLK
            (75, 85),  # Y2024_EXISTING_KNR
            (85, 95),  # Y2024_EXISTING_PNR
            (95, 105), # Y2024_EXISTING_ALL
            (105, 115),# Y2050_NO-BUILD_WLK
            (115, 125),# Y2050_NO-BUILD_KNR
            (125, 135),# Y2050_NO-BUILD_PNR
            (135, 145),# Y2050_NO-BUILD_ALL
            (145, 155),# Y2050_BUILD_WLK
            (155, 165),# Y2050_BUILD_KNR
            (165, 175),# Y2050_BUILD_PNR
            (175, 185) # Y2050_BUILD_ALL
        ]

        names = [
            "Route_ID", "Route_Name",
            "Count", "Y2024_EXISTING_WLK", "Y2024_EXISTING_KNR", "Y2024_EXISTING_PNR", "Y2024_EXISTING_ALL",
            "Y2050_NO-BUILD_WLK", "Y2050_NO-BUILD_KNR", "Y2050_NO-BUILD_PNR", "Y2050_NO-BUILD_ALL",
            "Y2050_BUILD_WLK", "Y2050_BUILD_KNR", "Y2050_BUILD_PNR", "Y2050_BUILD_ALL"
        ]
        
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
        started_collecting_data = False

        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+9\.01", line):
                in_table_9_01_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
                continue

            if in_table_9_01_section:
                if not started_collecting_data and i + 1 < len(lines):
                    header_line_check = lines[i].strip()
                    separator_line_check = lines[i+1].strip()

                    header_pattern_match = re.search(r"Stop_id1\s+.*?Station Name\s+.*?WLK\s+.*?KNR\s+.*?PNR\s+.*?XFR\s+.*?ALL", header_line_check)
                    separator_pattern_match = re.search(r"^=+\s+=+\s+=+.*", separator_line_check)

                    if header_pattern_match and separator_pattern_match:
                        started_collecting_data = True
                        continue 

                if started_collecting_data:
                    if "Total" in line and re.search(r"={20,}", lines[i+1] if i + 1 < len(lines) else ""):
                        actual_data_lines.append(line.rstrip()) 
                        in_table_9_01_section = False
                        break

                    if re.search(r"Table\s+\d+\.\d+", line) or (line.strip() and "Program STOPS" in line):
                        in_table_9_01_section = False
                        break
                    
                    if line.strip() and not re.fullmatch(r"={2,}", line.strip()) and not re.fullmatch(r"-{2,}", line.strip()):
                        actual_data_lines.append(line.rstrip())

        if not actual_data_lines:
            return pd.DataFrame(), metadata

        colspecs = [
            (0, 26),   # Stop_id1
            (26, 47),  # Station Name
            (47, 58),  # Y2024_EXISTING_WLK
            (58, 68),  # Y2024_EXISTING_KNR
            (68, 78),  # Y2024_EXISTING_PNR
            (78, 88),  # Y2024_EXISTING_XFR
            (88, 98),  # Y2024_EXISTING_ALL
            (98, 109), # Y2050_NO-BUILD_WLK
            (109, 119),# Y2050_NO-BUILD_KNR
            (119, 129),# Y2050_NO-BUILD_PNR
            (129, 139),# Y2050_NO-BUILD_XFR
            (139, 149),# Y2050_NO-BUILD_ALL
            (149, 160),# Y2050_BUILD_WLK
            (160, 170),# Y2050_BUILD_KNR
            (170, 180),# Y2050_BUILD_PNR
            (180, 190),# Y2050_BUILD_XFR
            (190, 200) # Y2050_BUILD_ALL
        ]

        names = [
            "Stop_id1", "Station_Name",
            "Y2024_EXISTING_WLK", "Y2024_EXISTING_KNR", "Y2024_EXISTING_PNR", "Y2024_EXISTING_XFR", "Y2024_EXISTING_ALL",
            "Y2050_NO-BUILD_WLK", "Y2050_NO-BUILD_KNR", "Y2050_NO-BUILD_PNR", "Y2050_NO-BUILD_XFR", "Y2050_NO-BUILD_ALL",
            "Y2050_BUILD_WLK", "Y2050_BUILD_KNR", "Y2050_BUILD_PNR", "Y2050_BUILD_XFR", "Y2050_BUILD_ALL"
        ]
        
        data_for_df = io.StringIO('\n'.join(actual_data_lines))
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names,
                         dtype={col: str for col in names})

        for col in df.columns:
            df[col] = df[col].str.strip()
            if col not in ["Stop_id1", "Station_Name"]:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)

        return df, metadata