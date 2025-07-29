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
            self.tables[alias] = df
            self.metadata[alias] = meta

    def extract_table_9_01(self):
        """
        Extract Table 9.01 and metadata from all loaded files.
        Populates self.tables and self.metadata.
        """
        for alias, file_path in self.files.items():
            df, meta = self._extract_table_9_01_from_prn(file_path)
            self.tables[f"{alias}_table_9_01"] = df # Store with a distinct key
            self.metadata[f"{alias}_table_9_01"] = meta # Store with a distinct key

    @staticmethod
    def _extract_metadata_from_prn(lines, start_index):
        """
        Helper method to extract common metadata from PRN file lines.
        """
        metadata = {}
        for meta_line_num in range(start_index - 7, start_index - 1):
            if meta_line_num >= 0:
                meta_line = lines[meta_line_num].strip()
                if "Program STOPS" in meta_line:
                    program_version_parts = meta_line.split(" - ", 1)
                    if len(program_version_parts) > 0:
                        metadata["Program"] = program_version_parts[0].replace("Program ", "").strip()
                    if len(program_version_parts) > 1 and "Version:" in program_version_parts[1]:
                        metadata["Version"] = program_version_parts[1].split("Version: ")[1].split(" - ")[0].strip()
                    elif "Version:" in meta_line:
                        version_match = re.search(r'Version:\s*(\S+)\s*-\s*(\d{2}/\d{2}/\d{4})', meta_line)
                        if version_match:
                            metadata["Version"] = f"{version_match.group(1)} - {version_match.group(2)}"
                elif "Run:" in meta_line:
                    parts = meta_line.split("Run:")
                    if len(parts) > 1:
                        run_system_part = parts[1].strip()
                        if "System:" in run_system_part:
                            run_parts = run_system_part.split("System:")
                            metadata["Run"] = run_parts[0].strip()
                            metadata["System"] = run_parts[1].strip()
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
            print(f"Error: The file '{file_path}' was not found.")
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if "Table    10.01" in line:
                in_table_10_01_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
                continue

            if in_table_10_01_section:
                if "=======================================" in lines[i] and i > 0 and "Route_ID" in lines[i-1]:
                    if i + 2 < len(lines):
                        started_collecting_data = True
                        continue
                    else:
                        in_table_10_01_section = False
                        break

                if started_collecting_data:
                    if "Table    10.02" in line or (line.strip() and "Program STOPS" in line):
                        in_table_10_01_section = False
                        break
                    if line.strip() and not all(char == '=' for char in line.strip()) and not all(char == '-' for char in line.strip()):
                        actual_data_lines.append(line.rstrip())

        if not actual_data_lines:
            print("Table 10.01 data content not found or incorrectly parsed.")
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
            print(f"Error: The file '{file_path}' was not found.")
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if "Table    9.01" in line:
                in_table_9_01_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
                continue

            if in_table_9_01_section:
                # Look for the header line before the data starts
                if "=================================================" in lines[i] and i > 0 and "Station Name" in lines[i-1]:
                    if i + 2 < len(lines):
                        started_collecting_data = True
                        continue
                    else:
                        in_table_9_01_section = False
                        break

                if started_collecting_data:
                    # Stop collecting when the next table or program info appears
                    if "Table    10.01" in line or (line.strip() and "Program STOPS" in line):
                        in_table_9_01_section = False
                        break
                    # Only append lines that are not empty and not separator lines
                    if line.strip() and not all(char == '=' for char in line.strip()) and not all(char == '-' for char in line.strip()):
                        actual_data_lines.append(line.rstrip())

        if not actual_data_lines:
            print("Table 9.01 data content not found or incorrectly parsed.")
            return pd.DataFrame(), metadata

        # Define column specifications for Table 9.01 based on your provided sample
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

        # Clean and convert numeric columns
        for col in df.columns:
            df[col] = df[col].str.strip()
            if col not in ["Stop_id1", "Station_Name"]:
                df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)

        return df, metadata