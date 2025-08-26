import pandas as pd
import io
import re
import os
from pathlib import Path

class StopsPRNExtractor:
    """
    A class to systematically extract tables from STOPS program .PRN output files.
    This class contains multiple static methods, each designed to parse a specific
    table format from the text-based .PRN files.
    """

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
    def _extract_table_10_01_from_prn(file_path, table_id):
        metadata = {}
        actual_data_lines = []
        in_table_section = False
        start_of_table_data = -1
        colspecs = []
        names = []
        header_line = None
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+" + re.escape(table_id), line):
                in_table_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            if in_table_section and start_of_table_data == -1:
                if re.search(r"Y20\d\d", line) and i + 2 < len(lines):
                    header_line = line
                    if re.search(r"Route_ID.*WLK.*KNR", lines[i+2]):
                        if i + 3 < len(lines) and re.search(r"^=+", lines[i+3]):
                            start_of_table_data = i + 4
                            break 

        if start_of_table_data == -1 or header_line is None:
            return pd.DataFrame(), metadata
        
        main_headers = re.finditer(r"(Y20\d\d\s+[\w-]+)", header_line)
        header_info = [{'name': m.group(1).strip().replace(' ', '_'), 'start': m.start()} for m in main_headers]
        
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
        
        if not actual_data_lines:
            return pd.DataFrame(), metadata
        
        data_for_df = io.StringIO('\n'.join(actual_data_lines))
        dtypes_dict = {col: str for col in names}
        
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names, dtype=dtypes_dict)

        for col in df.columns:
            if isinstance(df[col].dtype, object):
                df[col] = df[col].str.strip()
            if col not in ["Route_ID", "Route_Name"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)

        if not df.empty and "Route_Name" in df.columns:
            last_row = df.iloc[-1]
            if pd.notna(last_row["Route_Name"]):
                route_name_val = str(last_row["Route_Name"]).strip().lower()
                if route_name_val == "total":
                    df.at[df.index[-1], "Route_Name"] = "Total"
                    df.at[df.index[-1], "Route_ID"] = "Total"
        return df, metadata

    @staticmethod
    def _extract_table_9_01_from_prn(file_path, table_id):
        metadata = {}
        actual_data_lines = []
        in_table_section = False
        start_of_table_data = -1
        header_line = None
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+" + re.escape(table_id), line):
                in_table_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)

            if in_table_section and start_of_table_data == -1:
                if re.search(r"Y20\d\d", line) and i + 2 < len(lines):
                    header_line = line
                    if re.search(r"Stop_id1.*WLK.*KNR", lines[i+2]):
                        if i + 3 < len(lines) and re.search(r"^=+", lines[i+3]):
                            start_of_table_data = i + 4
                            break

        if start_of_table_data == -1 or header_line is None:
            return pd.DataFrame(), metadata
        
        main_headers = re.finditer(r"(Y20\d\d\s+[\w-]+)", header_line)
        header_info = [{'name': m.group(1).strip().replace(' ', '_'), 'start': m.start()} for m in main_headers]
        
        colspecs = [(0, 26), (26, 47)]
        names = ["Stop_id1", "Station_Name"]

        start_of_dynamic_columns = 47
        for i, main_header in enumerate(header_info):
            for j, sub_header in enumerate(["WLK", "KNR", "PNR", "XFR", "ALL"]):
                col_start = start_of_dynamic_columns + (i * 51) + (j * 10) - (1 if j > 0 else 0) # Adjusted for spacing
                col_end = col_start + 10
                if j == 0: # WLK is wider
                    col_end += 1
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
        
        if not actual_data_lines:
            return pd.DataFrame(), metadata

        data_for_df = io.StringIO('\n'.join(actual_data_lines))
        dtypes_dict = {col: str for col in names}
        
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names, dtype=dtypes_dict)

        for col in df.columns:
            if isinstance(df[col].dtype, object):
                df[col] = df[col].str.strip()
            if col not in ["Stop_id1", "Station_Name"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)

        if not df.empty and "Station_Name" in df.columns:
            last_row = df.iloc[-1]
            if pd.notna(last_row["Station_Name"]):
                station_name_val = str(last_row["Station_Name"]).strip().lower()
                if station_name_val == "total":
                    df.at[df.index[-1], "Station_Name"] = "Total"
                    df.at[df.index[-1], "Stop_id1"] = "Total"
        return df, metadata

    @staticmethod
    def _extract_table_10_02_from_prn(file_path, table_id):
        metadata = {}
        all_data_text = []
        in_table_section = False
        start_of_data = -1
        header_line = None
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}
        
        for i, line in enumerate(lines):
            if re.search(r"Table\s+" + re.escape(table_id), line):
                in_table_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            if in_table_section and start_of_data == -1:
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
        
        df['Route_Name'] = df['Group_Name'].apply(lambda x: x if pd.notna(x) and x.startswith('--') else pd.NA)
        
        df['Route_ID'] = df['Route_ID'].ffill()
        df['Route_Name'] = df['Route_Name'].ffill()
        
        df.loc[df['Group_Name'].str.startswith('--', na=False), 'Group_Name'] = pd.NA
        
        # MODIFIED: Removed 'na=False' which is an invalid argument for the .eq() method.
        is_total_header = df['Route_ID'].str.lower().str.strip().eq('total')
        df.loc[is_total_header, 'Route_Name'] = 'Total'
        
        # MODIFIED: Removed 'na=False' which is an invalid argument for the .eq() method.
        is_total_group_name = df['Group_Name'].str.lower().str.strip().eq('total')
        df.loc[is_total_group_name, 'Group_Name'] = 'Total'
        df.loc[is_total_group_name, 'Route_Name'] = 'Total'
        
        dynamic_cols = [name for name in names if name not in ["Route_ID", "Group_Name", "Count"]]
        final_names_ordered = ["Route_ID", "Route_Name", "Group_Name", "Count"] + dynamic_cols
        df = df[final_names_ordered]
        
        for col in df.columns:
             if col not in ["Route_ID", "Route_Name", "Group_Name"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        return df, metadata
    
    @staticmethod
    def _extract_table_11_from_prn(file_path, table_id):
        metadata = {}
        data_text = []
        in_table_section = False
        start_of_data = -1
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
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
                    if i + 2 < len(lines) and re.search(r"=", lines[i + 2]):
                        start_of_data = i + 3
                        break
        
        if start_of_data == -1 or header_line is None:
            return pd.DataFrame(), metadata

        colspecs = [(0, 9), (9, 29), (29, 43), (44, 52), (52, 61), (61, 70), 
                    (70, 79), (79, 88), (88, 97), (97, 106), (106, 115)]
        
        names = ["HH_Cars", "Sub_mode", "Access_mode", "Y2024_EXISTING_Model", "Y2024_EXISTING_Survey", 
                 "Y2050_NO_BUILD_Model", "Y2050_NO_BUILD_Survey", "Y2050_BUILD_Model", "Y2050_BUILD_Survey", 
                 "Y2050_BUILD_Project_Model", "Y2050_BUILD_Project_Survey"]
        
        for line in lines[start_of_data:]:
            if re.search(r"Table\s+\d+\.\d+", line) or re.search(r"Program STOPS", line):
                break
            if re.fullmatch(r"[-=]+\s*.*", line.strip()) or not line.strip():
                continue
            data_text.append(line.rstrip())
        
        if not data_text:
            return pd.DataFrame(), metadata
        
        data_io = io.StringIO('\n'.join(data_text))
        df = pd.read_fwf(data_io, colspecs=colspecs, header=None, names=names, dtype=str)

        for col in df.columns:
            if isinstance(df[col].dtype, object):
                df[col] = df[col].str.replace('|', '', regex=False).str.strip()
            
            if col not in ["HH_Cars", "Sub_mode", "Access_mode"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
                df[col] = df[col].fillna(0).astype(int)
        
        df['HH_Cars'] = df['HH_Cars'].mask(df['HH_Cars'].eq('')).ffill()
        df['Sub_mode'] = df['Sub_mode'].mask(df['Sub_mode'].eq('')).ffill()
        
        return df, metadata


def get_extraction_method(table_id_str):
    """Dynamically gets the correct extraction method based on the table ID."""
    if table_id_str.startswith("11"):
        return StopsPRNExtractor._extract_table_11_from_prn
    
    # Map specific table IDs to their extraction methods
    method_map = {
        "9.01": StopsPRNExtractor._extract_table_9_01_from_prn,
        "10.01": StopsPRNExtractor._extract_table_10_01_from_prn,
        "10.02": StopsPRNExtractor._extract_table_10_02_from_prn,
    }
    return method_map.get(table_id_str)


def run_extraction(config):
    """Main function to run the data extraction process from config."""
    print("--- 🎬 Starting Data Extraction ---")
    base_prn_dir = Path(config["prn_files_folderpath"])
    csv_output_dir = Path(config["csv_output_folderpath"])
    extraction_config = config["prn_files_data_extraction_config"]
    files_to_process = extraction_config["files_to_process"]
    tables_to_extract = extraction_config["tables_to_extract"]

    for file_info in files_to_process:
        alias = file_info["alias"]
        filename = file_info["filename"]
        
        file_path = base_prn_dir / filename

        if not file_path.exists():
            print(f"❗️ WARNING: File not found for alias '{alias}': {file_path}. Skipping.")
            continue
            
        print(f"\nProcessing File: '{file_path.name}' (Alias: '{alias}')")

        for table_id in tables_to_extract:
            print(f"  -> Attempting to extract Table {table_id}...")
            extraction_func = get_extraction_method(table_id)
            
            if not extraction_func:
                print(f"     - No extraction method found for Table {table_id}. Skipping.")
                continue

            df, metadata = extraction_func(str(file_path), table_id)
            
            if df.empty:
                print(f"     - No data found for Table {table_id} in this file.")
                continue

            table_folder_name = f"Table_{table_id.replace('.', '_')}"
            table_output_dir = csv_output_dir / table_folder_name
            table_output_dir.mkdir(parents=True, exist_ok=True)
            output_filename = f"[{alias}]__Table_{table_id.replace('.', '_')}.csv"
            output_path = table_output_dir / output_filename

            df.to_csv(output_path, index=False)
            print(f"     ✅ Successfully saved to: {output_path}")

    print("\n--- ✅ Data Extraction Complete ---")