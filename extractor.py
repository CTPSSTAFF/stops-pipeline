import pandas as pd
import io
import re
import os
import json
from pathlib import Path

class StopsPRNExtractor:
    """
    A class to systematically extract tables from STOPS program .PRN output files.
    This class contains multiple static methods, each designed to parse a specific
    table format from the text-based .PRN files.
    """
    _format_config = None

    @staticmethod
    def _get_table_format_config(config):
        """
        Reads the table format structure from the JSON file, converts it to a
        dictionary keyed by table_id, and caches it.
        """
        if StopsPRNExtractor._format_config is None:
            try:
                format_filepath = config.get("prn_table_format_structure_configfile")
                if not format_filepath:
                    print("ERROR: 'prn_table_format_structure_configfile' not specified in config.")
                    StopsPRNExtractor._format_config = {}
                    return {}
                
                format_file = Path(format_filepath)
                if not format_file.exists():
                    print(f"ERROR: Format definition file not found at {format_file.resolve()}")
                    StopsPRNExtractor._format_config = {}
                else:
                    with open(format_file, 'r', encoding='utf-8-sig') as f:
                        json_data_list = json.load(f)
                    StopsPRNExtractor._format_config = {item['table_id']: item for item in json_data_list}
            except Exception as e:
                print(f"ERROR: Could not read or parse {format_filepath}: {e}")
                StopsPRNExtractor._format_config = {}
        return StopsPRNExtractor._format_config

    @staticmethod
    def _generate_colspecs_from_widths(widths):
        """
        Generates a list of (start, end) tuples for pd.read_fwf from a list of widths.
        """
        colspecs = []
        start = 0
        for width in widths:
            end = start + width
            colspecs.append((start, end))
            start = end
        return colspecs

    @staticmethod
    def _extract_metadata_from_prn(lines, start_index):
        """Extracts metadata (Program, Version, Run, etc.) from the lines preceding a table."""
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
    def _extract_table_10_01_from_prn(file_path, table_id, config):
        """Specific extractor for Table 10.01 with dynamic year-based headers."""
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
                    if re.search(r"Route_ID.*WLK.*KNR", lines[i+2]):
                        if i + 3 < len(lines) and re.search(r"^=+", lines[i+3]):
                            start_of_table_data = i + 4
                            break 

        if start_of_table_data == -1 or header_line is None:
            return pd.DataFrame(), metadata
        
        main_headers = re.finditer(r"(Y20\d\d\s+[\w-]+)", header_line)
        header_info = []
        for m in main_headers:
            header_text = m.group(1).strip()
            generic_header_text = re.sub(r'^Y20\d\d\s+', '', header_text)
            generic_name = generic_header_text.replace(' ', '_').replace('-', '_')
            header_info.append({'name': generic_name, 'start': m.start()})
        
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
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names, dtype=str)

        for col in df.columns:
            if isinstance(df[col].dtype, object):
                df[col] = df[col].str.strip()
            if col not in ["Route_ID", "Route_Name"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)

        if not df.empty and "Route_Name" in df.columns and pd.notna(df.iloc[-1]["Route_Name"]) and str(df.iloc[-1]["Route_Name"]).strip().lower() == "total":
            df.at[df.index[-1], "Route_Name"] = "Total"
            df.at[df.index[-1], "Route_ID"] = "Total"
        return df, metadata

    @staticmethod
    def _extract_table_9_01_from_prn(file_path, table_id, config):
        """Specific extractor for Table 9.01 with dynamic year-based headers."""
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
        header_info = []
        for m in main_headers:
            header_text = m.group(1).strip()
            generic_header_text = re.sub(r'^Y20\d\d\s+', '', header_text)
            generic_name = generic_header_text.replace(' ', '_').replace('-', '_')
            header_info.append({'name': generic_name, 'start': m.start()})
        
        colspecs = [(0, 26), (26, 47)]
        names = ["Stop_id1", "Station_Name"]

        start_of_dynamic_columns = 47
        for i, main_header in enumerate(header_info):
            for j, sub_header in enumerate(["WLK", "KNR", "PNR", "XFR", "ALL"]):
                col_start = start_of_dynamic_columns + (i * 51) + (j * 10) - (1 if j > 0 else 0)
                col_end = col_start + 10
                if j == 0:
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
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names, dtype=str)

        for col in df.columns:
            if isinstance(df[col].dtype, object):
                df[col] = df[col].str.strip()
            if col not in ["Stop_id1", "Station_Name"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)

        if not df.empty and "Station_Name" in df.columns and pd.notna(df.iloc[-1]["Station_Name"]) and str(df.iloc[-1]["Station_Name"]).strip().lower() == "total":
            df.at[df.index[-1], "Station_Name"] = "Total"
            df.at[df.index[-1], "Stop_id1"] = "Total"
        return df, metadata
        
    @staticmethod
    def _extract_table_10_02_from_prn(file_path, table_id, config):
        """Specific extractor for Table 10.02 with dynamic year-based headers."""
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
        header_info = []
        for m in main_headers:
            header_text = m.group(1).strip()
            generic_header_text = re.sub(r'^Y20\d\d\s+', '', header_text)
            generic_name = generic_header_text.replace(' ', '_').replace('-', '_')
            header_info.append({'name': generic_name, 'start': m.start()})
        
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
        
        df["Route_ID"] = df["Route_ID"].str.strip().replace('', pd.NA).ffill()
        df['Route_Name'] = df['Group_Name'].apply(lambda x: x if pd.notna(x) and x.startswith('--') else pd.NA).ffill()
        df.loc[df['Group_Name'].str.startswith('--', na=False), 'Group_Name'] = pd.NA
        df["Group_Name"] = df["Group_Name"].str.strip().replace('', pd.NA)

        is_total_header = df['Route_ID'].str.lower().str.strip() == 'total'
        df.loc[is_total_header, 'Route_Name'] = 'Total'
        
        is_total_group_name = df['Group_Name'].str.lower().str.strip() == 'total'
        df.loc[is_total_group_name, 'Group_Name'] = 'Total'
        df.loc[is_total_group_name, 'Route_Name'] = 'Total'
        
        dynamic_cols = [name for name in names if name not in ["Route_ID", "Group_Name"]]
        final_names_ordered = ["Route_ID", "Route_Name", "Group_Name"] + dynamic_cols
        df = df[final_names_ordered]
        
        for col in dynamic_cols:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

        return df, metadata

    @staticmethod
    def _extract_table_11_XX_from_prn(file_path, table_id, config):
        """
        A function to extract tables 11.XX based on fixed-width format
        definitions provided in the prn_table_format_structure.json file.
        """
        metadata = {}
        data_text = []
        in_table_section = False
        start_of_data = -1
        
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
                if re.search(r"^[=-]+\s*.*", line.strip()) and i + 1 < len(lines):
                    start_of_data = i + 1
                    for j in range(start_of_data, min(start_of_data + 5, len(lines))):
                        if lines[j].strip() and not re.search(r"^[=-]+\s*.*", lines[j].strip()):
                            start_of_data = j
                            break
                    break
        
        if start_of_data == -1:
            return pd.DataFrame(), metadata

        format_config = StopsPRNExtractor._get_table_format_config(config)
        table_format = format_config.get(table_id)

        if not table_format or table_format.get("format_type") != "fixed_width":
            print(f"WARNING: No fixed_width format definition found for Table {table_id}. Skipping.")
            return pd.DataFrame(), metadata
        
        try:
            columns_def = table_format["columns"]
            names = [col["name"] for col in columns_def]
            widths = [col["width"] for col in columns_def]
            colspecs = StopsPRNExtractor._generate_colspecs_from_widths(widths)
        except (KeyError, TypeError) as e:
            print(f"ERROR: Invalid fixed_width format definition for Table {table_id} in JSON: {e}")
            return pd.DataFrame(), metadata

        for line in lines[start_of_data:]:
            if re.search(r"Table\s+\d+\.\d+", line) or re.search(r"Program STOPS", line) or "..." in line:
                break
            if not line.strip() or re.fullmatch(r"[-=]+\s*.*", line.strip()):
                continue
            data_text.append(line.rstrip())
        
        if not data_text:
            return pd.DataFrame(), metadata
        
        data_io = io.StringIO('\n'.join(data_text))
        
        df = pd.read_fwf(data_io, colspecs=colspecs, header=None, names=names, dtype=str)

        sep_cols = [col for col in df.columns if col.startswith('_sep')]
        df = df.drop(columns=sep_cols)

        if table_id.startswith('11.'):
            df = df[~df['HH_Cars'].str.strip().str.startswith('. . .', na=False)].copy()
            for col in df.columns:
                if isinstance(df[col].dtype, object):
                    df[col] = df[col].str.strip()
                if col not in ["HH_Cars", "Sub_mode", "Access_mode"]:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
            df['HH_Cars'] = df['HH_Cars'].mask(df['HH_Cars'].eq('')).ffill()
            df['Sub_mode'] = df['Sub_mode'].mask(df['Sub_mode'].eq('')).ffill()
        else:
            for col in df.select_dtypes(['object']):
                df[col] = df[col].str.strip()

        return df, metadata

    @staticmethod
    def _extract_district_table(file_path, table_id, config):
        """Extracts and pivots matrix-style 'District' tables."""
        metadata = {}
        data_lines = []
        in_table_section = False
        header_line = None
        start_of_data = -1

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+" + re.escape(table_id), line):
                in_table_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            if in_table_section and header_line is None and "Idist" in line:
                header_line = line
            
            if header_line and re.search(r"^=+", line.strip()):
                start_of_data = i + 1
                break
        
        if start_of_data == -1 or header_line is None:
            return pd.DataFrame(), metadata

        headers = header_line.strip().split()
        
        for line in lines[start_of_data:]:
            if "Total" in line or not line.strip():
                break
            data_lines.append(line)
            
        if not data_lines:
            return pd.DataFrame(), metadata

        data_io = io.StringIO("\n".join(data_lines))
        df = pd.read_csv(data_io, delim_whitespace=True, header=None, names=headers)
        
        df = df.melt(id_vars=['Idist'], var_name='Jdist', value_name='Trips')
        df.rename(columns={'Idist': 'Origin_District', 'Jdist': 'Destination_District'}, inplace=True)
        df = df[['Origin_District', 'Destination_District', 'Trips']]
        df['Trips'] = pd.to_numeric(df['Trips'], errors='coerce').fillna(0).astype(int)

        return df, metadata

    @staticmethod
    def _extract_station_group_table(file_path, table_id, config):
        """Extracts and pivots the two-line header 'Station Group' tables."""
        metadata = {}
        data_lines = []
        in_table_section = False
        header_line1 = None
        header_line2 = None
        start_of_data = -1

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        for i, line in enumerate(lines):
            if re.search(r"Table\s+" + re.escape(table_id), line):
                in_table_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            if in_table_section and "Origin Group" in line:
                header_line1 = lines[i+1] 
                header_line2 = lines[i+2] 
            
            if header_line2 and re.search(r"^=+", line.strip()):
                start_of_data = i + 1
                break
        
        if start_of_data == -1 or header_line2 is None:
            return pd.DataFrame(), metadata

        h1_parts = header_line1.strip().split()
        h2_parts = header_line2.strip().split()
        
        headers = [f"{h1}-{h2}" for h1, h2 in zip(h1_parts, h2_parts)] + h2_parts[len(h1_parts):]
        
        for line in lines[start_of_data:]:
            if "TOTAL" in line or not line.strip():
                break
            cleaned_line = re.sub(r'^\s*\d+-(.*?)\s*:', r'\1', line).strip()
            data_lines.append(cleaned_line)

        if not data_lines:
            return pd.DataFrame(), metadata

        data_io = io.StringIO("\n".join(data_lines))
        df = pd.read_csv(data_io, delim_whitespace=True, header=None)
        
        df.columns = ["Origin_Group"] + headers
        
        id_vars = ["Origin_Group", "TOTAL", "GOAL", "COUNT"]
        value_vars = [h for h in headers if h not in ["TOTAL", "GOAL", "COUNT"]]
        
        df_melted = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='Destination_Group', value_name='Value')

        return df_melted, metadata

def get_extraction_method(table_id_str, config):
    """
    Dynamically gets the extraction method based on the function name
    specified in the JSON config file.
    """
    format_config = StopsPRNExtractor._get_table_format_config(config)
    table_format = format_config.get(table_id_str)

    if not table_format:
        print(f"WARNING: No configuration found for Table {table_id_str}.")
        return None

    function_name = table_format.get("extraction_function")
    if not function_name:
        print(f"WARNING: 'extraction_function' not specified for Table {table_id_str} in config.")
        return None

    try:
        # Use getattr to get the method from the class by its string name
        extraction_func = getattr(StopsPRNExtractor, function_name)
        return extraction_func
    except AttributeError:
        print(f"ERROR: The function '{function_name}' specified for Table {table_id_str} does not exist in the StopsPRNExtractor class.")
        return None

def run_extraction(config):
    """Main function to run the data extraction process from config."""
    print("--- 🎬 Starting Data Extraction ---")
    
    base_prn_dir = Path(config["prn_files_folderpath"])
    csv_output_dir = Path(config["csv_output_folderpath"])
    csv_output_dir.mkdir(parents=True, exist_ok=True)
    
    extraction_config = config["prn_files_data_extraction_config"]
    files_to_process = extraction_config["files_to_process"]
    tables_to_extract = extraction_config["tables_to_extract"]

    for file_info in files_to_process:
        alias = file_info["alias"]
        filename = file_info["filename"]
        
        if file_info.get("is_full_folderpath", False):
            file_path = Path(filename)
        else:
            file_path = base_prn_dir / filename

        if not file_path.exists():
            print(f"❗️ WARNING: File not found for alias '{alias}': {file_path}. Skipping.")
            continue
            
        print(f"\nProcessing File: '{file_path.name}' (Alias: '{alias}')")

        for table_id in tables_to_extract:
            table_id_str = str(table_id)
            print(f"  -> Attempting to extract Table {table_id_str}...")
            extraction_func = get_extraction_method(table_id_str, config)
            
            if not extraction_func:
                print(f"     - No extraction method found for Table {table_id_str}. Skipping.")
                continue

            df, metadata = extraction_func(str(file_path), table_id_str, config)
            
            if df.empty:
                print(f"     - No data found for Table {table_id_str} in this file.")
                continue

            table_folder_name = f"Table_{table_id_str.replace('.', '_')}"
            table_output_dir = csv_output_dir / table_folder_name
            table_output_dir.mkdir(parents=True, exist_ok=True)
            output_filename = f"[{alias}]__Table_{table_id_str.replace('.', '_')}.csv"
            output_path = table_output_dir / output_filename

            df.to_csv(output_path, index=False)
            print(f"     ✅ Successfully saved to: {output_path}")

    print("\n--- ✅ Data Extraction Complete ---")