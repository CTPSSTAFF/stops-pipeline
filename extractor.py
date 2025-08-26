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
    def _extract_table_9_01_from_prn(file_path, table_id, config):
        """Extractor for Table 9.01. Uses column definitions from JSON config."""
        metadata = {}
        actual_data_lines = []
        in_table_section = False
        start_of_table_data = -1
        
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
                if (re.search(r"Stop_id1.*WLK.*KNR", line) or re.search(r"Stop_id1", line)):
                     if i + 1 < len(lines) and re.search(r"^=+", lines[i+1]):
                         start_of_table_data = i + 2
                         break
        if start_of_table_data == -1:
             return pd.DataFrame(), metadata

        # REFACTORED: Get column definitions from the JSON config
        format_config = StopsPRNExtractor._get_table_format_config(config)
        table_format = format_config.get(table_id)
        try:
            columns_def = table_format["columns"]
            names = [col["name"] for col in columns_def]
            widths = [col["width"] for col in columns_def]
            colspecs = StopsPRNExtractor._generate_colspecs_from_widths(widths)
        except (KeyError, TypeError) as e:
            print(f"ERROR: Invalid 'columns' format for Table {table_id} in JSON: {e}")
            return pd.DataFrame(), metadata
        
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
    def _extract_table_10_01_from_prn(file_path, table_id, config):
        """Extractor for Table 10.01. Uses column definitions from JSON config."""
        metadata = {}
        actual_data_lines = []
        in_table_section = False
        start_of_table_data = -1
        
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
                if (re.search(r"Route_ID.*WLK.*KNR", line) or re.search(r"Route_ID", line)):
                    # Find the "====" separator line that follows the header
                    if i + 1 < len(lines) and re.search(r"^=+", lines[i+1]):
                        start_of_table_data = i + 2
                        break
        
        if start_of_table_data == -1:
            return pd.DataFrame(), metadata
        
        # REFACTORED: Get column definitions from the JSON config
        format_config = StopsPRNExtractor._get_table_format_config(config)
        table_format = format_config.get(table_id)
        try:
            columns_def = table_format["columns"]
            names = [col["name"] for col in columns_def]
            widths = [col["width"] for col in columns_def]
            colspecs = StopsPRNExtractor._generate_colspecs_from_widths(widths)
        except (KeyError, TypeError) as e:
            print(f"ERROR: Invalid 'columns' format for Table {table_id} in JSON: {e}")
            return pd.DataFrame(), metadata
        
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
            # Infer which columns should be numeric based on name
            if col not in ["Route_ID", "Route_Name", "Station_Name", "Stop_id1", "Group_Name", "HH_Cars", "Sub_mode", "Access_mode"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)

        if not df.empty and "Route_Name" in df.columns and pd.notna(df.iloc[-1]["Route_Name"]) and str(df.iloc[-1]["Route_Name"]).strip().lower() == "total":
            df.at[df.index[-1], "Route_Name"] = "Total"
            df.at[df.index[-1], "Route_ID"] = "Total"
        return df, metadata

    @staticmethod
    def _extract_table_10_02_from_prn(file_path, table_id, config):
        """Extractor for Table 10.02. Uses column definitions from JSON config and handles indented groups."""
        metadata = {}
        all_data_text = []
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
                if re.search(r"Route_ID.*Count", line):
                    if i + 1 < len(lines) and re.search(r"^=+", lines[i + 1]):
                        start_of_data = i + 2
                        break
        
        if start_of_data == -1:
            return pd.DataFrame(), metadata
        
        # Get column definitions from the JSON config
        format_config = StopsPRNExtractor._get_table_format_config(config)
        table_format = format_config.get(table_id)
        try:
            columns_def = table_format["columns"]
            names = [col["name"] for col in columns_def]
            widths = [col["width"] for col in columns_def]
            colspecs = StopsPRNExtractor._generate_colspecs_from_widths(widths)
        except (KeyError, TypeError) as e:
            print(f"ERROR: Invalid 'columns' format for Table {table_id} in JSON: {e}")
            return pd.DataFrame(), metadata

        # MODIFIED: Robust data collection loop.
        # This loop now reads until the next table begins and filters out junk lines.
        for line in lines[start_of_data:]:
            # Stop processing ONLY if we hit the start of the next table or a new report page
            if re.search(r"Table\s+\d+\.\d+", line) or re.search(r"Program STOPS", line):
                break
            
            # Filter out empty lines and separator lines (e.g., '====' or '----')
            stripped_line = line.strip()
            if not stripped_line or re.fullmatch(r"[-=]{2,}", stripped_line):
                continue

            all_data_text.append(line)
        
        if not all_data_text:
            return pd.DataFrame(), metadata
        
        data_io = io.StringIO('\n'.join(all_data_text))
        df = pd.read_fwf(data_io, colspecs=colspecs, header=None, names=names, dtype=str)
        
        # Keep specialized cleanup logic for indented groups
        df["Route_ID"] = df["Route_ID"].str.strip().replace('', pd.NA).ffill()
        df['Route_Name'] = df['Group_Name'].apply(lambda x: x if pd.notna(x) and x.startswith('--') else pd.NA).ffill()
        df.loc[df['Group_Name'].str.startswith('--', na=False), 'Group_Name'] = pd.NA
        df["Group_Name"] = df["Group_Name"].str.strip().replace('', pd.NA)

        is_total_header = df['Route_ID'].str.lower().str.strip() == 'total'
        df.loc[is_total_header, 'Route_Name'] = 'Total'
        
        is_total_group_name = df['Group_Name'].str.lower().str.strip() == 'total'
        df.loc[is_total_group_name, 'Group_Name'] = 'Total'
        df.loc[is_total_group_name, 'Route_Name'] = 'Total'
        
        # Reorder columns to ensure Route_Name is in the right place
        if 'Route_Name' in df.columns:
            static_cols = ["Route_ID", "Route_Name", "Group_Name"]
            dynamic_cols = [name for name in names if name not in static_cols]
            final_names_ordered = static_cols + dynamic_cols
            df = df[[col for col in final_names_ordered if col in df.columns]]
        
        for col in df.columns:
            if col not in ["Route_ID", "Route_Name", "Group_Name"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

        return df, metadata

    @staticmethod
    def _extract_table_10_03_04_from_prn(file_path, table_id, config):
        """Extractor for Tables 10.03 & 10.04. Uses column definitions from JSON config."""
        metadata = {}
        actual_data_lines = []
        in_table_section = False
        start_of_table_data = -1
        
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
                if re.search(r"Route_ID.*Hours", line):
                    if i + 1 < len(lines) and re.search(r"^=+", lines[i+1]):
                        start_of_table_data = i + 2
                        break
        
        if start_of_table_data == -1:
            return pd.DataFrame(), metadata
        
        format_config = StopsPRNExtractor._get_table_format_config(config)
        table_format = format_config.get(table_id)
        try:
            columns_def = table_format["columns"]
            names = [col["name"] for col in columns_def]
            widths = [col["width"] for col in columns_def]
            colspecs = StopsPRNExtractor._generate_colspecs_from_widths(widths)
        except (KeyError, TypeError) as e:
            print(f"ERROR: Invalid 'columns' format for Table {table_id} in JSON: {e}")
            return pd.DataFrame(), metadata
        
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
        # FIX: Read all columns as strings first to prevent dtype inference errors.
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names, dtype=str)

        if "Route_Name" in df.columns:
            df['Route_Name'] = df['Route_Name'].str.lstrip(' -')

        # FIX: Robustly clean and convert data types after ensuring all are strings.
        for col in df.columns:
            df[col] = df[col].str.strip()
            
            if "Miles" in col or "Hours" in col:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            elif col not in ["Route_ID", "Route_Name"]:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        if not df.empty and pd.notna(df.iloc[-1]["Route_Name"]) and str(df.iloc[-1]["Route_Name"]).strip().lower() == "total":
            df.at[df.index[-1], "Route_Name"] = "Total"
            df.at[df.index[-1], "Route_ID"] = "Total"
        
        return df, metadata

    @staticmethod
    def _extract_table_10_05_from_prn(file_path, table_id, config):
        """Extractor for Table 10.05. Uses column definitions from JSON config."""
        metadata = {}
        actual_data_lines = []
        in_table_section = False
        start_of_table_data = -1
        
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
                if re.search(r"Route_ID.*ALL", line):
                    if i + 1 < len(lines) and re.search(r"^=+", lines[i+1]):
                        start_of_table_data = i + 2
                        break
        
        if start_of_table_data == -1:
            return pd.DataFrame(), metadata
        
        format_config = StopsPRNExtractor._get_table_format_config(config)
        table_format = format_config.get(table_id)
        try:
            columns_def = table_format["columns"]
            names = [col["name"] for col in columns_def]
            widths = [col["width"] for col in columns_def]
            colspecs = StopsPRNExtractor._generate_colspecs_from_widths(widths)
        except (KeyError, TypeError) as e:
            print(f"ERROR: Invalid 'columns' format for Table {table_id} in JSON: {e}")
            return pd.DataFrame(), metadata
        
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
        # FIX: Read all columns as strings first to prevent dtype inference errors.
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names, dtype=str)

        if "Route_Name" in df.columns:
            df['Route_Name'] = df['Route_Name'].str.lstrip(' -')

        # FIX: Robustly clean and convert data types.
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
    def _extract_table_10_06_from_prn(file_path, table_id, config):
        """
        Custom extractor for Table 10.06 using a fixed-width read followed by
        post-processing to handle its hierarchical structure.
        """
        metadata = {}
        data_lines = []
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
            
            if in_table_section and re.search(r"^=+", line):
                start_of_data = i + 1
                break
        
        if start_of_data == -1:
            return pd.DataFrame(), metadata

        # Collect all relevant lines from the table body
        for line in lines[start_of_data:]:
            if not line.strip() or "Total" in line or re.search(r"^\s*\.", line) or re.search(r"Table\s+\d+\.\d+", line):
                break
            data_lines.append(line)
        
        if not data_lines:
            return pd.DataFrame(), metadata

        # Get column definitions from the JSON config
        format_config = StopsPRNExtractor._get_table_format_config(config)
        table_format = format_config.get(table_id)
        try:
            columns_def = table_format["columns"]
            names = [col["name"] for col in columns_def]
            widths = [col["width"] for col in columns_def]
            colspecs = StopsPRNExtractor._generate_colspecs_from_widths(widths)
        except (KeyError, TypeError) as e:
            print(f"ERROR: Invalid 'columns' format for Table {table_id} in JSON: {e}")
            return pd.DataFrame(), metadata
        
        data_io = io.StringIO('\n'.join(data_lines))
        df = pd.read_fwf(data_io, colspecs=colspecs, header=None, names=names, dtype=str)
        
        # --- Post-processing to handle the hierarchical structure ---
        
        # 1. Drop the separator columns
        sep_cols = [col for col in df.columns if col.startswith('_sep')]
        df.drop(columns=sep_cols, inplace=True)

        # 2. Forward-fill the 'to_Route' columns to propagate their values down
        df['to_Route_ID'] = df['to_Route_ID'].str.strip().replace('', pd.NA)
        df['to_Route_Name'] = df['to_Route_Name'].str.strip().replace('', pd.NA)
        
        # FIX: Use reassignment to avoid FutureWarning and ensure operation works
        df['to_Route_ID'] = df['to_Route_ID'].ffill()
        df['to_Route_Name'] = df['to_Route_Name'].ffill()

        # 3. Drop any rows that are not real data (where from_Route_ID is empty)
        df.dropna(subset=['EXISTING_from_Route_ID'], inplace=True)
        df = df[df['EXISTING_from_Route_ID'].str.strip() != ''].copy()

        # 4. Clean up all columns: strip whitespace, fix names, and convert types
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()
                if 'Route_Name' in col:
                    df[col] = df[col].str.lstrip(' -')
            
            if 'Transfers' in col:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        return df, metadata

    @staticmethod
    def _extract_table_12_01_from_prn(file_path, table_id, config):
        """Extractor for Table 12.01. Uses column definitions from JSON config."""
        metadata = {}
        actual_data_lines = []
        in_table_section = False
        start_of_table_data = -1
        
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
                if (re.search(r"^={8,}", line)):
                    start_of_table_data = i + 1
                    break
        if start_of_table_data == -1:
             return pd.DataFrame(), metadata

        format_config = StopsPRNExtractor._get_table_format_config(config)
        table_format = format_config.get(table_id)
        try:
            columns_def = table_format["columns"]
            names = [col["name"] for col in columns_def]
            widths = [col["width"] for col in columns_def]
            colspecs = StopsPRNExtractor._generate_colspecs_from_widths(widths)
        except (KeyError, TypeError) as e:
            print(f"ERROR: Invalid 'columns' format for Table {table_id} in JSON: {e}")
            return pd.DataFrame(), metadata
        
        for line_to_collect in lines[start_of_table_data:]:
            if line_to_collect.strip().startswith("Total"):
                break
            if re.search(r"Table\s+\d+\.\d+", line_to_collect) or (line_to_collect.strip() and "Program STOPS" in line_to_collect):
                break
            if line_to_collect.strip():
                actual_data_lines.append(line_to_collect.rstrip())
        
        if not actual_data_lines:
            return pd.DataFrame(), metadata

        data_for_df = io.StringIO('\n'.join(actual_data_lines))
        df = pd.read_fwf(data_for_df, colspecs=colspecs, header=None, names=names, dtype=str)

        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()
            if col not in ["District"]:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

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

        # Specialized cleanup for Table 11.XX
        df = df[~df['HH_Cars'].str.strip().str.startswith('. . .', na=False)].copy()
        for col in df.columns:
            if isinstance(df[col].dtype, object):
                df[col] = df[col].str.strip()
            if col not in ["HH_Cars", "Sub_mode", "Access_mode"]:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        df['HH_Cars'] = df['HH_Cars'].mask(df['HH_Cars'].eq('')).ffill()
        df['Sub_mode'] = df['Sub_mode'].mask(df['Sub_mode'].eq('')).ffill()

        return df, metadata

    @staticmethod
    def _extract_district_table(file_path, table_id, config):
        """
        REVISED: Extracts and pivots matrix-style 'District' tables using manual parsing.
        This version correctly handles the table's structure by separating the row
        header from the numeric data, avoiding the errors caused by pd.read_csv.
        """
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

        # 1. Find the start of the table, the header line, and the start of the data
        for i, line in enumerate(lines):
            stripped_line = line.strip()
            if re.search(r"Table\s+" + re.escape(table_id), line):
                in_table_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)
            
            # FIX: Make header detection more specific. The header line must START with "Idist" or "District".
            if in_table_section and header_line is None and (stripped_line.startswith("Idist")):
            # if in_table_section and header_line is None and (stripped_line.startswith("Idist") or stripped_line.startswith("District")):
                header_line = line
            
            if header_line and re.search(r"^=+", stripped_line):
                start_of_data = i + 1
                break
        
        if start_of_data == -1 or header_line is None:
            # Add a warning if the header was not found, which is a common failure point.
            print(f"     - WARNING: Could not find a valid header row for Table {table_id}. Skipping.")
            return pd.DataFrame(), metadata

        # 2. Parse the headers from the identified header line
        headers = header_line.strip().split()
        if headers[0].lower() in ['idist', 'district']:
            headers[0] = "Origin_District"
        
        # 3. Collect the actual data lines, stopping at the "Total" summary row
        for line in lines[start_of_data:]:
            # The "Total" row signals the end of the main data matrix
            if line.strip().startswith("Total") or not line.strip():
                break
            data_lines.append(line.strip())
            
        if not data_lines:
            return pd.DataFrame(), metadata

        # 4. Manually parse each data row
        parsed_rows = []
        for line in data_lines:
            parts = line.split()
            if len(parts) > 1:
                parsed_rows.append(parts)

        if not parsed_rows:
            return pd.DataFrame(), metadata

        # 5. Create the DataFrame from the parsed rows and headers
        # Ensure that the number of columns assigned matches the data
        num_data_cols = len(parsed_rows[0])
        # A safety check in case the header has more parts than the data rows
        if len(headers) < num_data_cols:
             print(f"     - WARNING: Mismatch in Table {table_id}. Header has {len(headers)} columns, data has {num_data_cols}. Truncating data.")
             parsed_rows = [row[:len(headers)] for row in parsed_rows]
             num_data_cols = len(headers)

        df = pd.DataFrame(parsed_rows, columns=headers[:num_data_cols])
        
        # 6. Convert data types
        for col in df.columns:
            if col != 'Origin_District':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        return df, metadata

    @staticmethod
    def _extract_station_group_table(file_path, table_id, config):
        """
        Dynamically extracts various "Station Group" table formats.
        This version uses a robust manual parsing method based on content type
        (text vs. number) to correctly handle all table variations and summary columns.
        Table 2.04 has special handling to include its summary rows (TOTAL, GOAL, COUNT).
        """
        metadata = {}
        in_table_section = False
        header_line_list = []
        separator_index = -1
        is_two_line_header = False

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except FileNotFoundError:
            return pd.DataFrame(), {}

        # 1. FIND HEADERS AND DATA START
        for i, line in enumerate(lines):
            if re.search(r"Table\s+" + re.escape(table_id), line):
                in_table_section = True
                metadata = StopsPRNExtractor._extract_metadata_from_prn(lines, i)

            if in_table_section and separator_index == -1 and re.search(r"^=+", line.strip()):
                separator_index = i
                if separator_index > 0:
                    header_line_list.insert(0, lines[separator_index - 1])
                if separator_index > 1:
                    prev_line = lines[separator_index - 2].strip()
                    if prev_line and not re.search(r"^=+", prev_line) and "Group" not in prev_line and "District" not in prev_line:
                        header_line_list.insert(0, lines[separator_index - 2])
                        is_two_line_header = True
                break

        if separator_index == -1:
            return pd.DataFrame(), metadata

        start_of_data = separator_index + 1

        # 2. PARSE HEADERS TO GET FULL LIST OF EXPECTED COLUMNS
        headers = []
        if is_two_line_header:
            h1_parts = header_line_list[0].strip().split()
            h2_parts = header_line_list[1].strip().split()
            headers.extend(h2_parts)
            headers.extend(h1_parts[len(h2_parts):])
        else:
            parts = header_line_list[0].strip().split()
            headers = parts[1:]

        # 3. MANUALLY PARSE DATA ROWS BASED ON CONTENT
        parsed_rows = []
        
        # MODIFIED: Define the prefixes that signal the end of the data section.
        stop_prefixes = ("2-WAY",) # Always a stop word
        if table_id != "2.04":
            # For other tables, stop at the summary footer.
            stop_prefixes += ("TOTAL", "GOAL", "COUNT")
        
        for line in lines[start_of_data:]:
            stripped_line = line.strip()
            
            # MODIFIED: Use the new stop_prefixes tuple and stop on new page headers.
            if not stripped_line or stripped_line.upper().startswith(stop_prefixes) or "Program STOPS" in line:
                break
            
            parts = stripped_line.split()
            if not parts:
                continue
            
            first_number_idx = -1
            for i, part in enumerate(parts):
                try:
                    # A simple check to find the boundary between text label and numeric data.
                    # This will fail on text, colons, and hyphens, which is the desired behavior.
                    # Added replace for commas.
                    float(part.replace(',', ''))
                    first_number_idx = i
                    break 
                except ValueError:
                    continue

            if first_number_idx != -1:
                origin_group_raw = " ".join(parts[:first_number_idx])
                numbers = parts[first_number_idx:]
                
                # This regex cleans up labels like "1-Bostn :" to "Bostn"
                # and also handles "TOTAL :" to "TOTAL"
                origin_group = re.sub(r'^[\d\s-]*', '', origin_group_raw).replace(':', '').strip()
                
                # Only add rows that have a valid label.
                if origin_group:
                    parsed_rows.append([origin_group] + numbers)

        if not parsed_rows:
            return pd.DataFrame(), metadata

        # 4. CREATE DATAFRAME
        df = pd.DataFrame(parsed_rows)
        
        # 5. ASSIGN HEADERS AND HANDLE MISMATCH
        final_headers = ["Origin_Group"] + headers
        
        num_cols_data = len(df.columns)
        num_cols_header = len(final_headers)
        
        if num_cols_data != num_cols_header:
            print(f"     - WARNING: Column count mismatch in Table {table_id}. Data has {num_cols_data}, Header has {num_cols_header}. Adjusting.")
            min_cols = min(num_cols_data, num_cols_header)
            df = df.iloc[:, :min_cols]
            df.columns = final_headers[:min_cols]
        else:
            df.columns = final_headers

        # 6. CONVERT DATA TYPES AND CLEAN UP
        for col in df.columns:
            if col != 'Origin_Group':
                # Replace placeholder hyphens before converting to numeric
                df[col] = df[col].replace('-', pd.NA)
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df = df[df['Origin_Group'] != ''].reset_index(drop=True)

        return df, metadata
    
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
        extraction_func = getattr(StopsPRNExtractor, function_name)
        return extraction_func
    except AttributeError:
        print(f"ERROR: The function '{function_name}' specified for Table {table_id_str} does not exist in the StopsPRNExtractor class.")
        return None

def run_extraction(config):
    """Main function to run the data extraction process from config."""
    print("--- 🎬 Starting Data Extraction ---")
    
    base_prn_dir = Path(config["prn_files_folderpath"])
    extraction_config = config.get("prn_files_data_extraction_config", {})
    
    # REFACTORED: Read paths and table configs from their new locations
    output_base_dir = Path(extraction_config.get("output_base_folder", "extracted_csv_tables"))
    files_to_process = extraction_config.get("files_to_process", [])
    tables_to_extract_config = extraction_config.get("tables_to_extract", {})

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

        # REFACTORED: Loop through the table configuration dictionary
        for table_id_str, output_config in tables_to_extract_config.items():
            print(f"  -> Attempting to extract Table {table_id_str}...")
            extraction_func = get_extraction_method(table_id_str, config)
            
            if not extraction_func:
                print(f"       - No extraction method found for Table {table_id_str}. Skipping.")
                continue

            df, metadata = extraction_func(str(file_path), table_id_str, config)
            
            if df.empty:
                print(f"       - No data found for Table {table_id_str} in this file.")
                continue

            # REFACTORED: Build output path from the config templates
            subfolder = output_config.get("output_subfolder", f"Table_{table_id_str.replace('.', '_')}")
            filename_template = output_config.get("output_filename_template", f"[{alias}]__{table_id_str}.csv")
            
            table_output_dir = output_base_dir / subfolder
            table_output_dir.mkdir(parents=True, exist_ok=True)
            
            output_filename = filename_template.format(alias=alias)
            output_path = table_output_dir / output_filename

            df.to_csv(output_path, index=False)
            print(f"       ✅ Successfully saved to: {output_path}")

    print("\n--- ✅ Data Extraction Complete ---")