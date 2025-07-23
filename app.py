import os
import zipfile
import tempfile
import shutil
import pandas as pd
import xml.etree.ElementTree as ET
import json
import uuid
import logging
import re
import datetime
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_file
import threading
from werkzeug.utils import secure_filename
import PBI_dashboard_creator as PBI

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TableauWorkbookParser:
    """Class to parse Tableau workbook and extract visualization metadata"""
    
    def __init__(self, twbx_path):
        self.twbx_path = twbx_path
        self.temp_dir = tempfile.mkdtemp()
        self.twb_path = None
        self.datasources = {}
        self.worksheets = {}
        self.dashboards = {}
        self.data_source_mapping = {}  # Maps Tableau datasource IDs to file paths
        
    def extract_twbx(self):
        """Extract the .twbx file contents"""
        logger.info(f"Extracting {self.twbx_path}...")
        
        try:
            with zipfile.ZipFile(self.twbx_path, 'r') as zip_ref:
                zip_ref.extractall(self.temp_dir)
            
            # Find the .twb file
            twb_files = list(Path(self.temp_dir).glob("**/*.twb"))
            if twb_files:
                self.twb_path = str(twb_files[0])
                logger.info(f"Found .twb file: {self.twb_path}")
                return True
            else:
                logger.error("No .twb file found in the .twbx package")
                return False
                
        except Exception as e:
            logger.error(f"Error extracting .twbx file: {e}")
            return False
    
    def extract_data_sources(self, output_dir):
        """Enhanced extraction of data sources with better mapping to Tableau datasource names"""
        logger.info("Extracting data sources...")
        extracted_files = {}
        
        try:
            # Find Data directory
            data_dir = Path(self.temp_dir) / "Data"
            if data_dir.exists():
                logger.info(f"Found Data directory: {data_dir}")
                
                # Extract all CSV files
                csv_files = list(data_dir.glob("**/*.csv"))
                for csv_file in csv_files:
                    output_file = os.path.join(output_dir, csv_file.name)
                    shutil.copy2(csv_file, output_file)
                    
                    # Use filename without extension as key
                    key = csv_file.stem.lower()
                    extracted_files[key] = output_file
                    logger.info(f"Extracted {csv_file.name} to {output_file}")
                
                # Also look for Excel files
                excel_files = list(data_dir.glob("**/*.xlsx")) + list(data_dir.glob("**/*.xls"))
                for excel_file in excel_files:
                    output_file = os.path.join(output_dir, excel_file.name)
                    shutil.copy2(excel_file, output_file)
                    
                    key = excel_file.stem.lower()
                    extracted_files[key] = output_file
                    logger.info(f"Extracted {excel_file.name} to {output_file}")
            
            # If no Data directory or no files found, search entire archive
            if not extracted_files:
                logger.info("Searching entire archive for data files...")
                csv_files = list(Path(self.temp_dir).glob("**/*.csv"))
                excel_files = list(Path(self.temp_dir).glob("**/*.xlsx")) + list(Path(self.temp_dir).glob("**/*.xls"))
                
                for file in csv_files + excel_files:
                    output_file = os.path.join(output_dir, file.name)
                    shutil.copy2(file, output_file)
                    
                    key = file.stem.lower()
                    extracted_files[key] = output_file
                    logger.info(f"Extracted {file.name} to {output_file}")
            
            # After extraction, try to build mappings between Tableau datasource IDs and files
            if self.twb_path:
                try:
                    tree = ET.parse(self.twb_path)
                    root = tree.getroot()
                    
                    # Look for connection information that maps datasource IDs to files
                    for connection in root.findall(".//connection"):
                        ds_name = connection.get('datasource', '')
                        if ds_name:
                            # Look for dbname or filename attributes
                            dbname = connection.get('dbname', '')
                            filename = connection.get('filename', '')
                            
                            if dbname or filename:
                                file_ref = (dbname or filename)
                                # Extract just the filename without path
                                file_base = os.path.basename(file_ref)
                                file_name_without_ext = os.path.splitext(file_base)[0].lower()
                                
                                # Try to match with our extracted files
                                for key in extracted_files.keys():
                                    if key in file_name_without_ext or file_name_without_ext in key:
                                        self.data_source_mapping[ds_name] = key
                                        logger.info(f"Mapped Tableau datasource {ds_name} to file {key}")
                                        break
                    
                    # Also look for relations between datasources and worksheets
                    for worksheet in root.findall(".//worksheet"):
                        ws_name = worksheet.get('name', '')
                        if ws_name:
                            # Find the datasource this worksheet is using
                            for ds_dep in worksheet.findall(".//datasource-dependencies"):
                                ds_name = ds_dep.get('datasource', '')
                                if ds_name:
                                    # See if this has column references that mention a specific file
                                    for column in ds_dep.findall(".//column"):
                                        col_name = column.get('name', '')
                                        if '[' in col_name and ']' in col_name:
                                            # Extract table name if it exists
                                            # Format might be like: [filename.csv].[column]
                                            parts = col_name.split('.')
                                            if len(parts) > 1:
                                                table_part = parts[0].strip('[]')
                                                for key in extracted_files.keys():
                                                    if key in table_part.lower():
                                                        self.data_source_mapping[ds_name] = key
                                                        logger.info(f"Mapped Tableau datasource {ds_name} to file {key} via column reference")
                                                        break
                except Exception as e:
                    logger.warning(f"Error mapping datasources to files: {e}")
            
            return extracted_files
            
        except Exception as e:
            logger.error(f"Error extracting data sources: {e}")
            return extracted_files
    
    def parse_workbook(self):
        """Parse the .twb file to extract workbook structure"""
        if not self.twb_path:
            logger.error("No .twb file available to parse")
            return False
            
        try:
            logger.info(f"Parsing Tableau workbook: {self.twb_path}")
            
            # Parse XML
            tree = ET.parse(self.twb_path)
            root = tree.getroot()
            
            # Extract datasources
            for datasource in root.findall(".//datasource"):
                ds_name = datasource.get('name', '')
                if ds_name:
                    self.datasources[ds_name] = {
                        'name': ds_name,
                        'caption': datasource.get('caption', ds_name),
                        'columns': []
                    }
                    
                    # Extract columns
                    for column in datasource.findall(".//column"):
                        col_name = column.get('name', '')
                        if col_name:
                            self.datasources[ds_name]['columns'].append({
                                'name': col_name,
                                'caption': column.get('caption', col_name),
                                'datatype': column.get('datatype', 'string')
                            })
            
            # Extract worksheets
            for worksheet in root.findall(".//worksheet"):
                ws_name = worksheet.get('name', '')
                if ws_name:
                    self.worksheets[ws_name] = {
                        'name': ws_name,
                        'datasources': [],
                        'chart_type': self._determine_chart_type(worksheet),
                        'columns': {
                            'x_axis': [],
                            'y_axis': [],
                            'color': [],
                            'size': [],
                            'label': [],
                            'filter': []
                        },
                        'title': self._extract_title(worksheet, ws_name)
                    }
                    
                    # Extract datasources used
                    for ds_ref in worksheet.findall(".//datasource-dependencies"):
                        ds_name = ds_ref.get('datasource', '')
                        if ds_name and ds_name not in self.worksheets[ws_name]['datasources']:
                            self.worksheets[ws_name]['datasources'].append(ds_name)
                    
                    # Extract columns used and their roles
                    self._extract_column_roles(worksheet, ws_name)
            
            # Extract dashboards
            for dashboard in root.findall(".//dashboard"):
                db_name = dashboard.get('name', '')
                if db_name:
                    self.dashboards[db_name] = {
                        'name': db_name,
                        'title': dashboard.get('title', db_name),
                        'size': {
                            'width': dashboard.find(".//size").get('width', '800') if dashboard.find(".//size") is not None else '800',
                            'height': dashboard.find(".//size").get('height', '600') if dashboard.find(".//size") is not None else '600'
                        },
                        'worksheets': []
                    }
                    
                    # Extract worksheets in this dashboard
                    for zone in dashboard.findall(".//zone"):
                        ws_name = zone.get('name', '')
                        if ws_name and ws_name in self.worksheets and ws_name not in self.dashboards[db_name]['worksheets']:
                            self.dashboards[db_name]['worksheets'].append(ws_name)
                            
                            # Also store position information
                            position = {
                                'x': zone.get('x', '0'),
                                'y': zone.get('y', '0'),
                                'width': zone.get('w', '0'),
                                'height': zone.get('h', '0')
                            }
                            self.worksheets[ws_name]['position'] = position
            
            logger.info(f"Extracted {len(self.datasources)} datasources, {len(self.worksheets)} worksheets, {len(self.dashboards)} dashboards")
            return True
            
        except Exception as e:
            logger.error(f"Error parsing workbook: {e}")
            return False
    
    def _determine_chart_type(self, worksheet):
        """Enhanced chart type detection, especially for maps"""
        # Try to detect map visualizations more thoroughly
        # Check for map in any of these locations
        if (worksheet.find(".//map") is not None or 
            worksheet.find(".//map-layer") is not None or
            worksheet.find(".//geocoding") is not None or
            worksheet.find(".//*[@class='map']") is not None):
            return "map"
        
        # Also check for location-related column names that might indicate a map
        location_columns = []
        for column in worksheet.findall(".//column"):
            col_name = column.get('name', '').lower()
            if any(loc_term in col_name for loc_term in ['county', 'state', 'country', 'region', 'city', 'location']):
                location_columns.append(col_name)
        
        if location_columns:
            # If we have location columns and they're being used in the visualization, it's likely a map
            used_columns = []
            for col_elem in worksheet.findall(".//column") + worksheet.findall(".//column-instance"):
                col_name = col_elem.get('name', '') or col_elem.get('column', '')
                if col_name:
                    used_columns.append(col_name.lower())
            
            if any(loc_col in used_columns for loc_col in location_columns):
                return "map"
        
        # Rest of the original implementation
        if worksheet.find(".//piechart") is not None:
            return "pieChart"
            
        if worksheet.find(".//encoding[@class='line']") is not None:
            return "lineChart"
            
        if worksheet.find(".//encoding[@class='bar']") is not None:
            return "barChart"
            
        if worksheet.find(".//style[@class='text']") is not None:
            return "table"
        
        # Default to column chart as it's common
        return "columnChart"
    
    def _extract_title(self, worksheet, default_name):
        """Extract the title of a worksheet"""
        title_element = worksheet.find(".//title")
        if title_element is not None:
            caption = title_element.get('caption', '')
            if caption:
                return caption
        return default_name
    
    def _extract_column_roles(self, worksheet, ws_name):
        """Improved extraction of column roles to ensure we get the right columns for axes"""
        # Clear previous values to ensure we don't get duplicates
        self.worksheets[ws_name]['columns']['x_axis'] = []
        self.worksheets[ws_name]['columns']['y_axis'] = []
        self.worksheets[ws_name]['columns']['color'] = []
        self.worksheets[ws_name]['columns']['size'] = []
        self.worksheets[ws_name]['columns']['label'] = []
        self.worksheets[ws_name]['columns']['filter'] = []
        
        # Check for specific type of usage that indicates a measure (usually y-axis)
        measure_columns = []
        for column in worksheet.findall(".//column-instance[@type='measure']"):
            col_name = column.get('column', '')
            if col_name:
                measure_columns.append(col_name)
                logger.info(f"Found measure column: {col_name}")
        
        # Check for specific type of usage that indicates a dimension (usually x-axis)
        dimension_columns = []
        for column in worksheet.findall(".//column-instance[@type='dimension']"):
            col_name = column.get('column', '')
            if col_name:
                dimension_columns.append(col_name)
                logger.info(f"Found dimension column: {col_name}")
        
        # Check columns in rows shelf (typically y-axis)
        for column in worksheet.findall(".//rows//column"):
            col_name = column.get('name', '')
            if col_name:
                # If this column is also a measure, it's definitely y-axis
                if col_name in measure_columns:
                    self.worksheets[ws_name]['columns']['y_axis'].append(col_name)
                else:
                    self.worksheets[ws_name]['columns']['y_axis'].append(col_name)
        
        # Check columns in columns shelf (typically x-axis)
        for column in worksheet.findall(".//columns//column"):
            col_name = column.get('name', '')
            if col_name:
                # If this column is also a dimension, it's definitely x-axis
                if col_name in dimension_columns:
                    self.worksheets[ws_name]['columns']['x_axis'].append(col_name)
                else:
                    self.worksheets[ws_name]['columns']['x_axis'].append(col_name)
        
        # Check columns used for color
        for column in worksheet.findall(".//encoding[@class='color']//column"):
            col_name = column.get('name', '')
            if col_name:
                self.worksheets[ws_name]['columns']['color'].append(col_name)
        
        # Check columns used for size
        for column in worksheet.findall(".//encoding[@class='size']//column"):
            col_name = column.get('name', '')
            if col_name:
                self.worksheets[ws_name]['columns']['size'].append(col_name)
        
        # Check columns used for label
        for column in worksheet.findall(".//encoding[@class='text']//column"):
            col_name = column.get('name', '')
            if col_name:
                self.worksheets[ws_name]['columns']['label'].append(col_name)
        
        # Check columns used in filters
        for column in worksheet.findall(".//filter//column"):
            col_name = column.get('name', '')
            if col_name:
                self.worksheets[ws_name]['columns']['filter'].append(col_name)
        
        # Look for specific columns by name pattern that could be useful
        # Especially for map visualizations
        location_patterns = ['county', 'state', 'country', 'region', 'city', 'location']
        measure_patterns = ['value', 'count', 'sum', 'total', 'sales', 'profit', 'loss', 'colony_lost']
        
        for datasource in self.datasources.values():
            for column in datasource['columns']:
                col_name = column['name']
                col_caption = column.get('caption', '').lower()
                
                # Check if this looks like a location column for maps
                if any(pattern in col_caption for pattern in location_patterns):
                    if col_name not in self.worksheets[ws_name]['columns']['color']:
                        self.worksheets[ws_name]['columns']['color'].append(col_name)
                        logger.info(f"Found potential location column for map: {col_name}")
                
                # Check if this looks like a measure column
                if any(pattern in col_caption for pattern in measure_patterns):
                    if not self.worksheets[ws_name]['columns']['y_axis'] and col_name not in self.worksheets[ws_name]['columns']['y_axis']:
                        self.worksheets[ws_name]['columns']['y_axis'].append(col_name)
                        logger.info(f"Found potential measure column: {col_name}")
    
    def cleanup(self):
        """Clean up temporary files"""
        try:
            shutil.rmtree(self.temp_dir)
            logger.info("Cleaned up temporary files")
        except Exception as e:
            logger.error(f"Error cleaning up: {e}")

class PowerBIConverter:
    """Class to convert Tableau metadata to Power BI dashboard"""
    
    def __init__(self, output_dir, data_files, parser):
        self.output_dir = output_dir
        self.data_files = data_files
        self.parser = parser
        self.report_name = f"PBI_{datetime.datetime.now().strftime('%y%m%d%H%M')}"
        self.dashboard_path = os.path.join(output_dir, self.report_name)
        self.data_dir = os.path.join(self.dashboard_path, "Data")
    
    def create_dashboard(self):
        """Create Power BI dashboard based on Tableau workbook structure"""
        logger.info(f"Creating Power BI dashboard: {self.report_name}")
        
        try:
            # Create new dashboard
            PBI.create_new_dashboard(self.output_dir, self.report_name)
            
            # Create Data directory
            os.makedirs(self.data_dir, exist_ok=True)
            
            # Add data sources to the dashboard
            self._add_data_sources()
            
            # Add date table if appropriate
            try:
                PBI.add_tmdl_dataset(dashboard_path=self.dashboard_path, data_path=None, add_default_datetable=True)
                logger.info("Added default date table")
            except Exception as e:
                logger.warning(f"Could not add date table: {e}")
            
            # Process dashboards and worksheets
            if self.parser.dashboards:
                # If there are dashboards, convert each one to a page
                dashboard_count = 0
                for db_name, dashboard in self.parser.dashboards.items():
                    dashboard_count += 1
                    page_id = f"page{dashboard_count}"
                    page_name = re.sub(r'[^a-zA-Z0-9]', '', dashboard['title'])[:20]  # Shortened name
                    
                    # Add page
                    PBI.add_new_page(
                        self.dashboard_path,
                        page_name=page_name,
                        title=dashboard['title'],
                        subtitle=""
                    )
                    logger.info(f"Added page for dashboard: {dashboard['title']}")
                    
                    # Add worksheets to this page
                    self._add_worksheets_to_page(dashboard['worksheets'], page_id)
                    
                    # If this is not the first page, add navigation button from previous page
                    if dashboard_count > 1:
                        prev_page = f"page{dashboard_count-1}"
                        try:
                            PBI.add_button(
                                label=f"Previous Page",
                                dashboard_path=self.dashboard_path,
                                page_id=page_id,
                                button_id=f"nav_prev_{dashboard_count}",
                                height=40,
                                width=150,
                                x_position=50,
                                y_position=580,
                                page_navigation_link=prev_page
                            )
                        except Exception as e:
                            logger.error(f"Error adding previous button: {e}")
                    
                    # If this is not the last page, add navigation button to next page
                    if dashboard_count < len(self.parser.dashboards):
                        next_page = f"page{dashboard_count+1}"
                        try:
                            PBI.add_button(
                                label=f"Next Page",
                                dashboard_path=self.dashboard_path,
                                page_id=page_id,
                                button_id=f"nav_next_{dashboard_count}",
                                height=40,
                                width=150,
                                x_position=220,
                                y_position=580,
                                page_navigation_link=next_page
                            )
                        except Exception as e:
                            logger.error(f"Error adding next button: {e}")
            
            elif self.parser.worksheets:
                # If no dashboards but worksheets exist, create a page for each worksheet
                ws_count = 0
                for ws_name, worksheet in self.parser.worksheets.items():
                    ws_count += 1
                    page_id = f"page{ws_count}"
                    page_name = re.sub(r'[^a-zA-Z0-9]', '', worksheet['title'])[:20]  # Shortened name
                    
                    # Add page
                    PBI.add_new_page(
                        self.dashboard_path,
                        page_name=page_name,
                        title=worksheet['title'],
                        subtitle=""
                    )
                    logger.info(f"Added page for worksheet: {worksheet['title']}")
                    
                    # Add this worksheet to the page
                    self._add_worksheet_to_page(ws_name, page_id, 50, 150, 600, 400)
                    
                    # Add navigation buttons
                    if ws_count > 1:
                        prev_page = f"page{ws_count-1}"
                        try:
                            PBI.add_button(
                                label=f"Previous Page",
                                dashboard_path=self.dashboard_path,
                                page_id=page_id,
                                button_id=f"nav_prev_{ws_count}",
                                height=40,
                                width=150,
                                x_position=50,
                                y_position=580,
                                page_navigation_link=prev_page
                            )
                        except Exception as e:
                            logger.error(f"Error adding previous button: {e}")
                    
                    if ws_count < len(self.parser.worksheets):
                        next_page = f"page{ws_count+1}"
                        try:
                            PBI.add_button(
                                label=f"Next Page",
                                dashboard_path=self.dashboard_path,
                                page_id=page_id,
                                button_id=f"nav_next_{ws_count}",
                                height=40,
                                width=150,
                                x_position=220,
                                y_position=580,
                                page_navigation_link=next_page
                            )
                        except Exception as e:
                            logger.error(f"Error adding next button: {e}")
            
            else:
                # If no dashboards or worksheets, create a generic dashboard
                logger.warning("No dashboards or worksheets found, creating generic dashboard")
                PBI.add_new_page(
                    self.dashboard_path,
                    page_name="MainPage",
                    title="Converted Dashboard",
                    subtitle="No visualizations found in source file"
                )
                
                # Add text explaining the situation
                try:
                    PBI.add_text_box(
                        text="No visualizations were found in the source Tableau file.\n\nYou may need to add visualizations manually using the imported data sources.",
                        dashboard_path=self.dashboard_path,
                        page_id="page1",
                        text_box_id="info_text",
                        height=200,
                        width=400,
                        x_position=200,
                        y_position=200,
                        font_size=14
                    )
                except Exception as e:
                    logger.error(f"Error adding text box: {e}")
            
            logger.info(f"Dashboard created at: {self.dashboard_path}")
            return self.dashboard_path
            
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            return None
    
    def _add_data_sources(self):
        """Add data sources to Power BI dashboard"""
        if not self.data_files:
            logger.warning("No data files to add")
            return
            
        logger.info(f"Adding {len(self.data_files)} data sources")
        
        for source_name, file_path in self.data_files.items():
            # Copy the file to the dashboard's Data directory
            file_name = os.path.basename(file_path)
            dashboard_file = os.path.join(self.data_dir, file_name)
            
            try:
                shutil.copy2(file_path, dashboard_file)
                
                # Add to Power BI based on file type
                if file_name.lower().endswith('.csv'):
                    PBI.add_csv(self.dashboard_path, dashboard_file)
                    logger.info(f"Added CSV data source: {file_name}")
                elif file_name.lower().endswith(('.xlsx', '.xls')):
                    PBI.add_excel(self.dashboard_path, dashboard_file)
                    logger.info(f"Added Excel data source: {file_name}")
            except Exception as e:
                logger.error(f"Error adding data source {file_name}: {e}")
    
    def _add_worksheets_to_page(self, worksheet_names, page_id):
        """Add multiple worksheets to a page, arranging them in a grid"""
        if not worksheet_names:
            logger.warning(f"No worksheets to add to {page_id}")
            return
            
        num_ws = len(worksheet_names)
        
        if num_ws == 1:
            # Single worksheet - use full page
            ws_name = worksheet_names[0]
            self._add_worksheet_to_page(ws_name, page_id, 50, 150, 600, 400)
        
        elif num_ws == 2:
            # Two worksheets - side by side
            ws1 = worksheet_names[0]
            ws2 = worksheet_names[1]
            
            self._add_worksheet_to_page(ws1, page_id, 50, 150, 300, 400)
            self._add_worksheet_to_page(ws2, page_id, 370, 150, 300, 400)
        
        elif num_ws <= 4:
            # 3-4 worksheets - 2x2 grid
            positions = [
                (50, 150, 300, 200),   # top left
                (370, 150, 300, 200),  # top right
                (50, 370, 300, 200),   # bottom left
                (370, 370, 300, 200)   # bottom right
            ]
            
            for i, ws_name in enumerate(worksheet_names[:4]):
                x, y, w, h = positions[i]
                self._add_worksheet_to_page(ws_name, page_id, x, y, w, h)
        
        else:
            # More than 4 - just add the first 4 in a grid
            logger.warning(f"Page {page_id} has {num_ws} worksheets, only adding first 4")
            self._add_worksheets_to_page(worksheet_names[:4], page_id)
    
    def _add_worksheet_to_page(self, ws_name, page_id, x_position, y_position, width, height):
        """Add a single worksheet as a visualization to a page"""
        if ws_name not in self.parser.worksheets:
            logger.warning(f"Worksheet {ws_name} not found in parser data")
            return
            
        worksheet = self.parser.worksheets[ws_name]
        
        # Get chart type
        chart_type = worksheet['chart_type']
        
        # Get data source - with improved mapping
        if not worksheet['datasources']:
            logger.warning(f"No datasource found for worksheet {ws_name}")
            return
            
        datasource = worksheet['datasources'][0]  # Use first datasource
        
        # Try to use the mapping if available
        if hasattr(self.parser, 'data_source_mapping') and datasource in self.parser.data_source_mapping:
            mapped_source = self.parser.data_source_mapping[datasource]
            logger.info(f"Using mapped datasource: {datasource} -> {mapped_source}")
            datasource = mapped_source
        else:
            # Previous fallback logic
            datasource_short = datasource.split('.')[-1].lower()
            data_file_keys = [k.lower() for k in self.data_files.keys()]
            matching_keys = [k for k in data_file_keys if datasource_short in k or k in datasource_short]
            
            if matching_keys:
                datasource = matching_keys[0]
            elif self.data_files:
                # Last resort - use first file but log a warning
                datasource = list(self.data_files.keys())[0]
                logger.warning(f"No matching data source found for {datasource}, using {datasource} as fallback")
            else:
                # No data files available, add text box instead
                try:
                    PBI.add_text_box(
                        text=f"Visualization: {worksheet['title']}\nType: {chart_type}\nNo matching data source found",
                        dashboard_path=self.dashboard_path,
                        page_id=page_id,
                        text_box_id=f"text_{ws_name.replace(' ', '_')}",
                        height=height,
                        width=width,
                        x_position=x_position,
                        y_position=y_position,
                        font_size=14
                    )
                except Exception as e:
                    logger.error(f"Error adding text box: {e}")
                return
        
        # Get X and Y axis variables with improved selection
        x_axis_var = None
        y_axis_var = None
        
        # For x-axis, prefer dimensions (categories, dates)
        if worksheet['columns']['x_axis']:
            x_axis_var = self._clean_column_name(worksheet['columns']['x_axis'][0])
            logger.info(f"Using x-axis from worksheet: {x_axis_var}")
        
        # For y-axis, prefer measures (numeric values)
        if worksheet['columns']['y_axis']:
            y_axis_var = self._clean_column_name(worksheet['columns']['y_axis'][0])
            logger.info(f"Using y-axis from worksheet: {y_axis_var}")
        
        # If still missing x or y, analyze the data to find appropriate columns
        if not x_axis_var or not y_axis_var:
            try:
                # Read the data file to analyze its structure
                file_path = self.data_files[datasource]
                if file_path.lower().endswith('.csv'):
                    df = pd.read_csv(file_path)
                    
                    # If we need an x-axis column
                    if not x_axis_var:
                        # Look for common dimension columns
                        dim_candidates = []
                        for col in df.columns:
                            # Check if the column name suggests a dimension
                            if any(dim in col.lower() for dim in ['year', 'date', 'month', 'category', 'region', 'county']):
                                dim_candidates.append(col)
                        
                        if dim_candidates:
                            x_axis_var = dim_candidates[0]
                            logger.info(f"Selected x-axis based on name pattern: {x_axis_var}")
                        elif not df.empty:
                            # Try to find a good categorical column
                            # Prefer string, date, or small number of unique values
                            for col in df.columns:
                                if df[col].dtype == 'object' or pd.api.types.is_datetime64_any_dtype(df[col]):
                                    x_axis_var = col
                                    logger.info(f"Selected x-axis based on data type: {x_axis_var}")
                                    break
                            
                            # If still not found, use first column with reasonable number of unique values
                            if not x_axis_var:
                                for col in df.columns:
                                    if df[col].nunique() < len(df) / 2:  # Fewer than half the rows are unique
                                        x_axis_var = col
                                        logger.info(f"Selected x-axis based on cardinality: {x_axis_var}")
                                        break
                    
                    # If we need a y-axis column
                    if not y_axis_var:
                        # Look for common measure columns
                        measure_candidates = []
                        for col in df.columns:
                            # Check if column name suggests a measure
                            if any(measure in col.lower() for measure in ['value', 'count', 'sum', 'total', 'sales', 'lost', 'colony_lost']):
                                measure_candidates.append(col)
                        
                        if measure_candidates:
                            y_axis_var = measure_candidates[0]
                            logger.info(f"Selected y-axis based on name pattern: {y_axis_var}")
                        elif not df.empty:
                            # Try to find a numeric column for measures
                            numeric_cols = df.select_dtypes(include=['number']).columns
                            if len(numeric_cols) > 0:
                                # Don't use the same column as x-axis
                                for col in numeric_cols:
                                    if col != x_axis_var:
                                        y_axis_var = col
                                        logger.info(f"Selected y-axis based on data type: {y_axis_var}")
                                        break
                                
                                # If we couldn't find a different column, just use the first numeric
                                if not y_axis_var and len(numeric_cols) > 0:
                                    y_axis_var = numeric_cols[0]
                                    logger.info(f"Selected y-axis as first numeric column: {y_axis_var}")
                
                # Ensure we don't use the same column for both axes
                if x_axis_var and y_axis_var and x_axis_var == y_axis_var:
                    logger.warning(f"Same column selected for both axes: {x_axis_var}")
                    # Try to find an alternative for y-axis
                    df = pd.read_csv(file_path)
                    for col in df.columns:
                        if col != x_axis_var and pd.api.types.is_numeric_dtype(df[col]):
                            y_axis_var = col
                            logger.info(f"Selected alternative y-axis: {y_axis_var}")
                            break
            
            except Exception as e:
                logger.warning(f"Error analyzing data file: {e}")
        
        # If still missing x or y, use placeholder values (but try to avoid using the same value)
        if not x_axis_var:
            x_axis_var = "Category"
        if not y_axis_var:
            y_axis_var = "Value" if x_axis_var != "Value" else "Count"
        
        # Ensure we never use the same column for both axes
        if x_axis_var == y_axis_var:
            y_axis_var = "Value"
        
        # Get chart title
        chart_title = worksheet['title']
        
        # Choose appropriate visualization based on chart type
        chart_id = f"{ws_name.replace(' ', '_')[:10]}"  # Short ID
        
        if chart_type == "map":
            # For maps, we need a location column and a measure column
            try:
                # Try to identify the location column
                location_col = None
                for col in worksheet['columns']['color']:
                    clean_col = self._clean_column_name(col)
                    if any(loc_term in clean_col.lower() for loc_term in ['county', 'state', 'country', 'region', 'city', 'location']):
                        location_col = clean_col
                        break
                
                # If no location column found from color, try other columns
                if not location_col:
                    for col in worksheet['columns']['x_axis'] + worksheet['columns']['y_axis'] + worksheet['columns']['label']:
                        clean_col = self._clean_column_name(col)
                        if any(loc_term in clean_col.lower() for loc_term in ['county', 'state', 'country', 'region', 'city', 'location']):
                            location_col = clean_col
                            break
                
                # If we found a location column, create a map
                if location_col:
                    PBI.add_map(
                        dashboard_path=self.dashboard_path,
                        page_id=page_id,
                        map_id=chart_id,
                        data_source=datasource,
                        map_title=chart_title,
                        location_var=location_col,
                        color_var=y_axis_var,
                        height=height,
                        width=width,
                        x_position=x_position,
                        y_position=y_position
                    )
                    logger.info(f"Added map visualization for {ws_name} using location {location_col}")
                else:
                    # If no location column, try to fall back to a shape map
                    try:
                        # Assuming we have a shape file for Washington counties
                        wa_shape_file = "2019_53_WA_Counties9467365124727016.json"
                        
                        if os.path.exists(wa_shape_file):
                            PBI.add_shape_map(
                                dashboard_path=self.dashboard_path,
                                page_id=page_id,
                                map_id=chart_id,
                                data_source=datasource,
                                shape_file_path=wa_shape_file,
                                color_palette=["#efb5b9", "#e68f96", "#de6a73", "#a1343c", "#6b2328"],
                                static_bin_breaks=[0, 15.4, 30.8, 46.2, 61.6, 77.0],
                                map_title=chart_title,
                                location_var=x_axis_var,  # Use x-axis as location
                                color_var=y_axis_var,     # Use y-axis as measure
                                height=height,
                                width=width,
                                x_position=x_position,
                                y_position=y_position
                            )
                            logger.info(f"Added shape map for {ws_name}")
                        else:
                            # Fallback to column chart if no shape file
                            logger.warning(f"No location column or shape file found for map, falling back to column chart")
                            raise Exception("No shape file found")
                    except Exception as e:
                        logger.warning(f"Error creating map: {e}, falling back to column chart")
                        self._add_column_chart(ws_name, chart_id, page_id, datasource, chart_title, 
                                             x_axis_var, y_axis_var, x_position, y_position, width, height)
            except Exception as e:
                logger.error(f"Error adding map: {e}")
                self._add_fallback_viz(ws_name, chart_type, page_id, x_position, y_position, width, height)
        
        elif chart_type == "pieChart":
            # Add pie chart
            try:
                PBI.add_chart(
                    dashboard_path=self.dashboard_path,
                    page_id=page_id,
                    chart_id=chart_id,
                    chart_type="pieChart",
                    data_source=datasource,
                    chart_title=chart_title,
                    x_axis_var=x_axis_var,  # Categories
                    y_axis_var=y_axis_var,  # Values
                    y_axis_var_aggregation_type="Sum",
                    height=height,
                    width=width,
                    x_position=x_position,
                    y_position=y_position
                )
                logger.info(f"Added pie chart for {ws_name}")
            except Exception as e:
                logger.error(f"Error adding pie chart: {e}")
                self._add_fallback_viz(ws_name, chart_type, page_id, x_position, y_position, width, height)
        
        elif chart_type == "lineChart":
            # Add line chart
            try:
                PBI.add_chart(
                    dashboard_path=self.dashboard_path,
                    page_id=page_id,
                    chart_id=chart_id,
                    chart_type="lineChart",
                    data_source=datasource,
                    chart_title=chart_title,
                    x_axis_title=x_axis_var,
                    y_axis_title=y_axis_var,
                    x_axis_var=x_axis_var,
                    y_axis_var=y_axis_var,
                    y_axis_var_aggregation_type="Sum",
                    height=height,
                    width=width,
                    x_position=x_position,
                    y_position=y_position
                )
                logger.info(f"Added line chart for {ws_name}")
            except Exception as e:
                logger.error(f"Error adding line chart: {e}")
                self._add_fallback_viz(ws_name, chart_type, page_id, x_position, y_position, width, height)
        
        elif chart_type == "barChart":
            # Add bar chart (horizontal)
            try:
                PBI.add_chart(
                    dashboard_path=self.dashboard_path,
                    page_id=page_id,
                    chart_id=chart_id,
                    chart_type="barChart",
                    data_source=datasource,
                    chart_title=chart_title,
                    x_axis_title=y_axis_var,  # Swapped for horizontal bars
                    y_axis_title=x_axis_var,  # Swapped for horizontal bars
                    x_axis_var=x_axis_var,
                    y_axis_var=y_axis_var,
                    y_axis_var_aggregation_type="Sum",
                    height=height,
                    width=width,
                    x_position=x_position,
                    y_position=y_position
                )
                logger.info(f"Added bar chart for {ws_name}")
            except Exception as e:
                logger.error(f"Error adding bar chart: {e}")
                self._add_fallback_viz(ws_name, chart_type, page_id, x_position, y_position, width, height)
        
        elif chart_type == "table":
            # Add table
            try:
                PBI.add_table(
                    dashboard_path=self.dashboard_path,
                    page_id=page_id,
                    table_id=chart_id,
                    data_source=datasource,
                    table_title=chart_title,
                    height=height,
                    width=width,
                    x_position=x_position,
                    y_position=y_position
                )
                logger.info(f"Added table for {ws_name}")
            except Exception as e:
                logger.error(f"Error adding table: {e}")
                self._add_fallback_viz(ws_name, chart_type, page_id, x_position, y_position, width, height)
        
        else:
            # Default to column chart
            self._add_column_chart(ws_name, chart_id, page_id, datasource, chart_title, 
                                 x_axis_var, y_axis_var, x_position, y_position, width, height)
    
    def _add_column_chart(self, ws_name, chart_id, page_id, datasource, chart_title, 
                         x_axis_var, y_axis_var, x_position, y_position, width, height):
        """Helper method to add a column chart with consistent settings"""
        try:
            PBI.add_chart(
                dashboard_path=self.dashboard_path,
                page_id=page_id,
                chart_id=chart_id,
                chart_type="columnChart",
                data_source=datasource,
                chart_title=chart_title,
                x_axis_title=x_axis_var,
                y_axis_title=y_axis_var,
                x_axis_var=x_axis_var,
                y_axis_var=y_axis_var,
                y_axis_var_aggregation_type="Sum",
                height=height,
                width=width,
                x_position=x_position,
                y_position=y_position
            )
            logger.info(f"Added column chart for {ws_name}")
        except Exception as e:
            logger.error(f"Error adding column chart: {e}")
            self._add_fallback_viz(ws_name, "columnChart", page_id, x_position, y_position, width, height)
    
    def _add_fallback_viz(self, ws_name, chart_type, page_id, x_position, y_position, width, height):
        """Add a fallback text box when visualization creation fails"""
        try:
            PBI.add_text_box(
                text=f"Visualization: {ws_name}\nType: {chart_type}\n\nThis visualization could not be automatically converted. Please create it manually.",
                dashboard_path=self.dashboard_path,
                page_id=page_id,
                text_box_id=f"text_{ws_name.replace(' ', '_')}",
                height=height,
                width=width,
                x_position=x_position,
                y_position=y_position,
                font_size=14
            )
            logger.info(f"Added fallback text box for {ws_name}")
        except Exception as e:
            logger.error(f"Error adding fallback text box: {e}")
    
    def _clean_column_name(self, column_name):
        """Clean column name for use in Power BI"""
        # Extract the base name without table prefix or aggregation
        # Example: "[Datasource].[Sum:Sales]" -> "Sales"
        match = re.search(r'(?:\[.*\]\.)*(?:\[)?([^\[\]]+)(?:\])?$', column_name)
        if match:
            clean_name = match.group(1)
            # Remove aggregation prefixes like "Sum:"
            clean_name = re.sub(r'^(?:Sum|Avg|Min|Max|Count):', '', clean_name)
            return clean_name
        return column_name

# Create Flask app
app = Flask(__name__)

# Configure upload folder and allowed extensions
UPLOAD_FOLDER = "C:/PBI_temp/uploads"  # Shorter path
OUTPUT_FOLDER = "C:/PBI_temp/output"   # Shorter path
ALLOWED_EXTENSIONS = {'twbx'}

# Create necessary directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB max upload size

# Store conversion tasks and their status
conversion_tasks = {}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Create HTML template directly in the Flask route
@app.route('/')
def index():
    html = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Tableau to Power BI Converter</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            .drop-area {
                border: 2px dashed #ccc;
                border-radius: 10px;
                padding: 50px;
                text-align: center;
                margin: 20px 0;
                background-color: #f8f9fa;
            }
            .drop-area.highlight {
                background-color: #e9ecef;
                border-color: #007bff;
            }
            .progress-area {
                display: none;
                margin: 20px 0;
            }
            .log-area {
                height: 200px;
                overflow-y: auto;
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 5px;
                padding: 10px;
                font-family: monospace;
                margin-top: 15px;
            }
            #download-btn {
                display: none;
            }
        </style>
    </head>
    <body>
        <div class="container mt-5">
            <div class="row">
                <div class="col-md-8 offset-md-2">
                    <div class="card">
                        <div class="card-header bg-primary text-white">
                            <h3 class="mb-0">Tableau to Power BI Converter</h3>
                        </div>
                        <div class="card-body">
                            <p class="lead">Convert your Tableau dashboards to Power BI format</p>
                            
                            <div class="drop-area" id="drop-area">
                                <h4>Drop your Tableau file here</h4>
                                <p>or</p>
                                <input type="file" id="file-input" class="d-none" accept=".twbx">
                                <button class="btn btn-outline-primary" id="browse-btn">Browse Files</button>
                                <p class="mt-2 text-muted">Accepts .twbx files</p>
                            </div>
                            
                            <div id="file-info" class="mt-3 d-none">
                                <div class="alert alert-info">
                                    <strong>Selected file:</strong> <span id="file-name"></span>
                                </div>
                                <button class="btn btn-primary w-100" id="convert-btn">Convert to Power BI</button>
                            </div>
                            
                            <div class="progress-area" id="progress-area">
                                <h5 id="status-message">Processing...</h5>
                                <div class="progress mb-3">
                                    <div class="progress-bar progress-bar-striped progress-bar-animated" id="progress-bar" role="progressbar" style="width: 0%"></div>
                                </div>
                                <div class="log-area" id="log-area"></div>
                                <div class="mt-3">
                                    <a href="#" class="btn btn-success w-100" id="download-btn">Download Power BI Dashboard</a>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script>
            document.addEventListener('DOMContentLoaded', function() {
                const dropArea = document.getElementById('drop-area');
                const fileInput = document.getElementById('file-input');
                const browseBtn = document.getElementById('browse-btn');
                const fileInfo = document.getElementById('file-info');
                const fileName = document.getElementById('file-name');
                const convertBtn = document.getElementById('convert-btn');
                const progressArea = document.getElementById('progress-area');
                const progressBar = document.getElementById('progress-bar');
                const statusMessage = document.getElementById('status-message');
                const logArea = document.getElementById('log-area');
                const downloadBtn = document.getElementById('download-btn');
                
                let selectedFile = null;
                let taskId = null;
                let statusInterval = null;
                
                // Event Listeners
                browseBtn.addEventListener('click', () => fileInput.click());
                fileInput.addEventListener('change', handleFiles);
                convertBtn.addEventListener('click', startConversion);
                
                // Drag and Drop
                ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
                    dropArea.addEventListener(eventName, preventDefaults, false);
                });
                
                ['dragenter', 'dragover'].forEach(eventName => {
                    dropArea.addEventListener(eventName, highlight, false);
                });
                
                ['dragleave', 'drop'].forEach(eventName => {
                    dropArea.addEventListener(eventName, unhighlight, false);
                });
                
                dropArea.addEventListener('drop', handleDrop, false);
                
                // Functions
                function preventDefaults(e) {
                    e.preventDefault();
                    e.stopPropagation();
                }
                
                function highlight() {
                    dropArea.classList.add('highlight');
                }
                
                function unhighlight() {
                    dropArea.classList.remove('highlight');
                }
                
                function handleDrop(e) {
                    const dt = e.dataTransfer;
                    const files = dt.files;
                    handleFiles({ target: { files } });
                }
                
                function handleFiles(e) {
                    const files = e.target.files;
                    if (files.length) {
                        selectedFile = files[0];
                        if (selectedFile.name.toLowerCase().endsWith('.twbx')) {
                            fileName.textContent = selectedFile.name;
                            fileInfo.classList.remove('d-none');
                        } else {
                            alert('Please select a valid Tableau file (.twbx)');
                            resetFileSelection();
                        }
                    }
                }
                
                function resetFileSelection() {
                    selectedFile = null;
                    fileInput.value = '';
                    fileInfo.classList.add('d-none');
                }
                
                function startConversion() {
                    if (!selectedFile) return;
                    
                    // Show progress area
                    fileInfo.classList.add('d-none');
                    progressArea.style.display = 'block';
                    downloadBtn.style.display = 'none';
                    logArea.innerHTML = '';
                    
                    // Create form data
                    const formData = new FormData();
                    formData.append('file', selectedFile);
                    
                    // Upload file
                    fetch('/upload', {
                        method: 'POST',
                        body: formData
                    })
                    .then(response => response.json())
                    .then(data => {
                        if (data.task_id) {
                            taskId = data.task_id;
                            // Start checking status
                            checkStatus();
                            statusInterval = setInterval(checkStatus, 1000);
                        } else if (data.error) {
                            statusMessage.textContent = 'Error: ' + data.error;
                            progressBar.style.width = '100%';
                            progressBar.classList.add('bg-danger');
                        }
                    })
                    .catch(error => {
                        statusMessage.textContent = 'Error: ' + error.message;
                        progressBar.style.width = '100%';
                        progressBar.classList.add('bg-danger');
                        console.error('Error:', error);
                    });
                }
                
                function checkStatus() {
                    if (!taskId) return;
                    
                    fetch('/status/' + taskId)
                        .then(response => response.json())
                        .then(data => {
                            // Update progress
                            progressBar.style.width = data.progress + '%';
                            statusMessage.textContent = data.message;
                            
                            // Update log
                            if (data.log && data.log.length) {
                                logArea.innerHTML = '';
                                data.log.forEach(line => {
                                    if (line.trim()) {
                                        const logLine = document.createElement('div');
                                        logLine.textContent = line;
                                        logArea.appendChild(logLine);
                                    }
                                });
                                logArea.scrollTop = logArea.scrollHeight;
                            }
                            
                            // Check if complete
                            if (data.status === 'completed') {
                                clearInterval(statusInterval);
                                progressBar.classList.remove('progress-bar-animated');
                                progressBar.classList.add('bg-success');
                                downloadBtn.href = '/download/' + taskId;
                                downloadBtn.style.display = 'block';
                            } 
                            else if (data.status === 'failed' || data.status === 'error') {
                                clearInterval(statusInterval);
                                progressBar.classList.remove('progress-bar-animated');
                                progressBar.classList.add('bg-danger');
                            }
                        })
                        .catch(error => {
                            console.error('Error checking status:', error);
                        });
                }
            });
        </script>
    </body>
    </html>
    '''
    return html

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        # Generate a short unique ID for this task
        task_id = str(uuid.uuid4())[:8]  # Use only first 8 chars to keep paths shorter
        
        # Create folders for this task
        task_upload_folder = os.path.join(app.config['UPLOAD_FOLDER'], task_id)
        task_output_folder = os.path.join(app.config['OUTPUT_FOLDER'], task_id)
        
        os.makedirs(task_upload_folder, exist_ok=True)
        os.makedirs(task_output_folder, exist_ok=True)
        
        # Save the file with a short name
        filename = "tableau.twbx"  # Use a fixed short name
        file_path = os.path.join(task_upload_folder, filename)
        file.save(file_path)
        
        # Initialize task
        conversion_tasks[task_id] = {
            'status': 'uploading',
            'message': 'File uploaded, starting conversion...',
            'progress': 5,
            'file_path': file_path,
            'output_folder': task_output_folder,
            'log': []
        }
        
        # Start processing in background
        thread = threading.Thread(target=process_file, args=(task_id, file_path, task_output_folder))
        thread.daemon = True
        thread.start()
        
        return jsonify({'task_id': task_id})
    
    return jsonify({'error': 'Invalid file type'}), 400

# Continuing from where the code was cut off - completing the process_file function

def process_file(task_id, file_path, output_folder):
    """Process the file in background"""
    try:
        # Create log capture
        log_capture = []
        def log_handler(message):
            log_capture.append(message)
            conversion_tasks[task_id]['log'] = log_capture[-20:]  # Keep last 20 lines
        
        # Update status
        conversion_tasks[task_id]['status'] = 'parsing'
        conversion_tasks[task_id]['message'] = 'Parsing Tableau workbook...'
        conversion_tasks[task_id]['progress'] = 10
        log_handler("Parsing Tableau workbook...")
        
        # Parse Tableau workbook
        parser = TableauWorkbookParser(file_path)
        extract_success = parser.extract_twbx()
        
        if not extract_success:
            conversion_tasks[task_id]['status'] = 'failed'
            conversion_tasks[task_id]['message'] = 'Failed to extract Tableau workbook'
            conversion_tasks[task_id]['progress'] = 100
            log_handler("ERROR: Failed to extract Tableau workbook")
            return
        
        # Extract data sources
        conversion_tasks[task_id]['status'] = 'extracting'
        conversion_tasks[task_id]['message'] = 'Extracting data sources...'
        conversion_tasks[task_id]['progress'] = 30
        log_handler("Extracting data sources...")
        
        data_files = parser.extract_data_sources(output_folder)
        
        if not data_files:
            log_handler("WARNING: No data files found in the workbook")
        else:
            log_handler(f"Found {len(data_files)} data files")
        
        # Parse workbook structure
        conversion_tasks[task_id]['status'] = 'analyzing'
        conversion_tasks[task_id]['message'] = 'Analyzing workbook structure...'
        conversion_tasks[task_id]['progress'] = 50
        log_handler("Analyzing workbook structure...")
        
        parse_success = parser.parse_workbook()
        
        if not parse_success:
            log_handler("WARNING: Could not fully parse workbook structure")
        
        # Create Power BI dashboard
        conversion_tasks[task_id]['status'] = 'creating'
        conversion_tasks[task_id]['message'] = 'Creating Power BI dashboard...'
        conversion_tasks[task_id]['progress'] = 70
        log_handler("Creating Power BI dashboard...")
        
        converter = PowerBIConverter(output_folder, data_files, parser)
        dashboard_path = converter.create_dashboard()
        
        if dashboard_path:
            # Create zip file
            conversion_tasks[task_id]['status'] = 'zipping'
            conversion_tasks[task_id]['message'] = 'Creating dashboard package...'
            conversion_tasks[task_id]['progress'] = 90
            log_handler("Creating dashboard package...")
            
            zip_path = os.path.join(output_folder, "dashboard.zip")
            
            shutil.make_archive(
                os.path.join(output_folder, "dashboard"),
                'zip',
                dashboard_path
            )
            
            # Clean up
            parser.cleanup()
            
            # Update status
            conversion_tasks[task_id]['status'] = 'completed'
            conversion_tasks[task_id]['message'] = 'Dashboard created successfully!'
            conversion_tasks[task_id]['progress'] = 100
            conversion_tasks[task_id]['dashboard_path'] = dashboard_path
            conversion_tasks[task_id]['zip_path'] = zip_path
            log_handler("Dashboard created successfully!")
        else:
            conversion_tasks[task_id]['status'] = 'failed'
            conversion_tasks[task_id]['message'] = 'Failed to create dashboard'
            conversion_tasks[task_id]['progress'] = 100
            log_handler("ERROR: Failed to create dashboard")
    
    except Exception as e:
        conversion_tasks[task_id]['status'] = 'error'
        conversion_tasks[task_id]['message'] = f'Error: {str(e)}'
        conversion_tasks[task_id]['progress'] = 100
        log_handler(f"ERROR: {str(e)}")
        logger.exception("Error in process_file")

@app.route('/status/<task_id>', methods=['GET'])
def task_status(task_id):
    if task_id not in conversion_tasks:
        return jsonify({'error': 'Task not found'}), 404
    
    task = conversion_tasks[task_id]
    
    return jsonify({
        'status': task['status'],
        'message': task['message'],
        'progress': task['progress'],
        'log': task.get('log', [])
    })

@app.route('/download/<task_id>', methods=['GET'])
def download_file(task_id):
    if task_id not in conversion_tasks:
        return jsonify({'error': 'Task not found'}), 404
    
    task = conversion_tasks[task_id]
    
    if task['status'] != 'completed' or 'zip_path' not in task:
        return jsonify({'error': 'File not ready'}), 400
    
    zip_path = task['zip_path']
    
    if not os.path.exists(zip_path):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(
        zip_path,
        as_attachment=True,
        download_name='power_bi_dashboard.zip'
    )

if __name__ == '__main__':
    app.run(debug=True, port=5000)
