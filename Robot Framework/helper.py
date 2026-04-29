"""
Helper functions for Robot Framework data quality test.
Provides functions to:
- Extract HTML table into Pandas DataFrame
- Read partitioned Parquet dataset with optional filtering
- Compare two DataFrames
"""

import os
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime


def extract_html_table_to_dataframe(table_element):
    """
    Extract HTML table data from a Selenium web element into a Pandas DataFrame.
    
    Args:
        table_element: Selenium WebElement representing the table root (class="table")
    
    Returns:
        pd.DataFrame: Extracted table data
    
    Raises:
        ValueError: If table structure is invalid or no data found
    """
    try:
        from selenium.webdriver.common.by import By
        
        # Find all columns
        columns = table_element.find_elements(By.CLASS_NAME, "y-column")
        if not columns:
            raise ValueError("No columns found under table (class='y-column')")
        
        headers = []
        columns_cells = []
        max_rows = 0
        
        # Extract headers and cell data from each column
        for column in columns:
            try:
                header = column.find_element(By.XPATH, './/*[@id="header"]').text.strip()
            except Exception as e:
                raise ValueError(f"Failed to find header in column: {e}")
            
            headers.append(header)
            
            # Find all cell-text elements and exclude header text
            cells = column.find_elements(By.CSS_SELECTOR, ".cell-text")
            row_values = [cell.text.strip() for cell in cells if cell.text.strip() != header]
            columns_cells.append(row_values)
            max_rows = max(max_rows, len(row_values))
        
        if not headers:
            raise ValueError("No column headers extracted from table")
        
        if max_rows == 0:
            raise ValueError("No row data extracted from table")
        
        # Transpose column-oriented data to row-oriented
        rows = []
        for row_index in range(max_rows):
            row = [
                columns_cells[col_index][row_index]
                if row_index < len(columns_cells[col_index])
                else ""
                for col_index in range(len(columns_cells))
            ]
            rows.append(row)
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=headers)
        
        # Attempt type inference
        df = pd.concat([df.apply(pd.to_numeric, errors='ignore') for col in df.columns], axis=1, ignore_index=False)
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
        
        return df
    
    except Exception as e:
        raise ValueError(f"Failed to extract HTML table: {e}")


def read_parquet_dataset(parquet_folder, filter_date=None):
    """
    Read partitioned Parquet dataset into Pandas DataFrame.
    Optionally filters by partition_date column if filter_date is provided.
    
    Args:
        parquet_folder (str): Path to the parquet dataset folder
        filter_date (str): Optional filter date in YYYY-MM-DD format
    
    Returns:
        pd.DataFrame: Parquet dataset data
    
    Raises:
        FileNotFoundError: If parquet folder doesn't exist
        ValueError: If dataset is empty or filter_date format is invalid
    """
    parquet_path = Path(parquet_folder)
    
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet folder not found: {parquet_folder}")
    
    try:
        # Read the parquet dataset with partitioned format
        df = pd.read_parquet(parquet_path)
        
        if df.empty:
            raise ValueError(f"Parquet dataset is empty: {parquet_folder}")
        
        # Apply date filter if provided
        if filter_date:
            try:
                filter_date_obj = pd.to_datetime(filter_date)
            except Exception as e:
                raise ValueError(f"Invalid filter_date format (expected YYYY-MM-DD): {filter_date}. Error: {e}")
            
            # Check if partition_date column exists
            if 'partition_date' in df.columns:
                df['partition_date'] = pd.to_datetime(df['partition_date'])
                df = df[df['partition_date'].dt.date == filter_date_obj.date()]
                
                if df.empty:
                    raise ValueError(f"No data found for partition_date {filter_date}")
        
        return df
    
    except Exception as e:
        if isinstance(e, (FileNotFoundError, ValueError)):
            raise
        raise ValueError(f"Failed to read Parquet dataset: {e}")


def compare_dataframes(df_html, df_parquet):
    """
    Compare two DataFrames for exact match.
    
    Args:
        df_html (pd.DataFrame): DataFrame from HTML table
        df_parquet (pd.DataFrame): DataFrame from Parquet dataset
    
    Returns:
        dict: Comparison result with keys:
            - 'match' (bool): Whether DataFrames match exactly
            - 'html_rows' (int): Number of rows in HTML DataFrame
            - 'parquet_rows' (int): Number of rows in Parquet DataFrame
            - 'html_cols' (list): Column names in HTML DataFrame
            - 'parquet_cols' (list): Column names in Parquet DataFrame
            - 'differences' (list): List of differences found
    """
    result = {
        'match': False,
        'html_rows': len(df_html),
        'parquet_rows': len(df_parquet),
        'html_cols': list(df_html.columns),
        'parquet_cols': list(df_parquet.columns),
        'differences': []
    }
    
    # Check shape
    if df_html.shape != df_parquet.shape:
        result['differences'].append(
            f"Shape mismatch: HTML {df_html.shape} vs Parquet {df_parquet.shape}"
        )
    
    # Check columns
    html_cols = set(df_html.columns)
    parquet_cols = set(df_parquet.columns)
    
    if html_cols != parquet_cols:
        missing_in_parquet = html_cols - parquet_cols
        missing_in_html = parquet_cols - html_cols
        if missing_in_parquet:
            result['differences'].append(f"Columns missing in Parquet: {missing_in_parquet}")
        if missing_in_html:
            result['differences'].append(f"Columns missing in HTML: {missing_in_html}")
    
    # Align columns for comparison
    common_cols = list(html_cols & parquet_cols)
    if common_cols:
        df_html_aligned = df_html[common_cols].reset_index(drop=True)
        df_parquet_aligned = df_parquet[common_cols].reset_index(drop=True)
        
        # Compare values
        try:
            # Try direct comparison
            if not df_html_aligned.equals(df_parquet_aligned):
                # Find specific differences
                for col in common_cols:
                    if not df_html_aligned[col].equals(df_parquet_aligned[col]):
                        mismatched_rows = []
                        for idx in range(len(df_html_aligned)):
                            html_val = df_html_aligned[col].iloc[idx]
                            parquet_val = df_parquet_aligned[col].iloc[idx]
                            if html_val != parquet_val:
                                mismatched_rows.append({
                                    'row': idx,
                                    'html_value': str(html_val),
                                    'parquet_value': str(parquet_val)
                                })
                        result['differences'].append(
                            f"Column '{col}' mismatch: {len(mismatched_rows)} rows differ"
                        )
                        if mismatched_rows:
                            result['differences'].append(f"  Examples: {mismatched_rows[:3]}")
            else:
                result['match'] = True
        except Exception as e:
            result['differences'].append(f"Error during comparison: {e}")
    else:
        result['differences'].append("No common columns to compare")
    
    return result


def format_comparison_report(comparison_result):
    """
    Format comparison result into a readable test report string.
    
    Args:
        comparison_result (dict): Result from compare_dataframes()
    
    Returns:
        str: Formatted test report
    """
    report = []
    report.append("=" * 80)
    report.append("DATA QUALITY COMPARISON REPORT")
    report.append("=" * 80)
    report.append(f"HTML Table Rows: {comparison_result['html_rows']}")
    report.append(f"Parquet Dataset Rows: {comparison_result['parquet_rows']}")
    report.append(f"HTML Columns: {comparison_result['html_cols']}")
    report.append(f"Parquet Columns: {comparison_result['parquet_cols']}")
    report.append("-" * 80)
    
    if comparison_result['match']:
        report.append("✓ MATCH: HTML table and Parquet dataset are identical")
    else:
        report.append("✗ MISMATCH: Differences found between HTML table and Parquet dataset")
        report.append("\nDifferences:")
        for diff in comparison_result['differences']:
            report.append(f"  • {diff}")
    
    report.append("=" * 80)
    return "\n".join(report)
