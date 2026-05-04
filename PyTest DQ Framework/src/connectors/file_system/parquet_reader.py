import pandas as pd
import os
from pathlib import Path


class ParquetReader:
    """
    A utility class for reading Parquet files and partitioned datasets using pandas.
    """

    @staticmethod
    def read_parquet(file_path):
        """
        Read a Parquet file or partitioned dataset and return a pandas DataFrame.
        
        Args:
            file_path (str): The path to the Parquet file or partitioned dataset directory.
            
        Returns:
            pd.DataFrame: The data from the Parquet file/dataset.
        """
        try:
            path = Path(file_path)
            
            # Check if it's a directory (partitioned dataset)
            if path.is_dir():
                # For partitioned data, read all parquet files in the directory
                parquet_files = list(path.glob("**/*.parquet"))
                if not parquet_files:
                    raise FileNotFoundError(f"No parquet files found in directory {file_path}")
                
                # Read all parquet files and concatenate
                dfs = []
                for parquet_file in parquet_files:
                    df = pd.read_parquet(parquet_file)
                    dfs.append(df)
                
                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    return combined_df
                else:
                    raise ValueError(f"No data found in parquet files in {file_path}")
            else:
                # Single parquet file
                df = pd.read_parquet(file_path)
                return df
                
        except Exception as e:
            raise Exception(f"Failed to read parquet file/dataset {file_path}: {e}")

    @staticmethod
    def read_parquet_with_filter(file_path, filters=None):
        """
        Read a Parquet file or partitioned dataset with optional filtering.
        
        Args:
            file_path (str): The path to the Parquet file or partitioned dataset directory.
            filters (list): Optional filters to apply (pyarrow filter format).
            
        Returns:
            pd.DataFrame: The filtered data from the Parquet file/dataset.
        """
        try:
            path = Path(file_path)
            
            # Check if it's a directory (partitioned dataset)
            if path.is_dir():
                # For partitioned data, read all parquet files in the directory
                parquet_files = list(path.glob("**/*.parquet"))
                if not parquet_files:
                    raise FileNotFoundError(f"No parquet files found in directory {file_path}")
                
                # Read all parquet files and concatenate
                dfs = []
                for parquet_file in parquet_files:
                    df = pd.read_parquet(parquet_file, filters=filters)
                    dfs.append(df)
                
                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    return combined_df
                else:
                    raise ValueError(f"No data found in parquet files in {file_path}")
            else:
                # Single parquet file
                df = pd.read_parquet(file_path, filters=filters)
                return df
                
        except Exception as e:
            raise Exception(f"Failed to read parquet file/dataset with filters {file_path}: {e}")
