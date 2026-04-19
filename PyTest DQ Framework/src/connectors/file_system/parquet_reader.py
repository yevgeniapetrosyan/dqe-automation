import pandas as pd


class ParquetReader:
    """
    A utility class for reading Parquet files using pandas.
    """

    @staticmethod
    def read_parquet(file_path):
        """
        Read a Parquet file and return a pandas DataFrame.
        
        Args:
            file_path (str): The path to the Parquet file.
            
        Returns:
            pd.DataFrame: The data from the Parquet file.
        """
        try:
            df = pd.read_parquet(file_path)
            return df
        except Exception as e:
            raise Exception(f"Failed to read parquet file {file_path}: {e}")

    @staticmethod
    def read_parquet_with_filter(file_path, filters=None):
        """
        Read a Parquet file with optional filtering.
        
        Args:
            file_path (str): The path to the Parquet file.
            filters (list): Optional filters to apply (pyarrow filter format).
            
        Returns:
            pd.DataFrame: The filtered data from the Parquet file.
        """
        try:
            df = pd.read_parquet(file_path, filters=filters)
            return df
        except Exception as e:
            raise Exception(f"Failed to read parquet file with filters {file_path}: {e}")
