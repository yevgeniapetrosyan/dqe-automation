import pandas as pd

class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df, column_names=None):
        # Check for duplicate rows based on specified columns or all columns
        if column_names:
            duplicates = df.duplicated(subset=column_names)
        else:
            duplicates = df.duplicated()
        assert not duplicates.any(), "Duplicate records found"

    @staticmethod
    def check_count(df1, df2):
        # Check if row counts match
        assert len(df1) == len(df2), f"Row count mismatch: source={len(df1)}, target={len(df2)}"

    @staticmethod
    def check_data_completeness(df1, df2):
        # Check if all rows in df1 are present in df2
        missing = df1.merge(df2, how='left', indicator=True).loc[lambda x: x['_merge'] == 'left_only']
        assert missing.empty, f"Missing rows in target dataset: {missing}"

    @staticmethod
    def check_dataset_is_not_empty(df):
        # Check if DataFrame is not empty
        assert not df.empty, "Dataset is empty"

    @staticmethod
    def check_not_null_values(df, column_names=None):
        # Check for null values in specified columns or all columns
        if column_names:
            for col in column_names:
                assert df[col].notna().all(), f"Column {col} contains null values"
        else:
            assert df.notna().all().all(), "DataFrame contains null values"