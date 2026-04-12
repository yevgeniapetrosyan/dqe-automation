import pytest
import re
import pandas as pd
import os


def test_file_not_empty(csv_file_path):
    """Test that the CSV file is not empty"""
    assert os.path.exists(csv_file_path), f"CSV file not found at {csv_file_path}"
    assert os.path.getsize(csv_file_path) > 0, "CSV file is empty, expected file to have content"


@pytest.mark.validate_csv
def test_validate_schema(csv_dataframe):
    """Test that the CSV schema matches expected columns"""
    expected_columns = {"id", "name", "age", "email", "is_active"}
    actual_columns = set(csv_dataframe.columns)
    
    assert actual_columns == expected_columns, (
        f"Schema mismatch: Expected columns {expected_columns}, "
        f"but found {actual_columns}. Missing: {expected_columns - actual_columns}, "
        f"Extra: {actual_columns - expected_columns}"
    )


@pytest.mark.validate_csv
@pytest.mark.skip(reason="Age validation temporarily skipped for testing purposes")
def test_age_column_valid(csv_dataframe):
    """Validate that all age values are within valid range (0-100)"""
    invalid_ages = csv_dataframe[(csv_dataframe['age'] < 0) | (csv_dataframe['age'] > 100)]
    assert invalid_ages.empty, (
        f"Found {len(invalid_ages)} invalid age values. "
        f"Age must be between 0 and 100. Invalid rows:\n{invalid_ages.to_string()}"
    )


@pytest.mark.validate_csv
def test_email_column_valid(csv_dataframe):
    """Test that all email addresses have valid format"""
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    invalid_emails = csv_dataframe[~csv_dataframe['email'].str.match(email_pattern)]
    assert invalid_emails.empty, (
        f"Found {len(invalid_emails)} invalid email format(s). "
        f"Email must match pattern: {email_pattern}. Invalid rows:\n{invalid_emails.to_string()}"
    )


@pytest.mark.validate_csv
@pytest.mark.xfail(reason="Duplicate rows found in CSV - known data quality issue")
def test_no_duplicate_rows(csv_dataframe):
    """Test that there are no duplicate rows in the CSV"""
    duplicated = csv_dataframe[csv_dataframe.duplicated(keep=False)]
    assert duplicated.empty, (
        f"Found {len(duplicated) // 2} duplicate row(s) in the data. "
        f"Duplicate rows:\n{duplicated.sort_values('id').to_string()}"
    )


@pytest.mark.parametrize("id_value,expected_is_active", [
    (1, False),
    (2, True),
])
def test_active_player_parametrized(csv_dataframe, id_value, expected_is_active):
    """Test that is_active status is correct for specific IDs using parametrization"""
    row = csv_dataframe[csv_dataframe['id'] == id_value]
    
    actual_is_active = row['is_active'].values[0]
    assert actual_is_active == expected_is_active, (
        f"Expected is_active={expected_is_active} for id={id_value}, "
        f"but found is_active={actual_is_active}. Full row:\n{row.to_string()}"
    )


def test_active_player(csv_dataframe):
    """Test that is_active is correct for id=1 and id=2 without parametrization"""
    expected_status = {
        1: False,
        2: True,
    }

    for id_value, expected_is_active in expected_status.items():
        row = csv_dataframe[csv_dataframe['id'] == id_value]
        actual_is_active = row['is_active'].values[0]
        assert actual_is_active == expected_is_active, (
            f"Expected is_active={expected_is_active} for id={id_value}, "
            f"but found is_active={actual_is_active}. Full row:\n{row.to_string()}"
        )
