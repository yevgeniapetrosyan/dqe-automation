import pytest
import pandas as pd
import os

@pytest.fixture
def csv_dataframe():
    """Read the CSV file and return as a pandas DataFrame"""
    csv_path = os.path.join(os.path.dirname(__file__), "../src/data/data.csv")
    df = pd.read_csv(csv_path)
    return df


@pytest.fixture
def csv_file_path():
    """Return the path to the CSV file"""
    return os.path.join(os.path.dirname(__file__), "../src/data/data.csv")


@pytest.fixture(scope="session")
def csv_content(path_to_file):
    """Read the CSV file and return its content as a pandas DataFrame"""
    df = pd.read_csv(path_to_file)
    return df


@pytest.fixture(scope="session")
def validate_schema(actual_schema, expected_schema):
    """Validate that the actual schema matches the expected schema"""
    assert set(actual_schema) == set(expected_schema), (
        f"Schema mismatch: Expected {expected_schema}, "
        f"but found {actual_schema}. Missing: {set(expected_schema) - set(actual_schema)}, "
        f"Extra: {set(actual_schema) - set(expected_schema)}"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "validate_csv: mark test as a CSV validation test")
    config.addinivalue_line("markers", "unmarked: mark tests that have no explicit pytest marks")


def pytest_collection_modifyitems(config, items):
    """Dynamically mark tests without explicit marks as unmarked."""
    for item in items:
        if not list(item.iter_markers()):
            item.add_marker(pytest.mark.unmarked)
