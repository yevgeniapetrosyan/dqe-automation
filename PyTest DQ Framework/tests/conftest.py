import pytest
import pandas as pd
import os
from pathlib import Path
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.data_quality.data_quality_validation_library import DataQualityLibrary
from src.connectors.file_system.parquet_reader import ParquetReader

def pytest_configure(config):
    """Register custom pytest markers."""
    config.addinivalue_line("markers", "parquet_data: mark test as parquet data test")
    config.addinivalue_line("markers", "smoke: mark test as smoke test")
    config.addinivalue_line("markers", "facility_name_min_time_spent_per_visit_date: test for facility_name_min_time_spent_per_visit_date")
    config.addinivalue_line("markers", "example: mark test as example")

def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default="postgres", help="Database host")
    parser.addoption("--db_user", action="store", default="myuser", help="Database user")
    parser.addoption("--db_password", action="store", default="mypassword", help="Database password")
    parser.addoption("--db_name", action="store", default="mydatabase", help="Database name")
    parser.addoption("--db_port", action="store", default=5432, type=int, help="Database port")
    
    # Calculate default parquet path relative to workspace root
    # conftest.py -> tests -> PyTest DQ Framework -> dqe-automation -> DQ Automation (workspace root)
    workspace_root = Path(__file__).parent.parent.parent.parent
    default_facility_name_path = str(workspace_root / "parquet_data" / "facility_name_min_time_spent_per_visit_date")
    default_facility_type_path = str(workspace_root / "parquet_data" / "facility_type_avg_time_spent_per_visit_date")
    default_patient_sum_treatment_cost_path = str(workspace_root / "parquet_data" / "patient_sum_treatment_cost_per_facility_type")

    parser.addoption(
        "--facility_name_parquet_path",
        action="store",
        default=default_facility_name_path,
        help="Path to facility_name_min_time_spent_per_visit_date parquet file"
    )
    parser.addoption(
        "--facility_type_parquet_path",
        action="store",
        default=default_facility_type_path,
        help="Path to facility_type_avg_time_spent_per_visit_date parquet file"
    )
    parser.addoption(
        "--patient_sum_treatment_cost_parquet_path",
        action="store",
        default=default_patient_sum_treatment_cost_path,
        help="Path to patient_sum_treatment_cost_per_facility_type parquet file"
    )


@pytest.fixture(scope='session')
def db_connection(request):
    db_host = request.config.getoption("--db_host")
    db_user = request.config.getoption("--db_user")
    db_password = request.config.getoption("--db_password")
    db_name = request.config.getoption("--db_name")
    db_port = request.config.getoption("--db_port")
    
    try:
        with PostgresConnectorContextManager(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            db_port=db_port
        ) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.skip(f"Database connection failed: {e}")


@pytest.fixture(scope='session')
def data_quality_library():
    """Fixture to provide DataQualityLibrary instance."""
    return DataQualityLibrary()

@pytest.fixture(scope='module')
def target_data(request):
    test_file_path = str(request.node.fspath)
    test_file_name = os.path.basename(test_file_path)

    if "facility_type_avg_time_spent_per_visit_date" in test_file_name:
        parquet_path = request.config.getoption("--facility_type_parquet_path")
    elif "facility_name_min_time_spent_per_visit_date" in test_file_name:
        parquet_path = request.config.getoption("--facility_name_parquet_path")
    elif "patient_sum_treatment_cost_per_facility_type" in test_file_name:
        parquet_path = request.config.getoption("--patient_sum_treatment_cost_parquet_path")
    else:
        pytest.fail(f"Unknown test file for target_data fixture: {test_file_name}")

    try:
        reader = ParquetReader()
        df = reader.read_parquet(parquet_path)
        return df
    except Exception as e:
        pytest.fail(f"Failed to load target parquet data from {parquet_path}: {e}")

@pytest.fixture(scope='session')
def source_data(db_connection):
    """Fixture to load source data from database."""
    try:
        query = "SELECT * FROM src_generated_visits"
        df = db_connection.get_data_sql(query)
        return df
    except Exception as e:
        pytest.skip(f"Failed to load source data from database: {e}")

