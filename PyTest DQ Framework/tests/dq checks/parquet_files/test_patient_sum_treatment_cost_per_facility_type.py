"""
Description: Data Quality checks for patient_sum_treatment_cost_per_facility_type
Requirement(s): TICKET-0003
Author(s): Yevgenia Petrosyan
"""

import pytest


@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    """
    Description: Checks that the target dataset is not empty.
    """
    data_quality_library.check_dataset_is_not_empty(target_data)

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_sum_treatment_cost_non_negative(target_data):
    """
    Description: Checks that all values in the sum_treatment_cost column are non-negative.
    """
    assert (target_data['sum_treatment_cost'] >= 0).all(), "There are negative values in sum_treatment_cost column"

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_not_null_values(target_data, data_quality_library):
    """
    Description: Validates that specific columns in the dataset do not contain null values.
    """
    data_quality_library.check_not_null_values(target_data, ['facility_type', 'full_name', 'sum_treatment_cost'])

@pytest.mark.parquet_data
@pytest.mark.patient_sum_treatment_cost_per_facility_type
def test_check_no_duplicates(target_data, data_quality_library):
    """
    Description: Ensures there are no duplicate records across all columns in the dataset.
    """
    data_quality_library.check_duplicates(target_data, column_names=target_data.columns.tolist())
