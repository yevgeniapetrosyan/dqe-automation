"""
Description: Data Quality checks for facility_name_min_time_spent_per_visit_date
Requirement(s): TICKET-0001
Author(s): Yevgenia Petrosyan
"""

import pytest


@pytest.mark.parquet_data
@pytest.mark.smoke
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_dataset_is_not_empty(target_data, data_quality_library):
    """
    Description: Checks that the target dataset is not empty.
    """
    data_quality_library.check_dataset_is_not_empty(target_data)


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_min_time_spent_positive(target_data):
    """
    Description: Checks that all values in the min_time_spent column are positive.
    """
    assert (target_data['min_time_spent'] > 0).all(), "There are non-positive values in min_time_spent column"


@pytest.mark.parquet_data
@pytest.mark.facility_name_min_time_spent_per_visit_date
def test_check_not_null_values(target_data, data_quality_library):
    """
    Description: Validates that specific columns in the dataset do not contain null values.
    """
    data_quality_library.check_not_null_values(target_data, ['facility_name', 'visit_date', 'min_time_spent'])