# Data Quality Test Suite

## Overview
This Robot Framework test suite compares HTML table data from a Plotly-generated report with partitioned Parquet dataset to ensure data quality and consistency.

## Files

- **test.robot** - Main Robot Framework test suite with test cases and keywords
- **helper.py** - Python helper functions for:
  - Extracting HTML table data using Selenium
  - Reading partitioned Parquet datasets with optional filtering
  - Comparing DataFrames for exact match
  - Generating comparison reports
- **requirements.txt** - Python dependencies
- **generated_report/report.html** - Local HTML report containing the table to extract
- **parquet_data/** - Partitioned Parquet dataset directories:
  - `facility_name_min_time_spent_per_visit_date/`
  - `facility_type_avg_time_spent_per_visit_date/` (default used by test)
  - `patient_sum_treatment_cost_per_facility_type/`
- **results/** - Output directory for test results, screenshots, and reports

## Setup

### 1. Install Dependencies
```bash
cd "Robot Framework"
pip install -r requirements.txt
```

### 2. Verify Report File Exists
Ensure `generated_report/report.html` exists at the correct location.

### 3. Verify Parquet Data Exists
Ensure `parquet_data/` contains the partition folders.

## Running the Test

### Basic Test Execution
```bash
robot --outputdir ./results test.robot
```

### With Specific Parquet Partition
By default, the test uses `facility_type_avg_time_spent_per_visit_date`. To use a different partition, modify the `${PARQUET_FOLDER}` variable in test.robot:

```robot
${PARQUET_FOLDER}           ${CURDIR}${/}parquet_data${/}facility_name_min_time_spent_per_visit_date
```

### With Date Filtering
To filter Parquet data by date, set the `${FILTER_DATE}` variable:

```bash
robot --variable FILTER_DATE:2002-06-15 --outputdir ./results test.robot
```

### With Verbose Output
```bash
robot --outputdir ./results --loglevel DEBUG test.robot
```

## Test Outputs

After running the test, check the `results/` directory for:

- **log.html** - Detailed Robot Framework execution log
- **report.html** - Robot Framework test report
- **output.xml** - Machine-readable test results
- **01_page_loaded.png** - Screenshot of initial page load
- **02_table_visible.png** - Screenshot of visible table
- **comparison_report.txt** - Detailed comparison report with differences (if any)

## Test Workflow

1. **Setup**: Create results directory
2. **Browser Open**: Opens Chrome browser and navigates to HTML report (file:// URL)
3. **Page Load**: Takes screenshot after page loads
4. **Table Wait**: Waits for table to be visible (20 second timeout)
5. **Table Visible**: Takes screenshot of visible table
6. **HTML Extraction**: Extracts table data from HTML using Selenium
   - Finds all columns (class="y-column")
   - Extracts headers (id="header")
   - Extracts rows (class="cell-text")
   - Converts to pandas DataFrame
7. **Browser Close**: Closes browser after extraction
8. **Parquet Read**: Reads partitioned Parquet dataset
   - Discovers partition structure
   - Optionally applies date filter
   - Loads all partition data into pandas DataFrame
9. **Comparison**: Compares both DataFrames
   - Checks shape (rows × columns)
   - Checks column names
   - Compares values exactly
   - Reports differences if found
10. **Report**: Generates formatted comparison report
11. **Verification**: 
    - Saves report to file
    - Test passes if data matches exactly
    - Test fails with detailed differences if mismatch found

## Troubleshooting

### Error: "No columns found under table"
- Verify HTML structure hasn't changed
- Check that table still uses `class="table"` and `class="y-column"` structure
- Update CSS selectors in `helper.py` if needed

### Error: "Parquet folder not found"
- Verify parquet_data/ exists in Robot Framework directory
- Verify partition folder name matches ${PARQUET_FOLDER} variable

### Error: "No WebElement found with xpath"
- Ensure report.html is valid and loads correctly
- Check that Chrome/Chromium browser is installed
- Verify SeleniumLibrary and Selenium versions are compatible

### ChromeDriver Issues
- Ensure ChromeDriver version matches installed Chrome version
- SeleniumLibrary should auto-download ChromeDriver, but may need to be explicitly installed

## Customization

### Change Parquet Partition
Edit test.robot and change:
```robot
${PARQUET_FOLDER}           ${CURDIR}${/}parquet_data${/}<partition_folder_name>
```

### Add Date Filtering
Modify the test case to include filter:
```robot
${parquet_df}=    Read Parquet With Filter    ${PARQUET_FOLDER}    2002-06-15
```

### Modify Selenium Locators
If HTML structure changes, update the XPath in test.robot:
```robot
Wait Until Element Is Visible    xpath=//div[@class='table']    timeout=20
${table_element}=    SeleniumLibrary.Get WebElement    xpath=//div[@class='table']
```

## Dependencies

- **Robot Framework** - Test automation framework
- **SeleniumLibrary** - Browser automation for Robot Framework
- **Selenium** - WebDriver for browser control
- **pandas** - Data manipulation and comparison
- **pyarrow** - Parquet file reading
- **Chrome/Chromium** - Web browser

## Test Status

- ✅ Extracts HTML table correctly
- ✅ Reads partitioned Parquet data with filtering
- ✅ Performs exact DataFrame comparison
- ✅ Generates detailed comparison reports
- ✅ Supports optional date filtering
- ✅ Creates step-by-step screenshots for debugging

## Contact

For issues or questions about the test suite, check:
- Robot Framework documentation: https://robotframework.org/
- SeleniumLibrary documentation: https://robotframework.org/SeleniumLibrary/
- Pandas documentation: https://pandas.pydata.org/
