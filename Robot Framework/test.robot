*** Settings ***
Documentation     Data Quality Test - Compare HTML Report Table with Parquet Dataset
Library           SeleniumLibrary
Library           BuiltIn
Library           Collections
Library           String
Library           OperatingSystem


*** Variables ***
${REPORT_FILE}              ${CURDIR}${/}generated_report${/}report.html
${PARQUET_FOLDER}           ${CURDIR}${/}parquet_data${/}facility_type_avg_time_spent_per_visit_date
${FILTER_DATE}              ${EMPTY}
${RESULTS_DIR}              ${CURDIR}${/}results


*** Test Cases ***
Compare HTML Table With Parquet Dataset
    [Documentation]    Opens HTML report, extracts table, reads Parquet data, and compares them
    [Tags]             data-quality    comparison
    [Setup]            Ensure Results Directory Exists
    
    ${report_url}=    Set Variable    file:///${REPORT_FILE}
    
    # Open browser with headless Chrome
    Open Browser With Headless    ${report_url}
    
    # Take screenshot of initial page
    Capture Page Screenshot    ${RESULTS_DIR}${/}01_page_loaded.png
    
    # Wait for table to be visible
    Wait Until Element Is Visible    class:table    timeout=20
    
    # Take screenshot showing table
    Capture Page Screenshot    ${RESULTS_DIR}${/}02_table_visible.png
    
    # Extract table data from HTML
    ${html_df}=    Extract HTML Table To DataFrame
    Log    HTML Table extracted: ${html_df.shape[0]} rows × ${html_df.shape[1]} columns
    
    # Close browser after extraction
    SeleniumLibrary.Close Browser
    
    # Read Parquet dataset
    ${parquet_df}=    Read Parquet With Filter    ${PARQUET_FOLDER}    ${FILTER_DATE}
    Log    Parquet data loaded: ${parquet_df.shape[0]} rows × ${parquet_df.shape[1]} columns
    
    # Compare DataFrames
    ${comparison}=    Compare Dataframes    ${html_df}    ${parquet_df}
    
    # Format report
    ${report}=    Format Comparison Report    ${comparison}
    Log    ${report}    level=INFO
    
    # Save report to file
    Save Report To File    ${report}    ${RESULTS_DIR}${/}comparison_report.txt
    
    # Verify match - fail test if data doesn't match
    Should Be True    ${comparison}[match]    
    ...    msg=DATA MISMATCH DETECTED!\n${report}


*** Keywords ***
Ensure Results Directory Exists
    [Documentation]    Create results directory if it doesn't exist
    Create Directory    ${RESULTS_DIR}


Open Browser With Headless
    [Arguments]    ${url}
    [Documentation]    Open Chrome browser
    
    SeleniumLibrary.Open Browser    ${url}    chrome


Extract HTML Table To DataFrame
    [Documentation]    Extract HTML table element and convert to pandas DataFrame
    
    # Import helper module
    ${helper}=    Evaluate    __import__('sys').path.insert(0, r'''${CURDIR}''') or __import__('helper')
    
    # Get the table element
    ${table_element}=    SeleniumLibrary.Get WebElement    class:table
    
    # Call Python helper function
    ${df}=    Evaluate    __import__('helper').extract_html_table_to_dataframe($table_element)
    
    [Return]    ${df}


Read Parquet With Filter
    [Arguments]    ${parquet_path}    ${filter_date}=${EMPTY}
    [Documentation]    Read Parquet dataset with optional date filtering
    
    # Import helper module
    ${helper}=    Evaluate    __import__('sys').path.insert(0, r'''${CURDIR}''') or __import__('helper')
    
    ${df}=    Run Keyword If    '${filter_date}' == '${EMPTY}'
    ...    Evaluate    __import__('helper').read_parquet_dataset(r'${parquet_path}')
    ...    ELSE
    ...    Evaluate    __import__('helper').read_parquet_dataset(r'${parquet_path}', '${filter_date}')
    
    [Return]    ${df}


Compare Dataframes
    [Arguments]    ${html_df}    ${parquet_df}
    [Documentation]    Compare two DataFrames for exact match
    
    # Import helper module
    ${helper}=    Evaluate    __import__('sys').path.insert(0, r'''${CURDIR}''') or __import__('helper')
    
    ${comparison}=    Evaluate    __import__('helper').compare_dataframes($html_df, $parquet_df)
    
    [Return]    ${comparison}


Format Comparison Report
    [Arguments]    ${comparison}
    [Documentation]    Format comparison result to readable report
    
    # Import helper module
    ${helper}=    Evaluate    __import__('sys').path.insert(0, r'''${CURDIR}''') or __import__('helper')
    
    ${report}=    Evaluate    __import__('helper').format_comparison_report($comparison)
    
    [Return]    ${report}


Save Report To File
    [Arguments]    ${report_text}    ${file_path}
    [Documentation]    Save comparison report to text file
    
    Create File    ${file_path}    ${report_text}
    Log    Report saved to ${file_path}
