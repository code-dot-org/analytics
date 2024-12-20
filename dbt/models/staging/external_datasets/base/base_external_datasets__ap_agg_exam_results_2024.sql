/* 
    This model loads the ap aggregate exam data and formats it just enough that it can go into the AP aggregate staging script
    
    These files are typically 75+ columns.

    FIRST 5 COLUMNS:
    The first 5 columns must exist and be in the EXACT order listed below.

    NOTES: 
    - columns can have different names, they will be re-aliased at staging layer. 
    - columns given to us by the college board vary year to year.  The goal here is to eliminate manual edits to the CSV files.
    - Values can vary year to year, they will be normalized at staging by macros, just load the raw data as is here. 
    - First 5 cols are all varchar/text
    - content of first 5 cols must be in this order: 

        exam_year                - e.g. 2022
        pd_year                 - included for historical reasons, it exam_year - 1
        exam_group              - the name of the aggregate report e.g. 'national','csp_users_and_audit', etc.
        rp_id                   - included for historical reasons will be NULL for all years after 2022
        exam                    - e.g. the name of the exam the report is for.  It can vary and will be normalized later csa, cs principles, csp, COMPSCP etc.

    SUBSEQUENT COLUMNS:
    Subesquent ~70+ columns should all contain numbers of students earning certain exam scores, and all column names should follow the pattern: [demographic_group]_[score_type]

    For example:
    - black_1
    - black_2
    - black_3
    - black_4
    - black_5
    - black_total
    ...

    The number and order of these ^^^ columns does not matter. 
    
    ANNUAL TASK:
    DUPLICATE THIS FILE each year, replace the source, ensure correct columns are being reported.
*/ 

with ap_data AS (
    select * from {{ source('external_datasets','ap_agg_exam_results_2024') }}
)

select 
    '2024' as exam_year,
    '' as pd_year,
    tag_name as exam_group,
    '' as rp_id,
    exam_code as exam,
    AMIND_1,
    AMIND_2,
    AMIND_3,
    AMIND_4, 
    AMIND_5,
    AMIND_ALL,
    ASIAN_1,
    ASIAN_2,
    ASIAN_3,
    ASIAN_4,
    ASIAN_5,
    ASIAN_ALL,
    BLACK_1,
    BLACK_2,
    BLACK_3,
    BLACK_4,
    BLACK_5,
    BLACK_ALL,
    FEMALE_1,
    FEMALE_2,
    FEMALE_3,
    FEMALE_4,
    FEMALE_5,
    FEMALE_ALL,
    GENDER_ANOTHER_1,
    GENDER_ANOTHER_2,
    GENDER_ANOTHER_3,
    GENDER_ANOTHER_4,
    GENDER_ANOTHER_5,
    GENDER_ANOTHER_ALL,
    HISPANIC_1,
    HISPANIC_2,
    HISPANIC_3,
    HISPANIC_4,
    HISPANIC_5,
    HISPANIC_ALL,
    MALE_1,
    MALE_2,
    MALE_3,
    MALE_4,
    MALE_5,
    MALE_ALL,
    NHPI_1,
    NHPI_2,
    NHPI_3,
    NHPI_4,
    NHPI_5,
    NHPI_ALL,
    NORESPONSE_1,
    NORESPONSE_2,
    NORESPONSE_3,
    NORESPONSE_4,
    NORESPONSE_5,
    NORESPONSE_ALL,
    TOTAL_1,
    TOTAL_2,
    TOTAL_3,
    TOTAL_4,
    TOTAL_5,
    TOTAL_ALL,
    TWOMORE_1,
    TWOMORE_2,
    TWOMORE_3,
    TWOMORE_4,
    TWOMORE_5,
    TWOMORE_ALL,
    WHITE_1,
    WHITE_2,
    WHITE_3,
    WHITE_4,
    WHITE_5,
    WHITE_ALL 
from ap_data