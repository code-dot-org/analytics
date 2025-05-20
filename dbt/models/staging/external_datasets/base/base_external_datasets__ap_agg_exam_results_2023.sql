/* 
    This should be just a straight load of the data from source, and should be a 1:1 - CB dataset : dashboard table

    These files are typically 75+ columns.

    FIRST 5 COLUMNS:
    The first 5 columns must exist and be in the EXACT order listed below.

    NOTES: 
    - columns can have different names, they will be re-aliased at staging layer. 
    - columns given to us by the college board vary year to year.  If one of the columns is not included, please add it with null values in the raw csv that is sourced, or as output here.
    - Values can vary year to year, they will be normalized at staging by macros, just load the raw data as is here. 
    - First 5 cols are all varchar/text
    - content of first 8 cols must be in this order: 

        exam_year                - e.g. 2022
        pd_year                 - included for historical reasons will be NULL for all years after 2022
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
*/ 

with ap_data AS (
    select * from {{ source('external_datasets','stg_ap_agg_exam_results_raw_2023') }}
)
select 
    * 
from ap_data