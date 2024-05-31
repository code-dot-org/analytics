/* 
    This should be just a straight load of the data from source, and should be a 1:1 - CB dataset : dashboard table

    These files are typically 75+ columns.

    FIRST 8 COLUMNS:
    The first 8 columns must exist and be in the EXACT order listed below.

    NOTES: 
    - columns can have different names, they will be re-aliased at staging layer. 
    - columns given to us by the college board vary year to year.  If on the columns is not included, please add it with null values in the raw csv that is sourced, or as output here.
    - Values can vary year to year, they will be normalized at staging by macros, just load the raw data as is here. 
    - First 8 cols are all varchar/text
    - content of first 8 cols must be in this order: 

        examyr4                 - e.g. 2022
        exam name               - e.g. csa, cs principles, csp, etc.
        country_descr           - e.g. United States
        ai_code                 - 6-digit code
        high_school             - name of high school
        state_abbrev            - e.g. IL
        ap_school_type          - e,g public, non-public, etc.
        analysis_school_type    - e,g Public, Non-public

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
    select * 
    from {{source('external_datasets','stg_ap_school_level_exam_results_raw_2023')}}
)
select *

from ap_data

