

/* 
   This source data has BOTH csp and csa school-level results for 2022. This code splits the source
   data into CSP and CSA parts to adhere to naming conventions established for AP data
   
   WARNING: This CSA data for 2022 is bad
   The college board sent us CSA school-level results for schools that had audited as code.org CSP.
   Until we can refine this source data for schools that *actually* used Code.org's pilot CSA curriculum
   for the 2022 exam we SHOULD NOT LOAD 2022 CSA EXAM RESULTS DATA.

   I'm leaving this code here and the base table it creates so that (a) we have a set of tables that fit
   the naming conventions established for AP data (b) shows where/how to take on future work if we want to load this data
*/ 

with ap_data AS (
    SELECT * FROM public.seed_2022_school_level_ap_exam_results
    where 
        --false -- force it to return 0 rows

        subject_nm = 'Computer Sci A'
)
SELECT * FROM ap_data