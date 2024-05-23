

/* 
   This source data has BOTH csp and csa school-level results for 2022. This code splits the source
   data into CSP and CSA parts to adhere to naming conventions established for AP data
   
*/ 

with ap_data AS (
    SELECT * FROM public.seed_2022_school_level_ap_exam_results
    where 
        subject_nm = 'Computer Sci Prin'
)
SELECT * FROM ap_data