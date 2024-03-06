DROP VIEW IF EXISTS analysis.ap_exam_results_post_2022 CASCADE;

/*
This view was created to eventually replace the existing view: analysis.ap_exam_results.

Currently this view is scoped to post-2022 AP data, and is appended onto the end of the existing 
ap_exam_results data. A move done to preserve historical data calculations for now, that may need to change in the future.

The main achievemnet of this view is that it implements the extrapoloation formula for calculiting the number of
URG students in an external dataset by extrapolating the number of two more race students (Tr) in the external set
that are URG and adding that number to the number of BHNAPI students.  It's challenging because the formula requires
you to compute ratios involving 7 whole numbers: 4 from code.org platform and 3 from the external set, in order to solve
for the 8th value: the number of External URG students.
Doc: https://docs.google.com/document/d/1MzJ-1M6EkOXG5At6mEzbsse84b20aoozNyWUWocg8EI/edit

In general the view below stacks CTEs to do the following:

0. Assumptions/Depedencies: 
  a. raw aggregate AP exam data has been loaded into ap_exam_results_raw
  b. code.org demographics data has been logged into csp_csd_demographics_log for current exam year.
  
1. Make base exam results from raw data: 
  For every exam, group, year, gender, race: compute the number of students taking and passing the AP exam.

2. Compute exam results for bhnapi group and union onto base

3. Implement the URG caclulation across a series of CTEs culminating in a lookup table: tr_urg_calc_sheet
Use this to compose AP exam results for URG group for each exam/exam_group and union onto base.

4. (Unimplemented) compute the non_urg group. [total students (excluding other) - urg?]


*/

CREATE OR REPLACE VIEW analysis.ap_exam_results_post_2022 AS (

--Do the base calculations of takers and passers for the raw data available.
with takers_passers_base AS (
SELECT
  ap.exam_year,
  ap.pd_year,
  ap.exam_group,
  ap.rp_id,
  ap.exam,
  --group_name,
  CASE WHEN ap.group_name = 'total' THEN 'all' -- Beware this alias, all. 
                                               -- It historically is the value reported as 'total', 
                                               -- but in theory it should be the sum-of-all-genders to account for any non-reporting.
                                               -- however, historically the sum-of-all-genders always equals this total which is why it's
                                               -- used here.  There is an aggregate corallary 'all' for race which is the sum-of-all-races
       WHEN ap.group_name IN ('female','male','other_gender') THEN ap.group_name ELSE NULL 
  END AS gender,
  CASE 
    WHEN ap.group_name NOT IN ('total','female','male','other_gender') THEN ap.group_name ELSE NULL 
  END AS race,
  SUM(CASE WHEN ap.group_score IN ('1','2','3','4','5') THEN ap.num_students ELSE 0 END) num_takers,
  SUM(CASE WHEN ap.group_score IN ('3','4','5') THEN ap.num_students ELSE 0 END) num_passers,
  (num_passers::float)/(num_takers::float) AS pct_passed
  
FROM analysis.ap_exam_results_raw_long AS ap
WHERE ap.exam_year = '2023' --Followup work: This should eventually be done for ALL years when the old ap_exam_results can be deprecated.
GROUP BY 
  ap.exam_year,
  ap.pd_year,
  ap.exam_group,
  ap.rp_id,
  ap.exam,
  ap.group_name
),
-- Now start composing other aggregations of groups needed for reporting (all-race, no_response, bhnapi, urg, non_urg)
-- Build each and union all together at the end
all_race AS (

  SELECT 
    tp.exam_year,
    tp.pd_year,
    tp.exam_group,
    tp.rp_id,
    tp.exam,
    NULL as gender,
    'all' as "race",
    SUM(CASE WHEN tp.race IN ('bl','hi','hp','am','wh','as','tr') THEN tp.num_takers ELSE 0 END) all_num_takers,
    SUM(CASE WHEN tp.race IN ('bl','hi','hp','am','wh','as','tr') THEN tp.num_passers ELSE 0 END) all_num_passers,
    all_num_passers::float/all_num_takers::float AS pct_passed
  FROM takers_passers_base tp
  GROUP BY 1,2,3,4,5,6,7
)
, no_response AS (
  --SELECT * FROM tp_all_race
  --UNION ALL
  SELECT 
    tp.exam_year,
    tp.pd_year,
    tp.exam_group,
    tp.rp_id,
    tp.exam,
    NULL as gender,
    'no_response' as "race",
    
    -- 'no response' for race is not reported directly. We need to compute the difference between the 'total' (which is aliased as gender='all')
    -- and the sum-of-all-races
    SUM(CASE WHEN tp.gender = 'all' THEN tp.num_takers ELSE 0 END)
      - SUM(CASE WHEN tp.race IN ('bl','hi','hp','am','wh','as','tr') THEN tp.num_takers ELSE 0 END) no_response_takers,
    
    SUM(CASE WHEN tp.gender = 'all' THEN tp.num_passers ELSE 0 END)
      - SUM(CASE WHEN tp.race IN ('bl','hi','hp','am','wh','as','tr') THEN tp.num_passers ELSE 0 END) no_response_passers,
      
    no_response_passers::float/no_response_takers::float AS pct_passed
  FROM takers_passers_base tp
  GROUP BY 1,2,3,4,5,6,7

)
, bhnapi AS(
--SELECT * FROM tp_all_race
--UNION ALL
SELECT 
  tp.exam_year,
  tp.pd_year,
  tp.exam_group,
  tp.rp_id,
  tp.exam,
  NULL as gender,
  'bhnapi' as "race",
  SUM(CASE WHEN tp.race IN ('bl','hi','hp','am') THEN tp.num_takers ELSE 0 END) bhnapi_num_takers,
  SUM(CASE WHEN tp.race IN ('bl','hi','hp','am') THEN tp.num_passers ELSE 0 END) bhnapi_num_passers,
  bhnapi_num_passers::float/bhnapi_num_takers::float AS pct_passed
FROM takers_passers_base tp
GROUP BY 1,2,3,4,5,6,7
)
/*
  BEGIN: making CTEs to calculate the external URG value for these data sets. 
  See doc:
  TL;DR - 1. lookup stored,stable CDO demographic values for the right dataset
  2. compute all of the tr_total, bnapai, sr_totals etc needed for the calculation
  3. prepare a wide table that shows all the work (tr_urg_calc_sheet)
  4. Use that to populate the race='urg' values for all ap exam result sets.
  
*/
, cdo_demographics AS ( -- create row per year containing the cdo values needed to compute ext_tr_urg

  SELECT
    dataset_name,
    school_year,
    SUM(CASE WHEN bhnapi=1 THEN user_count ELSE 0 END)::float cdo_bhnapi,
    SUM(CASE WHEN urg=1 and race_group='tr' THEN user_count ELSE 0 END)::float cdo_tr_urg,
    SUM(case WHEN race_group = 'tr' THEN user_count ELSE 0 END)::float cdo_tr_total,
    SUM(case WHEN bhnapi=1 OR race_group='white' OR race_group='asian' THEN user_count ELSE 0 END) cdo_sr_total --dilemma: include 'other' here or no?
  FROM analysis.csp_csd_demographics_log cdo
  WHERE dataset_name = 'ballmer 2023' -- alter to include multiple years if nec.
    -- FOLLOWUP WORK: for this work retroactively, we'll need to put retroactive demographic data into this csp_csd_demographics_log
  GROUP BY 1,2
), ext_demographics AS ( -- Make separte pulls for takers and passers (and union) in order to normalize column names for tr calcs
  SELECT 
    tp.exam_year,
    tp.pd_year,
    tp.exam_group,
    tp.rp_id,
    tp.exam,
    'takers' AS exam_status,
    SUM(case when tp.race='bhnapi' THEN tp.num_takers ELSE 0 END)::float ext_bhnapi,
    SUM(case WHEN tp.race='tr' THEN tp.num_takers ELSE 0 END)::float ext_tr_total,
    SUM(case WHEN tp.race IN ('bl','hi','hp','am','wh','as') THEN num_takers ELSE 0 END)::float ext_sr_total
  FROM --tp_all_bhnapi tp
    (SELECT * FROM takers_passers_base 
    UNION ALL 
    SELECT * FROM bhnapi) tp
  GROUP BY 1,2,3,4,5,6
 
 UNION ALL
  
  SELECT 
    tp.exam_year,
    tp.pd_year,
    tp.exam_group,
    tp.rp_id,
    tp.exam,
    'passers' AS exam_status,   
    SUM(case when race='bhnapi' THEN num_passers ELSE 0 END) ext_bhnapi,
    SUM(case WHEN race='tr' THEN num_passers ELSE 0 END) ext_tr_total,
    SUM(case WHEN race IN ('bl','hi','hp','am','wh','as') THEN num_passers ELSE 0 END) ext_sr_total
  FROM --tp_all_bhnapi tp
       (SELECT * FROM takers_passers_base UNION ALL SELECT * FROM bhnapi) tp
  GROUP BY 1,2,3,4,5,6
), tr_urg_calc_sheet AS ( 
  --This is a wide CTE table to over-communicate the calcs and values that go into the tr_urg calculation for each data set
  --The only number needed is ext_urg_calc, but it should be derivable from all the other columns displayed
  
  SELECT
  ext.*,
  cdo.*,
  
  (cdo_tr_urg / cdo_tr_total) cdo_tr_pct,
  (cdo_bhnapi / cdo_sr_total) cdo_sr_pct,
  (ext_bhnapi / ext_sr_total) ext_sr_pct,
  (cdo_tr_pct / cdo_sr_pct) * ext_sr_pct AS ext_tr_pct,
  ext_tr_total * (cdo_tr_pct / cdo_sr_pct) * ext_sr_pct AS ext_tr_urg_calc,
  ext_bhnapi + ext_tr_urg_calc AS ext_urg_calc
  FROM cdo_demographics cdo
  LEFT JOIN ext_demographics ext ON ext.exam_year = ('20'||RIGHT(cdo.school_year,2)) -- I'm nervous about this join condition.
  -- In theory we want to cross-join the single-row of cdo demographics with every row in external data...for each year.
  -- but it works as of 01.03.24
)
, urg AS (
  --SELECT * FROM tp_all_bhnapi
  --UNION ALL
  SELECT
    u.exam_year,
    u.pd_year,
    u.exam_group,
    u.rp_id,
    u.exam,
    NULL as gender,
    'urg' as "race",
    round(SUM(CASE WHEN u.exam_status = 'takers' THEN u.ext_urg_calc ELSE 0 END)) urg_num_takers,
    round(SUM(CASE WHEN u.exam_status = 'passers' THEN u.ext_urg_calc ELSE 0 END)) urg_num_passers,
    
    urg_num_passers::float/urg_num_takers::float AS pct_passed
  FROM tr_urg_calc_sheet u
  GROUP BY 1,2,3,4,5,6,7
)
, non_urg AS (

  SELECT
    u.exam_year,
    u.pd_year,
    u.exam_group,
    u.rp_id,
    u.exam,
    NULL as gender,
    'non_urg' as "race",
    
    -- non_urg = the sum-of-all-races (race='all') MINUS the urg group
    -- use these already computed values from CTEs prior
    SUM(CASE WHEN u.race = 'all' THEN u.all_num_takers ELSE 0 END)
      - SUM(CASE WHEN u.race = 'urg' THEN u.all_num_takers ELSE 0 END) non_urg_num_takers,
    
    SUM(CASE WHEN u.race = 'all' THEN u.all_num_passers ELSE 0 END)
      - SUM(CASE WHEN u.race = 'urg' THEN u.all_num_passers ELSE 0 END) non_urg_num_passers,
   
    non_urg_num_passers::float/non_urg_num_takers::float AS pct_passed
     
  FROM (SELECT * FROM all_race UNION ALL SELECT * FROM urg) u --note: num_takers/passers get aliased as all_num_takers because all_race is first in the union
  GROUP BY 1,2,3,4,5,6,7
)
, tr_urg AS (
  --SELECT * FROM tp_all_bhnapi
  --UNION ALL
  SELECT
    u.exam_year,
    u.pd_year,
    u.exam_group,
    u.rp_id,
    u.exam,
    NULL as gender,
    'tr_urg' as "race",
    round(SUM(CASE WHEN u.exam_status = 'takers' THEN u.ext_tr_urg_calc ELSE 0 END)) urg_num_takers,
    round(SUM(CASE WHEN u.exam_status = 'passers' THEN u.ext_tr_urg_calc ELSE 0 END)) urg_num_passers,
    
    urg_num_passers::float/urg_num_takers::float AS pct_passed
  FROM tr_urg_calc_sheet u
  GROUP BY 1,2,3,4,5,6,7
)
SELECT * FROM takers_passers_base
UNION ALL
SELECT * FROM all_race
UNION ALL
SELECT * FROM no_response
UNION ALL
SELECT * FROM bhnapi
UNION ALL
SELECT * FROM tr_urg
UNION ALL
SELECT * FROM non_urg
UNION ALL
SELECT * FROM urg
) with no schema binding
;

GRANT ALL ON analysis.ap_exam_results_post_2022 TO GROUP admin;
GRANT SELECT ON analysis.ap_exam_results_post_2022 TO GROUP reader, GROUP reader_pii;  


SELECT exam_year, exam, exam_group, female_all FROM ap_exam_results_raw WHERE exam='csp' LIMIT 100;
