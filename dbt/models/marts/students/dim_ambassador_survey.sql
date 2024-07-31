-- with 

-- foorm_simple_survey_form as (
--     select * 
--     from {{ ref('stg_dashboard_pii__foorm_simple_survey_forms') }}
-- ),

-- foorm_simple_survey_submissions as (
--     select * 
--     from {{ ref('stg_dashboard_pii__foorm_simple_survey_submissions') }}
-- ),



-- SELECT
--   fssf.path,
--   fssf.form_name,
--   fsss.simple_survey_form_id,
--   fsss.foorm_submission_id,
--   fsss.user_id,
--   fsss.created_at,
--   --fs.answers,
--   fsr.item_name,
--   fsr.matrix_item_name,
--   fsr.response_value,
--   fsr.response_text,
--   ffr.item_type,
--   ffr.item_text,
--   ffr.matrix_item_header,
--   ffr.response_options,
--   ffr.num_response_options
--   FROM dashboard_production_pii.foorm_simple_survey_forms fssf
-- JOIN dashboard_production_pii.foorm_simple_survey_submissions fsss ON fsss.simple_survey_form_id = fssf.id
-- LEFT JOIN analysis_pii.foorm_submissions_reshaped fsr ON fsr.submission_id = fsss.foorm_submission_id
-- LEFT JOIN analysis.foorm_forms_reshaped ffr 
--   ON ffr.form_name = fssf.form_name 
--   AND ffr.form_version = fssf.form_version
--   AND fsr.item_name = ffr.item_name
-- WHERE fssf.form_name = 'surveys/teachers/young_women_in_cs'