CREATE OR REPLACE PROCEDURE public.run_section_log()
 LANGUAGE plpgsql
AS $$

BEGIN

RAISE INFO 'START building tables';
-- Essentially SELECT * FROM sections into a table.
DROP TABLE IF EXISTS public.bf_sections_log_today;
CREATE TABLE public.bf_sections_log_today AS (
  SELECT 
    id,
    user_id,
    name,
    created_at,
    updated_at,
    deleted_at,
    script_id,
    course_id,
    grade,
    login_type,
    first_activity_at,
    hidden
  FROM dashboard_production.sections
  WHERE created_at > '2019-07-01'
);



-- Figure out how todays pull of SELECT * FROM sections is different from last time, but only for key fields.
DROP TABLE IF EXISTS public.sections_changes_today;
CREATE TABLE public.sections_changes_today diststyle key distkey(id) AS (
  SELECT
    id,
    user_id,
    name,
    created_at,
    updated_at,
    deleted_at,
    script_id,
    course_id,
    grade,
    login_type,
    first_activity_at,
    hidden
  
  FROM bf_sections_log_today

  except

  SELECT -- have to pull fields explicity to ignore the pulled_at field when comparing
      id,
      user_id,
      name,
      created_at,
      updated_at,
      deleted_at,
      script_id,
      course_id,
      grade,
      login_type,
      first_activity_at,
      hidden
      --pulled_at
  FROM bf_sections_log
);

RAISE INFO 'tables built. Adding timestamps...';

--SELECT COUNT(*) num_section_records_changed FROM sections_changes_today;
-- add a pulled_at field into today's changes
ALTER TABLE public.sections_changes_today ADD pulled_at date;
UPDATE public.sections_changes_today SET pulled_at = (SELECT max(updated_at)::date FROM public.bf_sections_log_today);


--Use alter table append once we figure out the distribution style thing
--ALTER TABLE public.bf_sections_log APPEND FROM public.sections_changes_today;
RAISE INFO 'Unioning new records to sections_log';

CREATE TABLE public.bf_sections_log_BUILD AS (
  SELECT * FROM public.bf_sections_log 
  union all  
  SELECT * FROM public.sections_changes_today
);


-- SELECT COUNT(*), pulled_at FROM bf_sections_log_BUILD GROUP BY 2 ORDER BY pulled_at DESC LIMIT 100;

RAISE INFO 'Done. Renaming.';

DROP TABLE public.bf_sections_log;
ALTER TABLE public.bf_sections_log_BUILD RENAME TO bf_sections_log;

RAISE INFO 'Setting permissions';

GRANT ALL ON public.bf_sections_log TO GROUP admin;
GRANT SELECT ON public.bf_sections_log TO GROUP reader_pii;

--SELECT COUNT(*), pulled_at FROM bf_sections_log GROUP BY 2 ORDER BY pulled_at DESC LIMIT 100;


END;
$$
