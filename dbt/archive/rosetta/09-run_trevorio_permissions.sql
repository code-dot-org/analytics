CREATE OR REPLACE PROCEDURE public.set_trevorio_permissions()
 LANGUAGE plpgsql
AS $$
BEGIN

    -- rosetta_v2
    GRANT SELECT ON analysis.section_starts TO GROUP reader_trevorio;
    GRANT SELECT ON analysis.teachers TO GROUP reader_trevorio;
    GRANT SELECT ON analysis.students TO GROUP reader_trevorio;
    GRANT SELECT ON analysis.student_teacher_section_complete TO GROUP reader_trevorio;
    GRANT SELECT ON analysis.student_activity_stats TO GROUP reader_trevorio;
    GRANT SELECT ON analysis.section_stats TO GROUP reader_trevorio;
    GRANT SELECT ON analysis.section_grade TO GROUP reader_trevorio;
    
    -- school course status and school status
    GRANT SELECT ON analysis.school_course_status TO GROUP reader_trevorio;
    GRANT SELECT ON analysis.school_status TO GROUP reader_trevorio;
END;
$$
