CREATE OR REPLACE PROCEDURE analysis.run_rosetta()
 LANGUAGE plpgsql
AS $$
DECLARE
    last_proc_num integer;
    last_proc_name text;
    current_proc_name text;
    current_proc_num integer;
    v_error_message text;
    i integer := 0;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    elapsed_time INTEGER;
    num_procedures INTEGER;
  
BEGIN
    -- Create temporary table to store procedure names and numbers
    DROP TABLE IF EXISTS procedure_list_temp;
    RAISE INFO 'creating temp table';
    CREATE TEMPORARY TABLE procedure_list_temp(
        proc_num INTEGER,
        proc_name TEXT
    );
  
    -- Insert procedure names and numbers into temporary table
    INSERT INTO procedure_list_temp VALUES
        (1,'public.run_section_log'),
        (2,'public.run_course_structure'),
        (3,'public.rosetta_helper'),
        (4,'public.run_rosetta_v1'),
        (5,'public.run_rosetta_v2'),
        (6,'public.run_school_course_status'),
        (7,'public.run_user_surveys'),
        (8,'public.run_rosetta_intl'),
        (9,'public.set_trevorio_permissions');
    
    -- find max process number
    SELECT MAX(proc_num) INTO num_procedures FROM procedure_list_temp;

    -- Get the last procedure number that was executed from the rosetta_error_log, or start from the beginning. Find the last procedure by selecting the proc_num from rosetta_error_log with the max log_time
    SELECT procedure_name INTO last_proc_name FROM (
        SELECT procedure_name, status
        FROM analysis.rosetta_error_log
        ORDER BY log_time DESC LIMIT 1
    ) last_log WHERE status = 'error';

    IF last_proc_name IS NULL THEN
        last_proc_num := 1;
        RAISE INFO 'Last log message is not an error starting from first procedure';
        INSERT INTO analysis.rosetta_error_log (log_time, procedure_name, message, runtime, status)
        VALUES (CURRENT_TIMESTAMP, '', 'Starting rosetta build from clean start', 0, 'start');
        COMMIT;     
    ELSE
        SELECT proc_num INTO last_proc_num FROM procedure_list_temp WHERE proc_name = last_proc_name;
        
        INSERT INTO analysis.rosetta_error_log (log_time, procedure_name, message, runtime, status)
        VALUES (CURRENT_TIMESTAMP, last_proc_name, 'Picking up rosetta build from procedure that caused error', 0, 'restart');
        COMMIT;
    END IF;
   
    -- Loop through procedures, but start the loop at the procedure that was found in the last step
    i := last_proc_num;
    RAISE INFO 'Starting loop at i= %', i;
    WHILE i <= num_procedures LOOP
    
        SELECT proc_name INTO current_proc_name FROM procedure_list_temp WHERE proc_num = i;
        i := i + 1;

        -- Run procedure
        RAISE INFO 'Starting %', current_proc_name;
        
        INSERT INTO analysis.rosetta_error_log (log_time, procedure_name, message, runtime, status)
        VALUES (CURRENT_TIMESTAMP, current_proc_name, 'Starting procedure...', 0, 'start');
        COMMIT;
       
        BEGIN
          start_time := CURRENT_TIMESTAMP;
          EXECUTE 'CALL ' || current_proc_name || '()';
          RAISE INFO 'Finished %', current_proc_name;
          end_time := CURRENT_TIMESTAMP;
          elapsed_time := extract(epoch from  (end_time - start_time))::integer;

          -- Log success message
--           INSERT INTO analysis.rosetta_error_log (log_time, procedure_name, message, runtime, status)
--           VALUES (CURRENT_TIMESTAMP, current_proc_name, 'Procedure completed successfully', elapsed_time, 'success');
--           COMMIT;
        EXCEPTION WHEN OTHERS THEN
          v_error_message := SQLERRM;
          RAISE INFO 'Error in %: %', current_proc_name, v_error_message;

          -- Log error message
          end_time := CURRENT_TIMESTAMP;
          elapsed_time := extract(epoch from (end_time - start_time))::integer;
          INSERT INTO analysis.rosetta_error_log (log_time, procedure_name, message, runtime, status) 
          VALUES (CURRENT_TIMESTAMP, current_proc_name, v_error_message, elapsed_time, 'error');
          RETURN; -- Stop executing procedures if there is an error
      END;
      
         -- Log success message
      INSERT INTO analysis.rosetta_error_log (log_time, procedure_name, message, runtime, status)
      VALUES (CURRENT_TIMESTAMP, current_proc_name, 'Procedure completed successfully', elapsed_time, 'success');
      COMMIT;
      COMMIT;  -- Commit after the exception block to make sure start_time is still available for end_time calculation

    END LOOP;
  
    -- Drop the temporary table
    DROP TABLE procedure_list_temp;
    INSERT INTO analysis.rosetta_error_log (log_time, procedure_name, message, runtime, status)
    VALUES (CURRENT_TIMESTAMP, '', 'Finished rosetta build at '||CURRENT_TIMESTAMP, 0, 'finished');
END;
$$
