CREATE OR REPLACE PROCEDURE analysis.run_course_structure()
 LANGUAGE plpgsql
AS $$
begin

	create table analysis.course_structure_build as (

		select distinct
			-- manually updated (no longer updating)
			cn.course_name_short          as course_name_short,
			cn.course_name_long           as course_name_long,
			sn.script_name_short          as script_name_short,
			sn.script_name_long           as script_name_long,

			-- courses 
			c.id                          as course_id,
			c.name                        as course_name,

			-- scripts
			sc.id                         as script_id,
			sn.versioned_script_name      as versioned_script_name,
			sc.name                       as script_name,
			
			-- stages 
			st.id                         as stage_id,
			st.name                       as stage_name,
			st.relative_position          as relative_position,
			st.absolute_position          as stage_number,
			st.lockable                   as lesson_lockable,
			json_extract_path_text(
				lower(st.properties), 
				'unplugged', true)      as lesson_unplugged,
				
			-- levels
			case
				when 
						sl.script_id = '26'
						and lsl.level_id = '14633' 
				then '1' else lsl.level_id -- hard coded error correction, level_script_levels defines the first level of this script as id# 14,633 when user_levels defines this level as #1

			end                                       as level_id,
			le.name                                   as level_name,
			sl.position                               as level_number,
			le.type                                   as level_type,
			case
				when json_extract_path_text(
						lower(sl.properties), 
						'challenge', 
						true) = 'true' 
				then 1 else 0 end                   as challenge,

			case
				when json_extract_path_text(
						lower(le.properties), 
						'mini_rubric', 
						true) = 'true' 
				then 1 else 0 end                   as mini_rubric,

			case
				when json_extract_path_text(
						lower(le.properties), 
						'free_play', 
						true) = 'true' 
				then 1 else 0 end                   as free_play,

			json_extract_path_text(
                        	lower(le.properties), 
                        	'project_template_level_name', 
                        	true)                               as project_template_level_name,
			json_extract_path_text(
				lower(le.properties), 
				'submittable', 
				true)                               as submittable,
			case
				when sl.assessment = 1 
				then 1 else 0 end                   as assessment,
			
			case
				when sc.name like 'devices-20__' 
						then 'csd'
				
				when sc.name like '%hello%' 
						then 'hoc'
				
				when sc.name like 'microbit%' 
						then 'csd'
				
				when json_extract_path_text(
						lower(sc.properties), 
						'curriculum_umbrella', 
						true) = '' 
						then 'other' 
				
				else lower(json_extract_path_text(
						lower(sc.properties), 
						'curriculum_umbrella', 
						true)) 
				end                                 as course_name_true,
				
			rank () over (
				partition by 
						sl.script_id
				order by
						stage_number,
						sl.position)                  as level_script_order,


			coalesce(
				json_extract_path_text(
						lower(c.properties), 
						'family_name', 
						true),
				sc.family_name)                     as family_name,
						
			json_extract_path_text(
				lower(sc.properties), 
				'is_course', true)                  as is_standalone,
			
			regexp_replace(
				sc.name, 
				'((-)+\\d{4})', 
				'')                                 as unit,
			
			coalesce(
				-- from course info if available
				json_extract_path_text(
						lower(c.properties), 
						'version_year',
						true), 
				-- from script if course info not available
				json_extract_path_text(
						lower(sc.properties), 
						'version_year', 
						true))                      as version_year,

			coalesce(
				-- from course info if available, from script if not
				c.published_state, 
				sc.published_state)                 as published_state,

			coalesce(
				-- from course info if available, from script if not
				c.instruction_type, 
				sc.instruction_type)                as instruction_type,
			
			coalesce(
				-- from course info if available, from script if not
				c.instructor_audience, 
				sc.instructor_audience)             as instructor_audience,
			
			coalesce(
				-- from course info if available, from script if not
				c.participant_audience, 
				sc.participant_audience)            as participant_audience,

			json_extract_path_text(
				lower(sc.properties),
				'content_area', true)				as content_area,
			
			json_extract_path_text(
				lower(sc.properties),
				'topic_tags', true)					as topic_tags,
		
			col.contained_level_type                as group_level_type,
		
		     case
                when col.level_group_level_id is not null 
				then 'Y' else 'N' end                   as is_group_level,
			
			le.updated_at                             	as updated_at
	
		from dashboard_production.scripts 						as sc 
		-- starting off from scripts allows removes scripts without levels 
		-- (e.g. ai-ethics)
				
		left join dashboard_production.script_levels          as sl 
				on sc.id = sl.script_id
		left join dashboard_production.levels_script_levels   as lsl 
				on sl.id = lsl.script_level_id
		left join dashboard_production.stages                 as st 
				on st.id = sl.stage_id
		left join dashboard_production.levels                 as le 
				on le.id = lsl.level_id
		left join dashboard_production.course_scripts         as cs 
				on cs.script_id = sc.id
		left join dashboard_production.unit_groups            as c 
				on c.id = cs.course_id
		-- new updates 
		left join dashboard_production.contained_levels as col 
            on lsl.level_id = col.level_group_level_id 
		
        left join dashboard_production.parent_levels_child_levels   as plcl 
			on plcl.parent_level_id = col.level_group_level_id	
		
		-- manually updated spreadsheets
		left join analysis.course_names							as cn 
				on cn.versioned_course_id = c.id 
		left join analysis.script_names							as sn 
			on sn.versioned_script_id = sc.id 
            

		);

	drop table if exists analysis.course_structure CASCADE;

	alter table analysis.course_structure_build 
		rename to course_structure;
	
	grant all privileges
	on analysis.course_structure 
	to group admin;

	grant select
	on analysis.course_structure 
	to    group reader,
	      group reader_pii;

end;
$$
