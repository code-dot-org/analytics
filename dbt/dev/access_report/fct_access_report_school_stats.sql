with 

review_table as (
    select * 
    from {{ ref('stg_external_datasets__access_report_review_table') }}
    where grade_levels like '%hi%'
    and state not in ('AS', 'PR', 'VI', 'GU', 'MP')
    and school_type in ('public', 'charter')
),

schools as (
    select * 
    from {{ ref('dim_schools') }}
),

us_stats as (
    select 
        rt.access_report_year,
        rt.state,
        -- rt.school_name,
        -- rt.nces_school_id,
        -- rt.teaches_cs_final,
        -- s.total_students,
        -- s.count_student_am,
        -- s.count_student_as,
        -- s.count_student_hi,
        -- s.count_student_bl,
        -- s.count_student_wh,
        -- s.count_student_hp,
        -- s.count_student_tr,
        -- s.community_type,
        -- s.school_size_cat,
        -- s.frl_eligible_percent,
        -- s.frl_quartile,
        
        -- overall percentage
        count(
            case 
                when teaches_cs_final != 'E' then 1 
                else null 
            end)                                                                    as total_num_schools,
        count(
            case 
                when teaches_cs_final in ('Y', 'HY') then 1 
                else null 
            end)                                                                    as num_schools_teaching,
        
        num_schools_teaching::float / nullif(total_num_schools, 0)                  as pct_schools_teaching,
        
        -- small schools
        sum(
            case
                when 
                    s.school_size_cat = 'small' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_small_schools_teaching,

        sum(
            case
                when 
                    s.school_size_cat = 'small' 
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_small_schools_not_teaching,
        
        num_small_schools_teaching::float / nullif(
            num_small_schools_teaching + num_small_schools_not_teaching
            , 0
        )                                                                           as pct_small_schools_teaching,
        
        -- medium schools
        sum(
            case
                when 
                    s.school_size_cat = 'medium' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_medium_schools_teaching,

        sum(
            case
                when 
                    s.school_size_cat = 'medium' 
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_medium_schools_not_teaching,
        
        num_medium_schools_teaching::float / nullif(
            num_medium_schools_teaching + num_medium_schools_not_teaching
            , 0
        )                                                                           as pct_medium_schools_teaching,
        
        -- large schools
        sum(
            case
                when 
                    s.school_size_cat = 'large' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_large_schools_teaching,

        sum(
            case
                when 
                    s.school_size_cat = 'large' 
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_large_schools_not_teaching,
        
        num_large_schools_teaching::float / nullif(
            num_large_schools_teaching + num_large_schools_not_teaching
            , 0
        )                                                                           as pct_large_schools_teaching,

        -- urban schools

        sum(
            case
                when 
                    s.community_type = 'urban' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_urban_schools_teaching,

        sum(
            case
                when 
                    s.community_type = 'urban'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_urban_schools_not_teaching,
        
        num_urban_schools_teaching::float / nullif(
            num_urban_schools_teaching + num_urban_schools_not_teaching
            , 0
        )                                                                           as pct_urban_schools_teaching,
        
        -- suburban schools
        sum(
            case
                when 
                    s.community_type = 'suburban' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_suburban_schools_teaching,

        sum(
            case
                when 
                    s.community_type = 'suburban'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_suburban_schools_not_teaching,
        
        num_suburban_schools_teaching::float / nullif(
            num_suburban_schools_teaching + num_suburban_schools_not_teaching
            , 0
        )                                                                           as pct_urban_schools_teaching,
        
        -- rural schools
        sum(
            case
                when 
                    s.community_type = 'rural' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_rural_schools_teaching,

        sum(
            case
                when 
                    s.community_type = 'rural'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_rural_schools_not_teaching,
        
        num_rural_schools_teaching::float / nullif(
            num_rural_schools_teaching + num_rural_schools_not_teaching
            , 0
        )                                                                           as pct_rural_schools_teaching,
        
        --1st quartile FRL schools
        sum(
            case
                when 
                    frl_quartile = '1st quartile' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_first_quart_schools_teaching,

        sum(
            case
                when 
                    frl_quartile = '1st quartile'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_first_quart_schools_not_teaching,
        
        num_first_quart_schools_teaching::float / nullif(
            num_first_quart_schools_teaching + num_first_quart_schools_not_teaching
            , 0
        )                                                                           as pct_first_quart_schools_teaching,
        
        --2nd quartile FRL schools
        sum(
            case
                when 
                    frl_quartile = '2nd quartile' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_sec_quart_schools_teaching,

        sum(
            case
                when 
                    frl_quartile = '2nd quartile'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_sec_quart_schools_not_teaching,
        
        num_sec_quart_schools_teaching::float / nullif(
            num_sec_quart_schools_teaching + num_sec_quart_schools_not_teaching
            , 0
        )                                                                           as pct_first_quart_schools_teaching,
        
        --3rd quartile FRL schools
        sum(
            case
                when 
                    frl_quartile = '3rd quartile' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_third_quart_schools_teaching,

        sum(
            case
                when 
                    frl_quartile = '3rd quartile'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_third_quart_schools_not_teaching,
        
        num_third_quart_schools_teaching::float / nullif(
            num_third_quart_schools_teaching + num_third_quart_schools_not_teaching
            , 0
        )                                                                           as pct_third_quart_schools_teaching,
        
        --4th quartile FRL schools
        sum(
            case
                when 
                    frl_quartile = '4th quartile' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_fourth_quart_schools_teaching,

        sum(
            case
                when 
                    frl_quartile = '4th quartile'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_fourth_quart_schools_not_teaching,
        
        num_fourth_quart_schools_teaching::float / nullif(
            num_fourth_quart_schools_teaching + num_fourth_quart_schools_not_teaching
            , 0
        )                                                                           as pct_fourth_quart_schools_teaching,
        
        --students with access
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(total_students,0) 
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(total_students,0) 
            end)                                                                    as pct_total_students_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_am,0) 
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_am,0)  
            end)                                                                    as pct_students_native_american_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_as,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_as,0)  
            end)                                                                    as pct_students_asian_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_hi,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_hi,0) 
            end)                                                                    as pct_students_hispanic_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_bl,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_bl,0) 
            end)                                                                    as pct_students_black_access,

        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_wh,0) 
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_wh,0) 
            end)                                                                    as pct_students_white_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_hp,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_hp,0)  
            end)                                                                    as pct_students_native_hawaiian_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_tr,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_tr,0)  
            end)                                                                    as pct_students_two_races_access
    from review_table                                                               as rt
    left join schools                                                               as s 
        on rt.nces_school_id = s.school_id 
    {{ dbt_utils.group_by(2) }}
),

national_stats as (
    select 
        rt.access_report_year,
        'national'                                                                  as state,
        -- rt.school_name,
        -- rt.nces_school_id,
        -- rt.teaches_cs_final,
        -- s.total_students,
        -- s.count_student_am,
        -- s.count_student_as,
        -- s.count_student_hi,
        -- s.count_student_bl,
        -- s.count_student_wh,
        -- s.count_student_hp,
        -- s.count_student_tr,
        -- s.community_type,
        -- s.school_size_cat,
        -- s.frl_eligible_percent,
        -- s.frl_quartile,
        
        -- overall percentage
        count(
            case 
                when teaches_cs_final != 'E' then 1 
                else null 
            end)                                                                    as total_num_schools,
        count(
            case 
                when teaches_cs_final in ('Y', 'HY') then 1 
                else null 
            end)                                                                    as num_schools_teaching,
        
        num_schools_teaching::float / nullif(total_num_schools, 0)                  as pct_schools_teaching,
        
        -- small schools
        sum(
            case
                when 
                    s.school_size_cat = 'small' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_small_schools_teaching,

        sum(
            case
                when 
                    s.school_size_cat = 'small' 
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_small_schools_not_teaching,
        
        num_small_schools_teaching::float / nullif(
            num_small_schools_teaching + num_small_schools_not_teaching
            , 0
        )                                                                           as pct_small_schools_teaching,
        
        -- medium schools
        sum(
            case
                when 
                    s.school_size_cat = 'medium' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_medium_schools_teaching,

        sum(
            case
                when 
                    s.school_size_cat = 'medium' 
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_medium_schools_not_teaching,
        
        num_medium_schools_teaching::float / nullif(
            num_medium_schools_teaching + num_medium_schools_not_teaching
            , 0
        )                                                                           as pct_medium_schools_teaching,
        
        -- large schools
        sum(
            case
                when 
                    s.school_size_cat = 'large' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_large_schools_teaching,

        sum(
            case
                when 
                    s.school_size_cat = 'large' 
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_large_schools_not_teaching,
        
        num_large_schools_teaching::float / nullif(
            num_large_schools_teaching + num_large_schools_not_teaching
            , 0
        )                                                                           as pct_large_schools_teaching,

        -- urban schools

        sum(
            case
                when 
                    s.community_type = 'urban' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_urban_schools_teaching,

        sum(
            case
                when 
                    s.community_type = 'urban'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_urban_schools_not_teaching,
        
        num_urban_schools_teaching::float / nullif(
            num_urban_schools_teaching + num_urban_schools_not_teaching
            , 0
        )                                                                           as pct_urban_schools_teaching,
        
        -- suburban schools
        sum(
            case
                when 
                    s.community_type = 'suburban' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_suburban_schools_teaching,

        sum(
            case
                when 
                    s.community_type = 'suburban'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_suburban_schools_not_teaching,
        
        num_suburban_schools_teaching::float / nullif(
            num_suburban_schools_teaching + num_suburban_schools_not_teaching
            , 0
        )                                                                           as pct_urban_schools_teaching,
        
        -- rural schools
        sum(
            case
                when 
                    s.community_type = 'rural' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_rural_schools_teaching,

        sum(
            case
                when 
                    s.community_type = 'rural'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_rural_schools_not_teaching,
        
        num_rural_schools_teaching::float / nullif(
            num_rural_schools_teaching + num_rural_schools_not_teaching
            , 0
        )                                                                           as pct_rural_schools_teaching,
        
        --1st quartile FRL schools
        sum(
            case
                when 
                    frl_quartile = '1st quartile' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_first_quart_schools_teaching,

        sum(
            case
                when 
                    frl_quartile = '1st quartile'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_first_quart_schools_not_teaching,
        
        num_first_quart_schools_teaching::float / nullif(
            num_first_quart_schools_teaching + num_first_quart_schools_not_teaching
            , 0
        )                                                                           as pct_first_quart_schools_teaching,
        
        --2nd quartile FRL schools
        sum(
            case
                when 
                    frl_quartile = '2nd quartile' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_sec_quart_schools_teaching,

        sum(
            case
                when 
                    frl_quartile = '2nd quartile'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_sec_quart_schools_not_teaching,
        
        num_sec_quart_schools_teaching::float / nullif(
            num_sec_quart_schools_teaching + num_sec_quart_schools_not_teaching
            , 0
        )                                                                           as pct_sec_quart_schools_teaching,
        
        --3rd quartile FRL schools
        sum(
            case
                when 
                    frl_quartile = '3rd quartile' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_third_quart_schools_teaching,

        sum(
            case
                when 
                    frl_quartile = '3rd quartile'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_third_quart_schools_not_teaching,
        
        num_third_quart_schools_teaching::float / nullif(
            num_third_quart_schools_teaching + num_third_quart_schools_not_teaching
            , 0
        )                                                                           as pct_third_quart_schools_teaching,
        
        --4th quartile FRL schools
        sum(
            case
                when 
                    frl_quartile = '4th quartile' 
                    and teaches_cs_final in ('Y','HY') 
                then 1 
                else 0 
            end)                                                                    as num_fourth_quart_schools_teaching,

        sum(
            case
                when 
                    frl_quartile = '4th quartile'
                    and rt.teaches_cs_final in ('N','HN') 
                then 1 
                else 0 
            end)                                                                    as num_fourth_quart_schools_not_teaching,
        
        num_fourth_quart_schools_teaching::float / nullif(
            num_fourth_quart_schools_teaching + num_fourth_quart_schools_not_teaching
            , 0
        )                                                                           as pct_fourth_quart_schools_teaching,
        
        --students with access
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(total_students,0) 
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(total_students,0) 
            end)                                                                    as pct_total_students_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_am,0) 
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_am,0)  
            end)                                                                    as pct_students_native_american_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_as,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_as,0)  
            end)                                                                    as pct_students_asian_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_hi,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_hi,0) 
            end)                                                                    as pct_students_hispanic_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_bl,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_bl,0) 
            end)                                                                    as pct_students_black_access,

        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_wh,0) 
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_wh,0) 
            end)                                                                    as pct_students_white_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_hp,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_hp,0)  
            end)                                                                    as pct_students_native_hawaiian_access,
        
        (sum(
            case 
                when teaches_cs_final in ('Y','HY') 
                then nullif(count_student_tr,0)  
            end) * 1.0) 
        / sum( 
            case 
                when teaches_cs_final != 'E'
                then nullif(count_student_tr,0)  
            end)                                                                    as pct_students_two_races_access
    from review_table                                                               as rt
    left join schools                                                               as s 
        on rt.nces_school_id = s.school_id 
    {{ dbt_utils.group_by(2) }}
)

select * from us_stats 
union
select * from national_stats
order by state asc