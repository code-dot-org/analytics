{% macro ap_norm_exam_subject(exam_name) %}
    case
        when {{ exam_name }} in ('csa','Computer Sci A','COMSCA') then 'csa'
        when {{ exam_name }} in ('csp','Computer Sci Prin','COMSCP') then 'csp'
        when {{ exam_name }} in ('sum_csa_csp','COMSCP or COMSCA') then 'sum_csa_csp'
        else 'UNEXPECTED exam_name: ''' || {{exam_name }} || '''. SEE macro - ap_norm_exam_subject'
    end
{% endmacro %}


/*
    Assumption: the raw column names after cleaning will contain a demographic_group+score_group e.g. 'black_1' (black score of 1).
    Sometimes it's a long-form value like e.g. 'black_african_american_1' etc.

    This macro creates two new fields wherever it is inserted.
    It is intended to be used AP exam score staging models
*/
{% macro ap_split_column(orig_col_name)%}
        left(
            {{ orig_col_name }}, 
            length({{ orig_col_name }}) - strpos(reverse({{ orig_col_name }}), '_')
        ) as demographic_group_raw,
        right(
            orig_col_name, strpos(reverse(orig_col_name), '_') - 1
        ) as score_category_raw
{% endmacro%}




/*
    process the raw demographic group and produce two new columns:
    1. demographic_group (black, white, hispanic...male, female...overall...freshman etc.)
    2. demographic_category (race | gender | total | grade_level)
*/
{% macro ap_norm_demographic_group(demographic_group_raw) %}
case
    when {{ demographic_group_raw }} in ('bl', 'black', 'black_african_american') then 'black'
    when {{ demographic_group_raw }} in ('hi', 'hispanic','hispanic_latino') then 'hispanic'
    when {{ demographic_group_raw }} in ('as', 'asian', 'asian_asian_american') then 'asian'
    when {{ demographic_group_raw }} in ('am', 'american_indian_alaska_native','american_indian','amind') then 'american_indian'
    when {{ demographic_group_raw }} in ('wh', 'white') then 'white'
    when {{ demographic_group_raw }} in ('hp','native_hawaiian_other_pacific_islander','pacific_islander','nhpi') then 'hawaiian'
    when {{ demographic_group_raw }} in ('tr','two_or_more_races','twomore') then 'two_or_more'
    when {{ demographic_group_raw }} in ('other', 'other_race_ethnicity','other_race') then 'other_race'
    when {{ demographic_group_raw }} in ('race_ethnicity_no_response','race_no_response','no_response','noresponse') then 'race_no_response'
    when {{ demographic_group_raw }} in ('other_gender','gender_another') then 'other_gender'
    when {{ demographic_group_raw }} in ('overall','total') then 'total'
    else {{ demographic_group_raw }} -- default: return the raw - if unrecognized this will fail loudly when processed by by next case-when
end as demographic_group
-- Finally, categorize into demographic_category
, case
    when demographic_group in ('male', 'female', 'other_gender') then 'gender'
    when demographic_group in ('american_indian', 'asian', 'black', 'hispanic', 'hawaiian', 'two_or_more', 'white','other_race','race_no_response') then 'race'
    when demographic_group in ('total') then 'total'
    when demographic_group in ('freshman', 'sophomore', 'senior', 'junior') then 'grade_level'
    else 'UNEXPECTED group: '''|| demographic_group || '''. SEE macro - ap_norm_demographic_group(...)'
end as demographic_category
{% endmacro %}



/*
    Categorize the score category (detail or total) and then score_of (1-5)
    score_of is NULL in the case that category is 'total' -- this is a defensive measure against
    naive summing with certain aggregations.  the sum-of-num-students with scores 1-5 should equal the total.
    Thus we categorize total differently, and make score_of null so the students are not double-counted.
*/
{% macro ap_norm_score_category(score_category_input)%}

            case
                when {{ score_category_input }} in ('1', '2', '3', '4', '5')
                    then 'detail'
                when {{ score_category_input }} in ('total','all')
                    then 'total'
                else 'UNEXPECTED input: ''' || {{ score_category_input }} || '''. SEE macro - ap_norm_score_category'
            end score_category,
            case
                when score_category = 'detail' then {{ score_category_input }}
                when score_category = 'total' then NULL
                else 'UNEXPECTED category: ''' || score_category || '''. SEE macro - ap_norm_score_category'
            end score_of
{% endmacro %}

/*
    This macro attempts to normalize the names of aggregated ap score reports, which can vary year to year.

    AN IMPORTANT FUNCTION of this macro is to flag UNKNOWN or NEW report names with the "ERROR--" catch-all
    in the else clause.  This can be used to quickly identify new values when new data is onboarded that 
    should be added to / acknowledged in this macro.
*/
{% macro ap_norm_exam_group(exam_group) %}

    case 
        --when {{ exam_group }} in ('cdo_audit')                         then {{exam_group}}
        when {{ exam_group }} in ('national')                          then {{exam_group}}
        when {{ exam_group }} in ('rp all time')                          then 'csp_rp_alltime'
        when {{ exam_group }} in ('rp per year')                          then 'csp_rp_peryear'
        when {{ exam_group }} in ('csa pd all time','csa_all_time_pd') then 'csa_pd_alltime'
        when {{ exam_group }} in ('csp pd all time','csp_all_time_pd') then 'csp_pd_alltime'
        when {{ exam_group }} in ('csa_users','csa_heavy')             then 'csa_users' -- heavy users
        when {{ exam_group }} in ('csp_users','csp_heavy')             then 'csp_users' -- heavy users
        when {{ exam_group }} in ('csp_users_and_audit','csp_ballmer') then 'csp_users_and_audit' -- heavy+audit = "Ballmer"
        when {{ exam_group }} in ('csa_ballmer')                       then 'csa_users_and_audit' -- heavy+audit = "Ballmer"

        -- AFE REPORTS
        when {{ exam_group }} in 
            ('2019_and_2020_AFE','2019_AFE','2020_AFE')                then {{exam_group}} -- AFE teacher signup cohorts
        when {{ exam_group }} in ('csa_users_afe','csa_afe_eligible')  then 'csa_afe_eligible_schools' -- AFE eligible schools (started 2023)
        when {{ exam_group }} in ('csp_users_afe','csp_afe_eligible')  then 'csp_afe_eligible_schools' -- AFE eligible schools (started 2023)


        -- other PD and regional partner stuff --
        when {{ exam_group }} in (
            'csp pd per year',  -- I'm unsure what this is
            'pd_2016', -- these are CSP PD cohorts (I think)
            'pd_2017',
            'pd_2018',
            'pd_2019',
            'pd_2020',
            'rp per year',  -- regional partner reports (unsure how they work)
            'rp all time')                                              then {{ exam_group }}
        else 'UNEXPECTED exam_group: '''|| {{exam_group}} || '''. SEE macro - ap_norm_exam_group'
    end

{% endmacro%}


/*
    For the record that is the aggregate of all schools with fewer than 10 students, extract the number of schools, which is given in the school_name.
    For example: 
        - "CODE.ORG LESS THAN 10 AGGREGATE - n of schools = 795" should return the integer 795
        - "2023 CSA LESS THAN 10 AGGREGATE N=170" should return 170 

    ASSUMPTIONS:
    1. The school_name fits the pattern "%LESS%THAN%10%"
    2. There is an '=' sign in the school_name
    3. There is an integer somewhere after the '=' sign
*/
{% macro ap_extract_n_schools_from_aggregate(school_name) %}
    case
        when upper({{school_name}}) like '%LESS%THAN%10%' 
        then regexp_replace( upper({{school_name}} ),'.*[=][^0-9]*', '')::integer  -- remove any/all characters found before the '=' sign, and any non-numeric characters after it
        else 1::integer
    end
{% endmacro %}

