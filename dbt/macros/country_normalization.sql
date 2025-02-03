{% macro country_normalization(raw_country_name) %}
    case
        when lower({{raw_country_name }}) in ('åland','aland islands') then 'åland islands'
        when lower({{raw_country_name }}) in ('antigua & barbuda') then 'antigua and barbuda'
        when lower({{raw_country_name }}) in ('the bahamas') then 'bahamas'
        when lower({{raw_country_name }}) in ('caribbean netherlands') then 'bonaire, sint eustatius, and saba'
        when lower({{raw_country_name }}) in ('bosnia herzegovina','bosnia & herzegovina') then 'bosnia and herzegovina'
        when lower({{raw_country_name }}) in ('brunei') then 'brunei darussalam'
        when lower({{raw_country_name }}) in ('cape verde') then 'cabo verde'
        when lower({{raw_country_name }}) in ('cocos [keeling] islands') then 'cocos (keeling) islands'
        when lower({{raw_country_name }}) in ('dr congo','congo','congo, the democratic republic of the','democratic republic of congo','democratic republic of the congo') then 'congo, democratic republic of'
        when lower({{raw_country_name }}) in ('republic of the congo','congo republic') then 'congo, republic of'
        when lower({{raw_country_name }}) in ('ivory coast','cote d''ivoire') then 'côte d''ivoire'
        when lower({{raw_country_name }}) in ('czech republic') then 'czechia'
        when lower({{raw_country_name }}) in ('swaziland') then 'eswatini'
        when lower({{raw_country_name }}) in ('falkland islands (islas malvinas)') then 'falkland islands'
        when lower({{raw_country_name }}) in ('the gambia') then 'gambia'
        when lower({{raw_country_name }}) in ('iran, islamic republic of') then 'iran'
        when lower({{raw_country_name }}) in ('hashemite kingdom of jordan') then 'jordan'
        when lower({{raw_country_name }}) in ('republic of kosovo') then 'kosovo'
        when lower({{raw_country_name }}) in ('lao people''s democratic republic') then 'laos'
        when lower({{raw_country_name }}) in ('republic of lithuania') then 'lithuania'
        when lower({{raw_country_name }}) in ('macau') then 'macao'
        when lower({{raw_country_name }}) in ('federated states of micronesia','micronesia') then 'micronesia, federated states of'
        when lower({{raw_country_name }}) in ('republic of moldova','moldova') then 'moldova, republic of'
        when lower({{raw_country_name }}) in ('principality of monaco') then 'monaco'
        when lower({{raw_country_name }}) in ('myanmar [burma]','republic of the union of myanmar','myanmar (burma)') then 'myanmar'
        when lower({{raw_country_name }}) in ('the netherlands') then 'netherlands'
        when lower({{raw_country_name }}) in ('korea, democratic people''s republic of') then 'north korea'
        when lower({{raw_country_name }}) in ('macedonia','macedonia (fyrom)') then 'north macedonia'
        when lower({{raw_country_name }}) in ('kingdom of norway') then 'norway'
        when lower({{raw_country_name }}) in ('palestinian territory','palestinian territories') then 'palestine'
        when lower({{raw_country_name }}) in ('pitcairn islands') then 'pitcairn'
        when lower({{raw_country_name }}) in ('reunion') then 'réunion'
        when lower({{raw_country_name }}) in ('russian federation') then 'russia'
        when lower({{raw_country_name }}) in ('saint-barthélemy') then 'saint barthélemy'
        when lower({{raw_country_name }}) in ('saint helena','saint helena ascension and tristan da cunha') then 'saint helena, ascension, and tristan da cunha'
        when lower({{raw_country_name }}) in ('st kitts and nevis') then 'saint kitts and nevis'
        when lower({{raw_country_name }}) in ('collectivity of saint martin') then 'saint martin'
        when lower({{raw_country_name }}) in ('st vincent and grenadines') then 'saint vincent and the grenadines'
        when lower({{raw_country_name }}) in ('sao tome and principe') then 'são tomé and príncipe'
        when lower({{raw_country_name }}) in ('sint maarten (dutch part)') then 'sint maarten'
        when lower({{raw_country_name }}) in ('slovak republic') then 'slovakia'
        when lower({{raw_country_name }}) in ('republic of korea', 'korea, republic of','korea, south (rok)') then 'south korea'
        when lower({{raw_country_name }}) in ('syrian arab republic') then 'syria'
        when lower({{raw_country_name }}) in ('tanzania, united republic of') then 'tanzania'
        when lower({{raw_country_name }}) in ('democratic republic of timor-leste','east timor','timor leste') then 'timor-leste'
        when lower({{raw_country_name }}) in ('turkey','turkiye') then 'türkiye'
        when lower({{raw_country_name }}) in ('northern ireland') then 'united kingdom'
        when lower({{raw_country_name }}) in ('us','u.s.') then 'united states'
        when lower({{raw_country_name }}) in ('u.s. minor outlying islands') then 'united states minor outlying islands'
        when lower({{raw_country_name }}) in ('us virgin islands') then 'u.s. virgin islands'
        when lower({{raw_country_name }}) in ('viet nam') then 'vietnam'
        when {{raw_country_name}} = '' then NULL
        else lower({{raw_country_name }})
    end
{% endmacro %}