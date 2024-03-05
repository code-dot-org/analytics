with 
source as (
    select * 
    from {{ source('dashboard', 'lti_integrations') }}
),

renamed as (
    select 
        id          as lti_integration_id,
        name        as lti_integration_name,
        platform_id,
        platform_name,
        issuer,
        client_id,
        {# auth_redirect_url,
        jwks_url,
        access_token_url, #}
        created_at,
        updated_at
    from source)

select *  
from renamed