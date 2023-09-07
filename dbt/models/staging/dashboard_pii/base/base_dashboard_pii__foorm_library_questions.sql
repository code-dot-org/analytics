with 
source as (
      select * from {{ source('dashboard_pii', 'foorm_library_questions') }}
),

renamed as (
    select
        id          as foorm_library_question_id,
        library_name,
        library_version,
        question_name,
        question,
        created_at,
        updated_at,
        published   as is_published
    from source
)

select * from renamed