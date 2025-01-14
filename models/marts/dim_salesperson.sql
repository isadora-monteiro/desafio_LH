with
    salesperson as (
        select 
              id_entidade_empresarial
            , id_territorio_vendedor      
        from {{ ref('stg_sales_salesperson')}}
    ),
    salesperson_with_sk as (
        select
            {{ numeric_surrogate_key(['id_entidade_empresarial']) }} as sk_entidade_empresarial
            , *
        from salesperson
    )

select *
from salesperson_with_sk