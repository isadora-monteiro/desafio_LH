with
    salesperson as (
        select 
              id_vendedor
            , id_territorio_vendedor 
            , perc_comissao     
        from {{ ref('stg_sales_salesperson')}}
    ),
    salesperson_with_sk as (
        select
            {{ numeric_surrogate_key(['id_vendedor']) }} as sk_vendedor
            , *
        from salesperson
    )

select 
    sk_vendedor
    , id_territorio_vendedor
    , perc_comissao
from salesperson_with_sk