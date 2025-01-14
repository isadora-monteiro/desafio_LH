with
    customer as (
        select 
              id_cliente
            , id_pessoa
            , id_loja
            , id_territorio_vendas            
        from {{ ref('stg_sales_customer')}}
    ),
    customer_with_sk as (
        select
            {{ numeric_surrogate_key(['id_cliente']) }} as sk_cliente
            , *
        from customer
    )

select *
from customer_with_sk



