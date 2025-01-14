with
    product as (
        select 
              id_produto
            , nome_produto
            , custo_padrao_produto      
        from {{ ref('stg_production_product')}}
    ),
    product_with_sk as (
        select
            {{ numeric_surrogate_key(['id_produto']) }} as sk_produto
            , *
        from product
    )

select *
from product_with_sk