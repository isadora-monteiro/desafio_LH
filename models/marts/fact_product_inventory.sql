with
    inventory as (
        select 
            id_produto
            , id_localizacao
            , volume_produto
        from {{ ref('stg_production_productinventory')}}
    ),
    products as (
        select 
            sk_produto
            , id_produto
            , custo_unit   
        from {{ ref('dim_product')}}
    ),
    inventory_joined as (
        select
            p.sk_produto 
            , i.id_localizacao
            , i.volume_produto
            , p.custo_unit
        from inventory i 
        inner join products p on i.id_produto = p.id_produto
    ),
    inventory_with_sk as (
        select
            {{ numeric_surrogate_key(['id_produto', 'id_localizacao']) }} as sk_produto_localizacao
            , *
        from inventory_joined
    )

select 
    sk_produto_localizacao
    , sk_produto
    , id_localizacao
    , volume_produto
    , round(custo_unit, 2) as custo_unit
    , round(volume_produto * custo_unit, 2) as custo_total
from inventory_with_sk



