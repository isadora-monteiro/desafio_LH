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
            , id_subcategoria
            , id_categoria
            , nome_produto
            , nome_subcategoria 
            , nome_categoria 
            , classificacao
            , custo_padrao   
        from {{ ref('dim_product')}}
    ),
    inventory_joined as (
        select
            p.sk_produto
            , p.id_produto
            , p.id_subcategoria
            , p.id_categoria
            , p.nome_produto
            , p.nome_subcategoria   
            , p.nome_categoria  
            , i.id_localizacao
            , i.volume_produto
            , p.custo_padrao
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
    , id_produto
    , id_subcategoria
    , id_categoria
    , nome_produto
    , nome_subcategoria   
    , nome_categoria  
    , id_localizacao
    , volume_produto
    , round(custo_padrao, 2) as custo_padrao
    , round(volume_produto * custo_padrao, 2) as custo_total
from inventory_with_sk



