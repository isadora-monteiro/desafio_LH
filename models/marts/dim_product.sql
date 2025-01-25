with
    product as (
        select 
            id_produto
            , id_subcategoria
            , nome_produto
            , case 
                when flag_vendavel = 1 then 'Vendável'
                when flag_vendavel = 0 then 'Não Vendável'
            end as classificacao
            , custo_unit   
            , preco_unit
        from {{ ref('stg_production_product')}}
    ),
    subcategory as (
        select 
            id_subcategoria
            , nome_subcategoria  
            , id_categoria     
        from {{ ref('stg_production_productsubcategory')}}
    ),
    category as (
        select 
            id_categoria
            , nome_categoria     
        from {{ ref('stg_production_productcategory')}}
    ),    
    product_joined as (
        select 
            p.id_produto
            , p.nome_produto
            , s.nome_subcategoria
            , c.nome_categoria
            , p.classificacao
            , p.custo_unit
            , p.preco_unit
        from product p 
        left join subcategory s on p.id_subcategoria = s.id_subcategoria
        left join category c on s.id_categoria = c.id_categoria
    ),
    product_with_sk as (
        select
            {{ numeric_surrogate_key(['id_produto']) }} as sk_produto
            , *
        from product_joined
    )

select 
    sk_produto
    , id_produto
    , nome_produto
    , nome_subcategoria 
    , nome_categoria
    , classificacao
    , custo_unit
    , preco_unit
from product_with_sk
where classificacao = 'Vendável'