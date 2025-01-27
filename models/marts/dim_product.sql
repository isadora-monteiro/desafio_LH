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
            product.id_produto
            , product.nome_produto
            , subcategory.nome_subcategoria
            , category.nome_categoria
            , product.classificacao
            , product.custo_unit
            , product.preco_unit
        from product 
        left join subcategory on product.id_subcategoria = subcategory.id_subcategoria
        left join category on subcategory.id_categoria = category.id_categoria
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