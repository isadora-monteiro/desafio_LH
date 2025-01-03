with
    order_details as (
        select 
            sod.*
            , p.nome_produto
            , p.custo_padrao_produto         
        from {{ ref('stg_sales_salesorderdetail')}} sod
        left join {{ ref('stg_production_product')}} p
            on sod.id_produto = p.id_produto
    )

select *
from order_details
