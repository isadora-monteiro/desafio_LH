with
    orders as (
        select 
            soh.*
            , c.id_pessoa
            , c.id_loja 
            , c.id_territorio_cliente  
            , sp.*
            , sohsr.id_motivo_venda
            , sr.id_nome_motivo_venda
            , sr.id_tipo_motivo_venda   
        from {{ ref('stg_sales_salesorderheader')}} soh
        left join {{ ref('stg_sales_customer')}} c
            on soh.id_cliente = c.id_cliente
        left join {{ ref('stg_sales_salesperson')}} sp
            on soh.id_vendedor = sp.id_entidade_empresarial
        left join {{ ref('stg_sales_salesorderheadersalesreason')}} sohsr
            on soh.id_pedido = sohsr.id_pedido
        left join {{ ref('stg_sales_salesreason')}} sr
            on sohsr.id_motivo_venda = sr.id_motivo_venda

    )

select *
from orders