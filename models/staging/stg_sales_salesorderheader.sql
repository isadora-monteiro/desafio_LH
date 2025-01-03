with
    source_salesorderheader as (
        select
            cast(salesorderid as int) as id_pedido
            , orderdate as data_pedido
            , onlineorderflag as flag_pedido_online
            , customerid as id_cliente
            , salespersonid as id_vendedor
            , territoryid as id_territorio
            , creditcardid as id_cartao_credito
            , subtotal as subtotal
            , taxamt as taxa
            , freight as frete
            , totaldue as total 
        from {{ source('raw_sap_aw', 'salesorderheader') }}
    )

select *
from source_salesorderheader 


            
