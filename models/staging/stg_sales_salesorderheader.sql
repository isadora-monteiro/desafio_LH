with
    source_salesorderheader as (
        select
            cast(salesorderid as int) as id_pedido
            , to_date(LEFT(ORDERDATE, 10)) AS data_pedido
            , onlineorderflag as flag_pedido_online
            , customerid as id_cliente
            , salespersonid as id_vendedor
            , territoryid as id_territorio
            , shiptoaddressid as id_endereco
            , subtotal as subtotal
            , taxamt as taxa
            , freight as frete
            , totaldue as total 
        from {{ source('raw_sap_aw', 'salesorderheader') }}
    )

select *
from source_salesorderheader 


            
