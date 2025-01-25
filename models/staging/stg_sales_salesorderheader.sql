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
            , case
                when creditcardid is null then 'Outros'
                else 'Crédito'
            end as metodo_pagamento
        from {{ source('sales', 'salesorderheader') }}
        WHERE data_pedido >= DATE '2011-01-01'
    )

select *
from source_salesorderheader 


            
