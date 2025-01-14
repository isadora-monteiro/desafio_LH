with
    source_customer as (
        select
              cast(customerid as int) as id_cliente
            , personid as id_pessoa
            , storeid as id_loja
            , territoryid as id_territorio_vendas       
        from {{ source('raw_sap_aw', 'customer') }}
    )

select *
from source_customer