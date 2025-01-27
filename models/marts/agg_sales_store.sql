with
    
    product as (
        select 
            sk_produto
            , id_produto
            , nome_produto
            , nome_subcategoria 
            , nome_categoria
            , classificacao
            , custo_unit
            , preco_unit
        from {{ ref('dim_product')}}
    ),
    customer as (
        select 
            sk_cliente
            , tipo_cliente       
            , nm_cliente
            , sk_vendedor 
            , sk_territorio_vendas    
        from {{ ref('dim_customer')}}
    ),
    order_details as (
        select 
            sk_pedido
            , sk_produto
            , quantidade_produto 
        from {{ ref('fact_order_details')}}
    ),
    orders as (
        select
            sk_pedido
            , sk_cliente
            , data_pedido
            , sk_endereco
        from {{ ref('fact_orders')}}    
    ),
    orders_joined as (
        select
            orders.sk_pedido
            , orders.data_pedido
            , extract(year from orders.data_pedido) as ano
            , extract(month from orders.data_pedido) as mes
            , orders.sk_cliente
            , customer.nm_cliente
            , order_details.sk_produto
            , product.nome_produto
            , order_details.quantidade_produto
            , orders.sk_endereco
        from orders  
        inner join order_details on orders.sk_pedido = order_details.sk_pedido
        inner join product on order_details.sk_produto = product.sk_produto
        inner join customer on orders.sk_cliente = customer.sk_cliente
        where lower(customer.tipo_cliente) = 'b2b'
        order BY
            orders.sk_pedido
            , orders.sk_cliente
            , customer.nm_cliente
            , order_details.sk_produto
            , product.nome_produto
            , orders.data_pedido
            , ano
            , mes
    ), 
    territory as (
        select
            sk_endereco
            , cidade
            , cod_postal
            , nome_provincia
            , nome_territorio 
            , nome_regiao 
            , grupo
        from {{ ref('dim_territory')}}    
    ),
    orders_joined_territory as (
        select
            orders_joined.nome_produto
            , orders_joined.nm_cliente
            , case
                when territory.nome_regiao = 'United States' then territory.nome_provincia
                else territory.nome_regiao
            end as regiao
            , orders_joined.ano
            , orders_joined.mes
            , orders_joined.data_pedido
            , orders_joined.quantidade_produto
        from orders_joined 
        left join territory on orders_joined.sk_endereco = territory.sk_endereco
    )


select 
    regiao
    , nm_cliente
    , nome_produto
    , ano
    , mes
    , date_trunc('month', data_pedido) as data_ref
    , sum(quantidade_produto) as quantidade 
from orders_joined_territory
group BY
    regiao
    , nm_cliente
    , nome_produto
    , ano
    , mes
    , data_ref
order BY
    regiao
    , nm_cliente
    , nome_produto
    , ano
    , mes
    , data_ref
