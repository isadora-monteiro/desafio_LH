with
    salesterritory as (
        select 
            id_territorio_vendas  
            , nome_territorio 
            , cod_regiao
            , grupo
        from {{ ref('stg_sales_salesterritory') }} 
    ),
    countryregion as (
        select
            cod_regiao
            , nome_regiao 
        from {{ ref('stg_person_countryregion') }} 
    ),
    stateprovince as (
        select
            id_provincia
            , cod_provincia
            , cod_regiao
            , flag_unico_estado
            , nome_provincia
            , id_territorio_vendas  
        from {{ ref('stg_person_stateprovince') }} 
    ),
    address as (
        select
            id_endereco
            , cidade
            , cod_postal
            , id_provincia  
        from {{ ref('stg_person_address') }} 
    ),
    territory as (
        select 
            address.id_endereco
            , address.cidade
            , address.cod_postal
            , address.id_provincia
            , stateprovince.nome_provincia
            , salesterritory.nome_territorio 
            , countryregion.nome_regiao 
            , salesterritory.grupo
        from address
        left join stateprovince on address.id_provincia = stateprovince.id_provincia  
        left join salesterritory on stateprovince.id_territorio_vendas = salesterritory.id_territorio_vendas
        left join countryregion on stateprovince.cod_regiao  = countryregion.cod_regiao
        
    ),
    territory_with_sk as (
        select
            {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , *
        from territory
    )

select 
    sk_endereco
    , cidade
    , cod_postal
    , nome_provincia
    , nome_territorio 
    , nome_regiao 
    , grupo
from territory_with_sk