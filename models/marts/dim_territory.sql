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
    adress as (
        select
              id_endereco
            , endereco
            , cidade
            , cod_postal
            , id_provincia  
        from {{ ref('stg_person_address') }} 
    ),
    territory as (
        select 
              a.id_endereco
            , sp.id_provincia
            , st.id_territorio_vendas
            , cr.cod_regiao
            , st.grupo
        from adress a
        left join stateprovince sp on a.id_provincia = sp.id_provincia  
        left join salesterritory st on sp.id_territorio_vendas = st.id_territorio_vendas
        left join countryregion cr on sp.cod_regiao  = cr.cod_regiao
        
    ),
    territory_with_sk as (
        select
            {{ numeric_surrogate_key(['id_endereco']) }} as sk_endereco
            , *
        from territory
    )

select *
from territory_with_sk