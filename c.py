import pandas as pd
from prophet import Prophet

# Carregando dados
df = pd.read_csv('dados_vendas.csv')
df['data'] = pd.to_datetime(df['data'])
df = df.groupby(['data', 'produto', 'loja'])['quantidade'].sum().reset_index()

# Filtrando por um produto específico (ex.: sorvete)
produto_escolhido = 'sorvete'
df_produto = df[df['produto'] == produto_escolhido]

# Preparando para o Prophet
df_prophet = df_produto.groupby('data')['quantidade'].sum().reset_index()
df_prophet.columns = ['ds', 'y']

# Ajustando o modelo
modelo = Prophet(seasonality_mode='multiplicative')
modelo.fit(df_prophet)

# Criando futuro e previsão
future = modelo.make_future_dataframe(periods=90)  # Próximos 3 meses
forecast = modelo.predict(future)

# Visualizando resultados
fig = modelo.plot(forecast)
fig_components = modelo.plot_components(forecast)
