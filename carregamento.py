import psycopg2
import pandas as pd
import os
from sqlalchemy import create_engine

# Definindo as configurações de conexão com o Postgres
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "db_scr"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

# Caminho para os arquivos processados
pasta_processados = 'processados/'

# Função para carregar os dados no Postgres
def carregar_dados_postgres():
    # Conectando ao banco de dados Postgres
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
    
    # Carregar os arquivos processados na pasta
    arquivos_processados = [f for f in os.listdir(pasta_processados) if f.endswith('.csv')]
    
    for arquivo in arquivos_processados:
        caminho_arquivo = os.path.join(pasta_processados, arquivo)
        
        # Extraindo o ano e mês do nome do arquivo (formato 'processado_aaaaMM.csv')
        try:
            ano_mes = arquivo.replace('processado_', '').replace('.csv', '')
            ano = ano_mes[:4]  # Primeiro 4 caracteres para o ano
            mes = ano_mes[4:6]  # Os próximos 2 caracteres para o mês
            nome_tabela = f'Dados_{ano}{mes}'  # Nome da tabela no formato 'Dados_aaaaMM'
            
            print(f'Carregando o arquivo {arquivo} na tabela {nome_tabela}...')
            
            # Lendo o arquivo CSV
            dados = pd.read_csv(caminho_arquivo)
            
            # Carregando os dados na tabela do Postgres
            dados.to_sql(nome_tabela, engine, index=False, if_exists='replace')
            print(f'Arquivo {arquivo} carregado com sucesso na tabela {nome_tabela}!')
        except Exception as e:
            print(f'Erro ao carregar o arquivo {arquivo} para o Postgres: {e}')

# Executar a carga dos dados
def main():
    carregar_dados_postgres()

if __name__ == "__main__":
    main()
