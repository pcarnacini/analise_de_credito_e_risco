import os
import duckdb
import pandas as pd
import numpy as np

# Caminho para a pasta onde os arquivos CSV estão armazenados
pasta_dados = 'extraidos/'
pasta_saida = 'processados/'

# Criar pasta de saída se não existir
os.makedirs(pasta_saida, exist_ok=True)

# Conectando ao DuckDB (será criado um banco de dados temporário na memória)
conn = duckdb.connect(database=':memory:')

# Criar schema para organização dos dados
conn.execute("CREATE SCHEMA IF NOT EXISTS dados_raw;")
conn.execute("CREATE SCHEMA IF NOT EXISTS dados_norm;")

# Função para carregar e processar os arquivos CSV na pasta extraidos/
def processar_arquivos():
    arquivos_csv = [f for f in os.listdir(pasta_dados) if f.endswith('.csv')]
    
    # DataFrame para armazenar todos os dados consolidados
    dados_completos = pd.DataFrame()

    for arquivo in arquivos_csv:
        caminho_arquivo = os.path.join(pasta_dados, arquivo)
        nome_tabela = arquivo.replace('.csv', '')
        print(f'Processando o arquivo: {arquivo}')
        
        try:
            query = f'''
                CREATE TABLE dados_raw.{nome_tabela} AS
                SELECT * FROM read_csv_auto('{caminho_arquivo}', header=True, delim=';');
            '''
            conn.execute(query)
        except Exception as e:
            print(f'Erro ao criar a tabela dados_raw.{nome_tabela}: {e}')
            continue
        
        # Limpeza e transformação dos dados
        query_norm = f'''
            CREATE TABLE dados_norm.{nome_tabela} AS
            SELECT
                data_base,
                TRIM(REPLACE(uf, '-', '')) AS uf,
                TRIM(REPLACE(tcb, '-', '')) AS tcb,
                TRIM(REPLACE(sr, '-', '')) AS sr,
                TRIM(REPLACE(cliente, '-', '')) AS cliente,
                TRIM(REPLACE(ocupacao, '-', '')) AS ocupacao,
                TRIM(REPLACE(cnae_secao, '-', '')) AS cnae_secao,
                TRIM(REPLACE(cnae_subclasse, '-', '')) AS cnae_subclasse,
                TRIM(REPLACE(porte, '-', '')) AS porte,
                TRIM(REPLACE(modalidade, '-', '')) AS modalidade,
                TRIM(REPLACE(origem, '-', '')) AS origem,
                TRIM(REPLACE(indexador, '-', '')) AS indexador,
                TRIM(REPLACE(REPLACE(numero_de_operacoes, '<=', ''), ',', '')) AS numero_de_operacoes,
                TRY_CAST(TRIM(REPLACE(a_vencer_ate_90_dias, ',', '')) AS FLOAT) AS a_vencer_ate_90_dias,
                TRY_CAST(TRIM(REPLACE(a_vencer_de_91_ate_360_dias, ',', '')) AS FLOAT) AS a_vencer_de_91_ate_360_dias,
                TRY_CAST(TRIM(REPLACE(a_vencer_de_361_ate_1080_dias, ',', '')) AS FLOAT) AS a_vencer_de_361_ate_1080_dias,
                TRY_CAST(TRIM(REPLACE(a_vencer_de_1081_ate_1800_dias, ',', '')) AS FLOAT) AS a_vencer_de_1081_ate_1800_dias,
                TRY_CAST(TRIM(REPLACE(a_vencer_de_1801_ate_5400_dias, ',', '')) AS FLOAT) AS a_vencer_de_1801_ate_5400_dias,
                TRY_CAST(TRIM(REPLACE(a_vencer_acima_de_5400_dias, ',', '')) AS FLOAT) AS a_vencer_acima_de_5400_dias,
                TRY_CAST(TRIM(REPLACE(vencido_acima_de_15_dias, ',', '')) AS FLOAT) AS vencido_acima_de_15_dias,
                TRY_CAST(TRIM(REPLACE(carteira_ativa, ',', '')) AS FLOAT) AS carteira_ativa,
                TRY_CAST(TRIM(REPLACE(carteira_inadimplida_arrastada, ',', '')) AS FLOAT) AS carteira_inadimplida_arrastada,
                TRY_CAST(TRIM(REPLACE(ativo_problematico, ',', '')) AS FLOAT) AS ativo_problematico
            FROM dados_raw.{nome_tabela}
            WHERE data_base IS NOT NULL;
        '''
        
        try:
            conn.execute(query_norm)
        except Exception as e:
            print(f'Erro ao processar dados do arquivo {arquivo}: {e}')
            continue
        
        # Carregar os dados normalizados para análise
        dados_norm = conn.execute(f"SELECT * FROM dados_norm.{nome_tabela}").fetchdf()

        # Adicionar os dados do arquivo atual ao DataFrame completo
        dados_completos = pd.concat([dados_completos, dados_norm], ignore_index=True)

    # Substituindo valores nulos para variáveis numéricas com 0
    dados_completos.fillna({'numero_de_operacoes': 0, 'a_vencer_ate_90_dias': 0, 'a_vencer_de_91_ate_360_dias': 0,
                          'a_vencer_de_361_ate_1080_dias': 0, 'a_vencer_de_1081_ate_1800_dias': 0,
                          'a_vencer_de_1801_ate_5400_dias': 0, 'a_vencer_acima_de_5400_dias': 0,
                          'vencido_acima_de_15_dias': 0, 'carteira_ativa': 0, 'carteira_inadimplida_arrastada': 0,
                          'ativo_problematico': 0}, inplace=True)

    # Substituindo valores nulos em colunas categóricas com uma string padrão
    dados_completos.fillna({'uf': 'Desconhecido', 'tcb': 'Desconhecido', 'sr': 'Desconhecido',
                           'cliente': 'Desconhecido', 'ocupacao': 'Desconhecido', 'cnae_secao': 'Desconhecido',
                           'cnae_subclasse': 'Desconhecido', 'porte': 'Desconhecido', 'modalidade': 'Desconhecido',
                           'origem': 'Desconhecido', 'indexador': 'Desconhecido'}, inplace=True)

    # Tratamento de Outliers (usando IQR para remover outliers)
    def remover_outliers(df, coluna):
        Q1 = df[coluna].quantile(0.25)
        Q3 = df[coluna].quantile(0.75)
        IQR = Q3 - Q1
        limite_inferior = Q1 - 1.5 * IQR
        limite_superior = Q3 + 1.5 * IQR
        return df[(df[coluna] >= limite_inferior) & (df[coluna] <= limite_superior)]

    # Removendo outliers de variáveis numéricas
    for coluna in ['a_vencer_ate_90_dias', 'a_vencer_de_91_ate_360_dias', 'a_vencer_de_361_ate_1080_dias',
                   'a_vencer_de_1081_ate_1800_dias', 'a_vencer_de_1801_ate_5400_dias', 'a_vencer_acima_de_5400_dias',
                   'vencido_acima_de_15_dias', 'carteira_ativa', 'carteira_inadimplida_arrastada', 'ativo_problematico']:
        dados_completos = remover_outliers(dados_completos, coluna)

    # Cálculo de Taxa de Inadimplência
    dados_completos['taxa_inadimplencia'] = dados_completos['carteira_inadimplida_arrastada'] / dados_completos['carteira_ativa']

    # Risco de Crédito
    dados_completos['risco_credito'] = dados_completos['a_vencer_acima_de_5400_dias'] / dados_completos['carteira_ativa']

    # Segmentação de Risco de Crédito
    dados_completos['faixa_risco_credito'] = pd.cut(dados_completos['risco_credito'], bins=[0, 0.1, 0.5, 1, np.inf],
                                                    labels=['Baixo', 'Médio', 'Alto', 'Muito Alto'])

    # Segmentação de Inadimplência
    dados_completos['faixa_inadimplencia'] = pd.cut(dados_completos['taxa_inadimplencia'], bins=[0, 0.02, 0.1, 0.3, np.inf],
                                                   labels=['Baixo', 'Moderado', 'Alto', 'Muito Alto'])

    # Análise por Variáveis de Segmentação (ocupação, cliente, etc.)
    analise_segmentada = dados_completos.groupby(['faixa_risco_credito', 'faixa_inadimplencia', 'uf']).agg({
        'carteira_ativa': 'sum',
        'carteira_inadimplida_arrastada': 'sum',
        'numero_de_operacoes': 'sum'
    }).reset_index()

    # Salvar o DataFrame final processado para cada tabela
    dados_completos.to_csv(os.path.join(pasta_saida, 'processado_completo.csv'), index=False)
    analise_segmentada.to_csv(os.path.join(pasta_saida, 'analise_segmentada.csv'), index=False)
    print(f'Arquivo processado completo salvo em: {os.path.join(pasta_saida, "processado_completo.csv")}')
    print(f'Arquivo de análise segmentada salvo em: {os.path.join(pasta_saida, "analise_segmentada.csv")}')

# Executar o processamento
def main():
    processar_arquivos()
    conn.close()

if __name__ == "__main__":
    main()
