import os
import requests
from bs4 import BeautifulSoup
import zipfile

# URL base dos arquivos
url = 'https://dadosabertos.bcb.gov.br/dataset/scr_data'

# Os anos que serão utilizados no projeto: 2020, 2021, 2022, 2023 e 2024
anos = [2020, 2021, 2022, 2023, 2024]

# Pasta onde serão armazenados
pasta_dados = 'dados'
pasta_extracao = 'extraidos'

# Função para baixar o arquivo em pedaços
def download_file(url, folder, file_name):
    if not os.path.exists(folder):
        os.makedirs(folder)
    file_path = os.path.join(folder, file_name)

    # Faz o download dos arquivos em pedaços
    with requests.get(url, stream=True) as response:
        response.raise_for_status()  # Levanta uma exceção para códigos de erro HTTP
        with open(file_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):  # 8 KB por pedaço
                if chunk:  # Evita escrever pedaços vazios
                    file.write(chunk)
    print(f'Arquivo baixado em: {file_path}')
    return file_path

# Função para extrair o arquivo
def extract_zip(file_path, folder):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(folder)
    print(f'Arquivo extraído em: {folder}')
    return folder

# Função principal do pipeline
def scraper():
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontra os links dos arquivos
    for link in soup.find_all('a'):
        href = link.get('href')
        # Verifica se o link é um arquivo .zip e está dentro dos anos desejados
        if href is not None:
            for ano in anos:
                if href.endswith(f'{ano}.zip'):

                    # Utiliza a URL verdadeira do arquivo
                    file_url = f'{href}'
                    file_name = os.path.basename(href) # Nome do arquivo

                    # Baixar o arquivo .zip
                    print(f'Baixando arquivo: {file_name}')
                    zip_file_path = download_file(file_url, pasta_dados, file_name)

                    # Extrair o arquivo .zip
                    extract_zip(zip_file_path, pasta_extracao)

if __name__ == '__main__':
    scraper()
