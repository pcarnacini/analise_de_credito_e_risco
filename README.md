# Projeto de Análise de Crédito e Riscos

## Fonte de Dados
Para este projeto foi utilizado o Banco Central como meio de obter os dados necessários.
Utilizou-se do SCR - Sistema de Informações de Créditos, onde os relatórios são atualizados no último dia útil do mês.
Os dados são categorizados como CSV, e foram obtidos dos anos de 2020 a 2024.
Link para a página de dados: [Dados SCR](https://dadosabertos.bcb.gov.br/dataset/scr_data)
Link para o dicionário de dados: [Dicionário](https://www.bcb.gov.br/content/estabilidadefinanceira/scr/scr.data/scr_data_metodologia.pdf)

## Extração de dados
A extração se deu por meio de Web Scraping, buscando pelos anos + extensão .zip (Pois os arquivos estavam zipados e dentro continham os csvs).

## Transformação

### Normatização
Foi necessário fazer uma normatização de todas as planilhas visto que havia alguns campos vazios, e com caracteres desnecessários.
Foi considerado no número de operações que estavam "<=15" como "15" a fim de remover os caracteres desnecessários para padronizar o tipo da coluna.

### 