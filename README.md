![Badge em Desenvolvimento](https://img.shields.io/badge/Azure_Store-DataLake-blue)
![Badge em Desenvolvimento](https://img.shields.io/badge/Azure_HdInsinght-Cluster_Spark-green)
![Badge em Desenvolvimento](https://img.shields.io/badge/Jupyter-Notebook-blue)
### FRANCISCO JAIME DA SILVA


## Projeto Engenharia de dados com Azure HdInsinght


<p align="center"><img src="./images/Hdinsight.jpg" width="500"></p>

__*Foi Crianda uma estrutura de data lake no Azure Store*__

O projeto consiste em criar um data lake no Azure Store, fazer upload de dados processa-lo e converte-los para um formato mais adequado a analises levando em consideração espaço de armazenamento e performance do pyspark. 

---

### Etapas do Projeto

1. Foi Criado uma Azure Storage Acount onde foi criado um datalake com estruta abaixo:

<ul>
  <li>Bancket(landing) - Landing zone ou Zona de Pouso(dados em formato bruto)</li>
  <li>Bancket(processing) - Processing zone(dados pre-processados)</li>
  <li>Bancket(curated) - Cureted zone(Dados limpos, agregados e prontos para análises)</li>  
</ul> 
2.Foi realizado o upload dos arquivos de dados na zona de pouso(landing), extraídos do kaggle

<ul>
  <li>T201601PDPI+BNFT.csv</li>
  <li>T201602PDPI+BNFT.csv</li>
  <li>T201603PDPI+BNFT.csv</li>
  <li>T201604PDPI+BNFT.csv</li>   
</ul> 

3. Foi criada Identity Manager e concedido acesso de woner do data lake
4. Foi criado um cluster spark no Azure HdInsinght 

5. Fi coiada a aplicação para o nó master do cluster e utilizado spark-submit para executa-la
6. N aplicação foram excutadas as seguinte oprações:
  - Foi realizadas uma pequena limpeza, preprocessamento e conversão para o formato parquet, visando melhorar a performance nas proximas etapas do processo. O resultado(df-formatado.parquet) desse processo foi gravado na Proessing zone.
  - Foi realiada a leitura dos dados em formato parquet e criada a view Dados_Sql para permitir exploração dos mesmos via SQL.

  - Foi aplicada agragação nos dados da view Dados_Sql e gravado o resultado na Curated zone em formato parquet.(df-Dados-Agregados.parquet)

7. Foi utiizado o script abaixo para realiza o processamento dos dados.
<https://github.com/jaimesilvads/SparkAzureHdInsinght/blob/main/pyspark-hdinsight.py>

