
import pyspark.sql.functions as f
import pandas as pd
import re
import csv

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.types import ArrayType, LongType, IntegerType, DoubleType, FloatType
from pyspark.sql.window import Window
from datetime import date, datetime, timedelta
from functools import reduce
from tqdm import tqdm

MAIN_PATH = "C:/Users/renato.valentim/Documents/ElasticSearch"

def remove_accents_df(df, columns):
    """
    Remove acentos de colunas específicas em um DataFrame PySpark,
    aproximando-se do comportamento do unidecode com substituições regex.
    """
    # Dicionário de substituições: padrões de caracteres acentuados -> letra sem acento
    replacements = {
        # Vogais minúsculas com acentos
        '[àáâãäåāăą]': 'a',
        '[èéêëēĕėęě]': 'e',
        '[ìíîïĩīĭį]': 'i',
        '[òóôõöōŏő]': 'o',
        '[ùúûüũūŭůűų]': 'u',
        '[ýÿŷ]': 'y',

        # Vogais maiúsculas com acentos
        '[ÀÁÂÃÄÅĀĂĄ]': 'A',
        '[ÈÉÊËĒĔĖĘĚ]': 'E',
        '[ÌÍÎÏĨĪĬĮ]': 'I',
        '[ÒÓÔÕÖŌŎŐ]': 'O',
        '[ÙÚÛÜŨŪŬŮŰŲ]': 'U',
        '[ÝŸŶ]': 'Y',

        # Outros caracteres comuns
        '[çćĉċč]': 'c',
        '[ÇĆĈĊČ]': 'C',
        '[đď]': 'd',
        '[ĐĎ]': 'D',
        '[ñńņňŉ]': 'n',
        '[ÑŃŅŇ]': 'N',
        '[śŝşš]': 's',
        '[ŚŜŞŠ]': 'S',
        '[ţťŧ]': 't',
        '[ŢŤŦ]': 'T',
        '[źżž]': 'z',
        '[ŹŻŽ]': 'Z',

        # Caracteres especiais adicionais
        '[æ]': 'ae',  # æ → ae (como unidecode)
        '[Æ]': 'AE',
        '[œ]': 'oe',  # œ → oe (como unidecode)
        '[Œ]': 'OE',
        '[øō]': 'o',  # ø → o
        '[ØŌ]': 'O',
        '[ß]': 'ss',  # ß → ss (como unidecode)
        '[ſ]': 's',   # ſ → s (letra "s longa")

        # Acentos isolados ou marcas diacríticas
        '[`´ˆ˜¨]': '',  # Remove acentos soltos, se houver
    }
    
    # Aplica cada substituição para todas as colunas especificadas
    for column in columns:
        for pattern, replacement in replacements.items():
            df = df.withColumn(column, f.regexp_replace(f.col(column), pattern, replacement))
    
    return df

def clean_text_column(df, col_name, new_col_name=None):
    """
    Aplica transformações de limpeza de texto em uma coluna específica de um DataFrame do PySpark
    em uma única expressão encadeada.
    """
    new_col_name = new_col_name or col_name  # Usa o mesmo nome se não for passado um novo

    df_cleaned = (
        df.withColumn(new_col_name, f.col(col_name))
        .withColumn(
            new_col_name,
            f.when(f.col(new_col_name).isNotNull(), 
                   f.regexp_replace(f.col(new_col_name), '"items":', '')
            ).otherwise(f.col(new_col_name))
        )
        .withColumn(
            new_col_name,
            f.when(f.col(new_col_name).isNotNull(), 
                   f.regexp_replace(f.col(new_col_name), '-', ' ')
            ).otherwise(f.col(new_col_name))
        )
        .withColumn(
            new_col_name,
            f.when(f.col(new_col_name).isNotNull(), 
                   f.regexp_replace(f.col(new_col_name), r'\s+', ' ')
            ).otherwise(f.col(new_col_name))
        )
        .withColumn(
            new_col_name,
            f.when(f.col(new_col_name).isNotNull(), 
                   f.regexp_replace(f.col(new_col_name), r'[\r\n\t]', ' ')
            ).otherwise(f.col(new_col_name))
        )
        .withColumn(
            new_col_name,
            f.when(f.col(new_col_name).isNotNull(), 
                   f.regexp_replace(f.col(new_col_name), r'\s+', ' ')
            ).otherwise(f.col(new_col_name))
        )
        .withColumn(
            new_col_name,
            f.when(f.col(new_col_name).isNotNull(), 
                   f.regexp_replace(f.col(new_col_name), r'[^a-zA-Z0-9\s]', '')
            ).otherwise(f.col(new_col_name))
        )
        .withColumn(
            new_col_name,
            f.when(f.col(new_col_name).isNotNull(), 
                   f.trim(f.col(new_col_name))
            ).otherwise(f.col(new_col_name))
        )
    )

    return df_cleaned

def remove_terms_from_column(df, terms_to_remove, column_name, case_sensitive=False):
    """
    Função para remover termos indesejados
    """
    pattern = r"\b(" + "|".join(terms_to_remove) + r")\b"
    if not case_sensitive:
        pattern = r"(?i)" + pattern
    
    df_cleaned = (
      df.withColumn(column_name, f.regexp_replace(f.col(column_name), pattern, ""))
      .withColumn(column_name, f.regexp_replace(f.col(column_name), r"\s+", " "))
    )
    
    return df_cleaned

def generate_actions(rows):
    """
    Converter cada linha em um documento Elasticsearch
    """    
    for row in rows:
        yield {
            "_index": "ofertas_corpus",
            "_id": str(row['id_product']),  # Usar id_product como _id
            "_source": {
                "desc_product": row['desc_product']
            }
        }


def search_similar(desc, max_hits=None):
    """
    Buscar ofertas semelhantes
    """
    query = {
        "query": {
            "match": {
                "desc_product": desc
            }
        }
    }
    if max_hits is not None:
        query["size"] = max_hits  # Adiciona o parâmetro size apenas se max_hits for fornecido
    
    response = es.search(index="ofertas_corpus", body=query)
    return response['hits']['hits']

def normalize_scores(query_id, query_desc, results, max_range=100):
    """
    Normalizar scores de saída do Elastic Search
    """  
    if not results:
        return []
    # Normalizar com base no maior score, mas verificar se é exato
    max_score = max(hit['_score'] for hit in results)
    normalized = []
    for hit in results:
        orig_score = hit['_score']
        hit_desc = hit['_source']['desc_product']
        # Se o hit for exato (ignorando maiúsculas/minúsculas e espaços), atribuir 100
        if query_desc.strip().lower() == hit_desc.strip().lower():
            norm_score = float(max_range)  # 100.0 como float para consistência
        else:
            # Caso contrário, normalizar proporcionalmente ao maior score
            norm_score = (orig_score / max_score) * max_range if max_score > 0 else 0.0
        normalized.append((query_id, query_desc, hit['_id'], hit_desc, orig_score, norm_score))
    return normalized

# Função para processar alvos e criar o DataFrame
def perform_elastic_search(query_rows, num_results_per_query=1):
    """
    Processar query e entrega lista de tuplas com resultados
    """
    # Processar todas as linhas e coletar os resultados
    all_results = []
    for row in tqdm(query_rows, desc='Searching'):
        query_id = row['sku']
        query_desc = row['desc_product']
        results = search_similar(query_desc, max_hits=num_results_per_query)
        normalized_results = normalize_scores(query_id, query_desc, results, max_range=100)
        all_results.extend(normalized_results)

    return all_results

def save_csv(tuple_list, output_path, column_names, delimiter=";"):
    """
    Salva resultados em csv
    """
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        # Cabeçalho
        writer.writerow(column_names)
        # Dados
        writer.writerows(tuple_list)

# Criar spark session
spark = (
    SparkSession
    .builder
    .appName("Elastic-Search")
    .master("local[*]")
    .getOrCreate()
)

# Conectar ao Elasticsearch
es = Elasticsearch("http://localhost:9200")