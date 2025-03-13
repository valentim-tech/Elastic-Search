
import pyspark.sql.functions as f
import pandas as pd
import re
import csv
import json

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
ELASTIC_PATH = "C:/Users/renato.valentim/Elastic-Search"

def convert_string_columns_to_upper(df):
    """
    Transformar todas as colunas string do DataFrame em uppercase.
    """
    schema = df.schema
    
    # Iterar por todas as colunas no DataFrame
    for field in schema.fields:
        # Checkar se o column type é StringType
        if isinstance(field.dataType, StringType):
            # Converter a string column para uppercase
            df = df.withColumn(field.name, f.upper(f.col(field.name)))
    
    return df

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

def generate_actions_attributes(rows):
    """
    Converter cada linha em um documento Elasticsearch
    """    
    for row in rows:
        yield {
            "_index": "ofertas_corpus_attributes",
            "_id": str(row['id_product']),  # Usar id_product como _id
            "_source": {
                "desc_product": row['desc_product'],
                "preco_medio": row['preco_medio'],
                "categoria": row['categoria'],
                "marca": row['marca'],
                "cor": row['cor'],
                "atributo_1": row['atributo_1'],
                "atributo_2": row['atributo_2'],
                "atributo_3": row['atributo_3'],
                "atributo_4": row['atributo_4'],
                "atributo_5": row['atributo_5'],
                "atributo_6": row['atributo_6'],
            }
        }

def weighted_search_similar(row_data, max_hits=None):
    weights = {
        "desc_product": 0.5,
        "preco_medio": 1.0,
        "categoria": 1.0,
        "marca": 1.0,
        "cor": 1.0,
        "atributo_1": 0.5,
        "atributo_2": 0.5,
        "atributo_3": 0.5,
        "atributo_4": 0.5,
        "atributo_5": 0.5,
        "atributo_6": 0.5
    }
    
    should_clauses = []
    for field in weights:
        value = row_data[field]
        if value:
            should_clauses.append({
                "match": {
                    field: {
                        "query": value,
                        "boost": weights[field]
                    }
                }
            })

    query = {
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
            }
        }
    }

    if max_hits is not None:
        query["size"] = max_hits  # Adiciona o parâmetro size apenas se max_hits for fornecido

    response = es.search(index="ofertas_corpus_attributes", body=query)
    return response['hits']['hits']

def normalize_scores_attributes(row, query_desc, results, max_range=100):
    """
    Normalizar scores de saída do Elastic Search
    """  
    query_id = row['sku']

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
        normalized.append(
            (
                query_id, 
                query_desc, 
                hit['_id'], 
                hit_desc, 
                orig_score, 
                norm_score,
                hit['_source']['preco_medio'],
                hit['_source']['categoria'],
                hit['_source']['marca'],
                hit['_source']['cor'],
                hit['_source']['atributo_1'],
                hit['_source']['atributo_2'],
                hit['_source']['atributo_3'],
                hit['_source']['atributo_4'],
                hit['_source']['atributo_5'],
                hit['_source']['atributo_6']
            )
        )

    return normalized

# Função para processar alvos e criar o DataFrame
def perform_elastic_search_attributes(query_rows, num_results_per_query=1):
    """
    Processar query e entrega lista de tuplas com resultados
    """
    # Processar todas as linhas e coletar os resultados
    all_results = []
    for row in tqdm(query_rows, desc='Searching'):
        query_desc = row['desc_product']

        results = weighted_search_similar(row, max_hits=num_results_per_query)
        normalized_results = normalize_scores_attributes(row, query_desc, results, max_range=100)

        all_results.extend(normalized_results)

    return all_results

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

def save_csv(tuple_list, output_path, column_names, delimiter=","):
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