import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import requests

# Inicialização do Spark
spark = SparkSession.builder.appName("BreweryETL").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

# Configuração da URL da API
API_URL = "https://api.openbrewerydb.org/breweries"

def fetch_data():
    """
    Função para acessar a API e retornar os dados.
    """
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a API: {e}")
        return []

def save_bronze(data):
    """
    Função para salvar os dados brutos em formato JSON.
    """
    pd.DataFrame(data).to_json(
        "/data/bronze/breweries_raw.json", orient="records")

def transform_data(data):
    """
    Função para transformar os dados brutos em um DataFrame Spark.
    """
    from breweries_case.dags.common.tools_functions import round_columns, fill_blank_columns, schema
    df = spark.createDataFrame(data, schema=schema)
    df = round_columns(df=df, columns=["longitude", "latitude"], precision=2)
    df = fill_blank_columns(df=df, columns=["name",
                                            "brewery_type",
                                            "street",
                                            "city",
                                            "state",
                                            "country"
                                           ])
    return df

def save_silver(df):
    df.write.mode("overwrite").partitionBy("state").parquet("/home/coder/notebooks/case/data/silver/breweries_partitioned")

def main():
    data = fetch_data()
    save_bronze(data)
    df_silver = transform_data(data)
    save_silver(df_silver)

if __name__ == "__main__":
    main()