# Configuração da URL da API
API_URL = "https://api.openbrewerydb.org/breweries"

def fetch_data():
    """
    Função para acessar a API e retornar os dados.
    """
    import pandas as pd
    import requests
    import os

    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar a API: {e}")
        return []

def save_bronze():
    """
    Função para salvar os dados brutos em formato JSON.
    """
    import os
    import pandas as pd
    import json

    data = fetch_data()

    # Verifica se há dados
    if not data:
        print("Nenhum dado retornado pela API.")
        return

    # Define o diretório de saída e cria o diretório, se necessário
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "layers/bronze")
    os.makedirs(output_dir, exist_ok=True)
    
    # Define o caminho do arquivo de saída
    output_path = os.path.join(output_dir, "breweries_raw.json")

    # Salvando o arquivo no diretório especificado:
    with open(output_path, 'w') as f:
        f.write(json.dumps(data, indent=4))
    print("Dados salvos com sucesso em bronze.")

def transform_data():
    """
    Função para transformar os dados brutos em um DataFrame Spark, realizando transformações como,
    round columns para garantir arredondamento e o fill blank columns para garantir que colunas com
    null sejam preenchidas com "Unknown".
    """
    import os
    from common.tool_functions import round_columns, fill_blank_columns
    import pandas as pd

    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_data = os.path.join(base_dir, "layers/bronze/breweries_raw.json")

    try:
        df = pd.read_json(input_data)
    except Exception as e:
        raise FileNotFoundError(f"Erro ao carregar o arquivo JSON: {e}")

    # Transformações:
    df = round_columns(df=df, columns=["longitude", "latitude"], precision=2)
    df = fill_blank_columns(df=df,
                            columns=["name", "brewery_type", "street", "city", "state", "country"])

    return df

def save_silver():
    """
    Função para salvar os dados transformados em formato Parquet.
    """
    import os
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "layers/silver/")
    input_data = transform_data()

    # Itera por valores únicos da coluna de partição e salva em arquivos separados
    for value in input_data["state"].unique():
        partition_dir = os.path.join(output_dir, f"state={value}")
        os.makedirs(partition_dir, exist_ok=True)
        
        # Filtra os dados da partição e salva como arquivo parquet
        partition_df = input_data[input_data["state"] == value]
        partition_df.to_parquet(os.path.join(partition_dir, "breweries_partitioned.parquet"), index=False)

def create_gold_layer():
    """
    Função para criar a camada Gold,
    com a agregação da quantidade de cervejarias por tipo e localização (estado e cidade).
    """
    import os
    import pandas as pd

    base_dir = os.path.dirname(os.path.abspath(__file__))
    silver_dir = os.path.join(base_dir, "layers/silver/")
    output_dir = os.path.join(base_dir, "layers/golden/")
    os.makedirs(output_dir, exist_ok=True)

    # Carregar dados de todas as partições da camada Silver
    silver_files = [
        os.path.join(silver_dir, folder, "breweries_partitioned.parquet")
        for folder in os.listdir(silver_dir)
        if folder.startswith("state=")
    ]
    df_list = [pd.read_parquet(file) for file in silver_files]
    df = pd.concat(df_list, ignore_index=True)

    # Agregando os dados
    gold_df = (
        df.groupby(["brewery_type", "state", "city"])
        .size()
        .reset_index(name="brewery_count")
        .sort_values(by=["state", "city", "brewery_type"])
    )
    
    # Caminho do arquivo de saída
    output_path = os.path.join(output_dir, "breweries_aggregated.parquet")
    
    # Salvando como Parquet
    gold_df.to_parquet(output_path, index=False)
    print(f"Camada Gold criada com sucesso em: {output_path}")