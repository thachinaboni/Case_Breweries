def round_columns(df, columns, precision):
    """
    Função para arredondar colunas de um DataFrame
    Parâmetros:
    df: DataFrame a ser arredondado
    columns: Lista de colunas a serem arredondadas
    precision: int - Número de casas decimais para arredondamento
    """
    from pyspark.sql import functions as F

    for column in columns:
        df = df.withColumn(column, F.regexp_replace(F.col(column), ',', '.'))
        df = df.withColumn(column, F.regexp_replace(F.col(column), ' ', ''))
        
        df = df.withColumn(column, F.col(column).cast("double"))
        
        df = df.withColumn(column, F.round(F.col(column), precision))
    return df

def fill_blank_columns(df, columns):
    """
    Função com o objetivo de substituir todos os em branco por Unknown
    Parâmetros:
    ------------------------
    df: Dataframe a ser utilizado
    columns: colunas a receberem a substituição
    """
    from pyspark.sql import functions as F

    for column in columns:
        df = df.withColumn(column, F.when(F.col(column).isNull(), F.lit("Unknown")).otherwise(F.col(column)))
    return df