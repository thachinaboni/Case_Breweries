def round_columns(df, columns, precision):
    """
    Função para arredondar colunas de um DataFrame pandas
    Parâmetros:
    df: DataFrame pandas a ser arredondado
    columns: Lista de colunas a serem arredondadas
    precision: int - Número de casas decimais para arredondamento
    """
    import pandas as pd

    for column in columns:
        df[column] = df[column].astype(str).str.replace(',', '.', regex=False)
        df[column] = df[column].str.replace(' ', '', regex=False)
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].round(precision)
    return df

def fill_blank_columns(df, columns):
    """
    Função com o objetivo de substituir todos os em branco por Unknown
    Parâmetros:
    ------------------------
    df: Dataframe a ser utilizado
    columns: colunas a receberem a substituição
    """
    import pandas as pd
    
    for column in columns:
        df[column] = df[column].replace(['', ' '], None).fillna('Unknown')
    return df