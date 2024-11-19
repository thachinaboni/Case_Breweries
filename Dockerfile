# Use a imagem oficial do Airflow como base
FROM apache/airflow:2.10.3

# Instale dependências do sistema necessárias
USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \
    && apt-get clean

# Mude para o usuário airflow
USER airflow

# Copie o arquivo requirements.txt para dentro do contêiner
RUN pip install --upgrade pip
COPY requirements.txt /requirements.txt

# Instale as dependências no contêiner
RUN pip install --no-cache-dir -r /requirements.txt

# Defina o diretório de trabalho
WORKDIR /opt/airflow

# Defina o entrypoint padrão (opcional se você estiver usando a imagem oficial)
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]