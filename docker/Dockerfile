# Use a imagem base oficial do Jupyter com PySpark
FROM jupyter/pyspark-notebook:latest

# Definir o diretório de trabalho
WORKDIR /home/jovyan

# Copiar o arquivo de requisitos
COPY ../requirements.txt /home/jovyan/

# Instalar as dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código do projeto
COPY .. /home/jovyan/minio_datalake/

# Definir o PYTHONPATH para incluir o diretório do projeto
ENV PYTHONPATH="/home/jovyan/minio_datalake:${PYTHONPATH}"
