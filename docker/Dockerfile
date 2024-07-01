# Use the official Jupyter image with PySpark
FROM jupyter/pyspark-notebook:latest

# Atualizar e instalar dependências necessárias
USER root
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /home/jovyan

# Copy the requirements file
COPY ../requirements.txt /home/jovyan/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project code
COPY .. /home/jovyan/minio_datalake/

# Ensure the script directory exists with the correct permissions
RUN mkdir -p /home/jovyan/minio_datalake/scripts  \
    && chown -R jovyan:users /home/jovyan/minio_datalake/scripts  \
    && chmod -R 777 /home/jovyan/minio_datalake/scripts

# Set PYTHONPATH to include the project directory before running the settings script
ENV PYTHONPATH="/home/jovyan/minio_datalake:${PYTHONPATH}"

USER jovyan

# Run the settings script to generate the Spark init script
RUN python -c "import minio_datalake.settings"

# Configure Spark to use MinIO and download dependencies
RUN $SPARK_HOME/bin/spark-shell --packages com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.1.1,org.apache.hadoop:hadoop-common:3.3.4,org.apache.hadoop:hadoop-aws:3.3.4 -i /home/jovyan/minio_datalake/scripts/init_spark_dependencies.scala

# Set PYTHONPATH to include the project directory
ENV PYTHONPATH="/home/jovyan/minio_datalake:${PYTHONPATH}"
