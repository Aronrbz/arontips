# arontips
arontips


import sys
import boto3
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame

# Inicialización de Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuraciones
input_bucket = "poc-export-s3-parquet"
output_bucket = "poc-output-parquet-to-emr"

# Cliente S3 para listar archivos
s3 = boto3.client('s3')
response = s3.list_objects_v2(Bucket=input_bucket)

# Procesar cada archivo .csv
for obj in response.get("Contents", []):
    key = obj["Key"]
    
    if key.endswith(".csv"):
        print(f"Procesando archivo: {key}")
        
        # Obtener nombre lógico sin extensión ni fecha
        match = re.match(r"(.+?)_?\d{8}\.csv$", key)
        if not match:
            print(f"Nombre no válido para: {key}")
            continue

        table_name = match.group(1)
        table_name_clean = table_name.split("/")[-1]  # por si viene con subcarpeta

        # Leer CSV
        df = spark.read.option("header", "true").csv(f"s3://{input_bucket}/{key}")

        # Escribir en formato Parquet
        output_path = f"s3://{output_bucket}/{table_name_clean}/parquet.{table_name_clean}.parquet"
        df.write.mode("overwrite").parquet(output_path)

        print(f"✅ Guardado: {output_path}")

job.commit()
