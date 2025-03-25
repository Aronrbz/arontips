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
        from pyspark.sql.functions import col
        
        # Inicialización
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)
        
        # Buckets
        source_bucket = "poc-export-s3-parquet"
        target_bucket = "poc-output-parquet-to-emr"
        
        # Cliente S3
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=source_bucket)
        
        # Archivos CSV
        csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".csv")]
        
        # Función para limpiar nombre (quita fecha)
        def remove_date_suffix(filename):
            return re.sub(r"_\d{8}", "", filename).replace(".csv", "")
        
        # Procesar cada archivo
        for csv_key in csv_files:
            file_name = csv_key.split("/")[-1]
            base_name = remove_date_suffix(file_name)
            
            print(f"📦 Procesando archivo: {csv_key} → tabla: {base_name}")
        
            try:
                df = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(f"s3://{source_bucket}/{csv_key}")
        
                # Filtra columnas tipo fecha con fechas inválidas
                date_columns = [f.name for f in df.schema.fields if f.dataType.simpleString() == "timestamp"]
        
                for col_name in date_columns:
                    df = df.filter((col(col_name).isNull()) | (col(col_name) >= "1900-01-01"))
        
                # Escribir a Parquet
                output_path = f"s3://{target_bucket}/{base_name}/parquet.{base_name}.parquet"
                df.write.mode("overwrite").parquet(output_path)
        
                print(f"✅ Guardado correctamente: {output_path}")
        
            except Exception as e:
                print(f"❌ Error al procesar {csv_key}: {e}")
        
        job.commit()
