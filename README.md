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
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # S3 buckets
    source_bucket = "poc-export-s3-parquet"
    target_bucket = "poc-output-parquet-to-emr"
    
    # Cliente S3
    s3 = boto3.client('s3')
    
    # Listar todos los archivos .csv del bucket fuente
    response = s3.list_objects_v2(Bucket=source_bucket)
    csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".csv")]
    
    # Expresión para remover la fecha del nombre base
    def remove_date_suffix(filename):
        return re.sub(r"_\d{8}", "", filename).replace(".csv", "")
    
    # Procesar cada archivo CSV
    for csv_key in csv_files:
        file_name = csv_key.split("/")[-1]
        base_name = remove_date_suffix(file_name)
        
        print(f"Procesando archivo: {csv_key} como tabla: {base_name}")
        
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(f"s3://{source_bucket}/{csv_key}")
        
        df.write \
            .mode("overwrite") \
            .parquet(f"s3://{target_bucket}/{base_name}/parquet.{base_name}.parquet")
    
    job.commit()

