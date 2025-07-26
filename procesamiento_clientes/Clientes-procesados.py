import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, split
from pyspark.sql.functions import col, upper, lower, trim, to_date, upper, trim, col, regexp_replace

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_raw = spark.read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv("s3://datalake-energy-company-camilo/raw/clientes/year=2025/month=07/day=24/clientes.csv")

print("Esquema de datos después de la lectura correcta:")
df_raw.printSchema()
df_raw.show(5)

# Transformaciones
df_clean = df_raw.withColumn("nombre_limpio", 
                            regexp_replace(upper(trim(col("nombre"))), "[^A-Z0-9]", "")) \
                .withColumn("ciudad_limpia", 
                           regexp_replace(upper(trim(col("ciudad"))), "[^A-Z0-9]", "")) \
               .filter(col("nombre_limpio").isNotNull()) \
               .dropDuplicates()

df_final = df_clean

# --- 3. Escribir el resultado en formato Parquet ---
print("Escribiendo datos transformados en formato Parquet...")
df_final.coalesce(1).write \
    .mode("overwrite") \
    .parquet("s3://datalake-energy-company-camilo/processed/clientes/")

print("\n✅ Proceso completado. Datos guardados en la capa 'processed'.")

# Finalizar el job
job.commit()