import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

# --- Inicialización ---
# No es necesario 'args' para este script simple, pero es buena práctica mantenerlo
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 1. Leer datos de S3 con el delimitador correcto ---
# ## CORRECCIÓN 1: Se añade .option("delimiter", ";") para que Spark sepa cómo separar las columnas.
# Esto evita que lea "nombre_proveedor;tipo_energia" como una sola columna.
df_raw = spark.read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv("s3://datalake-energy-company-camilo/raw/transacciones/year=2025/month=07/day=24/transacciones.csv")

print("Esquema de datos después de la lectura correcta:")
df_raw.printSchema()
df_raw.show(5)

# Transformaciones
df_clean = df_raw.withColumn("tipo_transaccion", 
                            regexp_replace(upper(trim(col("tipo_transaccion"))), "[^A-Z0-9]", "")) \
                .withColumn("nombre_cliente_proveedor", 
                           regexp_replace(upper(trim(col("nombre_cliente_proveedor"))), "[^A-Z0-9]", "")) \
                .withColumn("tipo_energia", 
                           regexp_replace(upper(trim(col("tipo_energia"))), "[^A-Z0-9]", "")) \
               .filter(col("nombre_cliente_proveedor").isNotNull()) \
               .dropDuplicates()
               
# Convertir tipos de datos
df_clean = df_clean.withColumn("cantidad_comprada_kwh", col("cantidad_comprada_kwh").cast("double")) \
             .withColumn("precio_kwh_cop", col("precio_kwh_cop").cast("double")) \
            

# Agregar métricas calculadas
df_clean = df_clean.withColumn("eficiencia_precio", 
                               when(col("precio_kwh_cop") < 0.25, "Económico")
                               .when(col("precio_kwh_cop") < 0.27, "Promedio")
                               .otherwise("Costoso"))
                              

# Agregar métricas calculadas
df_clean = df_clean.withColumn("precio_total", col("precio_kwh_cop") * col("cantidad_comprada_kwh"))

df_final = df_clean

# --- 3. Escribir el resultado en formato Parquet ---
print("Escribiendo datos transformados en formato Parquet...")
df_final.coalesce(1).write \
    .mode("overwrite") \
    .parquet("s3://datalake-energy-company-camilo/processed/transacciones/")

print("\n✅ Proceso completado. Datos guardados en la capa 'processed'.")

# Finalizar el job
job.commit()