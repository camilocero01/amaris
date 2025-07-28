import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# Configuración Redshift
redshift_options = {
    "url": "jdbc:redshift://energy-datawarehouse.c7detdfy3unp.us-east-2.redshift.amazonaws.com:5439/dev",
    "user": "admin",
    "password": "EnergyDW2024!",
    "aws_iam_role": "arn:aws:iam::ACCOUNT:role/redshift-role-amaris"
}
# Leer datos procesados de S3
df_clientes = spark.read.parquet("s3://datalake-energy-company-camilo/processed/proveedores/")
# Transformaciones para dimensión
df_dim_clientes = df_clientes.select(
    col("nombre_proveedor"),
    col("tipo_energia")
).distinct()

# Limpiar staging
spark.read.format("jdbc") \
    .option("url", redshift_options["url"]) \
    .option("user", redshift_options["user"]) \
    .option("password", redshift_options["password"]) \
    .option("query", "TRUNCATE TABLE dw.proveedores_staging") \
    .load()
# Cargar a staging
df_dim_clientes.write \
    .format("jdbc") \
    .option("url", redshift_options["url"]) \
    .option("dbtable", "dw.proveedores_staging") \
    .option("user", redshift_options["user"]) \
    .option("password", redshift_options["password"]) \
    .mode("append") \
    .save()
# Ejecutar MERGE en Redshift
merge_sql = """
MERGE INTO dw.dim_proveedores AS target
USING (
    SELECT 
        nombre_proveedor,
        tipo_energia
    FROM dw.proveedores_staging
) AS source ON target.cliente_id = source.cliente_id
WHEN MATCHED THEN 
    UPDATE SET 
        nombre_proveedor = source.nombre_proveedor,
        tipo_energia = source.tipo_energia
WHEN NOT MATCHED THEN
    INSERT (nombre_proveedor, tipo_energia)
    VALUES (source.nombre_proveedor, source.tipo_energia);
"""
# Ejecutar MERGE
spark.read.format("jdbc") \
    .option("url", redshift_options["url"]) \
    .option("user", redshift_options["user"]) \
    .option("password", redshift_options["password"]) \
    .option("query", merge_sql) \
    .load()
job.commit()