import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Inicialización
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    print("=== INICIANDO CARGA A REDSHIFT ===")
    
    # Configuración Redshift con parámetros de conexión mejorados
    redshift_options = {
        "url": "jdbc:redshift://energy-datawarehouse.c7detdfy3unp.us-east-2.redshift.amazonaws.com:5439/dev",
        "user": "admin",
        "password": "EnergyDW2024!",
        "driver": "com.amazon.redshift.jdbc42.Driver",
        "aws_iam_role": "arn:aws:iam::ACCOUNT:role/redshift-role-amaris",
        "tempdir": "s3://datalake-energy-company-camilo/temp/",
        "forward_spark_s3_credentials": "true"
    }
    
    # Probar conexión primero
    if not test_redshift_connection():
        raise Exception("No se pudo establecer conexión con Redshift")
    
    print("=== LEYENDO DATOS DE S3 ===")
    # Leer datos procesados de S3
    df_clientes = spark.read.parquet("s3://datalake-energy-company-camilo/processed/proveedores/")
    
    print(f"Registros leídos: {df_clientes.count()}")
    
    # Transformaciones para dimensión
    df_dim_clientes = df_clientes.select(
        col("nombre_proveedor"),
        col("tipo_energia")
    ).distinct()
    
    print(f"Registros únicos: {df_dim_clientes.count()}")
    
    # Mostrar muestra de datos
    print("=== MUESTRA DE DATOS ===")
    df_dim_clientes.show(5)
    
    print("=== LIMPIANDO TABLA STAGING ===")
    # Función para probar conectividad
    def test_redshift_connection():
        """Prueba la conexión a Redshift"""
        try:
            print("=== PROBANDO CONEXIÓN A REDSHIFT ===")
            test_df = spark.read.format("jdbc") \
                .option("url", redshift_options["url"]) \
                .option("user", redshift_options["user"]) \
                .option("password", redshift_options["password"]) \
                .option("driver", redshift_options["driver"]) \
                .option("query", "SELECT 1 as test") \
                .load()
            
            test_df.show()
            print("✅ Conexión exitosa")
            return True
        except Exception as e:
            print(f"❌ Error de conexión: {str(e)}")
            return False
    
    # Método mejorado para ejecutar comandos SQL en Redshift
    def execute_redshift_command(sql_query):
        """Ejecuta un comando SQL en Redshift"""
        try:
            temp_df = spark.createDataFrame([(1,)], ["dummy"])
            temp_df.write \
                .format("jdbc") \
                .option("url", redshift_options["url"]) \
                .option("user", redshift_options["user"]) \
                .option("password", redshift_options["password"]) \
                .option("driver", redshift_options["driver"]) \
                .option("tempdir", redshift_options["tempdir"]) \
                .option("preactions", sql_query) \
                .option("dbtable", "dw.temp_dummy") \
                .mode("overwrite") \
                .save()
            print(f"✅ Comando ejecutado: {sql_query[:50]}...")
        except Exception as e:
            print(f"❌ Error ejecutando comando: {str(e)}")
            raise e
    
    # Limpiar staging
    truncate_sql = "TRUNCATE TABLE dw.proveedores_staging"
    execute_redshift_command(truncate_sql)
    
    print("=== CARGANDO A STAGING ===")
    # Cargar a staging con configuración mejorada
    df_dim_clientes.write \
        .format("jdbc") \
        .option("url", redshift_options["url"]) \
        .option("dbtable", "dw.proveedores_staging") \
        .option("user", redshift_options["user"]) \
        .option("password", redshift_options["password"]) \
        .option("driver", redshift_options["driver"]) \
        .option("tempdir", redshift_options["tempdir"]) \
        .option("forward_spark_s3_credentials", redshift_options["forward_spark_s3_credentials"]) \
        .mode("append") \
        .save()
    
    print("=== EJECUTANDO MERGE ===")
    # MERGE corregido (Redshift no soporta MERGE, usar UPSERT)
    upsert_sql = """
    BEGIN;
    
    -- Actualizar registros existentes
    UPDATE dw.dim_proveedores 
    SET 
        tipo_energia = s.tipo_energia,
        fecha_actualizacion = GETDATE()
    FROM dw.proveedores_staging s
    WHERE dw.dim_proveedores.nombre_proveedor = s.nombre_proveedor;
    
    -- Insertar nuevos registros
    INSERT INTO dw.dim_proveedores (nombre_proveedor, tipo_energia, fecha_creacion)
    SELECT 
        s.nombre_proveedor,
        s.tipo_energia,
        GETDATE()
    FROM dw.proveedores_staging s
    LEFT JOIN dw.dim_proveedores d ON s.nombre_proveedor = d.nombre_proveedor
    WHERE d.nombre_proveedor IS NULL;
    
    COMMIT;
    """
    
    # Ejecutar UPSERT
    execute_redshift_command(upsert_sql)
    
    print("=== LIMPIEZA FINAL ===")
    # Limpiar staging después del proceso
    execute_redshift_command("TRUNCATE TABLE dw.proveedores_staging")
    
    print("=== PROCESO COMPLETADO EXITOSAMENTE ===")
    
except Exception as e:
    print(f"Error en el proceso: {str(e)}")
    job.commit()
    raise e

job.commit()