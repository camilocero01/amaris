# prueba técnica de amaris consulting

## Ejercicio 1

## PUNTO 1

### 1. Creación de bucket

Se hace la creación del buket para la solución 
* - datalake-energy-company-camilo para el datalake
* - source-energy-company-camilo para la administración y recepción de archivos CSV

se propone la siguiente estructura 
* --raw/
* ---clientes/
* ----year=2025/
* -----month=07/
* ------day=25/
* ---proveedores/
* ----year=2025/
* -----month=07/
* ------day=25/
* ---transacciones/
* ----year=2025/
* -----month=07/
* ------day=25/

### 2. creación de Roles
se crean dos roles

* - Glue-Role-Amaris
para el uso de glue con los permisoso sobre AWSGlueServiceRole y AmazonS3FullAccess

* - Lambda-Role-Amaris
para la configuración de las lambdas con los permisos sobre AWSLambdaBasicExecutionRole y AmazonS3FullAccess

* - Athena-rol-amaris 
Para el acceso a athena  con los permisos sobre AmazonAthenaFullAccess y AWSGlueConsoleFullAccess

* - redshift-role-amaris 
Para el acceso a redshift con los permisos de AWSGlueConsoleFullAccess y AmazonS3ReadOnlyAccess

* - para el uso de ATHENA se crea el usuario camilo con los permisos sobre AmazonAthenaFullAccess y AWSGlueConsoleFullAccess

### 3. creación de Bases de datos
para la estrategía se proponen dos bases de datos, una para datos raw (energy_raw_db) y otro para datos procesados (energy_processed_db)

Al final la estructura la estratégia permite realizar el particionamiento por la fecha de carga
![alt text]({C39BA267-C651-4865-B2F1-75961D4727DF}.png)

## PUNTO 2
Se crean 3 jobs de ejemplo de transformación de datos, se guardan en formato parquet en la bd energy_processed_db

* Clientes-procesados https://github.com/camilocero01/amaris/tree/main/procesamiento_clientes
* transacciones-procesadas https://github.com/camilocero01/amaris/tree/main/transacciones-procesadas
* Procesamiento_proveedor https://github.com/camilocero01/amaris/tree/main/Procesamiento_proveedor

Se deja ejemplo del resultado final con la data procesada en archivos parquet
![alt text]({865A1F1F-5C1F-49E1-8ADA-889DA94BC72B}.png)

## PUNTO 3
Se hace la  creación de crawler para automatizar y procesar los datos
Se crean dos crawler, uno para los datos raw (energy-raw-crawler-v2) y otro para los datos procesados (energy-raw-crawler-v2)

También se crea lambda energy-data-ingestion se deja código de la función https://github.com/camilocero01/amaris/blob/main/lambda_function.py

## PUNTO 4 Uso de athena para consultas sql

27/07/2025 Para la prueba habilite una cuenta free de AWS, me encuentro que Athena esta restringido para la cuenta free, ya realice la solicitud de acceso a Athena pero aun no me lo habilitan, apenas tenga habilitado, termino este punto. Por ahora prefiero hacer entrega parcial y si hay más tiempo, entregaré esta parte.

28/07/2025 El día de hoy aun no me han habilitado Athena en la cuenta de AWS, con fin de poder terminar la prueba, migro la cuenta de free a paga y logro tener acceso a athena. A continuación se presentan las 3 consultar de ejemplo

### CONSULTA 1 Conteo de clientes x ciudad
SELECT ciudad, count(1) conteo 
FROM clientes
GROUP BY ciudad

### CONSULTA 2 Conteo de proveedores x tipo energía
SELECT tipo_energia,
	count(1) conteo
FROM proveedores
GROUP BY tipo_energia

### CONSULTA 3 Comportamiento del promedio x tipo de transacción
SELECT 
    tipo_transaccion,
    COUNT(*) as num_transacciones,
    AVG(cantidad_comprada_kwh) as kwh_promedio,
    AVG(precio_kwh_cop) as precio_promedio
FROM transacciones
GROUP BY tipo_transaccion;

Se deja evidencia de la ejecución y funcionamiento de Athena
![alt text]({69DB3C48-802E-482B-9166-B305E1BF9363}.png)


## PUNTO ADICIONAL 1 CREACIÓN DE IAC
Se usa CloudFormation, se realiza análisis y se genera IaC https://github.com/camilocero01/amaris/blob/main/Plantilla-amaris-template-1753560571699.yaml

## PUNTO ADICIONAL 3 migrar a redshift

CREATE SCHEMA dw;

-- Tabla temporal para proveedores
CREATE TABLE staging.proveedores_staging (
    tipo_energia VARCHAR(255),
    nombre_proveedor VARCHAR(255),
    fecha_registro_parsed VARCHAR(50)
);

-- Tabla temporal para proveedores
CREATE TABLE dw.dim_proveedores(
    tipo_energia VARCHAR(255),
    nombre_proveedor VARCHAR(255),
    fecha_registro_parsed VARCHAR(50)
);

Se deja el código en python que hace la migración en REDSHIFT https://github.com/camilocero01/amaris/tree/main/load-proveedor-a-redshift

# Ejercicio 2

Como he comentado en varios puntos, no he tenido la oportunidad de trabajar en la nube de AWS y aunque he desplegado en otras nubes ambientes, mi enfoque es muy fuerte hacia la ingenieria de datos en ambientes ya desplegados, para este punto requiero un poco de tiempo para investigar a conciencia sobre el punto propuesto y no se si tengo más tiempo, propongo enviar parcialmente lo que tengo y validar con ustedes cuanto tiempo más tengo para la entega completa

# Ejercicio 3

CREATE SCHEMA dw;

CREATE TABLE dw.proveedores_staging (
    tipo_energia VARCHAR(255),
    nombre_proveedor VARCHAR(255)
);

## Pregunta 1
¿Qué experiencias has tenido como ingeniero de datos en AWS? ¿Cuál ha sido el proyecto más retador y por qué?

He trabajado en un sin fin de proyectos con empresas grandes, donde más que especilizarme en una nube, he tenido el reto de tener que adaptarme a las herramientas disponbilizadas por el proyecto, para ingenieria de datos he pasado por ambientes como GCP, AZURE, IIS, Informatica power center, tableau, power BI(estos dos últimos no son para ingenieria de datos pero hay clientes que si le dan eses uso ) 

Mi unica experiencia en AWS fue el desarrollo de dos ENDPOINTS en AWS usando API REST en ambiente NODE JS 

Mi experiencia ha sido más en nube azure con Azure Databricks, Azure Datafactory, Azure Synapse ,Spark y Python
En ambiente GCP usando dataprep, Bigquery y Bigquery ML para entregar un modelo de marketing RFM
En amazon es la única nube que no he tenido experiencia implementando, pero realizando la prueba téncica de Amaris, reafirmo mi teoria de que en todas las nubes existe un análogo de las diferentes herramientas presentadas en cada nube, aunque no tengo experiencia en AWS logré contruir en AWS un pipeline completo sin ningún inconveniente.

## Pregunta 2
¿Que estrategias has aplicado para crear los recursos necesarios en AWS para mantener una arquitectura y pipelines de datos?
Mi experiencia ha sido en visualización, gobierno de datos y en mis últimos 3 años mas como ingeniero de datos, la mayoria de las veces he llegado a proyectos donde los ambientes ya estan desplegados y me he dedicado casi exclusivamente que a desarrollar los scripts en python(este es mi gran fuerte), desafortunadamente no he tenido la experienca a fondo en desplegar y mantener una arquitectura en nube.

## Pregunta 3
¿Qué consideraciones tomarías al decidir entre almacenar datos en Amazon S3, RDS o Redshift?
En los contactos que he tenido con el ambientes de nube, siempre presentan varias soluciones para almacenamiento de datos pero se debe tener en cuenta varios factores:
* **Volumen**: algunos casi ilimitados mientras que otros con limites
* **Costos**: algunos con costos bajos y otros con costos altos
* **Uso:** algunos estan optimizados para almacenamientos, otros para transacciones y otros para procesamientos

Inventigando un poco se recomienda:
* s3 como solución más economica mas enfocada a datos brutos y como ambiente de limpieza y preparación de datos, recomendada para almacenamiento de archivos brutos o procesados pero en tipo archivo, no recomendado para hacer analítica avanzada
* RDS como solución con costos medio, enfocado a datos transaccionales,  recomendada para transacciones frecuentas y mayor rendimiento para transaccionalidad
* Redshift: solución que puede ser más costosa, enfocada para data lista para análisis de datos, usado más para consultas analíticas

## Pregunta 4
¿Qué beneficios y desventajas ves al utilizar AWS Glue en comparación con Lambda o Step Functions para orquestación ETL?

**AWS Glue Ventjas:**
* Es serverless
* Escalamiento automático
* Fácil integración entre s3, RDS y redshift
* Manejo de bigdata

**AWS Glue desventajas:**
* Al ser serverless, el arranque es frio, siendo lento para iniciar
* Limitaciones en lenguajes de programación (python y escala)
* Menos control y dificil debug 
* Al estar optmizado para bigdata no es usable para tareas pequeñas

**Lambda Ventjas:**
* Cualquier lenguaje de programación
* Arranque rápido
* Integración amplia por triggers
* Más facil de debugger al poderse integrar con herramientas de desarrollo

**Lambda desventajas:**
* Limitado en recursos como topes en memoria y en tiempos máximo de ejecución
* Todo lo opuesto a glue, al no esta optimizado para bigdata, funciona mejor con ejecuciones cortas de pocos datos o en tiempo real.

**Step Functions Ventjas:**
* Orquestación visual
* Alta tolerancia a fallos
* Integración nativa con servicios de AWS
* Auditoria completa

**Step Functions desventajas:**
* No procesa datos directamente, depende de otros servicios para procesar
* Complejo de configurar
* Puede aumentar los costos

## Pregunta 5
¿Cómo garantizarías la integridad y seguridad de los datos de un datalake construido en Amazon S3?

Las nubes tienen una excelente gestión de seguridad, en los proyectos donde he participado como ingeniero de datos, es curioso como el proceso de habilitación de accesos y permisos a los diferentes recursos es un poco tediosos, por defecto la seguridad en nube viene cerrada y los administradores deben dar los permisos para poder habilitar el acceso a servicios, pero por el otro si no es un buen administrador de nube, también por error puede habilitar y abrir huecos de seguridad.
 
En general para AWS se deben tener en cuenta los siguiente elementos para garantizar la integridad y seguridad:

1. Políticas IAM granulares y uso del principio de menor privilegio. puede ser tedioso pero se recomienda desde el principio dedicar mucho tiempo a la definición y configuración de los roles necesarios para el proyecto
2. Se recomienda el uso de Lake Formation
3. REalizar auditorias regulares y realizar pruebas de penetración, he estado en compañias donde se propone jornada de hackaton con el fin de intentar encontrar huecos de seguridad entre áreas y premian con dinero los hallazgos.
4. Muy importante trabajar en formación de los colaboradores y en temas culturales frente a un buen manejo de la integridad y seguridad de los datos.


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
