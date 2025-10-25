# Obtener el dataset "Tasa Representativa del Mercado Historico" de la pagina datoscolombia
mkdir -p /home/vboxuser/data
wget -O /home/vboxuser/data/trm_historico.csv "https://www.datos.gov.co/api/views/mcec-87by/rows.csv?accessType=DOWNLOAD"

# Visualizar las primeras lineas del dataset
head /home/vboxuser/data/trm_historico.csv

# Creamos el archivo en un nano para procesar los datos en un dataframe
nano ~/eda_trm.py

# Importar las librerias necesarias

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, datediff, avg, max, min

# Iniciar sesión de Spark
spark = SparkSession.builder.appName("TRM_EDA").getOrCreate()

# Cargar el dataset
df = spark.read.option("header", "true").csv("/home/vboxuser/data/trm_historico.csv", inferSchema=True)

# Limpieza: convertir fechas y eliminar duplicados
df = (
    df.withColumn("VIGENCIADESDE", to_date(df.VIGENCIADESDE, "dd/MM/yyyy"))
      .withColumn("VIGENCIAHASTA", to_date(df.VIGENCIAHASTA, "dd/MM/yyyy"))
      .dropDuplicates()
)

# Calcular duración de cada rango
df = df.withColumn("duracion_dias", datediff(df.VIGENCIAHASTA, df.VIGENCIADESDE) + 1)

# EDA: usar funciones explícitas
eda_df = (
    df.groupBy("VIGENCIADESDE")
      .agg(
          avg("VALOR").alias("promedio_TRM"),
          max("VALOR").alias("max_TRM"),
          min("VALOR").alias("min_TRM")
      )
      .orderBy("VIGENCIADESDE")
)

# Mostrar resultados
eda_df.show(20)

# Guardar resultados en formato Parquet
eda_df.write.mode("overwrite").parquet("/home/vboxuser/data/trm_processed")

# Cerrar sesión
spark.stop()

# Procesamiento en tiempo real con Kafka y Spark Streaming
# Verificar el Parquet con PySpark
pyspark --master local[*]

# Shell
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CheckParquet").getOrCreate()
df = spark.read.parquet("/home/vboxuser/data/trm_processed")
df.show(20)
spark.stop()

# VERIFICACIÖN EN TIEMPO REAL CON KAFKA Y SPARK STREAMING
# Implementar el Productor 
nano producer.py

# Codigo del productor en Python
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

# Configuración del productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Ajusta si tu Kafka está en otro servidor
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializa datos a JSON
)

# Función para generar datos TRM simulados
def generate_trm_data():
    base_date = datetime(2025, 10, 23)  # Fecha de inicio (hoy)
    for day in range(10):  # Genera 10 días de datos
        date = base_date + timedelta(days=day)
        trm_value = 4000.50 + random.uniform(-10, 10)  # Valor TRM con variación aleatoria
        data = {
            "VIGENCIADESDE": date.strftime("%Y-%m-%d"),
            "VALOR": round(trm_value, 2)
        }
        print(f"Enviando: {data}")  # Para depuración
        producer.send('trm_topic', data)  # Envía al topic
        time.sleep(1)  # Simula intervalo de 1 segundo entre envíos
    producer.flush()  # Asegura que todos los mensajes se envíen

# Ejecutar el productor
if __name__ == "__main__":
    print("Iniciando productor de datos TRM...")
    generate_trm_data()
    print("Productor finalizado.")

# Crear el topic
/opt/Kafka/bin/kafka-topics.sh --create --topic trm_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Ejecutar el Productor
python3 producer.py

# Ejecutar el Consumidor
/opt/Kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trm_topic --from-beginning

# Crear el archivo de Consumidor en Spark Streaming
nano consumer.py

# Codigo del consumidor
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min

# Iniciar Spark Session
spark = SparkSession.builder.appName("TRMStreaming").getOrCreate()

# Leer del topic de Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trm_topic") \
    .load()

# Definir esquema para parsear los datos JSON
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Iniciar Spark Session
spark = SparkSession.builder.appName("TRMStreaming").getOrCreate()

# Leer del topic de Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trm_topic") \
    .load()

# Definir esquema para parsear los datos JSON
schema = StructType([
    StructField("VIGENCIADESDE", StringType()),
    StructField("VALOR", DoubleType())
])

# Parsear JSON y extraer campos
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Calcular estadísticas por fecha
result_df = parsed_df.groupBy("VIGENCIADESDE").agg(
    avg("VALOR").alias("promedio_TRM"),
    max("VALOR").alias("max_TRM"),
    min("VALOR").alias("min_TRM")
)

# Mostrar resultados en consola
query = result_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()

# Ejecutar el Consumidor en Spark Streaming
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --master local[*] \
  consumer.py









