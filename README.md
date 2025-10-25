# Tarea-3_Procesamiento-de-Datos-con-Apache-Spark_-tarea-4

La solución propuesta para la Tarea 3 se centra en el diseño e implementación de un sistema de procesamiento de datos distribuido, articulando tecnologías como Hadoop, Apache Spark y Kafka, en cumplimiento del resultado de aprendizaje orientado al manejo de grandes volúmenes de información. Para ello, se trabajó con el conjunto de datos “Tasa Representativa del Mercado Histórico” disponible en datos.gov.co, el cual contiene registros oficiales de la TRM en pesos colombianos (COP), estructurados en columnas como VALOR, UNIDAD, VIGENCIADESDE y VIGENCIAHASTA.
La implementación se dividió en dos componentes principales, siguiendo las directrices del Anexo 3 (Instructivo Spark Streaming y Kafka) para la configuración de la máquina virtual y los servicios asociados:
- Procesamiento en modo batch: Incluye la carga, limpieza, transformación y análisis exploratorio del dataset histórico mediante DataFrames en Spark, con persistencia en formato Parquet.
- Procesamiento en tiempo real: Consiste en la configuración de un topic en Kafka, la simulación de eventos mediante un generador de datos, y el desarrollo de una aplicación en Spark Streaming para el consumo y análisis estadístico de los flujos entrantes.
Esta arquitectura demuestra la eficiencia de Spark tanto en el tratamiento de datos estructurados como en el procesamiento de streams, superando las limitaciones de Hadoop en operaciones en memoria y escenarios de baja latencia. El desarrollo se realizó en Python por su simplicidad sintáctica, y se ejecutó en la máquina virtual preconfigurada (usuario: vboxuser, contraseña: bigdata).

Análisis de Resultados
Procesamiento en modo batch (EDA):
El conjunto de datos fue sometido a un proceso de limpieza —incluyendo la conversión de fechas al formato estándar y la eliminación de registros duplicados—, seguido de una transformación que permitió calcular la duración en días entre las vigencias. El análisis exploratorio agrupó los registros por fecha (VIGENCIADESDE) y calculó métricas como promedio, máximo y mínimo del valor de la TRM. Los resultados evidencian que cada fecha contiene un único valor (por ejemplo, para el 2 de diciembre de 1991, el promedio, máximo y mínimo son 643.42), lo que confirma la naturaleza diaria del registro sin variaciones intradía. Esta información resulta útil para estudios económicos retrospectivos, al evidenciar tendencias históricas de la TRM, especialmente en décadas como los años noventa, caracterizadas por valores significativamente bajos.
Procesamiento en tiempo real:
El productor simuló diez días de datos TRM con valores cercanos a los 4 000 COP, incorporando variaciones aleatorias para representar condiciones reales. La aplicación Spark Streaming consumió los mensajes desde el topic, parseó los objetos JSON y calculó estadísticas agregadas por fecha. Los resultados muestran métricas en vivo —por ejemplo, para el 23 de octubre de 2025, el promedio de TRM fue 3 994.38—, lo que evidencia la capacidad de Spark para procesar flujos de Kafka en micro-batches. Aunque los datos simulados son únicos por fecha (lo que implica que promedio, máximo y mínimo coinciden), el sistema está preparado para escalar y manejar múltiples registros por unidad temporal.
Estos resultados validan la capacidad de Spark para realizar análisis exploratorio eficiente y procesamiento en tiempo real escalable, cumpliendo con los objetivos técnicos y pedagógicos de la tarea.

Instrucciones para la Ejecución
1. Activación de servicios base:

Asegúrese de que ZooKeeper y Kafka estén activos, según lo indicado en el Anexo 3.
2. Descarga del dataset TRM:
mkdir -p /home/vboxuser/data
wget -O /home/vboxuser/data/trm_historico.csv "https://www.datos.gov.co/api/views/mcec-87by/rows.csv?accessType=DOWNLOAD"
head /home/vboxuser/data/trm_historico.csv


3. Procesamiento en modo batch (EDA y almacenamiento):
- Crear el script:
nano ~/eda_trm.py


- Pegar el código proporcionado y ejecutar:
spark-submit ~/eda_trm.py


- Verificar la salida estadística y confirmar la creación del archivo procesado en:
/home/vboxuser/data/trm_processed

4. Procesamiento en tiempo real:
- Crear el topic (si no existe):
/opt/Kafka/bin/kafka-topics.sh --create --topic trm_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


- Crear y ejecutar el productor:
nano producer.py
python3 producer.py


- Crear y ejecutar el consumidor:
nano consumer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 --master local[*] consumer.py


- Verificar el funcionamiento en vivo ejecutando el productor en una terminal paralela.
5. Finalización y monitoreo:
- Para detener los servicios, utilice Ctrl + C o finalice los procesos con kill.
- Verifique el estado de Spark en la interfaz web: http://192.168.1.6:4041, ajustando la IP según la configuración activa en su servidor Ubuntu y cliente Putty.
