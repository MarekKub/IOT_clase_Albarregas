from hdfs import InsecureClient
from kafka import KafkaConsumer
from json import loads
import csv 
import os
from datetime import datetime

# Configuración para guardar el CSV en HDFS

# Datos de conexión
HDFS_HOSTNAME = '172.17.10.30'

HDFSCLI_PORT = 9870

HDFSCLI_CONNECTION_STRING = f'http://{HDFS_HOSTNAME}:{HDFSCLI_PORT}'

hdfs_client = InsecureClient(HDFSCLI_CONNECTION_STRING)

variable_datetime_now = datetime.now()
ftt = variable_datetime_now.strftime("%d-%m-%Y %H_%M")
ft= variable_datetime_now.strftime("%d-%m-%Y %H")

topic = '2024_142_HUMEDAD'

# Configuración del consumidor Kafka
consumer = KafkaConsumer(
    topic,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda msg: loads(msg.decode('utf-8')),
    bootstrap_servers=['ceserver2:9092'])

# Contador de filas
row_count = 0

# Recupear teimpo actual
variable_datetime_now = datetime.now()

ftt = variable_datetime_now.strftime("%d-%m-%Y %H_%M")
ft= variable_datetime_now.strftime("%d-%m-%Y %H")

# Nombre del archivo CSV
csv_filename = f'{topic}_{ft}.csv'

# Verificar si el archivo CSV ya existe, si no, crearlo con el encabezado
if not os.path.exists(csv_filename):
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['fecha','valor'])  # Ajusta las columnas según tus datos

for msg in consumer:
    data = msg.value
    # Escribir los datos en el archivo CSV
    with open(csv_filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data[1] , [data.payload])  # Ajusta las columnas según tus datos ponemos data para confirmar los datos de entradas entran en el csv

    # Incrementar el contador de filas
    row_count += 1

    # Si alcanzamos 60 filas, subir el archivo CSV a HDFS y reiniciar el contador
    # Las filas se ceunta con len
    if row_count == 60:  

        # guardamos el datime.now() para poder usarlo para borrar fichero
        # Y formatear tiempo
        hdfs_client.write("/Pro_IOT/142/", csv_filename)  # Ejecutar comando para subir el archivo a HDFS
 
        # Borrar datos del CSV -> borrar el csv
        os.remove(csv_filename)

        row_count = 0  # Reiniciar contador de filas