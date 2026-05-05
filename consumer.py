import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import time

# --- CONFIGURACIÓN DE INFLUXDB ---
# Pega aquí el token largo que copiaste de la interfaz de InfluxDB
TOKEN = "jxPDYKY1KIoeZB8IALZPOyxoQU9c6UoIpyPo4O2H55VsSCYjO0cAoIyP7RPjps-i64R57T7S2Y36GhYZ4LVw5g==" 
ORG = "uaemex"
BUCKET = "kafka_data"
URL = "http://localhost:8086"

# Inicializar cliente de InfluxDB
client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

def create_consumer():
    """Create a connection to Kafka broker as a consumer"""
    consumer = KafkaConsumer(
        'orders', # Asegúrate que tu producer.py también use el tópico 'orders'
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='order-processing-group',
        auto_offset_reset='earliest'
    )
    return consumer

def process_order(order):
    """Procesa la orden y la guarda en InfluxDB para el Dashboard"""
    print(f"Processing order {order['order_id']} for {order['quantity']} units of {order['product']}")
    
    try:
        # Creamos el punto de datos para Grafana
        # Usamos 'product' como etiqueta y 'quantity' como el valor numérico a graficar
        point = Point("procesamiento_ordenes") \
            .tag("producto", order['product']) \
            .field("cantidad", float(order['quantity'])) \

        # Enviamos a la base de datos
        write_api.write(bucket=BUCKET, org=ORG, record=point)
        print("Datos enviados a InfluxDB correctamente.")
    except Exception as e:
        print(f"Error al enviar a InfluxDB: {e}")

def run_consumer():
    """Run the consumer, reading messages"""
    consumer = create_consumer()
    
    try:
        print("Consumer started. Waiting for messages...")
        for message in consumer:
            order = message.value
            process_order(order)
            print(f"Received from partition {message.partition}, offset {message.offset}")
            print("---")
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    finally:
        consumer.close()
        client.close() # Cerramos la conexión a InfluxDB
        print("Consumer and InfluxDB client closed")

if __name__ == "__main__":
    run_consumer()