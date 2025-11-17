import random
import json
import time

from kafka import KafkaProducer  
from config import TOPIC, BOOTSTRAP_SERVERS

# Parámetros "realistas" para las distribuciones
TEMP_MEAN = 25.0   # grados Celsius
TEMP_STD = 5.0     # desviación estándar
HUM_MEAN = 60.0    # porcentaje %
HUM_STD = 15.0

DIRECCIONES_VIENTO = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]


def clamp(valor, minimo, maximo):
    """Limita valor al rango [minimo, maximo]."""
    return max(minimo, min(maximo, valor))


def generar_medicion():
    """
    Genera una medición simulada de la estación:
    - temperatura: float con 2 decimales, en [0, 110]
    - humedad: entero en [0, 100]
    - dirección del viento: uno de {N, NO, O, SO, S, SE, E, NE}
    """

    # Temperatura con distribución aproximadamente normal
    temp = random.normalvariate(TEMP_MEAN, TEMP_STD)
    temp = clamp(temp, 0.0, 110.0)
    temp = round(temp, 2)

    # Humedad con distribución aproximadamente normal
    hum = random.normalvariate(HUM_MEAN, HUM_STD)
    hum = clamp(hum, 0.0, 100.0)
    hum = int(round(hum))

    # Dirección de viento uniforme
    viento = random.choice(DIRECCIONES_VIENTO)

    medicion = {
        "temperatura": temp,
        "humedad": hum,
        "direccion_viento": viento,
    }
    return medicion


def crear_producer():
    """
    Crea e inicializa un KafkaProducer que:
    - Se conecta al broker indicado en BOOTSTRAP_SERVERS.
    - Serializa el value como JSON codificado en UTF-8.
    - Serializa la key como string UTF-8.
    """
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    return producer


def run_producer():
    producer = crear_producer()
    print(f"Conectado a Kafka en {BOOTSTRAP_SERVERS}, topic = {TOPIC}")
    print("Enviando mediciones cada 15–30 segundos. Ctrl+C para detener.\n")

    try:
        while True:
            data = generar_medicion()

            # Enviar medición al topic de Kafka
            producer.send(
                TOPIC,
                key="sensor1",   # identifica al sensor (por si hubiera varios)
                value=data,      # se serializa como JSON
            )
            producer.flush()  # aseguramos que salga del buffer

            print("Enviado:", json.dumps(data, ensure_ascii=False))

            # Espera aleatoria entre 15 y 30 segundos
            delay = random.randint(15, 30)
            print(f"Siguiente medición en {delay} segundos...\n")
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\nProducer interrumpido por el usuario.")
    finally:
        producer.close()
        print("Producer cerrado.")


if __name__ == "__main__":
    run_producer()