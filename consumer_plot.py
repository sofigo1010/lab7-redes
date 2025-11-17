import json
from datetime import datetime

from kafka import KafkaConsumer

from config import TOPIC, BOOTSTRAP_SERVERS, GROUP_ID


def crear_consumer():
    """
    Crea un KafkaConsumer suscrito al topic indicado.
    - value_deserializer convierte el JSON (bytes) a dict de Python.
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",   # leer solo lo nuevo
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    return consumer


def run_consumer():
    consumer = crear_consumer()
    print(f"Escuchando topic = {TOPIC} en {BOOTSTRAP_SERVERS}")
    print("Esperando mensajes... Ctrl+C para detener.\n")

    # Listas para ir acumulando datos (las usaremos luego para graficar)
    tiempos = []
    temps = []
    hums = []
    vientos = []

    try:
        for msg in consumer:
            payload = msg.value  # ya es un dict gracias al deserializer

            # Extraer campos
            temp = payload.get("temperatura")
            hum = payload.get("humedad")
            viento = payload.get("direccion_viento")

            t = datetime.now().strftime("%H:%M:%S")

            tiempos.append(t)
            temps.append(temp)
            hums.append(hum)
            vientos.append(viento)

            print(
                f"[{t}] Recibido -> temp={temp} °C, "
                f"humedad={hum} %, viento={viento}"
            )

            # Solo para ver cuántos datos llevamos acumulados
            print(
                f"  Total muestras: {len(temps)} "
                f"(temp), {len(hums)} (hum), {len(vientos)} (viento)\n"
            )

    except KeyboardInterrupt:
        print("\nConsumer interrumpido por el usuario.")
    finally:
        consumer.close()
        print("Consumer cerrado.")


if __name__ == "__main__":
    run_consumer()