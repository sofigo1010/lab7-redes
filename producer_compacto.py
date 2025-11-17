import random
import time
from kafka import KafkaProducer
from config import TOPIC, BOOTSTRAP_SERVERS
from producer_sensor import generar_medicion  # reutiliza el simulador
from codec_payload import encode_medicion


def crear_producer_compacto():
    """
    KafkaProducer que envía payloads binarios (3 bytes).
    No usamos value_serializer porque ya mandamos bytes.
    """
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),  # solo la key
        # value: bytes -> sin serializer
    )
    return producer


def run_producer_compacto():
    producer = crear_producer_compacto()
    print(f"[COMPACTO] Conectado a Kafka en {BOOTSTRAP_SERVERS}, topic = {TOPIC}")
    print(
        "[COMPACTO] Enviando mediciones codificadas en 3 bytes "
        "cada 15–30 segundos. Ctrl+C para detener.\n"
    )

    try:
        while True:
            # 1) Generar medición "normal" como dict
            data = generar_medicion()

            # 2) Codificar en 3 bytes usando nuestro codec
            payload = encode_medicion(data)

            # 3) Enviar a Kafka como valor binario
            producer.send(
                TOPIC,
                key="sensor1",
                value=payload,
            )
            producer.flush()

            print("[COMPACTO] Original:", data)
            print(
                f"[COMPACTO] Payload (3 bytes) = {payload}  "
                f"(hex={payload.hex()})\n"
            )

            delay = random.randint(15, 30)
            print(f"[COMPACTO] Siguiente medición en {delay} segundos...\n")
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\n[COMPACTO] Producer interrumpido por el usuario.")
    finally:
        producer.close()
        print("[COMPACTO] Producer cerrado.")


if __name__ == "__main__":
    run_producer_compacto()