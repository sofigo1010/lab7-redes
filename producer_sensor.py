# producer_sensor.py
import random
import json
import time

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


if __name__ == "__main__":
    # Prueba rápida del simulador: imprimir algunas lecturas cada segundo
    print("Probando simulador de sensores (sin Kafka todavía)...\n")
    for i in range(5):
        data = generar_medicion()
        # Mostrar también en formato JSON como se enviaría después
        print(f"Medición {i+1}:", json.dumps(data, ensure_ascii=False))
        time.sleep(1)