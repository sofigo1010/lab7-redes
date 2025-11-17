import json
from datetime import datetime

from kafka import KafkaConsumer
import matplotlib.pyplot as plt

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


def setup_plot():
    """
    Configura la figura de matplotlib para graficar temperatura y humedad.
    Eje X: número de muestra (1, 2, 3, ...)
    """
    plt.ion()  # modo interactivo
    fig, ax = plt.subplots()
    line_temp, = ax.plot([], [], label="Temperatura (°C)")
    line_hum, = ax.plot([], [], label="Humedad (%)")

    ax.set_xlabel("Muestra")
    ax.set_ylabel("Valor")
    ax.set_title("Telemetría Estación Meteorológica (Kafka)")
    ax.legend()
    fig.tight_layout()

    return fig, ax, line_temp, line_hum


def actualizar_plot(ax, line_temp, line_hum, temps, hums):
    """
    Actualiza las curvas en la gráfica con los datos acumulados.
    """
    x = list(range(1, len(temps) + 1))

    line_temp.set_data(x, temps)
    line_hum.set_data(x, hums)

    ax.relim()          # recalcula límites
    ax.autoscale_view() # ajusta vista a los nuevos datos

    plt.draw()
    plt.pause(0.01)     # pequeño delay para refrescar


def run_consumer():
    consumer = crear_consumer()
    fig, ax, line_temp, line_hum = setup_plot()

    print(f"Escuchando topic = {TOPIC} en {BOOTSTRAP_SERVERS}")
    print("Esperando mensajes... Ctrl+C para detener.\n")

    tiempos = []
    temps = []
    hums = []
    vientos = []

    try:
        for msg in consumer:
            payload = msg.value  # dict

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
            print(
                f"  Total muestras: {len(temps)} "
                f"(temp/hum), {len(vientos)} (viento)\n"
            )

            # Actualizar gráfica
            actualizar_plot(ax, line_temp, line_hum, temps, hums)

    except KeyboardInterrupt:
        print("\nConsumer interrumpido por el usuario.")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()  # deja la última gráfica fija
        print("Consumer cerrado.")


if __name__ == "__main__":
    run_consumer()