import matplotlib.pyplot as plt
from datetime import datetime
from kafka import KafkaConsumer
from config import TOPIC, BOOTSTRAP_SERVERS, GROUP_ID
from codec_payload import decode_payload


def crear_consumer_compacto():
    """
    KafkaConsumer que recibe payloads binarios de 3 bytes.
    No usamos value_deserializer: msg.value llega como bytes y
    lo pasamos a decode_payload().
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Usamos un group_id distinto para no mezclar con el consumer anterior
        group_id=GROUP_ID + "-compacto",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        # value: bytes -> sin deserializer
    )
    return consumer


def setup_plot():
    plt.ion()
    fig, ax = plt.subplots()
    line_temp, = ax.plot([], [], label="Temperatura (°C)")
    line_hum, = ax.plot([], [], label="Humedad (%)")

    ax.set_xlabel("Muestra")
    ax.set_ylabel("Valor")
    ax.set_title("Telemetría (payload compacto de 3 bytes)")
    ax.legend()
    fig.tight_layout()

    return fig, ax, line_temp, line_hum


def actualizar_plot(ax, line_temp, line_hum, temps, hums):
    x = list(range(1, len(temps) + 1))
    line_temp.set_data(x, temps)
    line_hum.set_data(x, hums)

    ax.relim()
    ax.autoscale_view()
    plt.draw()
    plt.pause(0.01)


def run_consumer_compacto():
    consumer = crear_consumer_compacto()
    fig, ax, line_temp, line_hum = setup_plot()

    print(f"[COMPACTO] Escuchando topic = {TOPIC} en {BOOTSTRAP_SERVERS}")
    print("[COMPACTO] Esperando mensajes... Ctrl+C para detener.\n")

    tiempos = []
    temps = []
    hums = []
    vientos = []

    try:
        for msg in consumer:
            raw_payload = msg.value  # bytes (3 bytes)
            t = datetime.now().strftime("%H:%M:%S")

            # Decodificar los 3 bytes a dict legible
            data = decode_payload(raw_payload)

            temp = data["temperatura"]
            hum = data["humedad"]
            viento = data["direccion_viento"]

            tiempos.append(t)
            temps.append(temp)
            hums.append(hum)
            vientos.append(viento)

            print(
                f"[{t}] [COMPACTO] Payload crudo = {raw_payload} "
                f"(hex={raw_payload.hex()})"
            )
            print(
                f"[{t}] [COMPACTO] Decodificado -> "
                f"temp={temp:.2f} °C, humedad={hum} %, viento={viento}"
            )
            print(
                f"  [COMPACTO] Total muestras: {len(temps)} "
                f"(temp/hum), {len(vientos)} (viento)\n"
            )

            actualizar_plot(ax, line_temp, line_hum, temps, hums)

    except KeyboardInterrupt:
        print("\n[COMPACTO] Consumer interrumpido por el usuario.")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()
        print("[COMPACTO] Consumer cerrado.")


if __name__ == "__main__":
    run_consumer_compacto()