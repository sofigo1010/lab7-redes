"""
Funciones para codificar/decodificar una medición meteorológica
en un payload de exactamente 3 bytes (24 bits).

Layout de bits (de más significativo a menos significativo):

[ temp (14 bits) ][ humedad (7 bits) ][ viento (3 bits) ]

- temp:   14 bits, 0..110.00 °C con 2 decimales (almacenamos temp*100)
- hum:    7 bits,  0..100 (%)
- viento: 3 bits,  8 direcciones posibles
"""

DIRECCIONES_VIENTO = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]

# Mapas para codificar/decodificar la dirección de viento
VIENTO_A_CODE = {d: i for i, d in enumerate(DIRECCIONES_VIENTO)}
CODE_A_VIENTO = {i: d for i, d in enumerate(DIRECCIONES_VIENTO)}


def clamp(valor, minimo, maximo):
    return max(minimo, min(maximo, valor))


def encode_medicion(medicion: dict) -> bytes:
    """
    Recibe un dict con:
      {
        "temperatura": float,
        "humedad": int,
        "direccion_viento": str (una de DIRECCIONES_VIENTO)
      }
    y devuelve un payload de EXACTAMENTE 3 bytes.
    """

    temp = float(medicion["temperatura"])
    hum = int(round(medicion["humedad"]))
    viento = str(medicion["direccion_viento"])

    # Asegurar rangos válidos
    temp = clamp(temp, 0.0, 110.0)
    hum = clamp(hum, 0, 100)

    if viento not in VIENTO_A_CODE:
        raise ValueError(f"Dirección de viento inválida: {viento}")

    viento_code = VIENTO_A_CODE[viento]

    # -------- Temperatura en 14 bits --------
    # Guardamos temp con 2 decimales: 25.43 -> 2543
    temp_scaled = int(round(temp * 100))  # 0..11000

    if not (0 <= temp_scaled <= 11000):
        raise ValueError(f"Temperatura fuera de rango: {temp}")

    # temp_scaled cabe en 14 bits porque 2^14 = 16384
    # -------- Armar entero de 24 bits --------
    # Bits: [ temp(14) ][ hum(7) ][ viento(3) ]
    value_24 = (temp_scaled << (7 + 3)) | (hum << 3) | viento_code

    # Convertir entero de 24 bits a 3 bytes (big-endian)
    payload = value_24.to_bytes(3, byteorder="big")
    return payload


def decode_payload(payload: bytes) -> dict:
    """
    Recibe 3 bytes y devuelve un dict con la misma forma que la medición original.
    """
    if len(payload) != 3:
        raise ValueError(f"Payload debe tener 3 bytes, tiene {len(payload)}")

    # Convertir 3 bytes a entero de 24 bits
    value_24 = int.from_bytes(payload, byteorder="big")

    # Extraer campos (inverso de encode_medicion)
    viento_code = value_24 & 0b111                 # últimos 3 bits
    hum = (value_24 >> 3) & 0b1111111              # siguientes 7 bits
    temp_scaled = value_24 >> (7 + 3)              # bits restantes (14)

    if viento_code not in CODE_A_VIENTO:
        raise ValueError(f"Código de viento inválido: {viento_code}")

    viento = CODE_A_VIENTO[viento_code]
    temp = temp_scaled / 100.0  # regresar a °C con 2 decimales

    return {
        "temperatura": temp,
        "humedad": hum,
        "direccion_viento": viento,
    }


# Prueba rápida local 
if __name__ == "__main__":
    ejemplo = {
        "temperatura": 28.56,
        "humedad": 52,
        "direccion_viento": "E",
    }
    print("Original:", ejemplo)
    payload = encode_medicion(ejemplo)
    print("Payload (3 bytes):", payload, " -> hex:", payload.hex())
    decod = decode_payload(payload)
    print("Decodificado:", decod)