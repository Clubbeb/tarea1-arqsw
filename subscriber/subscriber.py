"""Subscriber de sismos para UNA ciudad (parametrizado).

Cada instancia:
  1. Declara su propia cola durable `sismos.<ciudad>` y la bindea al exchange
     fanout `sismos` (si el subscriber se cae, los avisos quedan en la cola).
  2. Por cada evento {id, latitud, longitud} calcula la distancia (haversine)
     entre el epicentro y la ciudad.
  3. Si la distancia es < 500 km pide el detalle a la API (pull):
     GET {API_URL}/sismos/{id} y lo muestra. Si no, lo descarta.

Uso (desde la raíz del repo):
    python subscriber/subscriber.py <ciudad>
    CIUDAD=<ciudad> python subscriber/subscriber.py
Ciudades: arica, coquimbo, valparaiso, concepcion, punta_arenas
"""

import json
import math
import os
import sys
import time
from datetime import datetime
from urllib.parse import quote

import pika
import requests

try:
    from ciudades import CIUDADES  # ejecutado como script: python subscriber/subscriber.py
except ImportError:
    from subscriber.ciudades import CIUDADES  # importado como paquete

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
API_URL = os.getenv("API_URL", "http://localhost:8000").rstrip("/")
EXCHANGE = "sismos"

UMBRAL_KM = 500.0
RADIO_TIERRA_KM = 6371.0088
TIMEOUT_API = 5  # segundos
ESPERA_REINTENTO_API = 5  # segundos antes de devolver el mensaje a la cola

INTENTOS_CONEXION = 10
ESPERA_CONEXION = 3  # segundos

# Acciones posibles tras procesar un mensaje
ACK = "ack"
REENCOLAR = "reencolar"


def log(ciudad, mensaje):
    print(f"[{ciudad}] {mensaje}", flush=True)


def haversine(lat1, lon1, lat2, lon2):
    """Distancia en km sobre la superficie terrestre entre dos puntos (grados)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * RADIO_TIERRA_KM * math.asin(math.sqrt(a))


def parsear_evento(body):
    """Valida el evento. Retorna (id, lat, lon) o lanza ValueError si está mal formado."""
    try:
        evento = json.loads(body)
        id_sismo = str(evento["id"])
        lat = float(evento["latitud"])
        lon = float(evento["longitud"])
    except (ValueError, KeyError, TypeError) as error:
        raise ValueError(f"evento mal formado ({error!r}): {body!r}") from error
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError(f"coordenadas fuera de rango: {body!r}")
    return id_sismo, lat, lon


def formatear_fecha(fecha_iso):
    try:
        fecha = datetime.fromisoformat(fecha_iso)
        return fecha.strftime("%d-%m-%Y %H:%M:%S (UTC%z)")
    except (TypeError, ValueError):
        return str(fecha_iso)


def formatear_detalle(detalle, distancia_km, nombre_ciudad):
    return (
        f"*** ALERTA SÍSMICA para {nombre_ciudad} ***\n"
        f"    Id:            {detalle.get('id')}\n"
        f"    Magnitud:      {detalle.get('magnitud')} {detalle.get('tipo_magnitud', '')}\n"
        f"    Profundidad:   {detalle.get('profundidad_km')} km\n"
        f"    Referencia:    {detalle.get('referencia_geografica')}\n"
        f"    Fecha local:   {formatear_fecha(detalle.get('fecha_local'))}\n"
        f"    Epicentro:     ({detalle.get('latitud')}, {detalle.get('longitud')})\n"
        f"    Distancia:     {distancia_km:.1f} km de {nombre_ciudad}\n"
        f"    Agencia:       {detalle.get('agencia')}"
    )


def procesar_mensaje(body, ciudad, api_url=API_URL):
    """Lógica del subscriber, independiente de RabbitMQ (para poder probarla).

    Retorna ACK o REENCOLAR según lo que deba hacerse con el mensaje.
    """
    info = CIUDADES[ciudad]
    nombre = info["nombre"]

    # Mensaje mal formado: se registra y se hace ack. Reencolarlo no sirve
    # (volvería a fallar siempre) y bloquearía la cola con prefetch=1.
    try:
        id_sismo, lat, lon = parsear_evento(body)
    except ValueError as error:
        log(ciudad, f"Mensaje descartado: {error}")
        return ACK

    distancia = haversine(lat, lon, info["latitud"], info["longitud"])
    if distancia >= UMBRAL_KM:
        log(ciudad, f"Sismo {id_sismo} no es de interés ({distancia:.1f} km)")
        return ACK

    log(ciudad, f"Sismo {id_sismo} es de interés ({distancia:.1f} km). Consultando detalle a la API...")
    url = f"{api_url}/sismos/{quote(id_sismo, safe='')}"
    try:
        respuesta = requests.get(url, timeout=TIMEOUT_API)
    except requests.RequestException as error:
        # API caída / timeout: es un error transitorio y el aviso SÍ es
        # relevante para la ciudad, así que no se debe perder. Se devuelve a la
        # cola (nack con requeue) tras una pequeña espera para no hacer un
        # bucle de reintentos agresivo. Cuando la API vuelva, se procesará.
        log(ciudad, f"Error al consultar la API ({error.__class__.__name__}). "
                    f"Se reintentará en {ESPERA_REINTENTO_API} s.")
        time.sleep(ESPERA_REINTENTO_API)
        return REENCOLAR

    if respuesta.status_code == 404:
        # El sismo no existe en la API: reintentar no lo hará aparecer.
        log(ciudad, f"La API no conoce el sismo {id_sismo} (404). Mensaje descartado.")
        return ACK
    if respuesta.status_code >= 500:
        # Error del servidor: se trata igual que una API caída (transitorio).
        log(ciudad, f"La API respondió {respuesta.status_code}. "
                    f"Se reintentará en {ESPERA_REINTENTO_API} s.")
        time.sleep(ESPERA_REINTENTO_API)
        return REENCOLAR
    if respuesta.status_code != 200:
        log(ciudad, f"Respuesta inesperada de la API ({respuesta.status_code}). Mensaje descartado.")
        return ACK

    try:
        detalle = respuesta.json()
    except ValueError:
        log(ciudad, f"La API devolvió un detalle no JSON para {id_sismo}. Mensaje descartado.")
        return ACK
    if not isinstance(detalle, dict):
        log(ciudad, f"La API devolvió un detalle con formato inesperado para {id_sismo}. Mensaje descartado.")
        return ACK

    log(ciudad, formatear_detalle(detalle, distancia, nombre))
    return ACK


def crear_callback(ciudad):
    """Callback de pika: procesa y luego confirma (ack manual) o reencola."""
    def callback(ch, method, properties, body):
        accion = procesar_mensaje(body, ciudad)
        if accion == REENCOLAR:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)
    return callback


def obtener_ciudad():
    """Ciudad desde el primer argumento o desde la variable CIUDAD."""
    ciudad = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CIUDAD")
    validas = ", ".join(CIUDADES)
    if not ciudad:
        print(f"Error: indique la ciudad como argumento o en la variable CIUDAD. "
              f"Opciones: {validas}", file=sys.stderr, flush=True)
        sys.exit(1)
    ciudad = ciudad.strip().lower()
    if ciudad not in CIUDADES:
        print(f"Error: ciudad '{ciudad}' no válida. Opciones: {validas}",
              file=sys.stderr, flush=True)
        sys.exit(1)
    return ciudad


def conectar_rabbitmq(ciudad):
    """Conecta a RabbitMQ reintentando (en docker-compose puede no estar listo)."""
    parametros = pika.ConnectionParameters(host=RABBITMQ_HOST)
    for intento in range(1, INTENTOS_CONEXION + 1):
        try:
            return pika.BlockingConnection(parametros)
        except pika.exceptions.AMQPConnectionError:
            log(ciudad, f"RabbitMQ en '{RABBITMQ_HOST}' no disponible "
                        f"(intento {intento}/{INTENTOS_CONEXION}), reintentando en {ESPERA_CONEXION} s...")
            time.sleep(ESPERA_CONEXION)
    log(ciudad, "No fue posible conectar a RabbitMQ. Abortando.")
    sys.exit(1)


def main():
    ciudad = obtener_ciudad()
    cola = f"sismos.{ciudad}"

    conexion = conectar_rabbitmq(ciudad)
    canal = conexion.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
    canal.queue_declare(queue=cola, durable=True)
    canal.queue_bind(exchange=EXCHANGE, queue=cola)
    canal.basic_qos(prefetch_count=1)
    canal.basic_consume(queue=cola, on_message_callback=crear_callback(ciudad), auto_ack=False)

    log(ciudad, f"Esperando sismos en la cola '{cola}' (umbral {UMBRAL_KM:.0f} km, API {API_URL})...")
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        log(ciudad, "Deteniendo subscriber.")
        canal.stop_consuming()
    conexion.close()


if __name__ == "__main__":
    main()
