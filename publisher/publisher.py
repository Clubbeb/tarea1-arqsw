"""Notificador de sismos (publisher).

Lee el detalle de los sismos desde data/sismos.json y publica en el exchange
fanout `sismos` un evento con la información MÍNIMA:

    {"id": "...", "latitud": ..., "longitud": ...}

El detalle completo NO viaja por RabbitMQ: cada subscriber interesado lo pide
a la API "Datos Sismos" (modelo pull).

Uso (desde la raíz del repo):
    python publisher/publisher.py <id_sismo>   # publica un sismo
    python publisher/publisher.py              # publica todos, con pausa entre ellos
"""

import json
import os
import sys
import time
from pathlib import Path

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
EXCHANGE = "sismos"

# Ruta por defecto resuelta desde este archivo (no desde el directorio actual).
RAIZ_REPO = Path(__file__).resolve().parent.parent
DATA_PATH = Path(os.getenv("DATA_PATH", RAIZ_REPO / "data" / "sismos.json"))

# Segundos de pausa entre sismos cuando se publican todos.
PAUSA_SEGUNDOS = float(os.getenv("PAUSA_SEGUNDOS", "3"))

INTENTOS_CONEXION = 10
ESPERA_CONEXION = 3  # segundos


def log(mensaje):
    print(f"[publisher] {mensaje}", flush=True)


def conectar_rabbitmq():
    """Conecta a RabbitMQ reintentando (en docker-compose puede no estar listo)."""
    parametros = pika.ConnectionParameters(host=RABBITMQ_HOST)
    for intento in range(1, INTENTOS_CONEXION + 1):
        try:
            return pika.BlockingConnection(parametros)
        except pika.exceptions.AMQPConnectionError:
            log(f"RabbitMQ en '{RABBITMQ_HOST}' no disponible "
                f"(intento {intento}/{INTENTOS_CONEXION}), reintentando en {ESPERA_CONEXION} s...")
            time.sleep(ESPERA_CONEXION)
    log("No fue posible conectar a RabbitMQ. Abortando.")
    sys.exit(1)


def cargar_sismos():
    with open(DATA_PATH, encoding="utf-8") as archivo:
        return json.load(archivo)


def construir_evento(sismo):
    """Evento mínimo: solo lo necesario para decidir interés y pedir el detalle."""
    return {
        "id": sismo["id"],
        "latitud": sismo["latitud"],
        "longitud": sismo["longitud"],
    }


def publicar(canal, evento):
    """Publica un evento. Retorna True si RabbitMQ confirmó que quedó encolado.

    El canal está en modo "publisher confirms" y se publica con mandatory=True:
    si ninguna cola está enlazada al exchange, RabbitMQ devuelve el mensaje
    (UnroutableError) en vez de descartarlo en silencio.
    """
    try:
        canal.basic_publish(
            exchange=EXCHANGE,
            routing_key="",
            body=json.dumps(evento),
            properties=pika.BasicProperties(
                delivery_mode=2,  # mensaje persistente
                content_type="application/json",
            ),
            mandatory=True,
        )
    except pika.exceptions.UnroutableError:
        log(f"ningún subscriber tiene su cola enlazada: evento {evento['id']} no entregado")
        return False
    except pika.exceptions.NackError:
        log(f"RabbitMQ rechazó (nack) el evento {evento['id']}: no entregado")
        return False
    log(f"Sismo publicado: id={evento['id']} "
        f"lat={evento['latitud']} lon={evento['longitud']}")
    return True


def main():
    sismos = cargar_sismos()

    if len(sys.argv) > 1:
        id_buscado = sys.argv[1]
        seleccion = [s for s in sismos if s["id"] == id_buscado]
        if not seleccion:
            ids = ", ".join(s["id"] for s in sismos)
            log(f"No existe un sismo con id '{id_buscado}'. Ids disponibles: {ids}")
            sys.exit(1)
    else:
        seleccion = sismos

    conexion = conectar_rabbitmq()
    canal = conexion.channel()
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="fanout", durable=True)
    canal.confirm_delivery()  # el broker confirma cada publicación

    no_entregados = 0
    for i, sismo in enumerate(seleccion):
        if i > 0:
            time.sleep(PAUSA_SEGUNDOS)
        if not publicar(canal, construir_evento(sismo)):
            no_entregados += 1

    entregados = len(seleccion) - no_entregados
    log(f"Listo: {entregados}/{len(seleccion)} sismo(s) entregado(s) al exchange '{EXCHANGE}'.")
    conexion.close()
    if no_entregados:
        sys.exit(1)


if __name__ == "__main__":
    main()
