# Tarea 1 INF326 Arquitectura de Software — Notificación de sismos

Sistema de notificación de sismos con el patrón **publish-subscribe** (RabbitMQ) y una adaptación
del **modelo pull** (servicio HTTP con FastAPI).

## Integrantes

| Nombre | Rol |
|--------|-----|
| Catalina Yañez | 202273010-2 |
| Eduardo Pacheco | 202273014-5 |
| Fernanda Ojeda | 202273025-0 |

## Arquitectura

```
[Publisher] ──evento {id, latitud, longitud}──▶ [RabbitMQ: exchange "sismos" (fanout)]
                                                   │ una cola durable por ciudad
                                                   ▼
                    [subscriber Arica | Coquimbo | Valparaíso | Concepción | Punta Arenas]
                                                   │ ¿distancia al epicentro < 500 km?
                                                   ▼ sí
                                     [API "Datos Sismos": GET /sismos/{id}] ──▶ detalle
```

1. El **publisher** avisa que ocurrió un sismo con la mínima información: `id` y coordenadas.
2. Los **5 subscribers** reciben todos los avisos y cada uno calcula la distancia al epicentro.
3. Solo si el sismo está a **menos de 500 km**, el subscriber **pide el detalle** a la API (pull).

| Carpeta / archivo | Contenido |
|-------------------|-----------|
| `api/main.py` | Servicio HTTP "Datos Sismos" (FastAPI) |
| `publisher/publisher.py` | Notificador de sismos |
| `subscriber/subscriber.py` | Subscriber (un solo script, parametrizado por ciudad) |
| `subscriber/ciudades.py` | Coordenadas de las 5 ciudades |
| `data/sismos.json` | Sismos de ejemplo (en memoria, sin base de datos) |
| `docker-compose.yml`, `Dockerfile` | Despliegue de todo el sistema |
| `DISCUSION.md` | Discusión de trade-offs y "Back of the Envelope" |

## Instrucciones de ejecución

### Prerrequisitos

- **Docker** con Docker Compose v2 (por ejemplo, Docker Desktop abierto). Revisar con `docker compose version`.
- Opcional, solo para ejecutar sin Docker: **Python 3.10+** y `pip install -r requirements.txt`.

### Con Docker (recomendado)

```bash
# 1. Levantar RabbitMQ, la API y los 5 subscribers
docker compose up --build -d

# 2. Esperar a que los 5 subscribers estén listos ("Esperando sismos en la cola ...")
docker compose logs -f subscriber-arica subscriber-coquimbo subscriber-valparaiso subscriber-concepcion subscriber-punta-arenas

# 3. En otra terminal, publicar sismos
docker compose run --rm publisher                     # publica todos (uno cada 3 s)
docker compose run --rm publisher 2019-01-20-013252   # publica solo uno

# 4. Detener todo (-v borra además las colas y mensajes guardados de RabbitMQ)
docker compose down
```

- Panel de RabbitMQ: http://localhost:15672 (usuario `guest`, clave `guest`).
- API: http://localhost:8000/sismos y documentación en http://localhost:8000/docs.
- Si se modifica el código, volver a construir con `docker compose up --build -d`.

**Importante:** hay que publicar después de que los subscribers hayan arrancado al menos una vez, porque
son ellos los que crean sus colas. Si se publica cuando no hay ninguna cola, el exchange fanout descarta
el aviso; el publisher lo detecta y lo informa ("evento no entregado").

### Sismos de ejemplo y resultado esperado

| id | Sismo | Ciudades que piden el detalle (< 500 km) |
|----|-------|-------------------------------------------|
| `2017-04-24-213830` | Valparaíso 2017, 6.9 Mw | Coquimbo (350 km), Valparaíso (40 km), Concepción (431 km) |
| `2014-04-01-234647` | Iquique 2014, 8.2 Mw | Arica (136 km) |
| `2011-01-02-202017` | Araucanía 2011, 7.2 Mw | Concepción (172 km) |
| `1996-08-31-185920` | Magallanes, 5.0 Ml | Punta Arenas (100 km) |
| `2007-04-21-175346` | Aysén 2007, 6.2 Mw | ninguna |
| `2019-01-20-013252` | Coquimbo 2019, 6.7 Mw | Coquimbo (10 km), Valparaíso (334 km) |

### Sin Docker (opcional)

Con RabbitMQ corriendo en `localhost` (por ejemplo `docker compose up -d rabbitmq`), desde la raíz del repo
y cada comando en una terminal distinta:

```bash
uvicorn api.main:app --port 8000
python subscriber/subscriber.py arica          # repetir con: coquimbo, valparaiso, concepcion, punta_arenas
python publisher/publisher.py                  # o: python publisher/publisher.py <id>
```

Variables de entorno: `RABBITMQ_HOST` (por defecto `localhost`), `API_URL` (por defecto
`http://localhost:8000`), `CIUDAD` (alternativa al argumento del subscriber), `PAUSA_SEGUNDOS`
(pausa del publisher entre sismos, por defecto 3) y `DATA_PATH` (ruta del JSON).

## Consideraciones de implementación

**Diseño de mensajes**

- **Evento (RabbitMQ):** `{"id": "2019-01-20-013252", "latitud": -30.04, "longitud": -71.382}`.
  Las coordenadas son lo único que necesita cada subscriber para decidir si le interesa, y el `id` es lo que
  necesita para pedir el detalle. No se envía nada más (ni magnitud ni profundidad), porque el criterio de
  interés es solo la distancia.
- **Detalle (HTTP, `GET /sismos/{id}`):** sigue la estructura de los informes de sismologia.cl: `id`,
  `fecha_local`, `fecha_utc`, `latitud`, `longitud`, `profundidad_km`, `magnitud`, `tipo_magnitud`,
  `referencia_geografica` y `agencia`. Si el id no existe, responde 404.
- La justificación campo por campo está en [DISCUSION.md](DISCUSION.md).

**Mensajería**

- Exchange `sismos` de tipo **fanout**, porque los 5 subscribers deben enterarse de cada sismo y cada uno
  decide por sí mismo.
- Cada ciudad tiene su propia **cola durable con nombre** (`sismos.<ciudad>`), los mensajes son
  **persistentes** y RabbitMQ guarda sus datos en un volumen. Así, si un subscriber se cae, sus avisos lo
  esperan en la cola.
- El publisher usa **publisher confirms** y `mandatory=True` para saber si el aviso llegó al menos a una cola.
- Los subscribers usan **ack manual** y `prefetch_count=1`:
  - un sismo lejano, un mensaje mal formado o un 404 de la API se confirman y se descartan con un log;
  - si la API está caída, no responde o devuelve un 5xx, el subscriber espera 5 s y **reencola** el aviso,
    para no perder un sismo que le interesa. Mientras la API siga caída, esto retrasa los siguientes avisos
    de esa ciudad (ver DISCUSION.md).

**Otros**

- **Distancia:** fórmula de **haversine** con un radio terrestre de 6371.0088 km (sin dependencias extra).
  El umbral es estricto: < 500 km.
- **Un solo script de subscriber**, parametrizado por la ciudad, en lugar de 5 copias del código.
  Docker Compose levanta 5 contenedores con la misma imagen y distinto valor de `CIUDAD`.
- **Datos en memoria:** la API carga `data/sismos.json` al iniciar; no hay base de datos ni endpoint POST,
  tal como permite el enunciado.
  - Los sismos se basan en eventos reales de Chile (fecha, coordenadas, profundidad y magnitud del catálogo
    de USGS) y la redacción de la referencia geográfica imita al CSN.
  - El sismo de Magallanes de 1996 es ilustrativo: está para tener un caso que solo le interese a Punta Arenas.
- **Arranque y fallas:**
  - Publisher y subscribers reintentan la conexión a RabbitMQ al iniciar (10 intentos cada 3 s), y Compose
    espera a que RabbitMQ esté sano antes de levantarlos.
  - Si RabbitMQ se cae durante la ejecución, el subscriber termina y Docker lo reinicia (`restart: on-failure`).
    Sin Docker, hay que volver a lanzarlo a mano.

## Discusión

La discusión de trade-offs y el uso del "Back of the Envelope" están en [DISCUSION.md](DISCUSION.md).
