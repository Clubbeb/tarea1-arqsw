# Tarea 1 INF326 Arquitectura de Software — Notificación de sismos

> **En construcción.** Este README resume el avance del equipo y qué sigue.

## Integrantes

| Nombre | Rol |
|--------|-----|
| _Catalina Yañez_ | _202273010-2_ |
| _Eduardo Pacheco_ | _202273014-5_ |
| _Fernanda Ojeda_ | _202273025-0_ |

## ¿De qué se trata?

Implementar un sistema de notificación de sismos con el patrón **publish-subscribe** y una
adaptación del **modelo pull**:

1. Un **publisher** avisa que ocurrió un sismo, con la **mínima información** posible.
2. **Cinco subscribers** (Arica, Coquimbo, Valparaíso, Concepción y Punta Arenas) reciben el aviso.
3. Cada subscriber calcula la distancia al epicentro. Si está a **menos de 500 km**, le interesa.
4. Solo en ese caso, el subscriber **pide el detalle** del sismo a un **servicio HTTP** (eso es el "pull").

```
[Publisher] ──▶ [RabbitMQ: exchange "sismos" (fanout)] ──▶ [5 subscribers]
                                                              │ ¿< 500 km?
                                                              ▼ sí
                                                   [Servicio HTTP: GET /sismos/{id}]
```

Tecnologías: **RabbitMQ** (mensajería), **FastAPI** (servicio HTTP), **Python** con `pika` y `requests`.

## Avance

- [x] **Etapa 1:** estructura del repo, dependencias y RabbitMQ con Docker Compose
- [ ] **Etapa 2:** datos de sismos de ejemplo + servicio HTTP (FastAPI)
- [ ] **Etapa 3:** publisher
- [ ] **Etapa 4:** subscribers (primero uno, después los 5)
- [ ] **Etapa 5:** dockerizar todo en `docker-compose.yml`
- [ ] **Etapa 6:** README final y archivo de discusión (trade-offs + Back of the Envelope)

## Estructura actual

```
.
├── api/                # (etapa 2) servicio HTTP "Datos Sismos"
├── publisher/          # (etapa 3) notificador de sismos
├── subscriber/         # (etapa 4) subscribers por ciudad
├── data/               # (etapa 2) sismos de ejemplo
├── docker-compose.yml  # por ahora solo levanta RabbitMQ
├── requirements.txt    # fastapi, uvicorn, pika, requests
├── .gitignore
└── README.md
```

## Cómo ponerse al día

### Prerrequisitos

- **Python 3.10 o superior** → revisa con `python --version`
- **Docker Desktop**, abierto al trabajar → revisa con `docker --version`
- **Git**

### Pasos

```bash
# 1. Clonar el repo (o "git pull" si ya lo tienes)
git clone <url-del-repo>
cd <carpeta-del-repo>

# 2. Crear y activar el entorno virtual
python -m venv .venv
source .venv/bin/activate        # Mac/Linux
.venv\Scripts\activate           # Windows

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Levantar RabbitMQ
docker compose up
```

### Verificar que funciona

Abre http://localhost:15672 e ingresa con usuario `guest` y clave `guest`.
Si ves el panel de RabbitMQ, estás al día. 🎉

En la pestaña **Exchanges** hay 7 exchanges que RabbitMQ crea por defecto; no los usamos.
El nuestro, `sismos`, aparecerá cuando el código lo declare (etapa 3).

Para detener RabbitMQ: `Ctrl + C` en la terminal, o `docker compose down`.

## Conceptos clave

- **Exchange:** el "buzón" de RabbitMQ donde el publisher deja los mensajes. RabbitMQ los reparte
  a las colas según el tipo de exchange.
- **Fanout:** tipo de exchange que entrega **una copia a todas** las colas conectadas. Lo usamos
  porque los 5 subscribers deben enterarse de cada sismo y cada uno decide si le interesa.
- **Cola:** cada subscriber tiene la suya; ahí esperan los mensajes hasta que los procesa.
- **Modelo pull:** el evento solo avisa; quien necesita más información la va a buscar
  (aquí, al servicio HTTP).

## Forma de trabajo

- Hacer `git pull` antes de empezar a trabajar.
- Commits pequeños, con mensajes que digan qué etapa o parte se avanzó.
- No subir `.venv/` ni `__pycache__/` (ya están en `.gitignore`).
- Si algo no funciona, anotar el error completo y avisar al grupo.

## Recursos

- Enunciado de la Tarea 1 (Aula)
- Informes de sismos reales para la estructura de datos: https://www.sismologia.cl
- Panel de RabbitMQ (con RabbitMQ corriendo): http://localhost:15672