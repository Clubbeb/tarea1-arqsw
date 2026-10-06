"""API "Datos Sismos": entrega el detalle de cada sismo (parte pull del sistema).

Los subscribers reciben por RabbitMQ solo el id y las coordenadas del sismo;
si les interesa, piden aquí el detalle completo con GET /sismos/{id}.

Ejecutar desde la raíz del repo:  uvicorn api.main:app --port 8000
"""
import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Ruta del JSON: por defecto data/sismos.json (relativa a este archivo, no al cwd).
# Se puede sobreescribir con la variable de entorno DATA_PATH.
RUTA_POR_DEFECTO = Path(__file__).resolve().parent.parent / "data" / "sismos.json"
DATA_PATH = Path(os.getenv("DATA_PATH", RUTA_POR_DEFECTO))


class Sismo(BaseModel):
    """Detalle de un sismo, inspirado en los informes de sismologia.cl (CSN)."""

    id: str  # formato AAAA-MM-DD-HHMMSS (hora UTC)
    fecha_local: str
    fecha_utc: str
    latitud: float
    longitud: float
    profundidad_km: float
    magnitud: float
    tipo_magnitud: str
    referencia_geografica: str
    agencia: str


def cargar_sismos(ruta: Path) -> dict[str, Sismo]:
    """Lee el JSON una sola vez y lo deja en memoria, indexado por id."""
    with open(ruta, encoding="utf-8") as archivo:
        lista = json.load(archivo)
    return {s["id"]: Sismo(**s) for s in lista}


# Datos en memoria (se cargan al iniciar la aplicación)
SISMOS = cargar_sismos(DATA_PATH)

app = FastAPI(
    title="Datos Sismos",
    description="Detalle de sismos para los subscribers del sistema de notificación.",
)


@app.get("/health")
def health():
    """Chequeo simple de que la API está arriba."""
    return {"estado": "ok", "sismos_cargados": len(SISMOS)}


@app.get("/sismos", response_model=list[Sismo])
def listar_sismos():
    """Lista todos los sismos disponibles."""
    return list(SISMOS.values())


@app.get("/sismos/{id_sismo}", response_model=Sismo)
def obtener_sismo(id_sismo: str):
    """Detalle de un sismo; 404 si el id no existe."""
    sismo = SISMOS.get(id_sismo)
    if sismo is None:
        raise HTTPException(status_code=404, detail="Sismo no encontrado")
    return sismo
