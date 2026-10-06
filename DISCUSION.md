# Discusión: diseño de mensajes, trade-offs y "Back of the Envelope"

Tarea 1 — INF326 Arquitectura de Software (USM)

El sistema implementa el patrón **publish-subscribe** con una variante del **modelo pull**. Un *Notificador de Sismos* publica un evento mínimo en un exchange `fanout` de RabbitMQ. Cinco *subscribers* (Arica, Coquimbo, Valparaíso, Concepción y Punta Arenas) reciben ese evento y calculan la distancia geodésica (haversine) entre el epicentro y su ciudad. Solo si la distancia es menor a 500 km piden el detalle al servicio HTTP *Datos Sismos* (FastAPI) con `GET /sismos/{id}`.

---

## 1. Diseño de mensajes

### 1.1 Mensaje 1: evento sismo (RabbitMQ, *push*)

```json
{"id": "2019-01-20-013252", "latitud": -30.040, "longitud": -71.382}
```

La regla de diseño fue incluir **solo los campos que el subscriber necesita para decidir si le interesa el evento y, si le interesa, para pedir el detalle**. Nada más.

| Campo | ¿Está? | Justificación |
|---|---|---|
| `latitud`, `longitud` | Sí | Son las entradas del criterio de interés (distancia < 500 km). Sin ellas el subscriber tendría que pedir el detalle siempre, lo que anula el sentido del modelo pull. |
| `id` | Sí | Es la clave para pedir el detalle (`GET /sismos/{id}`). Sirve además para correlacionar en los logs y como posible clave de idempotencia. La combinación de *publisher confirms*, mensajes persistentes, colas durables y ack manual da entrega *at-least-once*: no se pierden avisos, pero uno puede llegar dos veces (por ejemplo, si el subscriber muere después de procesar y antes del ack). El formato `AAAA-MM-DD-HHMMSS` (UTC) se puede leer a simple vista y ordenar, aunque el subscriber no necesita interpretarlo. |
| `magnitud` | **No** | El enunciado define el interés **solo por distancia**. Ningún subscriber usa la magnitud para decidir, así que en el evento sería "información extra no usable". El subscriber interesado la obtiene en el detalle. |
| `profundidad_km` | No | No interviene en el criterio. Está en el detalle. |
| `fecha_local` / `fecha_utc` | No | No interviene en el criterio. La fecha ya va implícita en el `id`, aunque no se usa para decidir. |
| `referencia_geografica`, `agencia`, `tipo_magnitud` | No | Son datos descriptivos para quien consume el informe y no sirven para filtrar. |

**Alternativa / extensión.** Si el criterio de interés cambiara, por ejemplo a "interesa si está a menos de 500 km **y** la magnitud es ≥ 5,0" o a un radio que crezca con la magnitud, entonces la magnitud sí debería ir en el evento. Lo mismo aplicaría a la profundidad si el criterio usara la distancia hipocentral en vez de la epicentral. El contenido del evento depende del **criterio de filtrado**, no de lo que el publisher sepa. Esto conecta con la sección 2.4: si el filtrado se moviera al broker (topic/headers exchange), los atributos usados para enrutar irían en la *routing key* o en los *headers*, no necesariamente en el cuerpo.

### 1.2 Mensaje 2: detalle del sismo (HTTP, *pull*)

`GET /sismos/{id}` → `200`:

```json
{
  "id": "2019-01-20-013252",
  "fecha_local": "2019-01-19T22:32:52-03:00",
  "fecha_utc": "2019-01-20T01:32:52Z",
  "latitud": -30.040,
  "longitud": -71.382,
  "profundidad_km": 63.0,
  "magnitud": 6.7,
  "tipo_magnitud": "Mw",
  "referencia_geografica": "10 km al SSO de Coquimbo",
  "agencia": "USGS"
}
```

o `404 {"detail": "Sismo no encontrado"}`.

La estructura sigue el estilo de los informes que publica el Centro Sismológico Nacional (sismologia.cl). Los valores de ejemplo del dataset (`data/sismos.json`) se tomaron del catálogo de USGS, por eso `agencia` dice `"USGS"`. Los informes del CSN traen hora local y UTC, latitud, longitud, profundidad, magnitud con su escala y una referencia geográfica del tipo "X km al <dirección> de <localidad>".

- **`fecha_local` y `fecha_utc`**: la UTC es la referencia técnica (catálogos, correlación entre agencias). La local es la que entiende la población. Ambas usan ISO 8601 con zona explícita para evitar ambigüedades por el cambio de horario de Chile.
- **`profundidad_km`**: a igual magnitud, un sismo superficial se siente distinto que uno intermedio. Es información útil para quien recibe el informe.
- **`magnitud` + `tipo_magnitud`**: un número de magnitud sin su escala (Ml, Mw, etc.) es ambiguo, porque las agencias informan escalas distintas y las van actualizando.
- **`referencia_geografica`**: da contexto que se lee directo, sin calcular nada a partir de las coordenadas.
- **`agencia`**: indica la procedencia del dato (en el dataset, USGS). Sirve para distinguir fuentes si se integraran varias (por ejemplo, CSN y USGS), que pueden informar valores distintos para el mismo sismo.
- **`latitud`/`longitud`** se repiten para que el recurso de detalle sea **autocontenido**: quien lo consulte sin haber visto el evento obtiene el informe completo.

No se agregaron campos de intensidad percibida (Mercalli), mecanismo focal ni datos de estaciones. Ningún subscriber de la tarea los usa y aumentarían el payload sin beneficio.

### 1.3 Qué gana y qué pierde el modelo pull

**Gana:**
- **Mensajes pequeños** en el broker. El costo de difundir a N subscribers (N copias en fanout) se paga sobre un payload de unas pocas decenas de bytes y no sobre el informe completo.
- **Fuente de verdad única.** El detalle vive solo en *Datos Sismos*. Si el CSN revisa la magnitud o la ubicación (las cifras preliminares se corrigen a menudo), los subscribers que piden el detalle obtienen la versión vigente y no una copia congelada en un mensaje.
- **Detalle solo para quien lo necesita.** Un sismo frente a Arica no genera tráfico de detalle desde Punta Arenas.
- **Evolución independiente.** Se pueden agregar campos al detalle sin cambiar el contrato del evento ni redesplegar el publisher.

**Pierde:**
- **Un round-trip HTTP adicional** antes de tener la información completa (latencia).
- **Una dependencia síncrona más.** Si la API no está disponible, el subscriber sabe que hubo un sismo cerca, pero no sabe cuán grande fue.
- **Posible inconsistencia temporal** si el evento llega antes de que el detalle exista en la API (ver 2.7).
- **Picos de carga en la API** correlacionados con los eventos de mayor interés (ver 2.3).

---

## 2. Trade-offs de la arquitectura implementada

### 2.1 Desacoplamiento vs. complejidad operativa

El broker desacopla al publisher de los subscribers en **espacio** (el publisher no conoce cuántos ni cuáles hay), en **tiempo** (un subscriber caído recibe los mensajes al volver, gracias a colas durables, siempre que haya declarado su cola al menos una vez) y en **sincronización** (el publisher no espera a nadie). Agregar una sexta ciudad no exige tocar el publisher. A cambio, hay que operar un componente más (RabbitMQ), con su configuración, monitoreo, credenciales y reintentos de conexión al arranque. El sistema también usa dos protocolos de comunicación (AMQP hacia el broker y HTTP hacia la API) en vez de uno, y cada uno tiene sus propios modos de falla.

**¿Es de consideración en este dominio?** Sí, y el desacoplamiento lo compensa. Los consumidores de información sísmica son heterogéneos y cambian en el tiempo: municipios, servicios de emergencia, medios, aplicaciones móviles. Que el publisher no tenga que conocerlos es un requisito real. El costo operativo de un broker es moderado y conocido. En la escala de la tarea (5 subscribers) la complejidad es apenas visible.

### 2.2 Latencia extra del pull vs. alerta temprana

Para tener la información completa, el subscriber interesado necesita el mensaje del broker más una petición HTTP. En una red local eso suma milisegundos, pero bajo carga o con la API degradada puede llegar a segundos.

**¿Es de consideración en este dominio?** Depende de qué sistema se esté modelando, y conviene distinguir dos casos:

- **Notificación informativa de un sismo ya ocurrido y localizado** (lo que hace este sistema, y lo que publica el CSN): el informe preliminar se publica minutos después del evento. Unos cientos de milisegundos más por el pull **no son relevantes**.
- **Alerta temprana de sismo o tsunami real**: los requisitos son de segundos. Una alerta temprana sísmica (tipo ShakeAlert) intenta avisar *antes* de que lleguen las ondas S, con márgenes de pocos segundos a decenas de segundos según la distancia. Ahí un round-trip síncrono a un servicio que puede estar saturado **sí es crítico**. Esos sistemas empujan la información necesaria para actuar (ubicación, magnitud estimada, nivel de alerta) en el mismo mensaje y no dependen de una consulta posterior.

La arquitectura implementada es adecuada para el primer caso y no lo sería para el segundo. Para acercarla al segundo se podría aplicar *event-carried state transfer*: incluir en el evento los pocos campos que permiten actuar (por ejemplo, la magnitud) y dejar el pull solo para el informe completo.

### 2.3 El servicio HTTP como punto único de falla y cuello de botella

*Datos Sismos* es una sola instancia con datos en memoria. Si cae, ningún subscriber obtiene detalles. Además, la carga que recibe **está correlacionada con los eventos importantes**. Un terremoto grande interesa a casi todos los subscribers al mismo tiempo, y todos piden el mismo recurso en el mismo instante (efecto *thundering herd*). Después del sismo principal vienen las **réplicas**: decenas o cientos en las primeras horas, todas en la misma zona, lo que mantiene a la API bajo carga sostenida justo cuando más se necesita.

**¿Es de consideración en este dominio?** Sí, y es probablemente el trade-off más importante. En sismología el peor caso de carga coincide con el momento de mayor demanda de información. Con 5 subscribers el efecto no se nota, pero el diseño debe mostrar cómo mitigarlo:
- Respuestas **cacheables** (los informes casi no cambian una vez publicados): cabeceras HTTP de caché, un CDN o un proxy inverso delante de la API.
- **Varias réplicas** de la API detrás de un balanceador. Como es de solo lectura, escala horizontalmente sin mayor dificultad.
- En los subscribers, **reintentos con backoff exponencial y jitter** para no sincronizar los reintentos de todos.
- **Degradación aceptable**: si la API falla, el subscriber igual puede informar "sismo a X km" con los datos del evento.

**Política de errores implementada en el subscriber.** Si la API responde **404**, el subscriber hace *ack* y descarta el aviso, porque reintentar no hará aparecer el sismo. Lo mismo ocurre con un mensaje mal formado. Si la API está **caída, no responde dentro del timeout (5 s) o responde 5xx**, el subscriber espera 5 s y hace `nack(requeue=True)`: el aviso vuelve a la cola y se reintenta indefinidamente hasta que la API responda. Con `prefetch_count=1` esto produce **head-of-line blocking**: mientras la API no responda, la cola de esa ciudad queda detenida en ese aviso y los siguientes esperan detrás, aunque no sean de interés. Las colas de las demás ciudades no se ven afectadas. El trade-off es **no perder un aviso relevante** a cambio de **retrasar los siguientes**. Una API que devuelva 5xx de forma permanente para un id concreto bloquearía la cola indefinidamente (un *poison message*). Hay alternativas que no se implementaron: un límite de reintentos con *dead-letter exchange* para apartar el aviso problemático, `x-delivery-limit` en *quorum queues* (RabbitMQ cuenta las reentregas y desvía el mensaje al superar el límite), o backoff exponencial en vez de una espera fija.

¿Importa en el dominio? Sí, pero de forma acotada. Durante una secuencia de réplicas llegan muchos avisos seguidos a la misma ciudad y, si la API está saturada justo en ese momento (el caso de 2.3), el bloqueo retrasa todos los avisos de la ciudad. Para notificación informativa se tolera un retraso de segundos o minutos. Lo que no es aceptable es un bloqueo indefinido, así que en un sistema real un límite de reintentos con dead-letter sería imprescindible.

### 2.4 Fanout (filtrado en el subscriber) vs. topic/headers exchange (filtrado en el broker)

Con `fanout`, **todos los subscribers reciben todos los eventos** y cada uno calcula la distancia. La alternativa es enrutar por región: el publisher etiqueta el evento (por ejemplo, con una celda geográfica o región como *routing key* en un `topic` exchange, o atributos en un `headers` exchange) y cada subscriber se suscribe solo a las regiones que caen dentro de su radio.

**¿Es de consideración en este dominio?** Con 5 subscribers, no: el fanout es lo razonable. Genera 5 copias de un mensaje de pocas decenas de bytes y 5 cálculos de haversine, un costo despreciable. Además mantiene la lógica de interés en el subscriber, que es quien la conoce, y el publisher no necesita saber nada de geografía. Con miles de subscribers (una app con usuarios suscritos por ubicación, por ejemplo) sí importa. El fanout produce N copias por evento aunque solo una fracción pequeña esté interesada, y el tráfico desperdiciado crece linealmente con N. En ese caso conviene filtrar en el broker, aunque eso traslada al publisher el conocimiento de la discretización geográfica (celdas, geohash) y el filtro por celda es aproximado, así que el subscriber igual debe verificar la distancia exacta.

### 2.5 Colas durables + mensajes persistentes + ack manual vs. avisos viejos

Cada subscriber tiene una cola **durable con nombre** (`sismos.<ciudad>`). El publisher marca los mensajes como **persistentes** y el subscriber confirma (**ack manual**) solo después de procesar. Además, el publisher usa *publisher confirms* y `mandatory=True`, y en Docker el broker tiene un volumen persistente. Si un subscriber se cae, sus avisos esperan en la cola y no se pierden. Si muere en mitad del procesamiento, el mensaje se reentrega.

Hay una salvedad: los avisos solo esperan si el subscriber **declaró su cola al menos una vez**. Un exchange fanout sin colas enlazadas descarta el mensaje. Por ejemplo, si se publica antes del primer arranque de los subscribers, el aviso se pierde. Con `mandatory=True` y confirms, el broker devuelve ese mensaje y el publisher informa "evento no entregado", así que la pérdida al menos queda registrada. La cola durable cubre las caídas *posteriores* al primer arranque, no la ausencia inicial del subscriber.

El costo es que, al reconectarse tras una caída larga, el subscriber procesa una ráfaga de avisos acumulados, algunos de hace horas. La entrega *at-least-once* también puede producir **duplicados**. Se podrían manejar con el `id` como clave de idempotencia (recordar los ids ya procesados), pero **no se implementó**: un duplicado solo genera un GET y un log repetidos, lo que es inocuo porque la operación es de solo lectura. Si el subscriber tuviera efectos externos (enviar SMS, activar sirenas), la deduplicación sí sería necesaria.

**¿Es de consideración en este dominio?** Sí. Hay que preguntarse si sirve un aviso de hace 2 horas. Para un registro o una bitácora municipal, sí sirve. Para una alerta a la población, no, e incluso puede ser contraproducente, porque podría hacer creer que hay un sismo en curso. La mitigación es un **TTL por mensaje o por cola** (`x-message-ttl`), opcionalmente con una *dead-letter exchange* para archivar los vencidos, o bien que el subscriber descarte eventos cuya fecha supere un umbral. En la tarea se prioriza no perder avisos. El TTL quedaría como parámetro que depende del uso que se le dé al aviso.

### 2.6 El broker como punto único de falla

Un solo nodo de RabbitMQ es un punto único de falla. Si cae, no se difunden eventos. Los mensajes ya encolados no se pierden: son persistentes, están en colas durables y el docker-compose monta un volumen persistente para los datos del broker. Gracias a los *publisher confirms*, el publisher sabe si el broker aceptó cada evento.

**¿Es de consideración en este dominio?** Para la tarea, no. Para un sistema real de información sísmica, sí, porque la disponibilidad se exige justamente durante un terremoto, cuando la infraestructura (energía, red) puede estar dañada. La mitigación estándar es un **cluster de RabbitMQ con *quorum queues*** (replicación basada en Raft), con nodos en distintas zonas o regiones. Los *publisher confirms* que ya usa el publisher siguen siendo necesarios en ese escenario.

### 2.7 Consistencia entre el evento y el detalle

El evento y el detalle están en sistemas distintos y no hay una transacción que los una. Si el publisher difunde el evento antes de que el detalle esté registrado en la API, el subscriber recibe un **404**. También puede haber versiones distintas: el subscriber que pide el detalle tarde ve una magnitud revisada.

**¿Es de consideración en este dominio?** Moderadamente. El enunciado asume que el detalle ya está en el servicio HTTP, así que en la tarea no ocurre. En un sistema real la regla sería "registrar primero, publicar después" (o el patrón *transactional outbox*), y el subscriber trataría un 404 como un error transitorio: reintentar unas pocas veces con backoff antes de rendirse. La implementación actual hace lo contrario, porque toma el 404 como definitivo y descarta el aviso (ver 2.3). Eso es correcto bajo el supuesto del enunciado, pero perdería avisos si existiera esta carrera. Que el informe se revise con el tiempo es una ventaja del pull (siempre se obtiene la versión vigente), pero se pierde la garantía de que todos los subscribers vean lo mismo.

### 2.8 Seguridad y autenticación

No hay autenticación en el broker (credenciales por defecto) ni en la API, y tampoco cifrado (TLS).

**¿Es de consideración en este dominio?** Queda **fuera del alcance** de la tarea, pero en un sistema real sería crítica. Un actor que pueda **publicar** eventos falsos podría causar pánico o evacuaciones innecesarias. La integridad y la autenticidad del publisher importan más que la confidencialidad, ya que la información sísmica es pública. Bastarían permisos por usuario en RabbitMQ (solo el notificador puede publicar en `sismos`), TLS y, en la API, acceso de solo lectura con limitación de tasa.

---

## 3. "Back of the Envelope": cómo estimar el uso del sistema

El objetivo no es calcular cifras, sino mostrar **qué datos del fenómeno se necesitan, dónde obtenerlos y cómo combinarlos** para dimensionar el broker y la API y verificar si la arquitectura elegida es suficiente.

### 3.1 Tasa de eventos

1. **Frecuencia de sismos en Chile.** Del catálogo del CSN (sismologia.cl) o del catálogo de USGS (filtrado por la región de Chile) se obtiene el número de sismos localizados por día en un período representativo de varios años. Chile es de los países más sísmicos del mundo, así que esta tasa no es despreciable. El dato concreto debe sacarse del catálogo y no suponerse.
2. **Distribución por magnitud.** La **ley de Gutenberg-Richter**, `log10 N(≥M) = a − b·M`, describe cuántos sismos superan cada magnitud. Ajustando `a` y `b` con el catálogo (`b` suele estar cerca de 1) se puede estimar la tasa de eventos por sobre la magnitud mínima que el sistema notifique. Esto importa porque el número de eventos depende mucho de ese umbral: bajar una unidad de magnitud multiplica los eventos por aproximadamente 10^b.
3. **Tasa promedio:** `λ_prom = (sismos en el catálogo con M ≥ M_min) / (segundos del período)`.

### 3.2 Peor caso: terremoto grande y réplicas

El dimensionamiento lo define el pico, no el promedio. Para medirlo:
- Tomar del catálogo terremotos de referencia, como el **27F (Maule, 2010)** o **Illapel (2015)**, y contar los eventos por minuto u hora en las primeras horas después del sismo principal.
- Modelar el decaimiento de las réplicas con la **ley de Omori-Utsu**, `n(t) = K / (c + t)^p`, ajustando `K`, `c` y `p` a esos casos.
- Definir `λ_pico` como la tasa máxima en una ventana corta (por ejemplo, un minuto) durante ese periodo, y el factor de ráfaga `F = λ_pico / λ_prom`.

### 3.3 Fracción de eventos de interés por subscriber

Para cada ciudad `i`, la fracción `f_i` es la proporción de sismos del catálogo cuyo epicentro está a menos de 500 km de esa ciudad: se cuentan los epicentros dentro del círculo de 500 km con la misma función haversine del sistema. La distribución geográfica de la sismicidad chilena es muy desigual (concentrada a lo largo de la zona de subducción, más baja en el extremo sur), así que `f_i` variará bastante entre, por ejemplo, Valparaíso y Punta Arenas. Para el peor caso conviene calcular `f_i` también solo sobre la secuencia de réplicas de cada terremoto de referencia: en esas secuencias, una ciudad cercana puede tener `f_i` cercano a 1.

### 3.4 Tamaño de los mensajes y ancho de banda

- `S_ev`: tamaño en bytes del evento serializado (se mide con `len(json.dumps(evento).encode())`), más la sobrecarga del protocolo AMQP por mensaje.
- `S_det`: tamaño de la respuesta HTTP de detalle, incluyendo cabeceras.

Fórmulas:

```
Mensajes entregados por el broker/s      M_broker ≈ λ × N              (fanout: N copias por evento)
Ancho de banda de salida del broker      B_broker ≈ λ × N × S_ev
Peticiones a la API/s                    RPS_api  ≈ λ × Σ_i f_i  ≈ λ × f̄ × N
Ancho de banda de salida de la API       B_api    ≈ RPS_api × S_det
Almacenamiento en colas tras una caída   Q_i      ≈ λ × T_caída × S_ev   (por subscriber caído)
```

donde `λ` se evalúa con `λ_prom` para el uso típico y con `λ_pico` para el dimensionamiento, `N` es el número de subscribers y `f̄` la fracción media de interés.

### 3.5 Escalamiento con N y simultaneidad

- Con **fanout**, la carga del broker crece **linealmente con N** aunque `f̄` sea pequeño. El cociente entre mensajes útiles y entregados es aproximadamente `f̄`. Si `f̄` es bajo y `N` es grande, la estimación justifica cambiar a un topic/headers exchange por región (sección 2.4), que reduce la entrega a `≈ λ × f̄ × N`.
- Para la API importa la **simultaneidad**, no solo la tasa media: en un sismo grande las `f_i × N` peticiones llegan casi en el mismo instante. El pico instantáneo es aproximadamente `N_interesados_por_evento / Δt`, donde `Δt` es la ventana en que los subscribers reciben el evento (milisegundos). Ese valor determina si basta una instancia o se necesita caché o réplicas (sección 2.3).
- Con el `T_caída` aceptable y el `λ_pico` se dimensiona el disco del broker y se decide el TTL de las colas.

### 3.6 Cómo justifican la arquitectura

El procedimiento lleva a una decisión:
- Si `λ_pico × N × S_ev` y `RPS_api` en el pico quedan muy por debajo de la capacidad de un nodo de RabbitMQ y de una instancia de FastAPI (capacidades que se miden con un benchmark propio o se toman de la documentación), **un único broker con fanout y una sola API bastan**, y la arquitectura implementada queda justificada.
- Si el pico de réplicas o un `N` grande acercan alguno de esos valores a la capacidad, la misma estimación indica **qué componente escalar primero**: la API (caché o réplicas) si domina `RPS_api`, y el enrutamiento por región o el cluster si domina `M_broker`.
- La comparación entre `S_ev` y `S_det` cuantifica la ventaja del pull. Difundir el detalle a todos costaría `λ × N × S_det`, mientras que el diseño actual cuesta `λ × N × S_ev + λ × f̄ × N × S_det`. El pull conviene mientras `f̄` sea menor que 1 y `S_det` sea bastante mayor que `S_ev`.

---

## 4. Conclusión

La arquitectura publish-subscribe con pull es apropiada para un sistema de **notificación informativa** de sismos en Chile. El evento contiene solo `id`, `latitud` y `longitud`, que es lo que exige el criterio de interés basado en la distancia. El detalle, con estructura tipo CSN, se obtiene bajo demanda desde una fuente de verdad única. Los trade-offs más relevantes en este dominio son el **pico correlacionado de carga** sobre la API durante terremotos y réplicas, la **vigencia de los avisos** acumulados en colas durables y la **latencia del pull**. Esta última no importa para informes posteriores al sismo, pero descartaría el diseño para alerta temprana. Con 5 subscribers el fanout, un solo broker y una sola API son suficientes. Un análisis *back of the envelope* basado en los catálogos del CSN o USGS, la ley de Gutenberg-Richter, la ley de Omori-Utsu y la geometría de los círculos de 500 km permitiría confirmar esa conclusión y anticipar en qué punto convendría pasar a enrutamiento por región, caché o réplicas de la API y un broker en cluster.
