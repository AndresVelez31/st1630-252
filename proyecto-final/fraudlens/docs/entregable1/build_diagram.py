#!/usr/bin/env python3
"""Genera arquitectura.drawio (editable en app.diagrams.net) y enlace_drawio.txt.

Uso: python3 build_diagram.py [carpeta_salida]
El PNG y el SVG se exportan desde este mismo .drawio (draw.io o render_diagram.js).
"""
import base64
import sys
import urllib.parse
import zlib
from xml.sax.saxutils import escape

OUT = sys.argv[1] if len(sys.argv) > 1 else "."

W, H = 1530, 1030
LANE_W, BOX_W, X0, GAP = 292, 268, 12, 13
LX = [X0 + i * (LANE_W + GAP) for i in range(5)]
BX = [x + 12 for x in LX]
FONT = "Helvetica"

LANES = [  # nombre, color fuerte, color suave
    ("1 · FUENTE", "#0F6E56", "#EEF6F2"),
    ("2 · INGESTIÓN", "#185FA5", "#EEF3FA"),
    ("3 · PROCESAMIENTO", "#854F0B", "#FBF4EA"),
    ("4 · ALMACENAMIENTO", "#534AB7", "#F2F1FB"),
    ("5 · CONSUMO", "#993556", "#FBEFF3"),
]
DEPLOY = {  # etiqueta de despliegue: texto, relleno, borde/texto
    "A": ("EC2-A", "#E2F0EE", "#2E7D78"),
    "B": ("EC2-B", "#ECE8FA", "#5B4BB7"),
    "AWS": ("AWS por uso", "#FFF1D6", "#B86E00"),
}
ALERT, LOOKUP, ORQ = "#C0392B", "#6B6B6B", "#5F5E5A"

cells = []


def vertex(cid, value, x, y, w, h, style):
    cells.append(
        f'<mxCell id="{cid}" value="{escape(value, {chr(34): "&quot;"})}" style="{style}" vertex="1" parent="1">'
        f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')


def edge(cid, src, tgt, color, dashed=False, label="", pts=(), exit_=None, entry=None, width=2):
    st = (f"edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;strokeColor={color};strokeWidth={width};"
          f"endArrow=block;endFill=1;endSize=7;fontSize=11;fontColor={color};fontStyle=1;"
          f"labelBackgroundColor=#FFFFFF;fontFamily={FONT};")
    if dashed:
        st += "dashed=1;dashPattern=6 4;"
    if exit_:
        st += f"exitX={exit_[0]};exitY={exit_[1]};exitDx=0;exitDy=0;"
    if entry:
        st += f"entryX={entry[0]};entryY={entry[1]};entryDx=0;entryDy=0;"
    geo = '<mxGeometry relative="1" as="geometry">'
    if pts:
        geo += '<Array as="points">' + "".join(f'<mxPoint x="{px}" y="{py}"/>' for px, py in pts) + "</Array>"
    geo += "</mxGeometry>"
    cells.append(f'<mxCell id="{cid}" value="{escape(label)}" style="{st}" edge="1" parent="1" '
                 f'source="{src}" target="{tgt}">{geo}</mxCell>')


def box(cid, lane, y, h, title, lines, deploy):
    color = LANES[lane][1]
    body = "<br>".join(lines)
    val = (f'<div style="text-align:left"><b style="color:{color};font-size:14px">{title}</b>'
           f'<br><span style="font-size:12.5px;color:#2A2A2A;line-height:1.35">{body}</span></div>')
    vertex(cid, val, BX[lane], y, BOX_W, h,
           f"rounded=1;arcSize=5;whiteSpace=wrap;html=1;align=left;verticalAlign=top;spacingLeft=9;"
           f"spacingTop=5;spacingRight=6;fillColor=#FFFFFF;strokeColor={color};strokeWidth=2;fontFamily={FONT};")
    text, fill, stroke = DEPLOY[deploy]
    pw = 46 if deploy != "AWS" else 78
    vertex(cid + "_tag", text, BX[lane] + BOX_W - pw - 6, y + h - 23, pw, 17,
           f"rounded=1;arcSize=50;html=1;fillColor={fill};strokeColor={stroke};fontColor={stroke};"
           f"fontSize=10;fontStyle=1;align=center;verticalAlign=middle;fontFamily={FONT};")


# ---------- Título ----------
vertex("title", "<b>FraudLens · Arquitectura de datos por capas</b>", 12, 8, 1100, 28,
       f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;align=left;verticalAlign=middle;fontSize=21;fontColor=#1F1F1F;fontFamily={FONT};")
vertex("subtitle",
       "Detección de fraude en pagos digitales (fintech colombiana) · streaming + batch sobre lakehouse "
       "Bronze/Silver/Gold · despliegue híbrido en AWS Academy (us-east-1)",
       12, 36, 1300, 20, f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;align=left;verticalAlign=middle;fontSize=13;fontColor=#5F5E5A;fontFamily={FONT};")

# ---------- Carriles ----------
for i, (name, strong, soft) in enumerate(LANES):
    vertex(f"lane{i}", "", LX[i], 66, LANE_W, 712, f"rounded=1;arcSize=3;fillColor={soft};strokeColor=none;html=1;")
    vertex(f"hdr{i}", name, BX[i], 74, BOX_W, 34,
           f"rounded=1;whiteSpace=wrap;html=1;fillColor={strong};strokeColor={strong};fontColor=#FFFFFF;"
           f"fontStyle=1;fontSize=15;fontFamily={FONT};")

# ---------- 1 · Fuente ----------
box("app", 0, 122, 116, "App móvil / API de pagos",
    ["Eventos de transacción en JSON", "~1.000 ev/s (pico 5.000 ev/s)", "Simulado: productor Python + Faker",
     "con escenarios de fraude y duplicados"], "A")
box("pg", 0, 262, 100, "PostgreSQL 16 (operacional)",
    ["Clientes y comercios (relacional)", "Contenedor que simula la BD de la app", "Extracción batch diaria"], "B")
box("csv", 0, 386, 100, "Listas de riesgo (CSV)",
    ["Comercios y dispositivos en lista negra", "Se publican en S3 landing/",
     "Versión nueva periódica"], "AWS")

# ---------- 2 · Ingestión ----------
box("kafka", 1, 122, 196, "Apache Kafka 3 brokers (KRaft)",
    ["Topic transacciones · 12 particiones", "Clave id_cliente · replicación 3",
     "acks=all · min.insync.replicas=2 (CP)", "Productor idempotente · retención 7 d",
     "Garantía: at-least-once", "Topic transacciones_dlq (inválidos)", "Imagen cp-kafka 7.6.0 (Lab 2a)"], "A")
box("extract", 1, 340, 146, "Airflow + PySpark (extracción)",
    ["JDBC PostgreSQL → Bronze (diario)", "Sensor S3: CSV nuevo → Bronze",
     "MERGE idempotente (at-least-once)", "Reintentos 3 · espera exponencial"], "B")

# ---------- 3 · Procesamiento ----------
box("stream", 2, 122, 226, "Spark Structured Streaming 3.5",
    ["Trigger 5 s · JSON inválido → DLQ", "Dedup id_transaccion (watermark 10 min)",
     "R1 · P1: ventana 5 min / 1 min (update)", "&nbsp;&nbsp;&gt;5 tx o &gt;$2.000.000 por cliente",
     "R2 · P2: dispositivo nuevo y monto", "&nbsp;&nbsp;&gt;3× promedio 30 d (join con Gold)",
     "R3 · P3: lista de riesgo (join)", "Checkpoint en S3 · restart automático"], "A")
box("batch", 2, 372, 152, "Spark batch (PySpark + Delta)",
    ["Corre dentro de Airflow (patrón Lab 4a)", "Silver → Gold cada 10 min",
     "Dimensiones SCD2 + hechos (MERGE)", "Métricas P4–P6 · OPTIMIZE ZORDER",
     "Reproceso por rule_version (solo Gold)"], "B")

# ---------- 4 · Almacenamiento ----------
box("bronze", 3, 122, 82, "BRONZE · Delta Lake en S3",
    ["Crudo, append-only · offset Kafka", "ingest_ts · catálogos y listas"], "AWS")
box("silver", 3, 224, 112, "SILVER · Delta Lake en S3",
    ["Limpio, tipado, sin duplicados (MERGE)", "Datos personales con HMAC-SHA-256",
     "Cuarentena con motivo · alertas"], "AWS")
box("gold", 3, 356, 168, "GOLD · Delta Lake en S3",
    ["Esquema en estrella, particionado por fecha", "fact_transacciones · fact_alertas",
     "dim_cliente (SCD2) · dim_comercio", "dim_dispositivo · dim_tiempo · dim_regla",
     "Time travel 90 días (auditoría)"], "AWS")
box("ddb", 3, 548, 128, "Amazon DynamoDB (on-demand)",
    ["Tabla alertas_fraude", "PK id_cliente · SK ts_alerta#regla", "PutItem condicional (clave determinista)",
     "Lectura eventual para analistas (AP)"], "AWS")

# ---------- 5 · Consumo ----------
box("metabase", 4, 122, 96, "Metabase",
    ["Dashboards de fraude por comercio,", "ciudad y franja horaria", "Conector Amazon Athena"], "B")
box("athena", 4, 356, 120, "Amazon Athena + Glue Catalog",
    ["Tablas Gold registradas en Glue", "(table_type = DELTA, patrón Lab 1b)", "SQL para P4, P5 y P6 · &lt; 10 s"], "AWS")
box("api", 4, 548, 104, "API de alertas (FastAPI)",
    ["GET /alertas por cliente, regla", "y rango de tiempo · lee DynamoDB", "Latencia alerta P95 &lt; 30 s"], "B")
box("sns", 4, 672, 92, "Amazon SNS",
    ["Topic alertas-fraude → correo", "al analista · solo alertas nuevas"], "AWS")

# ---------- Flujos de datos ----------
F, I, P, S = (LANES[i][1] for i in range(4))
TRUNK = BX[3] - 24          # tronco vertical de las alertas (entre Procesamiento y Almacenamiento)
XB = BX[3] - 14             # bajada de la extracción batch hacia Bronze
edge("e_app_k", "app", "kafka", F, exit_=(1, 0.4), entry=(0, 0.235))
edge("e_pg_x", "pg", "extract", F, exit_=(1, 0.5), entry=(0, 0.3), pts=[(BX[1] - 18, 312), (BX[1] - 18, 384)])
edge("e_csv_x", "csv", "extract", F, exit_=(1, 0.5), entry=(0, 0.7), pts=[(BX[1] - 18, 436), (BX[1] - 18, 442)])
edge("e_k_st", "kafka", "stream", I, exit_=(1, 0.35), entry=(0, 0.302))
edge("e_x_br", "extract", "bronze", I, exit_=(1, 0.2), entry=(0, 0.75),
     pts=[(BX[2] - 12, 369), (BX[2] - 12, 360), (XB, 360), (XB, 183)])
edge("e_st_br", "stream", "bronze", P, exit_=(1, 0.124), entry=(0, 0.25))
edge("e_st_sv", "stream", "silver", P, exit_=(1, 0.699), entry=(0, 0.5))
edge("e_br_sv", "bronze", "silver", S, exit_=(0.5, 1), entry=(0.5, 0))
edge("e_sv_gd", "silver", "gold", S, exit_=(0.5, 1), entry=(0.5, 0))
edge("e_b_gd", "batch", "gold", P, exit_=(1, 0.72), entry=(0, 0.72))
edge("e_gd_at", "gold", "athena", S, exit_=(1, 0.3), entry=(0, 0.35))
edge("e_at_mb", "athena", "metabase", LANES[4][1], exit_=(0.5, 0), entry=(0.5, 1))
edge("e_dd_api", "ddb", "api", S, exit_=(1, 0.4), entry=(0, 0.45))
# Alertas (baja latencia)
edge("e_st_dd", "stream", "ddb", ALERT, dashed=True, exit_=(1, 0.93), entry=(0, 0.4),
     pts=[(TRUNK, 332), (TRUNK, 599)])
edge("e_st_sns", "stream", "sns", ALERT, dashed=True, exit_=(1, 0.93), entry=(0, 0.5),
     pts=[(TRUNK, 332), (TRUNK, 718)])
# Lookup de dimensiones desde Gold (join stream-static)
edge("e_gd_st", "gold", "stream", LOOKUP, dashed=True, exit_=(0, 0.1), entry=(0.95, 1), width=1.5,
     pts=[(BX[2] + 0.95 * BOX_W, 373)])

# ---------- Orquestación ----------
vertex("orq", "", LX[0], 796, LX[4] + LANE_W - LX[0], 136,
       "rounded=1;arcSize=4;fillColor=#F4F2EC;strokeColor=#8A877F;strokeWidth=1.5;html=1;")
vertex("orq_t",
       '<b style="font-size:14px;color:#3D3C38">ORQUESTACIÓN · Apache Airflow 2.9 (LocalExecutor, EC2-B)</b>'
       '<span style="font-size:12px;color:#5F5E5A">&nbsp;&nbsp;— dependencias all_success: un fallo detiene los pasos '
       "siguientes · 3 reintentos con espera exponencial · notificación por SNS</span>",
       LX[0] + 14, 802, 1400, 24, f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;align=left;verticalAlign=middle;fontFamily={FONT};")
TASK = (f"rounded=1;html=1;fillColor=#FFFFFF;strokeColor={ORQ};fontColor=#2A2A2A;fontSize=12;"
        f"fontFamily=Courier New;align=center;verticalAlign=middle;")


def dag(prefix, y, label, sub, tasks):
    vertex(prefix + "_l", f'<b style="font-size:13px">{label}</b><br><span style="font-size:11.5px;color:#5F5E5A">{sub}</span>',
           LX[0] + 14, y - 4, 200, 36, f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;align=left;verticalAlign=middle;fontFamily={FONT};")
    x = 236
    prev = None
    for i, t in enumerate(tasks):
        tid = f"{prefix}{i}"
        w = 8.2 * len(t) + 22
        vertex(tid, t, x, y, w, 28, TASK)
        if prev:
            edge(f"{tid}_e", prev, tid, ORQ, width=1.5)
        prev = tid
        x += w + 26


dag("d1", 834, "DAG catalogos_diario", "diario 02:00 o al llegar un CSV",
    ["extract_postgres", "sensor_csv_riesgo", "load_bronze", "silver_catalogos"])
dag("d2", 880, "DAG fraude_pipeline", "cada 10 minutos",
    ["check_streaming_vivo", "gold_dimensiones", "gold_hechos", "quality_checks", "registrar_glue"])
vertex("orq_n", "El job de streaming corre continuo en EC2-A (restart automático desde el checkpoint); "
       "check_streaming_vivo verifica su latido y alerta si lleva más de 2 min sin avanzar.",
       LX[0] + 14, 912, 1300, 18, f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;align=left;verticalAlign=middle;fontSize=11.5;fontColor=#5F5E5A;fontFamily={FONT};")

# ---------- Leyenda ----------
LY = 950
vertex("lg_t", "<b>Leyenda</b>", 14, LY, 70, 20, f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;fontSize=12.5;align=left;verticalAlign=middle;fontFamily={FONT};")
legend_edges = [("Flujo de datos", S, False), ("Alertas de fraude (baja latencia)", ALERT, True),
                ("Lookup de dimensiones desde Gold (join stream-static)", LOOKUP, True)]
x = 92
for i, (txt, col, dsh) in enumerate(legend_edges):
    st = (f"html=1;strokeColor={col};strokeWidth=2;endArrow=block;endFill=1;endSize=7;"
          + ("dashed=1;dashPattern=6 4;" if dsh else ""))
    cells.append(f'<mxCell id="lg_e{i}" style="{st}" edge="1" parent="1"><mxGeometry relative="1" as="geometry">'
                 f'<mxPoint x="{x}" y="{LY + 10}" as="sourcePoint"/><mxPoint x="{x + 44}" y="{LY + 10}" as="targetPoint"/>'
                 '</mxGeometry></mxCell>')
    tw = 7.0 * len(txt) + 10
    vertex(f"lg_x{i}", txt, x + 52, LY, tw, 20, f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;fontSize=12;align=left;verticalAlign=middle;fontFamily={FONT};")
    x += 52 + tw + 18
for i, key in enumerate(["A", "B", "AWS"]):
    text, fill, stroke = DEPLOY[key]
    desc = {"A": "t3.large · ingesta y streaming", "B": "t3.large · batch, orquestación y consumo",
            "AWS": "servicio administrado que cobra por uso"}[key]
    pw = 46 if key != "AWS" else 78
    xx = 14 + i * 400 if i < 2 else 14 + 2 * 400
    vertex(f"lg_d{i}", text, xx, LY + 30, pw, 17,
           f"rounded=1;arcSize=50;html=1;fillColor={fill};strokeColor={stroke};fontColor={stroke};fontSize=10;"
           f"fontStyle=1;align=center;verticalAlign=middle;fontFamily={FONT};")
    vertex(f"lg_dt{i}", desc, xx + pw + 8, LY + 28, 300, 20,
           f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;fontSize=12;align=left;verticalAlign=middle;fontFamily={FONT};")
vertex("lg_n", "Mismo docker-compose.yml con perfiles streaming y batch; también corre completo en un portátil. "
       "Latencia: evento → alerta &lt; 30 s (P95) · evento → Gold &lt; 15 min.",
       14, LY + 50, 1400, 18, f"text;html=1;fillColor=none;strokeColor=none;fillColor=none;strokeColor=none;fontSize=11.5;fontColor=#5F5E5A;align=left;verticalAlign=middle;fontFamily={FONT};")

model = (f'<mxGraphModel dx="{W}" dy="{H}" grid="0" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" '
         f'fold="1" page="1" pageScale="1" pageWidth="{W}" pageHeight="{H}" background="#FFFFFF" math="0" shadow="0">'
         '<root><mxCell id="0"/><mxCell id="1" parent="0"/>' + "".join(cells) + "</root></mxGraphModel>")
mxfile = (f'<mxfile host="app.diagrams.net"><diagram id="fraudlens-arq" name="Arquitectura FraudLens">'
          f"{model}</diagram></mxfile>")
with open(f"{OUT}/arquitectura.drawio", "w", encoding="utf-8") as f:
    f.write(mxfile)

# Enlace editable: draw.io abre el diagrama embebido en la URL (#R + deflate + base64)
raw = urllib.parse.quote(mxfile, safe="")
comp = zlib.compressobj(9, zlib.DEFLATED, -15)
data = base64.b64encode(comp.compress(raw.encode()) + comp.flush()).decode()
with open(f"{OUT}/enlace_drawio.txt", "w", encoding="utf-8") as f:
    f.write("https://app.diagrams.net/#R" + urllib.parse.quote(data, safe="") + "\n")
print("ok", len(cells), "celdas")
