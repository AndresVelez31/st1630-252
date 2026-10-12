#!/usr/bin/env python3
"""Genera arquitectura.drawio (notación AWS de draw.io) y enlace_drawio.txt.

Uso: python3 build_diagram.py [carpeta_salida]
El PNG y el SVG se exportan desde el .drawio con render_diagram.js (visor oficial de draw.io).
"""
import base64
import sys
import urllib.parse
import zlib
from xml.sax.saxutils import escape

OUT = sys.argv[1] if len(sys.argv) > 1 else "."
W, H = 1840, 1236
FONT = "Helvetica"
INK = "#232F3E"
COLW = 290
LAYERS = [("1 · FUENTE", "#0F6E56", "#F1F8F5"), ("2 · INGESTIÓN", "#185FA5", "#F1F5FB"),
          ("3 · PROCESAMIENTO", "#854F0B", "#FCF7EF"), ("4 · ALMACENAMIENTO", "#534AB7", "#F5F4FC"),
          ("5 · CONSUMO", "#993556", "#FCF2F6")]
ALERT, LOOKUP = "#D13212", "#7D8998"

# Estilos de la biblioteca AWS 2023 de draw.io (Sidebar-AWS4.js)
PTS = ("points=[[0,0,0],[0.25,0,0],[0.5,0,0],[0.75,0,0],[1,0,0],[0,1,0],[0.25,1,0],[0.5,1,0],[0.75,1,0],[1,1,0],"
       "[0,0.25,0],[0,0.5,0],[0,0.75,0],[1,0.25,0],[1,0.5,0],[1,0.75,0]];")
RES = (PTS + "sketch=0;outlineConnect=0;fontColor=#232F3E;fillColor={c};strokeColor=#ffffff;dashed=0;"
       "verticalLabelPosition=bottom;verticalAlign=top;align=center;html=1;fontSize=12;fontStyle=0;aspect=fixed;"
       "shape=mxgraph.aws4.resourceIcon;resIcon=mxgraph.aws4.{i};")
ICON = ("sketch=0;outlineConnect=0;fontColor=#232F3E;gradientColor=none;fillColor={c};strokeColor=none;dashed=0;"
        "verticalLabelPosition=bottom;verticalAlign=top;align=center;html=1;fontSize=12;fontStyle=0;aspect=fixed;"
        "pointerEvents=1;shape=mxgraph.aws4.{i};")
GROUP = ("points=[[0,0],[0.25,0],[0.5,0],[0.75,0],[1,0],[1,0.25],[1,0.5],[1,0.75],[1,1],[0.75,1],[0.5,1],[0.25,1],"
         "[0,1],[0,0.75],[0,0.5],[0,0.25]];outlineConnect=0;gradientColor=none;html=1;whiteSpace=wrap;fontSize=13;"
         "fontStyle=1;container=0;pointerEvents=0;collapsible=0;recursiveResize=0;shape=mxgraph.aws4.group;"
         "grIcon=mxgraph.aws4.{g};strokeColor={s};fillColor={f};verticalAlign=top;align=left;spacingLeft=30;"
         "fontColor={fc};dashed={d};fontFamily=" + FONT + ";")
C_COMPUTE, C_STORAGE, C_DB, C_ANALYTICS, C_INTEG = "#ED7100", "#7AA116", "#C925D1", "#8C4FFF", "#E7157B"

cells = []


def vertex(cid, value, x, y, w, h, style):
    cells.append(
        f'<mxCell id="{cid}" value="{escape(value, {chr(34): "&quot;"})}" style="{style}" vertex="1" parent="1">'
        f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')


def text(cid, value, x, y, w, h, size=12, color=INK, align="left", bold=False):
    vertex(cid, value, x, y, w, h,
           f"text;html=1;fillColor=none;strokeColor=none;align={align};verticalAlign=middle;whiteSpace=wrap;"
           f"fontSize={size};fontColor={color};fontStyle={1 if bold else 0};fontFamily={FONT};")


def edge(cid, src, tgt, color=INK, dashed=False, pts=(), exit_=None, entry=None, width=1.8, label=""):
    st = (f"edgeStyle=orthogonalEdgeStyle;rounded=0;html=1;strokeColor={color};strokeWidth={width};endArrow=block;"
          f"endFill=1;endSize=6;fontSize=11;fontColor={color};labelBackgroundColor=#FFFFFF;fontFamily={FONT};")
    if dashed:
        st += "dashed=1;dashPattern=6 4;"
    if exit_:
        st += f"exitX={exit_[0]};exitY={exit_[1]};exitDx=0;exitDy=0;"
    if entry:
        st += f"entryX={entry[0]};entryY={entry[1]};entryDx=0;entryDy=0;"
    geo = '<mxGeometry relative="1" as="geometry">'
    if pts:
        geo += '<Array as="points">' + "".join(f'<mxPoint x="{a}" y="{b}"/>' for a, b in pts) + "</Array>"
    cells.append(f'<mxCell id="{cid}" value="{escape(label)}" style="{st}" edge="1" parent="1" source="{src}" '
                 f'target="{tgt}">{geo + "</mxGeometry>"}</mxCell>')


def card(cid, layer, x, y, w, h, icon_style, title, lines, icon=40, tag=None):
    """Tarjeta de componente: borde del color de la capa, ícono AWS a la izquierda y texto."""
    color = LAYERS[layer][1]
    vertex(cid, "", x, y, w, h, f"rounded=1;arcSize=4;html=1;fillColor=#FFFFFF;strokeColor={color};strokeWidth=1.6;")
    vertex(cid + "_i", "", x + 8, y + 9, icon, icon, icon_style)
    body = "<br>".join(lines)
    val = (f'<b style="font-size:13.5px;color:{color}">{title}</b><br>'
           f'<span style="font-size:11.8px;color:#232F3E">{body}</span>')
    vertex(cid + "_t", val, x + icon + 14, y + 3, w - icon - 18, h - 6,
           f"text;html=1;fillColor=none;strokeColor=none;align=left;verticalAlign=top;whiteSpace=wrap;"
           f"spacingTop=2;fontFamily={FONT};")
    if tag:
        vertex(cid + "_tag", tag, x + w - 74, y + h - 22, 68, 16,
               "rounded=1;arcSize=50;html=1;fillColor=#FFF4E5;strokeColor=#D86613;fontColor=#B4530B;fontSize=10;"
               f"fontStyle=1;align=center;verticalAlign=middle;fontFamily={FONT};")


CONTAINER = ICON.format(c=C_COMPUTE, i="container_1")

# ---------- Título ----------
text("title", "<b>FraudLens · Arquitectura de datos por capas</b>", 16, 8, 1200, 28, size=21)
text("subtitle", "Detección de fraude en pagos digitales (fintech colombiana) · cómputo local con Docker Compose y "
     "servicios del sandbox AWS Academy Data Engineering · notación AWS Architecture Icons", 16, 36, 1500, 20,
     size=12.5, color="#5F6B7A")

# ---------- Franjas de capas (de arriba hacia abajo) ----------
BANDS = [(70, 146), (222, 192), (420, 222), (648, 192), (846, 156)]
for i, ((name, strong, soft), (by, bh)) in enumerate(zip(LAYERS, BANDS)):
    vertex(f"band{i}", "", 14, by, W - 28, bh, f"rounded=0;fillColor={soft};strokeColor=none;html=1;")
    vertex(f"hdr{i}", name, 22, by + 6, 40, bh - 12,
           f"rounded=1;arcSize=20;html=1;horizontal=0;fillColor={strong};strokeColor=none;fontColor=#FFFFFF;"
           f"fontStyle=1;fontSize=13;fontFamily={FONT};")

# ---------- Grupos de despliegue ----------
LX0, LX1 = 80, 1000
vertex("g_local", "Equipo local del grupo · Docker Compose (portátil de 16 GB, perfiles streaming, batch y consumo)",
       LX0, 74, LX1 - LX0, 936, GROUP.format(g="group_on_premise", s="#7D8998", f="none", fc="#5A6C86", d=0))
AX0, AX1 = 1022, 1700
vertex("g_cloud", "AWS Cloud", AX0, 74, AX1 - AX0, 936, GROUP.format(g="group_aws_cloud_alt", s=INK, f="none", fc=INK, d=0))
vertex("g_region", "Region us-east-1 · sandbox AWS Academy Data Engineering", AX0 + 10, 108, AX1 - AX0 - 20, 894,
       GROUP.format(g="group_region", s="#00A4A6", f="none", fc="#147EBA", d=1))
vertex("cfn_i", "", AX1 - 214, 152, 34, 34, RES.format(c=C_INTEG, i="cloudformation"))
text("cfn_t", "<b>AWS CloudFormation</b><br>crea el stack en cada sesión", AX1 - 174, 150, 164, 38, size=11.5)

C1, C2, CW = 100, 556, 424            # columnas del grupo local
A1, A2, AW = 1046, 1370, 304          # columnas dentro de AWS

# ---------- 1 · Fuente ----------
card("prod", 0, C1, 108, CW, 100, CONTAINER, "Productor de transacciones",
     ["Python + Faker: simula la app y la API de pagos", "Eventos JSON · ~1.000 ev/s, pico 5.000",
      "Fraude, duplicados y JSON malformado (semilla fija)"], tag="streaming")
card("pg", 0, C2, 108, CW, 100, CONTAINER, "PostgreSQL 16 (operacional)",
     ["Clientes y comercios (relacional)", "Simula la base de datos de la app", "Extracción batch diaria"], tag="batch")
card("csv", 0, A1, 146, AW + 10, 66, ICON.format(c=C_STORAGE, i="bucket_with_objects"), "Listas de riesgo (CSV)",
     ["Se publican en s3://…/landing/", "Comercios y dispositivos en lista negra"])

# ---------- 2 · Ingestión ----------
card("kafka", 1, C1, 232, CW, 172, CONTAINER, "Apache Kafka · 3 brokers KRaft",
     ["cp-kafka 7.6.0 (Lab 2a)", "Topic transacciones · 12 particiones · clave id_cliente",
      "Replicación 3 · acks=all · min.insync.replicas=2 (CP)", "Productor idempotente · retención 7 días (replay)",
      "Garantía at-least-once", "Topic transacciones_dlq (mensajes inválidos)"], tag="streaming")
card("extract", 1, C2, 232, CW, 110, CONTAINER, "Airflow + PySpark (extracción)",
     ["JDBC PostgreSQL → Bronze (diario)", "S3KeySensor: CSV nuevo en landing/ → Bronze",
      "MERGE idempotente · 3 reintentos con espera"], tag="batch")

# ---------- 3 · Procesamiento ----------
card("stream", 2, C1, 430, CW, 202, CONTAINER, "Spark Structured Streaming",
     ["Spark 3.5.1 + Delta 3.1.0 · trigger 5 s", "Esquema · JSON inválido → DLQ",
      "Dedup id_transaccion (watermark 10 min)", "R1 · P1: ventana 5 min / 1 min (modo update)",
      "R2 · P2: dispositivo nuevo y monto &gt; 3× promedio 30 d", "R3 · P3: lista de riesgo (join con Gold)",
      "Checkpoint en S3 junto a los datos"], tag="streaming")
card("batch", 2, C2, 430, CW, 128, CONTAINER, "Spark batch dentro de Airflow",
     ["Patrón del Lab 4a · cada 10 minutos", "Silver → Gold: dimensiones SCD2 y hechos (MERGE)",
      "Métricas P4–P6 · OPTIMIZE ZORDER", "Reproceso por rule_version (solo Gold)"], tag="batch")

# ---------- 4 · Almacenamiento ----------
card("mongo", 3, C1, 660, CW, 124, CONTAINER, "MongoDB 7 · replica set de 3 nodos",
     ["Colección alertas_fraude · _id determinista", "Upsert idempotente · índices cliente, regla, hora",
      "Escritura w=majority (CP)", "Lectura secondaryPreferred (disponible)"], tag="streaming")
card("backup", 3, C2, 660, CW, 92, ICON.format(c="#7D8998", i="generic_database"), "Respaldo local del lakehouse",
     ["aws s3 sync al cerrar cada sesión del sandbox", "Se restaura con el DAG sesion_aws"], tag="batch")
vertex("g_s3", "", AX0 + 20, 654, AX1 - AX0 - 40, 182,
       f"rounded=0;html=1;fillColor=none;strokeColor={C_STORAGE};strokeWidth=1.5;dashed=1;dashPattern=8 4;")
vertex("g_s3_i", "", AX0 + 20, 654, 24, 24, RES.format(c=C_STORAGE, i="s3"))
text("g_s3_t", "<b>Amazon S3</b> · bucket fraudlens-lake (Delta Lake)", AX0 + 50, 654, 420, 24, size=12.5, color="#3F8624")
BK = ICON.format(c=C_STORAGE, i="bucket")
SW = 198
card("bronze", 3, AX0 + 30, 684, SW, 140, BK, "Bronze",
     ["bronze/ · crudo,", "append-only", "offset Kafka,", "ingest_ts"], icon=28)
card("silver", 3, AX0 + 30 + SW + 12, 684, SW, 140, BK, "Silver",
     ["silver/ · limpio,", "sin duplicados", "PII con HMAC", "cuarentena, alertas"], icon=28)
card("gold", 3, AX0 + 30 + 2 * (SW + 12), 684, SW, 140, BK, "Gold (estrella)",
     ["fact_transacciones,", "fact_alertas", "dim_cliente (SCD2)…", "time travel 90 d"], icon=28)

# ---------- 5 · Consumo ----------
card("api", 4, C1, 858, CW, 96, CONTAINER, "API de alertas (FastAPI)",
     ["GET /alertas por cliente, regla y rango", "Lee MongoDB · alerta en &lt; 30 s (P95)"], tag="consumo")
card("metabase", 4, C2, 858, CW, 96, CONTAINER, "Metabase",
     ["Dashboards de fraude por comercio,", "ciudad y franja horaria · conector Athena"], tag="consumo")
card("athena", 4, A1, 858, AW, 128, RES.format(c=C_ANALYTICS, i="athena"), "Amazon Athena",
     ["SQL sobre Gold: P4, P5 y P6", "Menos de 10 s (30 días)", "Tablas en Glue Data Catalog",
      "(table_type = DELTA, Lab 1b)"], icon=44)
vertex("glue_i", "", A1 + AW - 46, 940, 38, 38, ICON.format(c=C_ANALYTICS, i="glue_data_catalog"))
card("sns", 4, A2, 858, AW - 14, 100, RES.format(c=C_INTEG, i="sns"), "Amazon SNS",
     ["Topic alertas-fraude → correo", "Solo alertas nuevas", "y fallos de Airflow"], icon=44)
vertex("users", "<b>Analistas</b><br>de fraude",
       AX1 + 50, 870, 56, 56, ICON.format(c=INK, i="users").replace("fontSize=12", "fontSize=12;fontFamily=" + FONT))

# ---------- Flujos ----------
GRAY = "#7D8998"
edge("e1", "prod", "kafka", exit_=(0.5, 1), entry=(0.5, 0))
edge("e2", "kafka", "stream", exit_=(0.5, 1), entry=(0.5, 0))
edge("e5", "pg", "extract", exit_=(0.5, 1), entry=(0.5, 0))
edge("e6", "csv", "extract", exit_=(0, 0.5), entry=(1, 0.3), pts=[(1006, 179), (1006, 265)])
edge("e7", "extract", "bronze", exit_=(1, 0.8), entry=(0, 0.3), pts=[(1014, 320), (1014, 726)])
edge("e3", "stream", "bronze", exit_=(1, 0.94), entry=(0.5, 0), pts=[(AX0 + 129, 620)])
edge("e4", "stream", "silver", exit_=(1, 0.86), entry=(0.5, 0), pts=[(AX0 + 339, 604)])
edge("e8", "bronze", "silver", exit_=(1, 0.6), entry=(0, 0.6))
edge("e9", "silver", "gold", exit_=(1, 0.6), entry=(0, 0.6))
edge("e10", "batch", "gold", exit_=(1, 0.45), entry=(0.5, 0), pts=[(AX0 + 549, 488)])
edge("e11", "gold", "athena", exit_=(0.3, 1), entry=(0.8, 0), pts=[(AX0 + 509, 842), (A1 + 243, 842)])
edge("e12", "athena", "metabase", exit_=(0, 0.35), entry=(1, 0.42))
edge("e13", "mongo", "api", exit_=(0.5, 1), entry=(0.5, 0))
edge("e14", "backup", "g_s3", GRAY, dashed=True, exit_=(1, 0.85), entry=(0, 0.4), width=1.4)
cells[-1] = cells[-1].replace("endArrow=block;", "endArrow=block;startArrow=block;startFill=1;")
edge("e15", "sns", "users", ALERT, dashed=True, exit_=(1, 0.3), entry=(0, 0.5))
# Alertas de baja latencia
edge("a1", "stream", "mongo", ALERT, dashed=True, exit_=(0.5, 1), entry=(0.5, 0))
edge("a2", "stream", "sns", ALERT, dashed=True, exit_=(0, 0.9), entry=(0.5, 1),
     pts=[(90, 612), (90, 994), (A2 + 145, 994)])

# ---------- Orquestación ----------
OY = 1018
vertex("orq", "", 14, OY, W - 28, 116, "rounded=1;arcSize=5;fillColor=#F4F2EC;strokeColor=#8A877F;html=1;")
vertex("orq_i", "", 26, OY + 9, 28, 28, CONTAINER)
text("orq_t", '<b>ORQUESTACIÓN · Apache Airflow 2.9 (LocalExecutor, perfil batch)</b>'
     '<span style="color:#5F6B7A">&nbsp;&nbsp;— dependencias all_success: un fallo detiene los pasos siguientes · '
     '3 reintentos con espera exponencial · notificación por SNS</span>', 62, OY + 8, 1700, 30, size=13)
TASK = ("rounded=1;html=1;fillColor=#FFFFFF;strokeColor=#5F5E5A;fontColor=#232F3E;fontSize=12;"
        "fontFamily=Courier New;align=center;verticalAlign=middle;")


def dag(prefix, x0, y, label, sub, tasks):
    text(prefix + "_l", f'<b>{label}</b><br><span style="color:#5F6B7A;font-size:11.5px">{sub}</span>',
         x0, y - 5, 190, 36, size=12.5)
    x, prev = x0 + 196, None
    for i, t in enumerate(tasks):
        tid, w = f"{prefix}{i}", 8.2 * len(t) + 20
        vertex(tid, t, x, y, w, 26, TASK)
        if prev:
            edge(f"{tid}_e", prev, tid, "#5F5E5A", width=1.3)
        prev, x = tid, x + w + 22


dag("d1", 26, OY + 44, "catalogos_diario", "diario o al llegar un CSV",
    ["extract_postgres", "sensor_csv_riesgo", "load_bronze", "silver_catalogos"])
dag("d3", 1010, OY + 44, "sesion_aws", "al iniciar cada sesión",
    ["crear_stack", "restaurar_respaldo", "registrar_glue"])
dag("d2", 26, OY + 80, "fraude_pipeline", "cada 10 minutos",
    ["check_streaming_vivo", "gold_dimensiones", "gold_hechos", "quality_checks", "registrar_glue"])

# ---------- Leyenda ----------
LY = 1146
text("lg", "<b>Leyenda</b>", 16, LY, 80, 20, size=12.5)
items = [("Flujo de datos", INK, False), ("Alertas de fraude (baja latencia)", ALERT, True),
         ("Respaldo y restauración del lakehouse por sesión", GRAY, True)]
x = 96
for i, (t, col, dsh) in enumerate(items):
    st = f"html=1;strokeColor={col};strokeWidth=1.8;endArrow=block;endFill=1;endSize=6;" + ("dashed=1;dashPattern=6 4;" if dsh else "")
    cells.append(f'<mxCell id="lg_e{i}" style="{st}" edge="1" parent="1"><mxGeometry relative="1" as="geometry">'
                 f'<mxPoint x="{x}" y="{LY + 10}" as="sourcePoint"/><mxPoint x="{x + 42}" y="{LY + 10}" as="targetPoint"/>'
                 "</mxGeometry></mxCell>")
    tw = 6.9 * len(t) + 12
    text(f"lg_t{i}", t, x + 50, LY, tw, 20, size=12)
    x += 50 + tw + 22
vertex("lg_c", "", 16, LY + 30, 24, 24, CONTAINER)
text("lg_ct", "Contenedor Docker en el equipo local (perfil indicado en la etiqueta naranja)", 46, LY + 32, 520, 20, size=12)
vertex("lg_s", "", 600, LY + 30, 24, 24, RES.format(c=C_ANALYTICS, i="athena"))
text("lg_st", "Servicio de AWS que cobra por uso; el sandbox lo borra al cerrar la sesión y CloudFormation lo recrea",
     630, LY + 32, 760, 20, size=12)
text("lg_n", "Sin EC2, EBS, IP elásticas, RDS ni CloudWatch (el sandbox solo permite EC2 de hasta 2 GB y no ofrece DynamoDB, "
     "Kinesis, MSK ni MWAA). Latencia: evento → alerta &lt; 30 s (P95) · evento → Gold &lt; 15 min.",
     16, LY + 60, 1780, 20, size=11.5, color="#5F6B7A")

model = (f'<mxGraphModel dx="{W}" dy="{H}" grid="0" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" '
         f'fold="1" page="1" pageScale="1" pageWidth="{W}" pageHeight="{H}" background="#FFFFFF" math="0" shadow="0">'
         '<root><mxCell id="0"/><mxCell id="1" parent="0"/>' + "".join(cells) + "</root></mxGraphModel>")
mxfile = (f'<mxfile host="app.diagrams.net"><diagram id="fraudlens-arq" name="Arquitectura FraudLens">'
          f"{model}</diagram></mxfile>")
with open(f"{OUT}/arquitectura.drawio", "w", encoding="utf-8") as f:
    f.write(mxfile)

raw = urllib.parse.quote(mxfile, safe="")
comp = zlib.compressobj(9, zlib.DEFLATED, -15)
data = base64.b64encode(comp.compress(raw.encode()) + comp.flush()).decode()
with open(f"{OUT}/enlace_drawio.txt", "w", encoding="utf-8") as f:
    f.write("https://app.diagrams.net/#R" + urllib.parse.quote(data, safe="") + "\n")
print("ok", len(cells), "celdas")
