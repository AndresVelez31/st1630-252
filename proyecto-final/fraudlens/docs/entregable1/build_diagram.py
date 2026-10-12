#!/usr/bin/env python3
"""Genera arquitectura.drawio, arquitectura.svg y arquitectura.png de FraudLens
a partir de una única especificación (mismas cajas, mismo texto)."""
import sys
from xml.sax.saxutils import escape
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

OUT = sys.argv[1] if len(sys.argv) > 1 else "."
W, H = 1600, 1000
BW, GAP, X0 = 284, 40, 10
CX = [X0 + i * (BW + GAP) for i in range(5)]
FS = 14          # tamaño de letra del cuerpo (unidades del lienzo)
LH = 19          # alto de línea

HEAD = ["1 · FUENTE", "2 · INGESTIÓN", "3 · PROCESAMIENTO", "4 · ALMACENAMIENTO", "5 · CONSUMO"]

# id: (columna, y, alto, título, [líneas])
BOX = {
 "app":   (0, 70, 110, "App móvil / API de pagos",
           ["Eventos de transacción (JSON)", "~1.000 ev/s (pico 5.000 ev/s)", "Simulado con productor Faker"]),
 "pg":    (0, 215, 90, "PostgreSQL 16 (operacional)",
           ["Catálogos de clientes y", "comercios · carga batch diaria"]),
 "csv":   (0, 415, 90, "Listas de riesgo (CSV)",
           ["Comercios y dispositivos en", "lista negra · versión periódica"]),
 "kafka": (1, 70, 170, "Apache Kafka (KRaft, 3 brokers)",
           ["Topic transacciones · 12 partic.", "Clave id_cliente · 3 réplicas",
            "acks=all · min.insync.replicas=2", "Idempotente · retención 7 días",
            "At-least-once · CP", "DLQ: transacciones_dlq"]),
 "extract": (1, 380, 115, "Airflow + PySpark (extract)",
           ["Lee PostgreSQL (JDBC) y CSV", "Escribe Parquet en landing", "(MinIO) · diario o con CSV nuevo",
            "At-least-once + MERGE"]),
 "ss":    (2, 70, 245, "Spark Structured Streaming",
           ["Lee Kafka · parsea JSON", "JSON inválido → DLQ", "Dedup id_transaccion (WM 10 min)",
            "Calidad → cuarentena (Silver)", "Trigger 5 s · salida update",
            "R1 · P1: ventana 5 min/cliente", "R2 · P2: dispositivo nuevo y",
            "   monto >3× prom. 30 d (Gold)", "R3 · P3: lista de riesgo (join)",
            "Alertas: upsert por _id", "Checkpoint en MinIO"]),
 "batch": (2, 380, 135, "Spark batch (PySpark)",
           ["Landing → Bronze → Silver", "Silver → Gold (star schema)", "Tasa de fraude, P95, top 10",
            "Reproceso por rule_version", "Disparado por DAGs Airflow"]),
 "bronze": (3, 70, 85, "BRONZE · Delta Lake en MinIO",
           ["Crudo, append-only", "Linaje: offset Kafka + ingest_ts"]),
 "silver": (3, 190, 105, "SILVER · Delta Lake en MinIO",
           ["Limpio, tipado, dedup (MERGE)", "Datos personales con hash", "Cuarentena y tabla de alertas"]),
 "gold":  (3, 330, 170, "GOLD · Delta Lake en MinIO",
           ["Star schema consultable", "fact_transacciones · fact_alertas", "dim_cliente (SCD2) · dim_comercio",
            "dim_dispositivo · dim_tiempo", "dim_regla", "Actualizado cada 10 min", "Time travel 90 días"]),
 "mongo": (3, 535, 125, "MongoDB (replica set, 3 nodos)",
           ["alertas_fraude (baja latencia)", "Escritura w=majority (CP)",
            "Lectura secondaryPreferred (AP)", "Upsert por _id, sin duplicados"]),
 "meta":  (4, 70, 85, "Metabase",
           ["Dashboards de fraude por", "comercio, ciudad y hora"]),
 "trino": (4, 190, 105, "Trino (motor de consulta)",
           ["SQL sobre Delta Lake (Gold)", "Responde P4, P5 y P6", "Conector delta_lake"]),
 "api":   (4, 535, 125, "API de alertas (FastAPI)",
           ["GET /alertas por cliente,", "regla y fecha", "Webhook al equipo de fraude",
            "Latencia alerta P95 < 30 s"]),
}

NARROW = {"batch": 40}
def geom(k):
    c, y, h, *_ = BOX[k]
    n = NARROW.get(k, 0)
    return CX[c] + n, y, BW - n, h

def right(k, fy=0.5):
    x, y, w, h = geom(k); return (x + w, y + h * fy)
def left(k, fy=0.5):
    x, y, w, h = geom(k); return (x, y + h * fy)
def top(k, fx=0.5):
    x, y, w, h = geom(k); return (x + w * fx, y)
def bottom(k, fx=0.5):
    x, y, w, h = geom(k); return (x + w * fx, y + h)

def R(k, dy):
    x, y, w, h = geom(k); return (x + w, y + dy)
def L(k, dy):
    x, y, w, h = geom(k); return (x, y + dy)

BUS = CX[2] + BW + GAP * 0.62          # tronco vertical entre procesamiento y almacenamiento
LK  = CX[2] + BW + GAP * 0.28        # tronco de lookup (Gold → streaming)

# aristas: (puntos, estilo)  estilo: flow | alert | lookup
EDGES = [
 ([R("app", 55), L("kafka", 55)], "flow"),
 ([R("pg", 45), (CX[0]+BW+GAP/2, geom("pg")[1]+45), (CX[0]+BW+GAP/2, geom("extract")[1]+35), L("extract", 35)], "flow"),
 ([R("csv", 45), L("extract", 80)], "flow"),
 ([R("kafka", 55), L("ss", 55)], "flow"),
 ([R("extract", 60), L("batch", 60)], "flow"),
 ([R("ss", 40), L("bronze", 40)], "flow"),
 ([R("ss", 180), L("silver", 60)], "flow"),
 ([R("batch", 55), (BUS, geom("batch")[1]+55), (BUS, geom("bronze")[1]+65), L("bronze", 65)], "flow"),
 ([(BUS, geom("silver")[1]+85), L("silver", 85)], "flow"),
 ([(BUS, geom("gold")[1]+70), L("gold", 70)], "flow"),
 ([bottom("bronze"), top("silver")], "flow"),
 ([bottom("silver"), top("gold")], "flow"),
 ([R("gold", 100), (CX[3]+BW+GAP/2, geom("gold")[1]+100), (CX[3]+BW+GAP/2, geom("trino")[1]+52), L("trino", 52)], "flow"),
 ([top("trino"), bottom("meta")], "flow"),
 ([right("mongo"), left("api")], "flow"),
 # alertas: streaming -> MongoDB (punteado rojo), por el canal libre junto al batch
 ([(CX[2]+18, geom("ss")[1]+geom("ss")[3]), (CX[2]+18, 690), (bottom("mongo", .5)[0], 690), bottom("mongo")], "alert"),
 # lookup de dimensiones desde Gold (punteado azul)
 ([L("gold", 150), (LK, geom("gold")[1]+150), (LK, geom("ss")[1]+215), R("ss", 215)], "lookup"),
]

STY = {
 "flow":   dict(color="#222222", dash=None, w=1.6),
 "alert":  dict(color="#B3261E", dash=(6, 4), w=2.0),
 "lookup": dict(color="#1F5FA8", dash=(2, 3), w=2.0),
}

# ---------------- Orquestación ----------------
OR_Y, OR_H = 705, 205
DAGS = [
 ("DAG catalogos_diario", "diario o al llegar un CSV", 790, ["extract_batch", "load_bronze", "spark_silver"]),
 ("DAG fraude_pipeline", "cada 10 minutos", 850, ["gold_dimensiones", "gold_fact", "quality_checks", "refresh_metabase"]),
]
TASK_X0, TASK_W, TASK_GAP, TASK_H = 500, 215, 40, 38
OR_NOTE = "El job de streaming corre continuo (Spark standalone: 1 master + 2 workers); un sensor de Airflow verifica que esté activo."

LEG_Y = 937
FOOT = "Latencia objetivo: evento → alerta < 30 s (P95) · evento → Gold < 15 min. Almacenamiento de objetos: MinIO (compatible S3, un nodo en local). Todo en Docker Compose."

# =====================================================================
# PNG / SVG con matplotlib
# =====================================================================
def render(fmt, dpi=None):
    fig = plt.figure(figsize=(W / 100, H / 100))
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W); ax.set_ylim(H, 0); ax.axis("off")
    pt = 0.72  # unidades → puntos
    font = "DejaVu Sans"

    def rbox(x, y, w, h, fc="white", ec="#222222", lw=1.8, r=10):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle=f"round,pad=0,rounding_size={r}",
                                    fc=fc, ec=ec, lw=lw))

    for i, t in enumerate(HEAD):
        rbox(CX[i], 15, BW, 38, fc="#2B2B2B", ec="#2B2B2B", r=8)
        ax.text(CX[i] + BW / 2, 34, t, color="white", ha="center", va="center",
                fontsize=15 * pt, fontweight="bold", family=font)
    for k, (c, y, h, title, lines) in BOX.items():
        x, _, bw_, _ = geom(k)
        rbox(x, y, bw_, h)
        ax.text(x + 10, y + 16, title, ha="left", va="center", fontsize=(FS + 0.5) * pt,
                fontweight="bold", family=font)
        for j, ln in enumerate(lines):
            ax.text(x + 10, y + 16 + LH * (j + 1) + 2, ln, ha="left", va="center",
                    fontsize=FS * pt, family=font)

    # orquestación
    rbox(X0, OR_Y, W - 2 * X0, OR_H)
    ax.text(X0 + 15, OR_Y + 22, "ORQUESTACIÓN · Apache Airflow 2.x", ha="left", va="center",
            fontsize=15 * pt, fontweight="bold", family=font)
    ax.text(X0 + 15 + 372, OR_Y + 22, "— cada tarea depende de la anterior (falla limpia si hay error upstream; 3 reintentos con espera creciente)",
            ha="left", va="center", fontsize=FS * pt, family=font)
    for name, sub, cy, tasks in DAGS:
        ax.text(X0 + 15, cy - 9, name, ha="left", va="center", fontsize=FS * pt, fontweight="bold", family=font)
        ax.text(X0 + 15, cy + 12, sub, ha="left", va="center", fontsize=FS * pt, family=font)
        for i, t in enumerate(tasks):
            tx = TASK_X0 + i * (TASK_W + TASK_GAP)
            rbox(tx, cy - TASK_H / 2, TASK_W, TASK_H, lw=1.3, r=6)
            ax.text(tx + TASK_W / 2, cy, t, ha="center", va="center", fontsize=FS * pt, family="DejaVu Sans Mono")
            if i < len(tasks) - 1:
                ax.annotate("", xy=(tx + TASK_W + TASK_GAP, cy), xytext=(tx + TASK_W, cy),
                            arrowprops=dict(arrowstyle="-|>", color="#222222", lw=1.6, shrinkA=0, shrinkB=0))
    ax.text(X0 + 15, OR_Y + OR_H - 18, OR_NOTE, ha="left", va="center", fontsize=(FS - 1) * pt, family=font)

    # aristas
    for pts, st in EDGES:
        s = STY[st]
        xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
        ax.plot(xs[:-1] if len(pts) == 2 else xs[:-1] + [xs[-1]], ys[:-1] if len(pts) == 2 else ys[:-1] + [ys[-1]],
                color=s["color"], lw=s["w"] * pt * 1.4, ls=(0, s["dash"]) if s["dash"] else "-",
                solid_capstyle="butt", zorder=1)
        ax.annotate("", xy=pts[-1], xytext=pts[-2],
                    arrowprops=dict(arrowstyle="-|>", color=s["color"], lw=s["w"] * pt * 1.4,
                                    shrinkA=0, shrinkB=0, mutation_scale=16), zorder=3)
    ax.text(CX[2] + 40, 674, "alertas", ha="left", va="center", fontsize=FS * pt,
            color=STY["alert"]["color"], family=font, style="italic")

    # leyenda
    ax.text(X0 + 5, LEG_Y, "Leyenda", fontsize=FS * pt, fontweight="bold", va="center", family=font)
    items = [("flow", "Flujo de datos", 130), ("alert", "Alertas de fraude (baja latencia)", 400),
             ("lookup", "Lookup de dimensiones desde Gold (join stream-static)", 840)]
    for st, label, lx in items:
        s = STY[st]
        ax.plot([lx, lx + 70], [LEG_Y, LEG_Y], color=s["color"], lw=s["w"] * pt * 1.4,
                ls=(0, s["dash"]) if s["dash"] else "-")
        ax.annotate("", xy=(lx + 80, LEG_Y), xytext=(lx + 66, LEG_Y),
                    arrowprops=dict(arrowstyle="-|>", color=s["color"], lw=s["w"] * pt * 1.4,
                                    shrinkA=0, shrinkB=0, mutation_scale=14))
        ax.text(lx + 92, LEG_Y, label, fontsize=FS * pt, va="center", family=font)
    ax.text(X0 + 5, 970, FOOT, fontsize=(FS - 1.5) * pt, va="center", family=font, color="#333333")

    path = f"{OUT}/arquitectura.{fmt}"
    if fmt == "png":
        fig.savefig(path, dpi=dpi or 200, facecolor="white")
    else:
        plt.rcParams["svg.fonttype"] = "none"
        fig.savefig(path, facecolor="white")
    plt.close(fig)
    return path

# =====================================================================
# .drawio (mxGraph XML)
# =====================================================================
def drawio():
    cells = []
    nid = [2]
    def new():
        nid[0] += 1; return f"c{nid[0]}"
    ids = {}
    def vertex(x, y, w, h, value, style, key=None):
        i = new()
        if key: ids[key] = i
        cells.append(f'<mxCell id="{i}" value="{escape(value, {chr(34): "&quot;"})}" style="{style}" vertex="1" parent="1">'
                     f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')
        return i
    base = f"rounded=1;arcSize=4;whiteSpace=wrap;html=1;align=left;verticalAlign=top;spacingLeft=8;spacingTop=4;" \
           f"fontFamily=Helvetica;fontSize={FS};strokeColor=#222222;strokeWidth=2;fillColor=#FFFFFF;"
    for i, t in enumerate(HEAD):
        vertex(CX[i], 15, BW, 38, t, "rounded=1;arcSize=20;whiteSpace=wrap;html=1;fillColor=#2B2B2B;strokeColor=#2B2B2B;"
               f"fontColor=#FFFFFF;fontStyle=1;fontSize=15;fontFamily=Helvetica;align=center;verticalAlign=middle;")
    for k, (c, y, h, title, lines) in BOX.items():
        gx, _, gw, _ = geom(k)
        html = f"<b>{escape(title)}</b><br>" + "<br>".join(escape(l).replace("   ", "&nbsp;&nbsp;&nbsp;") for l in lines)
        vertex(gx, y, gw, h, html, base, key=k)
    # orquestación
    vertex(X0, OR_Y, W - 2 * X0, OR_H,
           "<b>ORQUESTACIÓN · Apache Airflow 2.x</b> — cada tarea depende de la anterior (falla limpia si hay error upstream; 3 reintentos con espera creciente)",
           base)
    vertex(X0 + 15, OR_Y + OR_H - 30, 1300, 24, OR_NOTE,
           f"text;html=1;align=left;verticalAlign=middle;fontFamily=Helvetica;fontSize={FS-1};")
    for name, sub, cy, tasks in DAGS:
        vertex(X0 + 15, cy - 20, 520, 40, f"<b>{name}</b> · {sub}",
               f"text;html=1;align=left;verticalAlign=middle;fontFamily=Helvetica;fontSize={FS};")
        prev = None
        for i, t in enumerate(tasks):
            tx = TASK_X0 + i * (TASK_W + TASK_GAP)
            tid = vertex(tx, cy - TASK_H / 2, TASK_W, TASK_H, t,
                         f"rounded=1;arcSize=15;whiteSpace=wrap;html=1;fontFamily=Courier New;fontSize={FS};"
                         "align=center;verticalAlign=middle;strokeColor=#222222;fillColor=#FFFFFF;")
            if prev:
                e = new()
                cells.append(f'<mxCell id="{e}" style="edgeStyle=orthogonalEdgeStyle;endArrow=block;endFill=1;strokeColor=#222222;strokeWidth=2;" '
                             f'edge="1" parent="1" source="{prev}" target="{tid}"><mxGeometry relative="1" as="geometry"/></mxCell>')
            prev = tid
    # aristas con puntos explícitos (se ven igual que en el PNG)
    for pts, st in EDGES:
        s = STY[st]
        dash = "dashed=1;dashPattern=6 4;" if st == "alert" else ("dashed=1;dashPattern=2 3;" if st == "lookup" else "")
        style = (f"edgeStyle=none;rounded=0;html=1;endArrow=block;endFill=1;strokeColor={s['color']};"
                 f"strokeWidth=2;{dash}")
        e = new()
        mid = "".join(f'<mxPoint x="{p[0]:.0f}" y="{p[1]:.0f}"/>' for p in pts[1:-1])
        arr = f'<Array as="points">{mid}</Array>' if mid else ""
        cells.append(f'<mxCell id="{e}" style="{style}" edge="1" parent="1"><mxGeometry relative="1" as="geometry">'
                     f'<mxPoint x="{pts[0][0]:.0f}" y="{pts[0][1]:.0f}" as="sourcePoint"/>'
                     f'<mxPoint x="{pts[-1][0]:.0f}" y="{pts[-1][1]:.0f}" as="targetPoint"/>{arr}</mxGeometry></mxCell>')
    vertex(CX[2] + 40, 664, 70, 20, "alertas",
           f"text;html=1;fontStyle=2;fontColor={STY['alert']['color']};fontFamily=Helvetica;fontSize={FS};align=left;")
    # leyenda
    vertex(X0, LEG_Y - 12, 90, 24, "<b>Leyenda</b>", f"text;html=1;align=left;fontFamily=Helvetica;fontSize={FS};")
    for st, label, lx in [("flow", "Flujo de datos", 130), ("alert", "Alertas de fraude (baja latencia)", 400),
                          ("lookup", "Lookup de dimensiones desde Gold (join stream-static)", 840)]:
        s = STY[st]
        dash = "dashed=1;dashPattern=6 4;" if st == "alert" else ("dashed=1;dashPattern=2 3;" if st == "lookup" else "")
        e = new()
        cells.append(f'<mxCell id="{e}" style="edgeStyle=none;html=1;endArrow=block;endFill=1;strokeColor={s["color"]};strokeWidth=2;{dash}" edge="1" parent="1">'
                     f'<mxGeometry relative="1" as="geometry"><mxPoint x="{lx}" y="{LEG_Y}" as="sourcePoint"/><mxPoint x="{lx+80}" y="{LEG_Y}" as="targetPoint"/></mxGeometry></mxCell>')
        vertex(lx + 92, LEG_Y - 12, 520, 24, label, f"text;html=1;align=left;fontFamily=Helvetica;fontSize={FS};")
    vertex(X0, 958, W - 2 * X0, 24, FOOT, f"text;html=1;align=left;fontFamily=Helvetica;fontSize={FS-2};fontColor=#333333;")
    xml = ('<mxfile host="app.diagrams.net"><diagram id="fraudlens-arq" name="Arquitectura FraudLens">'
           f'<mxGraphModel dx="1600" dy="1000" grid="0" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" '
           f'pageScale="1" pageWidth="{W}" pageHeight="{H}" math="0" shadow="0"><root><mxCell id="0"/><mxCell id="1" parent="0"/>'
           + "".join(cells) + '</root></mxGraphModel></diagram></mxfile>')
    with open(f"{OUT}/arquitectura.drawio", "w", encoding="utf-8") as f:
        f.write(xml)

if __name__ == "__main__":
    drawio()
    print(render("png"), render("svg"))
