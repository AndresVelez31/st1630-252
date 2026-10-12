# FraudLens: Entregable 1 (diagnóstico, requisitos y arquitectura)

**ST1630 Sistemas Intensivos en Datos · Universidad EAFIT · Prof. Andrés Sacre Alzate**
Dominio: fintech colombiana (billetera digital y pagos inmediatos). Detección de fraude en pagos en tiempo real.

## Equipo

| Integrante | Bloque a cargo |
|---|---|
| Andrés Felipe Vélez Álvarez | Infraestructura e ingestión (docker-compose, CloudFormation y `sesion_aws`, Kafka, productor) |
| Samuel Samper Cardona | Procesamiento streaming (Spark Structured Streaming, reglas R1–R3, alertas en MongoDB y SNS) |
| Sebastián Salazar Henao | Batch y lakehouse (Bronze/Silver/Gold en Delta Lake sobre S3, Glue y consultas P4–P6) |
| Hellen Yanes Doria | Orquestación y consumo (Airflow, MongoDB, Athena, Metabase, FastAPI) |

## Problema

Los analistas de riesgo de FraudLens necesitan detectar patrones de fraude en menos de 2 minutos desde la
transacción y analizar el histórico completo para ajustar las reglas, sin afectar la base operacional.

## Arquitectura (resumen)

Diseñada para el **sandbox AWS Academy Data Engineering** (ADR-06): ese sandbox solo permite EC2 de hasta 2 GB,
no ofrece DynamoDB, Kinesis, MSK ni MWAA y borra todos los recursos al terminar cada sesión. Por eso el cómputo
corre en el equipo local con un mismo `docker-compose.yml` (perfiles `streaming`, `batch` y `consumo`) y en AWS solo
se usan servicios que cobran por uso, creados con CloudFormation al iniciar cada sesión (DAG `sesion_aws`).

| Capa | Herramienta | Dónde corre |
|---|---|---|
| Fuente | Productor Python/Faker, PostgreSQL 16, CSV de listas de riesgo | Local; CSV en S3 `landing/` |
| Ingestión | Apache Kafka 3 brokers (KRaft, `cp-kafka:7.6.0`), Airflow + PySpark | Local |
| Procesamiento | Spark Structured Streaming 3.5.1 + Delta 3.1.0; Spark batch dentro de Airflow | Local |
| Almacenamiento | Delta Lake en Amazon S3 (Bronze/Silver/Gold, esquema en estrella); MongoDB 7 (replica set) para alertas | AWS y local |
| Consumo | Amazon Athena + Glue Data Catalog, Amazon SNS, Metabase, API FastAPI | AWS y local |
| Orquestación | Apache Airflow 2.9 (LocalExecutor): `catalogos_diario`, `fraude_pipeline` y `sesion_aws` | Local |

El diagrama usa la notación oficial AWS Architecture Icons de draw.io. Costo estimado: unos USD 5 de los 50 del
sandbox, sin EC2, EBS, IP elásticas, RDS ni CloudWatch.

## Contenido de esta carpeta

| Archivo | Descripción |
|---|---|
| `e1_fraudlens.pdf` | Documento del Entregable 1 (secciones 1 a 5, referencias y anexos A–C) |
| `arquitectura.png` / `arquitectura.svg` | Diagrama de arquitectura por capas exportado |
| `arquitectura.drawio` | Fuente editable del diagrama |
| `enlace_drawio.txt` | Enlace editable en draw.io (el mismo de la Sección 3) |
| `e1_fraudlens.src.tex`, `e1_fraudlens.tex`, `img/` | Fuente LaTeX del documento |
| `build_diagram.py`, `render_diagram.js`, `build_pdf.py` | Scripts que generan el `.drawio`, el PNG/SVG y el PDF |

Diagrama editable: [abrir en app.diagrams.net](https://app.diagrams.net/#R7V3Jdts4Fv0aL6PDWdRS1pBKl524rFRXd290IBKWGFMEQ5COXYv%2B9sZEigNk0TYHy81UTkqkOEDAfcDFGy%2F02f7xzvPhhabsEI4v9PmFpoEwHLke2EZgj0cBjMmpC31xoc%2FESXKx5%2FJL7yKQuD4M8CcQ%2FaTXaUoA9pB%2FOY1%2BJl4MnTiJAPliSa%2B9Itdmz9s%2Ffo5AuLtGLvTJBe4jv0%2B1DYU%2Fy30SZzTd4me2Ufpq5XBi5f0tXqmmZxPPhVic46dihPzYC4snHRQEpIGFcyCK0K%2FiZXfIdwsnQrCFlRMrB%2FjVs395brwr%2FzD6xW%2FQ2%2B7i8g%2FcAOd%2BG6EkEC%2B80PQl%2B8O%2F3oP0YeJJeAdc9Ct3ivVshFCc9vEM%2Bn5uyPh9yyPfZg2PYBDXuSH2YgofetMD8BOYttryye2XG%2FJhSz%2FkBl%2B5mGkXl2PyoQQQl8LQBTGiF4UoouMDQoDTh5FGZM8TPz5%2BSns8ho%2F0%2FC7e%2B%2BSESj4SXPsz5JPn6PMABeS6SxxH6B6WTgLf2wbk0Id39AkPMIo9MpJTcXrvuS59yeWvHWnrKgQOfeMvAlz6DhTEAn2aKo7Tx5ORI%2F8tabfx60RjFXG8BHvPpwD%2FDfoPkL4z%2B120DfDx6IioeQmCaA%2Fj6Ilckt4gkCRkx%2BZHv3Iw1BQBnl0OgumFQEB%2Fmz34MPrkgwCAHAw42cjxMIdkmB3vYqZfXOpBOtZs9qAfYMBlgo286229mEgSg4pm33kBuXfHhNVH%2B40HAgKWSR5HDn%2FuPkxiRA59RIaPCzf5d46ce0ixNEP7EGHIeoZAB0YPnuPxN7LpB4PA3aBHisu%2FVvRfB7hwT6%2Bdg5jicxFsvQDCyAu2%2BbcHKAb5nybujpydwDZ95RfSGPz%2BQKtqI7MKW3NpXY6n%2FcI2nQ%2FzuDWluFXejtsNGXpFMomVhotNy9BNOyM3VHSWVpf20jw2XrkhbqKvjEJfjZVqX9mqVu0r1bDe3lk7N6r2lZqXiOWfi6%2FfF890H%2B0GEDnpzKkUO2iHIu9vgjTgy3taWVoL0zrW0yUsi8WziGW1JAV6y9hOsZSOVxXbhgTZqm40A2317dA2l5e9QDvrutPYnmiNYLvaWVoe21%2B%2Bfl6svn%2Bh0%2F1E%2F9oaxlXbXE7NM8a4jHhIQW43NH9rbwX5bDleLHsBuaHVncCzXn4jyKudpedBfnP7bbZYTa%2B%2FkHn8W2sYt01jqVyeMcYNrd5Enm1L34px%2Fa0YN5fGctYLxi3D7ngir3aWUdh8Xl1PZ4uvbYPc1I0ped%2F5gtwyjW4ncuPtE7m2tHoBuW1YdUFuNsPEq51l5kE%2B%2B%2FZ19ed1e%2BieTHTzrKm4bWr10G00wMW3a6GaKA3Z4mfihXnNBVdIbKOEnc1GU6LKuNDsEEUxpaNTNfb8VLVCd9PKZ9JhM9o7MKJKXswwEEHSs1SDMWPaxpipVp64ugQne8R0KyW4hMgLYvbDzUvyl7x1plyYc%2FoMcjSimCucKB%2BPiyfU6hF9RvFE%2BXhcPKGWH6%2BW3q%2BWG5g7UTkqPF4pvV%2FJNZD81S9REvteQNCeKo%2BpZGwj4HrwAPjqfHNKF6NLRMOhwkfeFYm3sJGA0eIB8gFR2DW%2BD0LsbTI9TQSdJMLeA7yFmD%2BcnsU7ENLP%2B8ctVbuPwC9sjKiuOWTNpyoq6bdrFKzDCO49XFVHEbEez%2B3JxJbrrsp6qRiFVeUVJl1CEHnFjua6IlFITa2ZTacZF%2BDdYd5vcZpIF7N0x15dAyeaZJqY6FYT04Tjo8StTBNcsTg7fDdI6CChXELJRw6aNWCmkoqQHiwRrQlp9orOhFRVyoq1qphaY7s1MY3g1qM2hpKc3orTSoI%2FQYDjTwXl6MvMDIOYD2J%2BEHMBOIl8K8rUmFqtyrdqjBeX07x8q63Lt16Ub1WpKhEsUyLg9qQBuu7cBWvv9GZUKpNSuZTJplQ%2BqzJauIxJjeQNquQNquwNVblXK2%2BoziZCGqvnZLNK%2BW5VcrdaupvJNL6HdE%2FCJUUq4EeN7MW96WKsmuNLmbDcsT%2FllSoVkyuwgf4Nwl7MZG2%2BQXGM9iflyIF0PihZc%2FOWVok9FeCQ%2F6w775G2Qz4zRBCjJHIgnxfInIJlMwRb%2Fu9QtAe83c2oN%2ByiVVaVbJdTK1Ve%2FvSGxC8%2B7VmSY8XL7NdLPUbEjVF6xiFbYep8wIz%2FMXDuU08EsiID5iKAD2b9d2e5V2WW%2B34cTkytjBKJPcOSwaQBn5MwQu5LNIZFhZdxfBDzGqvqelu0QadOXurIamrpU06ufIYm6dLsxrf2aZ2l7%2FVz9RGSVp7C52P6e45I0rubvcucUD02o2dkct2UHjqFRwoXdVxLrWk0BZZnJ%2BocZKyfCfVMZH35iVPiqaqPzPCR82c6vtODfGXXp3P2DXlZ4sTMOZApOuMIBBg4DhltiOvN%2B%2BkZQnmD041TR3a5cQLF1cY9xTu28bvQLqkbDKDaWnqHgr194tM1hf0DwlDoXNnh9OZL%2BmuED1y5xQxRzFXtH6tvX%2FO7yf%2BqI4VKvAIfyI%2FGQt%2FrOVRvbPKvyg9bCq87eqWbhD4RCpc9%2B%2Bnw%2FD3wGZVwEdcxY0iWDtbYO%2B8Hd7%2FLepp3Y9dumVx8ZYuk2Ml8pxfMtdbXPqMkeGqVIFmWRAVhNCV5YFuRvZyWv%2BY6aCqnFkJjYcoWwrltWdmGOXf%2B0jB1ZtzPz6KKZGNdmYGPMqI2x9EwS%2ButXbXkWZKNptqAIincfjz%2BYppWn%2FxlO7CXM2IvpmX0x1623XEXhONtBFd%2FXKUGWrKyohBGgLIXavid9ExgZj5FNcSZRXgPI%2BEjT5oaQb%2Fa0kOrVgWOswHMPl2I6WAHBwZUYTmPhMwVogRS87TrgcgDA%2BcQ65Cq9Mg5tlLGIUZqYBu1RnCiWL2xDQc%2FfER1iVHqUYknlq5K%2BIbVTJe%2BB74xnpLJ3vp4fGOTOKTn1r%2B8eLdGmx%2FkKbgp1JR3b6bZGfGgoOmKeVx5OAbZChx5EG%2FFxn62%2Bmf%2FtGPFVB%2FJhqoiglQJj3V205L%2BXWgXtnIxJb9s6YPAZRvbZbWBsxxfeWK0AYcM2w%2FsFHusT7uCRu3BbTQwivSGirrZmFQEQTMllMJqQBLuwd096H5NKoXYtK7C13St5hZ4rDXUqcMu%2BIx1%2BJqhdrYYcbQ0vRwJCavM91Myt%2B3onP87l%2FyDOptGIW0oTCI6Yf9%2BC8is2e%2Fa5ISfxPykjEfWSOHLJkEo%2BaAB6UaYzNmeUzVNHH6lKoARe9XvHB88QDYya4fvxqsvuIVMa5%2FfKRfCt4BzT7EA2BBnZ%2FdeMPIC%2FBQ4o4g%2FALOlhfOAG%2BlPyZtbPBfuQxTzJh0eG0FyKt8UetJlDulzkGkOQh88Sd%2FwGZBeisXltO3xJ585zaHAgTX7du36P%2FmL9pCc%2FcF61AseuFe87zHzxmCyOGKy0HSznvpAbWKvJGaawWjRgtFCt7XO1AiQKevij0fbypaL%2BrStiShP0a0DcTtj80WXxC3FS2fUzYvufPQr83G4eSJLWXTPV19YVuD3rVj4x%2FySejkUDC9MmWBd0CBj5TJCwd8ijI7ZGJDcsqH%2FDp9WhFqI9ylMa6IECXxAmV4hp5eQvqP80OvF7efFcVqlM2pFIS08P3g2I4ipvWigMnJLSH0qoxgNyt5gD2nYHqJLEmK0RWQ47eyex5SyX7SufjJ0pR6PIfvapnp1oDFnrH8y9ElnNEbApWkWI2SsanAQnGUVRwnL0Ofyg3T%2F2a81RDSO%2FJ6RmhGtOfSZ3UIfqVwJlRKFOPK2W5YVgGZ%2BkDmP4p8J3BfUbMLLs6AiKdGW%2BdUf1UfNoZuEXDmVU8Bw%2FvQLENjuedPpskz1TVI2dVsIh7xROaWieGeZHdnPYPcqnEypuWN7j1hLk9AFlC%2FJHl9I13Wj8cfnTEE55kbRvyfDxI1hFtizADw%2BGISB0Rxf9CFhRBDtUnKo0ElUcSVvLeZP4rarzNhUMbv9QOwHcU73mWaXlf2U2Q4692xCSFnmir7mR8JbzHxtuGMNd7AZSKFMv2Xoek1SOLGam8kGBVcLCq6M73fAC3MM%2FiPRwrJ6qzYtVJtIEMw6dWCFZ6zc6pIVcrR0TQozp1PSQxFKV%2B9M8dVvhBGIo3wSbdpb3BZpFAieiGBNeVgSy6KKVh6ZrKMS72NkRFAmak1LDZSr2VwTjImwkl3qEyy0WBLycs2o5YTQU4eZAW8M4cYz0W%2BsfFu%2F3Xz%2Fcv3lP1QX9p9vt%2FPFrdTgGSEHYpQlho8SH65J63HGPzHpvSqZGjRkyivJUBOZRoUAD%2FqxhvVjpt6dvzDZH9FdS9c8qJQ3s3X1mGXV5kFGQ5068KAz1o5ZVnc8iKOlaR4kJKzCMa7p2%2BaXwpMn5%2BfDfIZYXow4ZUU6q3%2Fhor4jrwlui%2FVFgE8GG%2BB1VmIk%2Bx1rz2Wtp9oqL%2BAKmnI7%2Fwwxuf24cY67KwWu5zBulDlqzVg%2FbWmI04yVVIqATB%2FnRJ4oeUML9%2BzBD0ROPD3riHWVVcnBkGDbBdHTTQTvYMR1l8x%2BiUMUsNxTA%2F05pguyrLr0R7Ubk9tBFdSCKmhsaR2qgpx7qvf%2BaByorAuqzYEmWlOd%2Bi7iptIEth%2BMA20h4T%2Bes3ZBDFg4bisKoS6JkMBMV0zoFpLFynermbl9cA93KMGwZ9pDhplHSpF%2FngKH8R7KRwgtANGRRGblamcVrRCzVUHCizjfEH5H9J759LN4GgrW7N0Dy5ApWWqzDNtqTiYGJUvDSpax3p2xabvGL67t8hJxqkYHH8iEWU7pSg9uQEyGgva9TZXLjUVpn64tYumyPra1Zjp5SOh6vgldj8S3f9SErjQAujOpkzoCGg3JXJ0srnvwN0o9XGSc6qB84ekA0vKxrPg05WNcC5K6SV2xM5NzqbiqL21LM7rO26qMa5R6kpW5aAIZG%2BEX%2F%2FEMC2apV%2B1qr6oT2SLXyBaJdeuQjqTtdCRNgcUqbR0m1a2DZrdTrVtApavddDEQpqcdczrrLAveElFCN%2FmzajtAGMLA%2FYQCMv9WvkR3d9wUIsLYJfeTLSjZSa%2FjYaucAX5SUh%2FZY0mhb62lashYuLx8uEVHs%2FpcdHi3DovOmSw6WcH4HhYdAZWuFp3Ux63XRSeddQqLju%2FtQ0%2B%2B6mAWHJDPzV3Jy%2FHlS6aa%2Fe16KnmGkzA8wEDYooVBfFiH0tVE6XMd2jIvxQ%2B3ChnjPlch2qnDGnQma5Bh97cGMaB0tQJxh2SWHoF6fbAqCn3nRbhjYevF9FSSNYRdVlo4Dt%2B63v6Qoor%2BPu4iPsllayzfEnt7UbPjgRkUJ4VousFZSe1zTQKh1%2F2SVCog3rqbr23WrEPQRCwg6dHBx%2FeMfXxtq7s6BBQrTS9JQriqSXwOBYey2Z1O4EuAY%2FZd3%2BvT58V3Hv59aB4P%2B5F52rKYJLKW0SiFqtss%2FZEHj%2BZDikT24DSsOov99tPYb4XH0tNeuZmYg0ftUY9a26pbfKApIZE4upCZACd7NLi6vNKZdqJ350xLbuDeiB%2BOa5TdaTvlGmm3DoTjjIOruyQcGWC6Yh3XmeD3yi3mBH4bBCI3q4eQxQgJjiGqCEi3xI6XuMAVpIPcGNBqiCzQhxUoytkUERUn9sBpvGNq2IFAyJxlOyUQB9APLKJxh9kuWQQQMvXx9BXGaRKR2S4az8%2FCu3Vwkj1fJ1l7ZhyB7od0khXzQDvloKR0TCJ6htGY5HWmAkr9bSvkpJeUhywzM0abCBZS0NwYXNNzYwrGdSMxKlzD4FBdUs0pbkSuvqwShaxUB9j4IKsN9Zl2uqbMAdMMzcj%2FfLSVdYNNWIRPSMRTyEwftAuU%2BeLq%2B5Q3l2fkUTeD2ihfYeq03kgztJYS0WzJyL6HnfGR2fn8Q01p%2F9I407UjpKYdL42JUY3JksUL6U14KgX4A%2FJLfXzaIJZttRovzE36dCCX50suF2PVHF%2F%2B%2F0RgBbgxqbN7Y5ZU5jqnlauvq545ZVopTJgQPx0yAeWyHTooiqDEbrjiyQQP5keaMVrmBMPUgMD3Dwy0mijy%2F5v4GZp9mvjJPL2aqFaeYFZJ8GT0YQB8USK3DmYLWuOOGWR2%2BbthkFLkNEct%2BRA2g8Vxye5pj6t0Ulbl1myiYJp6mvdAdwvTXkNRvENbRJC5OJy9PJoCoLIil4ByYJI00w0M3GkUIZp%2BbOMj556fWnp%2B%2BjhyJMbZKg27%2BgyEfYq0S%2BDcb1k7ZYz3KGLgoxf%2Fi%2F4uFh9Lj%2F6dtoV8nj%2BKn8wOntKDgAxC7iZ6%2BO%2F8d4fb2FF6H%2B942tvPI4nqJChNEKw0QsJpOwbRFsal6sVV2EXQB7H3UHyJDEMHAGV318OUNmDqvDGVq32dB9WhKFEfqDIHVJ35TLWtQipXsLMPTFkDpmphKoeoA75OYEotIGqkt4IpBz%2B0CioyIrREdHaB0AEdnnwjCuEcPLuLllJ1PMnzsdM3aJZZInC8Da%2FG%2BHjAeB2MqwWM2zXnzS4wXihqnMf5IVlJ9zAvBYWktRGXdW8Ya1azMNcHmL8c5hPjHfGDfM3LdwJz1VSLsU9FmL8dtcaA2ldMztYZoPaQ1KNz1OpWCbXCXtkYau0BtS9HrfUqSmG1Atp8mrN2QPsaWE0GWJ03rPKJjPKwShNL9AGq1LF7QNVLUGWY72iJzWWMbgNVL15fzXFxfTVsu9n1VR2sMjV1nfprdJ12%2B5A9pNLJI%2FYQH9A9ZvUiZm3h1Xh0w67Zk%2BduaADkg5noFcpX%2FXXaV0NrBeb5gJc80PPhtL0s%2BoM26MyNRbkCj4UZlKeD6QVTHetqSgWADpgy5JjCpJ9i6cki1F4LvKw9bwHeMyUdLFHS4YjOx3zVPsdoiZAeSm8VGKkomNELPDu2j89VXWPeVu1PedmrekGe%2Firgme1ssFPv%2FzzqMnfCHmAH1AF1DaHu%2Fa7Fx1Tc2RrdB%2FC0AXhNAa9oEnwV8NRubSsB7mMTPSklB1Wf30KXLp9MjOd33Ga6hMtvePOOG0U%2FXx86ZkrixYyltpjJhMWe2uPxsihRjXjtF11rFLUaJabaqixcs4nsDqT%2FhjxGZ5THSCt5YinauLNkvhQrNQpMfbv948%2FF6vt09oUGhE%2F0r%2FlkPdMQOLtC8I6ikemZRZFf0cqfi0foJCybD43vDmFEkEJLUXF1%2BbPpEo8GNuVzBy%2Bty%2FG0GsOU5SUMNjiUHfNUu3T2UlxIS5XAwPFYwBLw%2FTVOHAdizGPpkyANVeLlzwn8IStsyrIrAsz%2Bj71twjIs4nz36Gy%2Bp%2BAKYnaZqAlKAMhKhMLHkIgEfbOfvy1AsXdHMJ%2Bvy85TLBWjw%2FqJj6pdsEt%2FLj6yw3JdpQofRMis6pw8ViShu3oDkbuuuvZPS5mI%2FkZ47Xog8lBDMYBVUanEBdIQxYr8pI1QUFoa1%2FfhlpXGZeIwW%2F3zfHAoLRzXCxIr070pyVMtiyHXrSaAWDUzC7%2FNdYhwvI0gPsG1XlU7YWkuzOmpATgWGjcjVNuj9nrlK%2Fz1ogReTQyXVp45LE2SV1wdSeic1siIVVUXGAYYRWsHP6xJx%2BAtGoYsP2T6xBzVGrTJSJ%2Fk%2FtitjeAadqsFyAavrAXQG9cCZK96ixbgddvubDLL77mFvPSg6nEllmIfAXed92IbZFQEBY%2BtkZkXvhryqiojdVK9p3l51QZ5bUdeVZm8ar3Ja9X6LgqsZUx8ENq80I4Ve2T3zIb0QTbbkU1NJpt9mYldvc6GGUPsoWANfuGet8psd%2BwFnuOx7bEDXKrZoc3LlDfDRvnlGUVLya473irr1a2yE0EQrXFMJHNYGYpVUcvxxL0yOL26Z44gGbYkIsNHPoXAd4dNcynLmm7X2jVb45HV2rANi3sri7su2yjrvW2UdU0inluPjAERzy3LIzyIZt7iro1r8W7dGI27UGjpwwa5JTmVbZD13jbIWh0SzlP5rUMvhNSFoGcmLqg3y2S%2B94IkRnjg3g0YqSQFhttj3pqEee%2Bgc7%2FmjlZesF0%2FeA8DgTthqJrI3I6MkdLSqqBVSTerFe16exhgUSh5GLFcIUHNHik1xqxFdZo2MO52VnJNxri13hi3psllk8yqu0HLXXIVUtRRnam0K2WGNrDtlmRUxra1%2Fth21Rz1MyHIj5%2FWjP0MYlo0Run6yFAOf9QaItvVBlkbrFQtiazMSqX1ZqXSjEGR9QKRtSdKrZW1MzE1BjFtR0x1mZgaPYmpvz2txbqCT6R3j5TyGxRFx3XTJUWRalT96m2ZxqEBr3p%2Fu4ZKZXS6yn9RVzTeCvhqvF65y8sFNrgoiruei%2BTT7RNP4vJbedKrZHAdV7V6Sz%2F5gdLaPy6IJVvRdyBs70XUjBqypir2yFKqtLgN2VNfJHtNRibXiBruTTy1sTVqSkJ11T75rGZltKrDnWZlvAqVm2h03wb8oMYW0l0sfo3F8g3ieywqQaviQiLAmj4ZjfMCrLUnwNqLBPhoop8PJsCmbY%2F044qFVwuzpSsvem6zgl1VAN9mflAK7zXuIJUPOHUhC%2FsD93CHEpomTYSgFjwbB4k%2FpkjWnweSRPp1Qx9peek3W5N%2BZ0gRcDYpAiq7rLFk4ZBUQNWMZqBSTRFAYEAWfeiy6WBOJnymU2KF4NmcAX8mXohYiLzDA9s1O4v89wKXDBWbedgdPouDj72fCWTF4wMQgeDHwCeetx5XMFHVpJlae1vvGvW9h0rU77MStT0zjiiXP2QlapH9tSHjsNLrVIyrU%2FEKRg%2Be42V6lOlfK%2FLvT2p8UBy0YUlGOG1LMGK9wKdoDAJ3gx7ZHE3TsaCIXcpmaweSgyidmgt0j5HFmY8Sd4miPWBQEI%2BIIA2QGObsZxihcnrSHlvtTdpBFT0eHb7FTOPZeRaXK%2F7hyw3HCRn2qYppF2D%2Bxe2cwivwUhT8xdP40AW%2BBCvM64CTZX9Phid9CwfpDmC21NMTny8FqgKWbeSOwIheMn8KwB7NL%2FlrfyeTHfZEG65Xv6dtuP5rOiVEYUQ%2BXqV6CJayB1Kahkoly3lNctZakQxIqPsVOjIK5r%2FjZmIy7nHIxiN92GeWPL36KJofjTqfjt6hJKgyy0HBw7Yvy0EWQVTIyNOQ7YAcRog6Ch920HR9uEZ0h5uedD1AVo197iIyOqQrFv8D)

### Cómo regenerar

```bash
python3 build_diagram.py .                                   # arquitectura.drawio + enlace_drawio.txt
npm i playwright-core   # y una copia de jgraph/drawio (src/main/webapp) para los íconos AWS
DRAWIO_WEBAPP=/ruta/drawio/src/main/webapp node render_diagram.js arquitectura.drawio arquitectura.png arquitectura.svg
cp arquitectura.png img/ && python3 build_pdf.py             # e1_fraudlens.pdf (requiere latexmk)
```

## Bitácora de delegación

Herramienta usada: Claude (Anthropic).

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Definición del problema y preguntas P1–P6 (Sección 1) | Parcial | Redactada por el equipo. El agente (Claude) buscó cifras de mercado con su fuente (Banco de la República, Asobancaria, Nequi) y las incorporó. |
| Requisitos funcionales y no funcionales (Sección 2) | Parcial | Redactados por el equipo. El agente señaló inconsistencias (RF-04 frente a RNF-03, CAP por componente, RF faltante para la API) y los adaptó al despliegue en AWS. |
| Elección de tecnologías (Kafka, Spark, Delta Lake, Airflow) | No | Decisión de diseño central tomada por el equipo antes de usar el agente. |
| Adaptación al sandbox de AWS Academy (cómputo local; S3, Glue, Athena y SNS con CloudFormation) | Parcial | Propuesta por el agente tras revisar los laboratorios y las restricciones del sandbox; el equipo la aprobó. MongoDB se mantiene como lo eligió el equipo. |
| Redacción de los ADR (Sección 4) | Parcial | El equipo definió contexto, alternativas y decisión; el agente ayudó a redactar y a citar los conceptos del curso. |
| Plan de implementación (Sección 5) | Parcial | Reparto de bloques acordado por el equipo; el agente redactó el cronograma y la tabla de riesgos. |
| Dibujo del diagrama en draw.io (notación AWS) y exportación | Sí | El agente generó el .drawio con los íconos oficiales de AWS a partir del diseño del equipo (`build_diagram.py`). Bajo valor pedagógico. |
| Maquetación en LaTeX, referencias APA y anexos | Sí | Tarea de formato; bajo valor pedagógico. |
