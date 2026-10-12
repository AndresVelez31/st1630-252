# FraudLens: Entregable 1 (diagnóstico, requisitos y arquitectura)

**ST1630 Sistemas Intensivos en Datos · Universidad EAFIT · Prof. Andrés Sacre Alzate**
Dominio: fintech colombiana (billetera digital), detección de fraude en pagos en tiempo real.

## Equipo

| Integrante | Bloque a cargo |
|---|---|
| Andrés Felipe Vélez Álvarez | Ingestión e infraestructura (Kafka, docker-compose, productor) |
| Samuel Samper Cardona | Procesamiento streaming (Spark Structured Streaming, reglas R1–R3) |
| Sebastián Salazar Henao | Batch y lakehouse (Bronze/Silver/Gold en Delta Lake) |
| Hellen Yanes Doria | Orquestación y consumo (Airflow, MongoDB, FastAPI, Trino, Metabase) |

## Problema

Los analistas de riesgo de FraudLens necesitan detectar patrones de fraude en menos de 2 minutos
desde que ocurre la transacción y analizar el histórico completo para ajustar las reglas, sin
afectar la base operacional. El sistema combina streaming (Kafka + Spark Structured Streaming),
un lakehouse Bronze/Silver/Gold en Delta Lake sobre MinIO con modelo en estrella en Gold,
MongoDB para alertas de baja latencia y orquestación con Airflow.

## Contenido de esta carpeta

| Archivo | Descripción |
|---|---|
| `e1_fraudlens.pdf` | Documento del Entregable 1 (secciones 1 a 5) |
| `arquitectura.png` / `arquitectura.svg` | Diagrama de arquitectura por capas exportado |
| `arquitectura.drawio` | Fuente editable del diagrama |
| `enlace_drawio.txt` | Enlace editable en draw.io (el mismo de la Sección 3) |
| `e1_fraudlens.tex`, `img/` | Fuente LaTeX del documento |
| `build_diagram.py` | Script que genera el .drawio, PNG y SVG desde una sola especificación |

Diagrama editable: [abrir en app.diagrams.net](https://app.diagrams.net/#R7V1ZU%2BO4Fv41qQoPTXnN8pgNhpkAaULPVN2XlGIricCxgmyzzMP97VebHdsSgwEnDLdMP3QsS7Ks7ztHR%2BdIcssebZ9XKIAty9jgKG7Z45Zlgd3u1EdgTcA2Og1hTJNa9qRlj2QizYx8kXVFQOIHMIx%2BAPLA8llGCLZQ3ByQhwTF0IsTAuiNM5Z3SvNm9W2fzwnYbS6xDwOawX8W5cyOYYi6%2FBeZYqQpa5I%2BOpcwR3%2FDNKNMTZAPI5kmkmKMgxjtiokeDkPawEIaIAQ%2FFbOtcOAXEnZgDZWEuQcCNfUv5Meb8ouxG79BtN7E5RfcgjS3TIg2wMdPuSTedQTjOO3EEQyCHCai3Nkrd7OWERjGVQp4tijxCIJk%2F3ZGa2S1hl0G66%2FJ1e1EtjV%2BSXuA4CT0IavDbNlDQDwJEi1pD582lBfzHfBYyhPlAE3bxNtA5qaMDEY4wIRXZVtD9o%2BmRzHB91B3Z4XDOJd%2Bxv9k%2Blw2ykyvRUNMV16fgS0KGNF%2Bg8EjjJEHWIMDtA5pmkd7CdJah4%2BQsFvBQN7YIt9n1Q5l79Db8PnVLjbznId4C2PyQrM8F1mbst0Vl0975lg9R6RtcqSxe5KwkqzrrOI9nPSHRPQVdB0FXSuP7sXV%2BWR%2Be9Ea2a2%2BfdWg%2FAmUbdv5OphdBWY7D%2FPs5no0mQ8uL6gsXzcofwLljtv7OpQ7CspOHuXB9HIwmlw1MH8e5n7P%2BjqYuwrMbh7m0fXV%2FNdlg%2B%2BnhmTb6HwdwD0F4JbVCegThkv6Y81%2BDHY7Zi2ygXloPyJWnNVPhXx2wSxnKKxMHKUl6d2scFYbSVMmj7RjWGZRMiYgjIDnIVF%2FSJPav8%2Bvr07Ukv81T7nxasBH%2BghWQ3uHPEz%2Fdws3NEXnaJsE1LoVhjhrMMF%2B4sWUM9SuBPcU5aocdt6mcMqfAK5iDXtizPJHtDgK11OeZ9zbp9yy2%2FwxrxI0z2RHKxv8L7uTzgwsVb4yaanfuuwa1YhspuU%2BxeT%2B20ye0YnnmsD5zynNaHYYgfAOEtrrOATBSTX%2BjkDMqDowA7ze09gLEFMWEX97pYxH34fQp0R55ekBsmaz1SWIvQ2rBwGCQMPD2nlYWaP26%2BBh%2BvB%2FIuIURTHIuEMQjNZMNbVH8z%2BrsjDHqBdOnmiHIxSjR54EQ7VIwB7K3CZwzb0kGRFpX0c59UslQl75DOWGj3Xz0TkuH80qQzzwNoyKf4DVPeNG%2B48bQPvfotUbbPq2ZL1FoorkpEghLz%2B24xAWVJ8p%2B4%2FCdqrhdgAeIX%2BFhVSr%2BbKsOYRr4P4uoLBHagXAu2c9BnhHZAW3KDxFYfQSeqcEyqIMeqX4hQ%2B3OxyXH0wgTcrbKSzR5y0Z61oxiH8EEETxDxx6hYpGMzXzePqzZQ%2FKfbbwg4dGAOt3iFS2TLq1SKBVQQIRWQX4idvWQ%2BYmeaEoknsmivCZcsKLK8reFDKuFSyd9u%2Fj4ehEDhR0iNHY5JFH0JIXBOQhYY54PoQYAQh9irxaon2Jwovrkzytuf3ChrG9lc0fZoQJfMQV5IO%2F%2BOXk5nzScL5%2Bztu9yua4Wwfp7bdJn1J8HpOExW4owPwC0m7ek64K39NxKyMjrTmCLIXNJdVSItVA4aO05RGfG7YmVqvXafVZw5lGVtU09JOdGJpyiprJ2F%2BXbFwzxDhzops20GcAv%2FQQL%2BF4wpAPunNEWUU0hW8JWq%2FZHNVg7pfCUBrxeumPZOcDNmCVy94UYigzUwwzbBIO%2BFNd0WTay9lgq1RRcNTPLFFFzuTcC7lu9kP%2FB1smZeEy2lW43lKB4mik6bwWm0cGumLuvj1lloDBB2Dacec48DXddlP0PNui3akdXLK977AWt0FAhZBJwoD3cUSvWBO442BB2a4CvYHe%2FY5WlilRrioblVa%2Fx7vqMG45tWg0p7pGS%2Bf0bTmOVx280%2BG2oCSGBId%2Fw1Ki0BU6T5dIL2Rm8sFaQ4nPbkXU2N8CnZ4BUSYZPOAOxRRg1nfFD0YtoeYUWYNULD0Y4Uw6SBLABZ9bYs1sdEy1ByDCJSfyjwfnTLNlllAjL5%2BWl37vbRPA0ZkAdi0C474tMMOb66v%2FTPJqegwDrp6n4B6WNGglxwhJGKcYWcFuB0OfmpeBZlCaohDcQaHW8WoVcZs3MyO4KUqxh1G8iKOGi%2FWHsarq7l4tTOxUUN0X0z8nNzUycYq2OySpGKMdSGnpSxuyzac6Gi08BiI%2BsqO6kzmmue9ETKg2INpoOJ%2B3Ilmfx2AZZIocSAumIXH9sdh%2B1TmVUQuNu2%2FT%2BPx6Oq6RxPO8xSBoGCUB45dmnrACXrx41enH72ZkLJf10Tbn7WvPR2Or5F2g96XTW1%2B6OB8pFIwR8%2BjpixG4ZrKi2P10Qkpp%2FrcMGgI%2Bx0rndxrLCW1lNPORrzDsG0XnYCN7dcuebR%2FXiVchUn6JwzUeM9OhLT3MDHWY86OH2MdVvehSVBapIW60l%2BAOcKccd0TrDHjuyJOrYNlKzi24wzThhQeXZic690m6aDaCVLZ9QF5mBK4gEc6Y9kBX6JdmGszeMGKSYdDRjb26jxvaH4D2rl0xeGRatQw5FcLql5COByCC1WhN1eFmiQHxo%2FJMU9Dptdi5oJiHEuFFe%2BELyUkTnzzEEqSjGudWhYD5LUEh99JtsVizIxZdSGOkokIV0ZAILwlUDaNXfYiQWhUhf9zMkd4QV%2FJv1tEF5tkyf9lG%2BoRFwOtvSFr%2FOrnjGt9WlTj6fkVcZuga7TMQxfRORZaeT27FCrt9DWKYzazjkVpIGrGclSvobTQG7V9wucH4njeNTQQeEsRM4qIKVh2SwtTIXih1Bu6d8%2FKVhDe%2BGfIPwfQjj%2FlWhXj19c3PX5P57WC037CRW%2F2dLifZR7Wt02c9%2FYWf2mj1nf00i845oXBfMAceTEnKGQ7YklzEBaK9AgFPC5inhduwiPtIWF9SC5ZnSnYRDyhKkvJ1HIj5SoSPRbhVqI6H3Aj2CPS4lJ00RD7AelCVxqab%2BqQLAZt6NLYagp6wDHd4mVIq2gebKRUIkVZFjMKEmxtpPCeKAZ0bBXRsFx5jk%2B%2Bfi2IRaeHeYv5imIhlUpxsScinVWG0t1j2AkE7Fq3EDPEh4Z6ZSKwr7bPX9pgX41ThIIUifhe1soXqlahj1wN7qoQk7D1N1IGqOB3sTh2oVwjTjQfnXNvEgC%2FiXaRLV%2FQKSrvAhQ%2BiQQDX3D%2BWZKtdjouYcxDEuhr737U0gDl1eFYsNUwklzwtZBi1oibmmzre2l2S79URTgjiInwFnzQ9W30fyOsK94Dq1TVK%2BrVrnmpM4jQMV%2FveEUuNqgQY%2BIuljFc3sGlh67ru18LWVaCB%2FhqmO6UwiTdUKYYgmOxTh9QOG7Dd6jTDMsDevUg6Q0GK0HtMDvF89tB%2F7kI2WU%2BIB0uKgpqHaxgXSKh2NoEBoINo8Qmf6zfVA8yWMNwvIrnkouH7K1ag%2BcV6qv9dCd%2FREb53LMLbRlVLSrgPFju0gwEK4VuGVCmalsSaUMG3NJx69jENJ1v1SK1x4FNrdksnHTL%2B2iilSrZTzz6uTrItPXYsSN6AVtFyOjpo9jcdSFJNURhIBAWPMpCoU%2FIHtrgifll4bLV0o6Yq205Hp7z7XSlv6SjvHI3y6tyYwBWB0WaxzcLlDen1pLc6X6znv%2BsMOVW0RdIfbYacdv4r%2FRZy9%2FlwT3OjyOEv6cFP9IssOpN7fdIpQb90%2BFY5JicQk6Xyx%2BGVKlJO8SpXJFBWKuIwZe9TEbl%2Bg5yCnNUx6kHOMashR%2FuQxy7TbHwXWfR6g21T3%2BCzivnTdu2JI1rwYRql09qGRvlero1G5YpqVACO2SBndcxePapbOZrvgKrbsRrkFOQc54MyV94yp1RUJ3J2gxxVlqUtNeYHkVP25pgHRM5pkFOQs9yakFMqqhM5t0FOQc6x3Zpkzj6ModvvdLUNPquY37TrNnQ7DY2UXra6NdFIqahOBdBtkFPlyahJdSsV1Ylc411iVo1VPtv9g0Kn1tQ%2FIHaNf0nT41a%2FLuyyzbAHwM5tnDo8JtEpmSAfVJnK7g4rtcZqNppMq6dv8lnVAmnLajOb3MbLRAs4Tu%2BfFW9lKik1uYcznNzGz6RRA26%2FV5MaUGqqE7tje5qGttUxJ1rsfBBtsiA3u5iBOIaEhZ3ZiezOl7oRu0VMbLOuAbpT0Xf%2FXiVfbnGnb7yzYcUCNej4Y%2FvGzDP3bNB7L9csvhvwK%2Bdg5alT76NzsLIfrncgJ0yqRUoNPquYP21XfUxTNw69dtaVupo4%2F3EYS%2F2ITKbAKq8mLi1PPsDxfZ2OoyyC6Wp3T9bxGa8KR6ZN4QuVTaBf7v3e9dzHXLZdWlTXt9SNqf1D7U90Kzmg%2Fv8sJ7vU5%2Bkiq%2FeqOwW8ckV12k3qlp%2BzILnLjk3wwYe2Lhx1h4Jlvcl17RaFesjePzjZv6mp6Rg1yUNmNxxBHjrqjqBBdkpJ8SwnzWFl%2F2YxcfpfKSYd8%2BBi8k2t5J5Tk5j0rSOKibq5Z4rxPT%2F1VYwb%2BR1aNCniqekp3ewk%2Buxsih9RTPvP%2B5eLT9%2F%2BUvFRD%2FrIHVyEl3eQHwrKT%2B6A%2FPODpfPRs7ON9qZsdp6R0Z713cJRpNoqJHi5Ckz5ZQX2qYJBsAUeDCkasigHnLcMy28LyENZjbaHtzsKOT9h1Zjb4swxfsAEOz0y%2BzIL9kBwwuq%2BzaWOqS7gC%2F9HtBIcwdMD0UYzYxJ%2Fh7HPUzvs7YNj3k8nern%2FxLmQ9v2n4tPE9Hv0%2B0z8W%2Fb25H8%3D)

## Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Definición del problema y preguntas P1–P6 (Sección 1) | No | Redactada por el equipo; un agente (Claude) la revisó contra la rúbrica y señaló ajustes (p. ej. cálculo de volumen). |
| Requisitos funcionales y no funcionales (Sección 2) | Parcial | Redactados por el equipo; el agente señaló inconsistencias (RF-04 vs RNF-03, CAP de Gold vs MongoDB, RF faltante para la API) y el equipo decidió cómo corregirlas. |
| Elección de tecnologías y arquitectura | No | Decisión de diseño central; el equipo definió componentes y flujos antes de usar el agente. |
| Redacción de los ADR (Sección 4) | Parcial | El equipo definió contexto, alternativas y decisión; el agente ayudó a redactar y a citar los conceptos del curso. |
| Plan de implementación (Sección 5) | Parcial | Reparto de bloques acordado por el equipo; el agente redactó el cronograma y la tabla de riesgos y el equipo los ajustó. |
| Dibujo y exportación del diagrama | Sí | El agente redibujó en draw.io el diseño del equipo y generó `build_diagram.py` para exportar PNG/SVG. Bajo valor pedagógico. |
| Maquetación del documento (LaTeX, formato, referencias) | Sí | Formato y pulido de redacción; bajo valor pedagógico. |
