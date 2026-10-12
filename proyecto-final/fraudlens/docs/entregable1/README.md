# FraudLens: Entregable 1 (diagnóstico, requisitos y arquitectura)

**ST1630 Sistemas Intensivos en Datos · Universidad EAFIT · Prof. Andrés Sacre Alzate**
Dominio: fintech colombiana (billetera digital y pagos inmediatos). Detección de fraude en pagos en tiempo real.

## Equipo

| Integrante | Bloque a cargo |
|---|---|
| Andrés Felipe Vélez Álvarez | Infraestructura e ingestión (docker-compose, EC2, S3, Kafka, productor) |
| Samuel Samper Cardona | Procesamiento streaming (Spark Structured Streaming, reglas R1–R3, alertas en DynamoDB y SNS) |
| Sebastián Salazar Henao | Batch y lakehouse (Bronze/Silver/Gold en Delta Lake sobre S3, Glue y consultas P4–P6) |
| Hellen Yanes Doria | Orquestación y consumo (Airflow, Athena, Metabase, FastAPI) |

## Problema

Los analistas de riesgo de FraudLens necesitan detectar patrones de fraude en menos de 2 minutos desde la
transacción y analizar el histórico completo para ajustar las reglas, sin afectar la base operacional.

## Arquitectura (resumen)

Despliegue **híbrido en AWS Academy** (ADR-06): el cómputo corre en contenedores con un mismo `docker-compose.yml`
y los servicios administrados solo se usan si cobran por uso.

| Capa | Herramienta | Dónde corre |
|---|---|---|
| Fuente | Productor Python/Faker, PostgreSQL 16, CSV de listas de riesgo | EC2-A, EC2-B, S3 `landing/` |
| Ingestión | Apache Kafka 3 brokers (KRaft, `cp-kafka:7.6.0`), Airflow + PySpark | EC2-A, EC2-B |
| Procesamiento | Spark Structured Streaming 3.5.1 + Delta 3.1.0; Spark batch dentro de Airflow | EC2-A, EC2-B |
| Almacenamiento | Delta Lake en Amazon S3 (Bronze/Silver/Gold, esquema en estrella); Amazon DynamoDB | AWS (por uso) |
| Consumo | Amazon Athena + Glue, Metabase, API FastAPI, Amazon SNS | AWS y EC2-B |
| Orquestación | Apache Airflow 2.9 (LocalExecutor): `catalogos_diario` y `fraude_pipeline` | EC2-B |

## Contenido de esta carpeta

| Archivo | Descripción |
|---|---|
| `e1_fraudlens.pdf` | Documento del Entregable 1 (secciones 1 a 5, referencias y anexos A–C) |
| `arquitectura.png` / `arquitectura.svg` | Diagrama de arquitectura por capas exportado |
| `arquitectura.drawio` | Fuente editable del diagrama |
| `enlace_drawio.txt` | Enlace editable en draw.io (el mismo de la Sección 3) |
| `e1_fraudlens.src.tex`, `e1_fraudlens.tex`, `img/` | Fuente LaTeX del documento |
| `build_diagram.py`, `render_diagram.js`, `build_pdf.py` | Scripts que generan el `.drawio`, el PNG/SVG y el PDF |

Diagrama editable: [abrir en app.diagrams.net](https://app.diagrams.net/#R7V1bd6M4Ev41eYwPCLDxo6892UnSmXbP7M6%2B%2BMig2Ey4NYh0Mg%2F721c3sAD50gnGzjTd53QbAbKsqvpKVapSXRmT4OXR89EV0DZRiq%2BM6RUAMI57rgfXCQzSXogwaboyZlfGRDSShz2XP%2FqYwMz1UZhew%2BQbfQ5oIQwQvzlKvmUeRg7OEkhuzOmzt%2BTZor%2Fg5VMC481d5CKfPOC%2B8Pd0y9B4X%2B6raNHylnWSf7XUsPD%2BRvmDojXzXJSKNt6Eo8jHXlxudKIwJAMstcEkib6XH3uMfLfUEMM1qjUsHOjXW%2F%2FtuXhT%2FWH0xi%2FIW29w9QeuoPO0TqIsFF94BYw5%2B8NvBzDvTDyfbqAbfZea2MwmUYTzOZ4g35dIxt%2Bb77hbDDxBIT7mBexhyj70pWfoZygfdd8nr49X5MOafpCIr11NwNV4QD5UGMSlbOhCHNGH4iih9IExTPPOyCCK%2FsSPx6%2F5jGP0Qts3OPBJg04%2BEr72J5FP%2BjGmYRSS58YpTqInVGk89jnoe%2BuQXProkX7TM0qwRyg%2BEs2B57o%2B6y8KseBIoIvrvCtCTX1O%2F4r2OQw8nzL5L8h%2FRrS%2F4rfR%2FtHLTqroshShKEA4eSWP5C8A%2FoaQH5tffZdYUdcEA20kNswfhIL910XHWw4gHwQTqBkizVZqnpgiQmrHu5oYV2MjzOnNEIR%2BQCGXC0Z911t7mEgTYxdgP3oheXfDBNaPgpUHQ8IwQ5mXCMkQmcxwzd4YM0nC7JU0WiX0C3z4hDZRltLP4yQKCYXAfOGReSecNv9ERVzqj8BH7HtonTFspIMeTVcEa6J8qKN%2FL%2Bi%2FDnRR8MqHmaXXCKb4Wmdj%2BwgMqht1BrXm1swatcugRr%2FOoYaSQ7X3c6gPQ6QpIKtCMAbCyBXEgokj5syokIRM2Ww278%2FBLrpIJG9%2B5vr1mQNDUJ%2B4Qf7ee2Zu4yb1idNlsZn%2FPrv%2FOjswl983BPUXMXTo3e9kCbBbLsjkavP%2BzOrXJle%2BU%2BFgoTAFp4tR6BXOt07M4cAs0Wlg1unUt%2Bt0MsxmGFxvmsGN%2BagdBjf0wVk5vD5zQObwm%2FtPs8XXG6oRhsZ9o5yu29Z8ZKk4vbhziZxu5NruTKwOGmX1%2BXhuzlpi9T44L5jXZ86QWf3hy%2BfJbDG6uyGY%2FrlRTrctc66NVZxe3LlETu8b5wV1o1lOB8QQGbfD6UNwXlCvz5xZskRv70aT2f0pWN0yzBH5DgWrF3cukdWHxnlB3WwY1GfzudHWAt04L6rXp86SeX3y%2BX7x%2B12zTD4cGpZ6jV7cuUQm14F5PkCHcbzTb%2BZ6zyXi9L9l1KE3pk6Ea2Hnj5idX9zaesYK15uiA4dTYFS1nq5TNuUj3Yxf6n2O6FC1gLtunj2fuTvmFDcfbnJHjnDdqFx1YkBJtSWNYagcZWlEoGeRMZWGDkb0L2nzvRBd54QZ6T3Dqg9%2B9ky4gXuV6DhxAsMUlhxRzJ%2Fzr8Xn%2B%2FpQ%2F6f3NOqH0BAhyFy4o2LPoW4gq3JrWH994QWZD92IDVuLk8jNHMw8mw%2BveBOFhbNqDp%2BoG6r6vsMeQamDQph42x9ReM0oM7tZ7BORcEvTz%2Be2PN%2BkmTHWDhfqLjy1DsPBAdcTjmJObscL17fsmelw2%2FKV3mZfIxq%2BCEnrK5A8x43d3gB%2BJ%2Fe7g3btfD1XVIcgRNf7jWDIEsN1DUdmE3A9Op7A2l5wn4G5NpupphzMBtOBXQf3UnsO5poC7HO%2BIfyNCf8fcFqejoRaiYaAKJ4qDc2%2BgoSD91MwXn8UJfAQpXidoMVvt6RTyrwUCqMYJURkoxD6EgBeEvxPfI8yVyrA0iEkShwOpWT8CfLrw98OdUJGgkLkMsj%2BxvYCUgbpbD%2BBbiRMc0hml0ynVzuZvRCdU1I4%2BdaE6xFUhx1qnx21Qf9Y1Na0JmR%2BJ2iPGwPtycxWeGyp2Tk2xyqzs9T%2B8UDbyA3WFkDbSZ8%2FCmrfeimGxbox8VC6jjj0TRZ%2FXCpkSxjNFrheGkeph71n1sSW6z79XTTcBK0TqFh4M5MkW9GFcbHEXxgMp0OXbRPP62%2F9gZJUgukwQ8%2F0O4iOE60uZd8Ors8N14bdbxGuibQr8ZpHAPBAlSyNmkJuMvP6VOlLGdv9mabVkbvU%2FgGQW7NL1DT7Ro2aA%2Fs0yP0EH5%2FgWbG7tJV3yO0CnQ0Fsl%2F5qDWKXyvKFIlYvv76BdLBXCSKE8DxHNnhEoWoFP6lC7YhnCXu1ZbfPnxGjHpLhy%2Fj5fcTxPwe8sLaqPcBnSfKL5CxQfFu4IU9L0xfQ6cnukkZjHHN%2BKA0Bh4k943noiCOcH1IpEkeEAslqnf1CZJJwTyqCHLnEMTXPoseikKC%2F7U3lLO5dP1vfMhe%2BMx6032P%2BYEUw78J4JqpQSe%2BFmKgDXr9nsZ7uIWUdQEsc9PPp%2BEqm%2FBtabjqJvrxfqRhvyFY7DxJ76aiZVplu3LYnicJMTcD%2FjDKzUse%2Feh74QF%2FeCW4kDxxMEJVl8lFarh%2FTccTOnLZMXY1A1dkdUr394ooU%2FqLmKsnUm8SoDBlSoXYJ0wVMMuMWR%2FRjh6rXdzNvnya1dUSsCtaRfHtXxANrhU7JKXwD5RS5x798BITbUP0GvWSdarh3KrByD0fB1WD2W8MVjqPVePKQWX3nEo58PD4s%2BqGUmTXft2Qa4IFTjKaHYJcflFE%2BBs96yJNnsRbr%2Bk%2BrkYjLkrGDt9e1krr9Aq4T29%2Fq%2F%2BAKXKzmJtA0tKfg%2Ft3SLg54FNFM5%2BoVaPG%2BFKM9oPO1QzdF2eJFGy07F2xra9L13YWuxCrdQf5HwYUeMNVGu%2B7Zk%2FTb8GE2hp3QMq3roAJ6G662FEXyT%2B5yVf7OaWA3AfAf47kJZS0J5W%2FgNA4eu%2FoDRbxO%2BDb%2BAFyqTJnWVuay%2Bfpr4jNGN%2By5%2BkkKlKUIyyFxs%2B9mTUnLe9UtTO1Qc5TTO7ikndTskRJjwm9CTMcBZzpMAtc%2BIk1eCXutC0NXo0bPdq4I61NYX9n3b2bjLZV2XIC7Vl3Ysv4Y%2BnvfJ%2BbAJls3lFzb4p8DC92%2FylhGYMu4YskymFZNljtGOJk6%2BXjvjMT7ogB4ymGJV0v0g0d6MKS8q6pfy8g1mHuOl1MhItyzDjM2eRhDML%2BU3z7HUP%2BIU6Yi5NoHJMNQ7saGg99WWF8fvh6c3fzX2pF%2Fvfzl%2Bnsi8pUJKrPQXSrQ6joJPPR8pnumuVrkpTMeV3%2FdcrmfMrGGBzrSbRAQzjVGYuN6xpL09vTNcLLdEZlU8qN2K9sxl8%2B3zPYKqBM6BYCy09IXh1foKZJMmoGTnjoGArd6yj0X%2BXfEj0%2BpvQIjnwPsDpeAk5kpb%2FEJWPTgVhYmdG6iKLwRSTIT4zIlVycthC5mktz9PLfBk0JcxfA0GTaiA6MMkFtvbUIhlQsJz8GOC9ubv%2BgS8mPCM63XhB7Ap2xF8McqVPm55GzLvYvwaf5kTZknUwjjdlSnruJfrkbTa4Xv4yugdVX%2BHgyxpKI%2Bej4C0Ek%2FFvFfJL%2Bkg7XLwLXizC1w7kfoCkg6ID9lMBu6O2Fpq2ZyfwxYP3T59vpxwT1WfotQwHMB4moc9SnuRsTORiNYb0QoEfkbBQL70e2Kborto3drUDz9l3XC%2BSANmBzx07pQCv2jAiEVvdQ3vAovYg9GgdQbU3Q2lf8kq9eIBIjn9k5fEO%2BqcGD04R2g5nr0cg3EbH2k3t2LkTfGNaxYdB9uxl86rTNKbWNysdzKm3juquPomxGAfybLX6nryEMoulYpB6G1y5B8tC91PhnuOI5gVwLLPOc7VpY8a87I5wX9BZOhSIhA9iB4A8ZvsEo4FaC6%2BUJjXSWHBFC7SIaK%2BCFfKNZYaXcFudwIhoYkLEOCDOzn0C6y%2FOISJ%2Bjh04BXIQCsEz7SAXQxMmeBDI6%2FD8l%2Fvet9qwN8gJcwfS8Xv7S4TD7lcBdMd7LQ3qySt6sIpi49fM5RCRTsY6fKM748DIXusJJT14M%2F4LsOO5Ezg0vpaQjkYRSKMYR3jAn0U%2BMyJUTiNpC5NoJQkf79ptI28iFuNtvfT8hzX55w1VvMXUDCvn9GFBcgx0RkPLJZwdkTCCGfrS%2B2CV5ug2%2FIctpsqxNoAuLvHL%2BI%2BrDtImk%2BUTQXmO2QqdD1Kaz26%2Bj3HNUjwfSV%2Bp4IJYnIlbWDyZ%2F%2F8ESCqAclFOEoIqp5JFCaYf0F4D0x3tfmji0nENEt%2F5uFPJBJRG9xfU3jL0Pg%2FfbQ%2F4KjzZ1A8xhitm9i3S%2FfJp95fkM2zFX0gomXAOoHCqUS8hKfF0Ef9Yd6j5CJZdUzZ8CWSr41gNEwX1oqUDd6ED9UkD9eI%2BK1sgRoF63dG9%2B6d4H7R0FlfJ6Vh9q3b64X1zygSECsK8LJ4ocOO%2FQ2HzFhihzV%2Bd%2B6lJhIB6QvtUC7FSnDm0vAW37x4amD0Ejktotn0%2B5fB6Y7W1foiU9%2Bvbp8CHxyF2jfO6iBG%2BiNd0bm21bx2X6SgT9kcPPUOiOaN1AcrnyI%2BeJN809P%2B%2BMXAmSVhWrokZc6Xw%2FmfI%2BXCF%2FXJQI3HWeupLk6MXD%2F8lHQz7%2FST5rPVNcTSlVtfziNb8ICV3%2BI1%2Fwt8gaIW%2FYvsiu8jc5Iejs72cvCtBZ4qDKmegYJmuEK2d21VmRHt%2BKvefyd6j4astUxdtH8lm8Xr50bPZeNrPexGbGSZgsP3JZ5jHp8Jz3chkhEHyVHmBJ2nLR0weRtV3Uu9IrMYegXHHz0PO2WUFOPoK3szw98LDj%2BTPx%2FOAkPF%2BcWHuhTG%2BKCpDHMr1pgoaZ%2FmmZ4nZ5fs9xQs3yfOlUrtPyvPFGoNfASdheOu5TZvztaThnWVK8LFdJx2rvZTXwNng9zcK1dPiezGrbXNq2IbZaXNDoD%2FdCbP15be%2FzQ2C963ndNhqG8BS3Llh7cvybFazS6RunFSwdvNEoPI1oyUeXnUay3spsyioAHbP9ELP1h8M3MVu7vLZNuj0Lr62S1nltT4Rzs7xWSgRomte2RtifEucd4rQtc%2F1Z4q0mOU0%2BY%2BOCOI1Y%2Fmu347R%2FEqfJBwbInJbnm54H0Vpns3%2Bi8hy80QY6jbEtHVF3MXy2dpcQd3j2brfO27w6p1mkbRPlZTbbxlCfhdEgXgardhltzy5%2Bs4xWijE5reLU3qQ49dNsRkpR%2BTKnyalTZ%2BE1l4CaMny0Q7UW9r7N06BakZBdAjVO5nM5ONyW12gTzRiCNtZoxRc1yWQuTDfFz6QXDxBjlIQsOgto5k4%2BHL5NvZqtukAEf7btxR7qFS%2BzsX93vPq8NRw272VWB312gnEhgtGybzBMP6JgDHS7YcEgtlbbW%2Bj9Mf1bkwtdsECTklF81ZkkQyvv0Ry7Th62sFBWG2TNbbL%2FqGjYtt3rV87AbnavMUq%2BHWb0XYHDpiJY2JyD2UTpthrZg8F8B4dLotFIeHYZIIb1%2FEbdyh8qBfQaDeSykyld7i5etzqcfmBWkw%2BMqTEx7Hqewecvv%2F0%2BW3wdTW5YSRfjXk4XKGqubo%2F7B72hKJEZOQSoXpCTsfMXaP6YyNTZmwF3ZPZEdfjW3JpZI2VqyP6yNeJ8f5PlrtFDpVkeWsqSIPxlmjkOSlNedyYL2Vl1vs8T3bCHQvrbfX5eKUzZ%2F6m3zljKXOmEO4OJsVQ7jh9Qqi4at30tjLD3WKnaymPvy1kp5aSMimTRBJrdwfch%2Bd6aKInGY587kKXRTp3kMobZWj1ZQjfzAtelqjENpKa5%2BtJ%2FjzQaqgyk6egTP50csoPJl3lRxOZzkfR6LtIuecoHodEYtBGrP8UThqgg%2BGgNk1xQWF3GjkN3c6ihKEyoYtAm9IWrazX%2BzGsVxrwiZ3pAJe9N3tl18hfnonopozzTTV5G1hKqJmTV5LFiMPfo%2Bw9l8jRBLaNKLlOh4I2eQsM3cnxYHsIrESxlpU9ZwHle9ayj2JZipm71wGGa9fWeMZT%2B2Ccj4BK17HXOaXd6%2B67EJk3Zd290BefQVvK%2F6fqZXMFuvuaXqO5H0F3KMSydyIooWmIuWLIwHhZfHfT0Yf2V5sUXdOLbjvjqKvEFZxNfo653ee2AYiXeybAswwNT69nnXSoZnai2I6pAJarGuUQVnMzo5odpLGMvRvQsjjPb3JW6lxnzIHWW9W7LetBv0bIGdcvaoeWml2legn35zIpOdEpjj3Vt150hut3vaSdSGaBuXbOKDa5cQbYjmGRcm3ZPO0yyE2p50NnT7Wh5oLKnwdnsaQDUoppXdO6kVM5KHVRdYCopbcuGBp0N3ZLIqmxocDYbGtRt6G8ZEQT8umRLo05qyxZ03%2BqZ2vaPfliCjX5v0IYTG3SmdUsSrDKtwflMa7NG9fz492S5Zqe%2BdxIsn9ZgH6N3W5Nas5PadqTWUEmteSappUFhYf14ZPrIX9EqP5y7cEvk58PyaCTshVmU13WggVojHsaVoJT8OhovCDMcBaympU5mjcdApaxLVgiTafaYxxYO%2BVHdaj8IFTUW1UQHk9G4ETItrIToq3z8d%2BrxkJJneiG%2BtygZBLgrrKgvDZ9h%2BDdMehfuACvYumcdYOyWXGVDXREmZah8ZXoDwOSv9wct5rX1XgkgQLXTtXXylpQOI9sleDz18tHAQ6uubQYq35nWCBGRViNEO9lwx8L0e8FXcRxTeb77lYnkWkG8tCevQK%2F4PWsdcU1S6%2BgN2oDQ6aXump772V9FnQYXYoUHpWWZuhiJOixSumb3TihV%2Bg9JVXNZTkfkVZxN8AiMVfzOb5Y9QzcPddWs9NX3GEbFufrlwodknbWCrJKhXxRCGXaCyckGQJVsCtEEpn5K0QQ%2FJJo7Eq3%2BYaJp2XpTotkHVruiWd9TuI2ipywuNKO8D1jYOaIQHpHWvyJufDDT5ppYSJgW4%2BhEVpDTMI4QWcM%2Bpci6mrJQ0KixQkFgrs1mKukHs8F0YCv8WHL7R6hYUaagwqF1qjJBlHy4Tj9s9HyKAnKGlBeuEa9h81r2bXSCeLWtDpPTcFCv16U09puSQb0r1vXOlIYzC6F%2BlBCKs8xoimeUfMuoQMoZi6%2Fcw5hmQdTJpaDZmQUTdIWdGiKlfYSInqquExNRoMgTS549h2VpQpdoQ161mV5%2By7i3f8WSjncT%2B%2BeUyqGmnVUq63s3d14aMG8dsRRRcu1EQRylqPca%2BEUKeYwSMufMTJF3dl4LWGY7MRgGK4bIo2FY2vcJYh%2FhYt8nEwnmON%2Fm8XusQLfwSbAsePRMc9grhQaLXZud5Vqp1cQqug5lzaHsrDCzquW8Lb7r0zszv%2B7fvbnA7QFd07Qjs%2BHfsM1DLpOIBqxv7fEExpu7yEXbJ1wPrhMYSA9RriWX%2Fwc%3D)

### Cómo regenerar

```bash
python3 build_diagram.py .                                   # arquitectura.drawio + enlace_drawio.txt
npm i mxgraph@4.2.2 playwright-core && node render_diagram.js arquitectura.drawio arquitectura.png arquitectura.svg
cp arquitectura.png img/ && python3 build_pdf.py             # e1_fraudlens.pdf (requiere latexmk)
```

## Bitácora de delegación

Herramienta usada: Claude (Anthropic).

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Definición del problema y preguntas P1–P6 (Sección 1) | Parcial | Redactada por el equipo. El agente (Claude) buscó cifras de mercado con su fuente (Banco de la República, Asobancaria, Nequi) y las incorporó. |
| Requisitos funcionales y no funcionales (Sección 2) | Parcial | Redactados por el equipo. El agente señaló inconsistencias (RF-04 frente a RNF-03, CAP por componente, RF faltante para la API) y los adaptó al despliegue en AWS. |
| Elección de tecnologías (Kafka, Spark, Delta Lake, Airflow) | No | Decisión de diseño central tomada por el equipo antes de usar el agente. |
| Despliegue híbrido en AWS y cambio de MongoDB a DynamoDB | Parcial | Propuestos por el agente tras revisar los laboratorios del curso y el límite de instancias del Learner Lab; el equipo los aprobó. |
| Redacción de los ADR (Sección 4) | Parcial | El equipo definió contexto, alternativas y decisión; el agente ayudó a redactar y a citar los conceptos del curso. |
| Plan de implementación (Sección 5) | Parcial | Reparto de bloques acordado por el equipo; el agente redactó el cronograma y la tabla de riesgos. |
| Dibujo del diagrama en draw.io y exportación | Sí | El agente generó el .drawio a partir del diseño del equipo con `build_diagram.py`. Bajo valor pedagógico. |
| Maquetación en LaTeX, referencias APA y anexos | Sí | Tarea de formato; bajo valor pedagógico. |
