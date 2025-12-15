 TrafiVision

TrafiVision es un proyecto de análisis y modelado de tráfico urbano en la ciudad de Madrid, basado en datos abiertos, visión por computador y técnicas de ingeniería de datos. El objetivo principal es diseñar e implementar un proceso ETL (Extracción, Transformación y Carga) automatizado que permita integrar información visual procedente de cámaras públicas con datos meteorológicos, generando un dataset estructurado y preparado para análisis y modelado predictivo.

## Descripción del proyecto

El sistema recopila imágenes en tiempo real desde cámaras de tráfico del Ayuntamiento de Madrid y las combina con datos meteorológicos obtenidos a través de la API Open-Meteo. A partir de las imágenes capturadas, se aplica un modelo de visión por computador (YOLOv8) para detectar vehículos y clasificar el nivel de tráfico en categorías bajo, medio o alto.

Toda la información se unifica en un conjunto de datos final en formato CSV, que incluye variables temporales, climáticas y contextuales, y que sirve como base para el análisis exploratorio y la experimentación con modelos de aprendizaje automático.

## Arquitectura y flujo de trabajo

El proyecto sigue un enfoque ETL completamente automatizado:

- Extracción de imágenes de tráfico desde cámaras públicas.
- Extracción de datos meteorológicos en tiempo real.
- Transformación y limpieza de datos (normalización temporal, eliminación de inconsistencias y generación de variables derivadas).
- Carga e integración en un dataset final estructurado.
- Modelado predictivo experimental mediante plataformas visuales de inteligencia artificial.

## Estructura del repositorio

├── dataset_final_limpio.csv Dataset final limpio y estructurado
├── dataset_final(A).csv Versión intermedia del dataset
├── etl_camaras_madrid.py Script de extracción de imágenes
├── etl_tiempo.py Script de extracción de datos meteorológicos
├── limpiar_hora_dataset.py Limpieza y normalización de la hora
├── merge_datasets_final.py Unión de los distintos CSV
├── yolo_final.py Detección de vehículos con YOLOv8
├── TrafiVisionAltair.zip Experimentos de modelado en Altair AI Studio
├── MemoriaTrafiVision 80%.docx.pdf Documento del proyecto (entrega 80%)
└── README.md


## Dataset

El archivo dataset_final_limpio.csv integra información procedente de tres fuentes principales:

- Cámaras de tráfico del Ayuntamiento de Madrid.
- Datos meteorológicos de la API Open-Meteo.
- Etiquetas de tráfico generadas automáticamente mediante YOLOv8.

Cada registro contiene, entre otras, las siguientes variables:
- Hora y fecha de captura.
- Ubicación de la cámara.
- Temperatura y precipitación.
- Franja horaria.
- Tipo de día (laborable / no laborable).
- Nivel de tráfico estimado.

## Modelado predictivo

La fase de modelado se ha desarrollado de forma experimental utilizando entornos visuales de inteligencia artificial (Altair AI Studio / Azure Machine Learning Studio). El dataset final se ha conectado directamente a estas plataformas para evaluar distintos algoritmos de clasificación supervisada.

Los modelos probados incluyen:
- Árboles de decisión
- Random Forest
- Naive Bayes
- k-Nearest Neighbors
- Gradient Boosted Trees

El mejor rendimiento se obtuvo con modelos basados en árboles, alcanzando una precisión aproximada del 72–73%. Estos resultados están condicionados por el tamaño actual del dataset (alrededor de 3.000 registros) y por la limitada diversidad temporal y meteorológica de las muestras.

## Ejecución del proyecto

1. Instalar dependencias:
```bash
pip install -r requirements.txt
Ejecutar los scripts ETL en el siguiente orden:

bash
Copiar código
python etl_camaras_madrid.py
python etl_tiempo.py
python yolo_final.py
python limpiar_hora_dataset.py
python merge_datasets_final.py
Utilizar el dataset final para análisis o modelado en herramientas de inteligencia artificial.

Estado del proyecto
Pipeline ETL implementado y funcional.

Dataset final limpio, coherente y documentado.

Modelado predictivo experimental completado.

Proyecto preparado para ampliación o integración futura en interfaces de visualización.

Trabajo futuro
Como evolución del proyecto, se contempla la ampliación del conjunto de datos, la integración del modelo predictivo en una interfaz gráfica en Python y la automatización del reentrenamiento del modelo a medida que se recojan nuevos datos.

Fuentes de datos y herramientas
Ayuntamiento de Madrid – Cámaras de tráfico

Open-Meteo – API meteorológica

Ultralytics – YOLOv8

Python 3

Pandas

Requests

Torch


### requirements.txt (para copiar y pegar)

```txt
pandas
requests
schedule
opencv-python
ultralytics
torch
torchvision
numpy
