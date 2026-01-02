TrafiVision

TrafiVision es un proyecto académico de análisis y predicción del tráfico urbano en la ciudad de Madrid. El sistema combina técnicas de ingeniería de datos, visión por computador y aprendizaje automático para construir un flujo completo que va desde la obtención de datos reales hasta su explotación mediante una aplicación gráfica interactiva.

El proyecto ha sido desarrollado en el contexto de la asignatura Proyecto de Computación I, con un enfoque práctico y orientado a datos reales.

Objetivo del proyecto

El objetivo principal de TrafiVision es diseñar e implementar un pipeline ETL automatizado capaz de:

Recopilar imágenes de tráfico desde cámaras públicas.

Integrar información meteorológica en tiempo real.

Procesar y limpiar los datos obtenidos.

Generar un dataset estructurado y reutilizable.

Entrenar modelos predictivos para estimar el nivel de tráfico.

Ofrecer una interfaz gráfica para la consulta y predicción.

Descripción general

El sistema obtiene imágenes desde cámaras de tráfico del Ayuntamiento de Madrid y las combina con datos meteorológicos procedentes de la API Open-Meteo. Sobre las imágenes capturadas se aplica un modelo de visión por computador basado en YOLOv8, encargado de detectar vehículos y estimar la densidad del tráfico.

Toda la información generada se integra en un dataset final en formato CSV que incluye variables temporales, meteorológicas y contextuales. Este conjunto de datos sirve como base tanto para el análisis exploratorio como para el entrenamiento de modelos de aprendizaje automático.

El proyecto se completa con una aplicación gráfica en Python, desde la cual es posible visualizar los datos y realizar predicciones de forma interactiva.

Flujo de trabajo (ETL)

El funcionamiento del sistema sigue una arquitectura ETL claramente definida:

Extracción de imágenes desde cámaras de tráfico públicas.

Extracción de datos meteorológicos en tiempo real.

Limpieza y transformación de los datos (normalización temporal y variables derivadas).

Detección de vehículos mediante YOLOv8.

Integración de todas las fuentes en un dataset final.

Entrenamiento y uso de modelos predictivos.

Estructura del repositorio
TrafiVision/
├── app/
│   ├── main.py
│   ├── ventana_principal.py
│   ├── ver_csv.py
│   ├── documentacion.py
│   ├── prediccion.py
│   ├── train_models.py
│   └── logo.png
│
├── dataset_final_limpio.csv
├── dataset_final(A).csv
├── etl_camaras_madrid.py
├── etl_tiempo.py
├── limpiar_hora_dataset.py
├── yolo_final.py
│
├── MemoriaTrafiVision 80%.pdf
├── README.md
└── requirements.txt

Dataset

El archivo dataset_final_limpio.csv integra información procedente de tres fuentes principales:

Cámaras de tráfico del Ayuntamiento de Madrid.

Datos meteorológicos obtenidos mediante la API Open-Meteo.

Resultados de detección de vehículos generados por YOLOv8.

Cada registro contiene variables como:

Fecha y hora de captura.

Ubicación de la cámara.

Franja horaria.

Tipo de día (laborable o no laborable).

Temperatura y condiciones de lluvia.

Número de vehículos detectados.

Nivel de tráfico estimado (Bajo, Medio o Elevado).

El dataset final cuenta con aproximadamente 2.950 registros y 12 variables.

Modelado predictivo

A partir del dataset generado se han entrenado distintos modelos de clasificación supervisada, entre ellos:

Árboles de decisión

Logistic Regression

K-Nearest Neighbors

Random Forest

Naive Bayes

Los mejores resultados se obtuvieron con modelos basados en árboles, alcanzando una precisión aproximada del 72–73%. Estos resultados están condicionados por el tamaño del dataset y por la limitada variabilidad temporal y meteorológica de los datos.

Aplicación gráfica

El proyecto incluye una aplicación gráfica desarrollada en Python que permite:

Visualizar el dataset completo en formato tabla.

Consultar la documentación del proyecto desde la propia interfaz.

Realizar predicciones seleccionando distintas condiciones de tráfico.

Mostrar probabilidades asociadas a cada nivel de tráfico.

Visualizar el árbol de decisión cuando se utiliza este modelo.

La interfaz mantiene una estética coherente en tonos rojo, blanco y negro, y todas las ventanas se abren maximizadas para mejorar la experiencia de uso.

Ejecución del proyecto

Instalar dependencias:

pip install -r requirements.txt


Ejecutar el pipeline ETL:

python etl_camaras_madrid.py
python etl_tiempo.py
python yolo_final.py
python limpiar_hora_dataset.py


Entrenar los modelos predictivos:

python -m app.train_models


Ejecutar la aplicación gráfica:

python -m app.main

Estado actual

Pipeline ETL completamente funcional.

Dataset final limpio y estructurado.

Modelos predictivos entrenados y evaluados.

Aplicación gráfica integrada y operativa.

Trabajo futuro

Como posibles mejoras futuras se plantea:

Ampliar el conjunto de datos.

Automatizar el reentrenamiento de los modelos.

Incorporar nuevas métricas y visualizaciones.

Extender la aplicación con nuevas funcionalidades.

Tecnologías utilizadas

Python 3

Pandas y NumPy

OpenCV

Ultralytics YOLOv8

Torch y Torchvision

Scikit-learn

CustomTkinter

Matplotlib

requirements.txt
pandas
numpy
requests
opencv-python
ultralytics
torch
torchvision
scikit-learn
joblib
matplotlib
customtkinter
Pillow
