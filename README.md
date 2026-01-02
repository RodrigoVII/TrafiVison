TrafiVision

TrafiVision es un proyecto académico de análisis y predicción del tráfico urbano en la ciudad de Madrid, desarrollado en el marco de la asignatura Proyecto de Computación I. El sistema combina ingeniería de datos, visión por computador y aprendizaje automático para construir un flujo completo que va desde la recolección de datos hasta la predicción del nivel de tráfico mediante una aplicación gráfica.

El objetivo principal del proyecto es diseñar e implementar un proceso ETL (Extracción, Transformación y Carga) automatizado que permita integrar información visual procedente de cámaras públicas con datos meteorológicos, generando un dataset estructurado y preparado para análisis y modelado predictivo.

Descripción del proyecto

El sistema recopila imágenes en tiempo real desde cámaras de tráfico del Ayuntamiento de Madrid y las combina con datos meteorológicos obtenidos a través de la API Open-Meteo. A partir de las imágenes capturadas, se aplica un modelo de visión por computador basado en YOLOv8 para detectar vehículos y estimar el nivel de tráfico.

Toda la información se unifica en un conjunto de datos final en formato CSV, que incluye variables temporales, climáticas y contextuales. Este dataset sirve como base para el análisis exploratorio y para el entrenamiento de modelos de aprendizaje automático orientados a la predicción del tráfico.

Además, el proyecto incorpora una aplicación gráfica desarrollada en Python que permite visualizar el dataset, consultar la documentación y realizar predicciones de forma interactiva.

Arquitectura y flujo de trabajo

El proyecto sigue un enfoque ETL completamente automatizado:

Extracción de imágenes de tráfico desde cámaras públicas.

Extracción de datos meteorológicos en tiempo real.

Transformación y limpieza de datos, incluyendo normalización temporal y generación de variables derivadas.

Detección de vehículos mediante YOLOv8 y clasificación del nivel de tráfico.

Integración de todas las fuentes en un dataset final estructurado.

Entrenamiento de modelos predictivos y uso desde la interfaz gráfica.

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

Etiquetas de tráfico generadas automáticamente mediante YOLOv8.

Cada registro incluye, entre otras, las siguientes variables:

Fecha y hora.

Ubicación de la cámara (calle).

Temperatura.

Condiciones de lluvia.

Franja horaria.

Tipo de día (laborable o no laborable).

Número de vehículos detectados.

Nivel de tráfico estimado (Bajo, Medio o Elevado).

El dataset contiene aproximadamente 2.950 registros y 12 variables.

Modelado predictivo

A partir del dataset final se han entrenado distintos modelos de clasificación supervisada, entre ellos:

Árboles de decisión

Logistic Regression

K-Nearest Neighbors

Random Forest

Naive Bayes

Los mejores resultados se obtuvieron con modelos basados en árboles, alcanzando una precisión aproximada del 72–73%. Estos resultados están condicionados por el tamaño actual del dataset y por la limitada diversidad temporal y meteorológica de los datos disponibles.

Aplicación gráfica

La aplicación gráfica desarrollada en Python permite:

Visualizar el dataset completo en formato tabla.

Consultar la documentación del proyecto desde la propia aplicación.

Realizar predicciones seleccionando condiciones de tráfico reales.

Mostrar las probabilidades de cada nivel de tráfico.

Visualizar el árbol de decisión cuando se utiliza este modelo.

Todas las ventanas se abren maximizadas y mantienen una estética coherente en tonos rojo, blanco y negro.

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

Estado del proyecto

Pipeline ETL implementado y funcional.

Dataset final limpio, coherente y documentado.

Modelos predictivos entrenados y evaluados.

Aplicación gráfica integrada y operativa.

Trabajo futuro

Como posibles ampliaciones del proyecto se contempla el aumento del tamaño del dataset, la automatización del reentrenamiento de los modelos, la incorporación de métricas adicionales y la mejora de las visualizaciones gráficas.

Tecnologías utilizadas

Python 3

Pandas

NumPy

Requests

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
