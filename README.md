TrafiVision

TrafiVision es un proyecto académico orientado al análisis y predicción del tráfico urbano en la ciudad de Madrid. El sistema combina técnicas de ingeniería de datos, visión por computador y aprendizaje automático para construir un flujo completo que va desde la obtención de datos reales hasta su explotación mediante una aplicación interactiva.

El proyecto ha sido desarrollado en el contexto de las asignaturas Proyecto de Computación I y Proyecto de Computación II, siguiendo una evolución progresiva desde un prototipo experimental basado en scripts hasta una arquitectura más estructurada preparada para futuras ampliaciones.

Objetivo del proyecto

El objetivo principal de TrafiVision es diseñar e implementar un sistema capaz de analizar datos reales de tráfico urbano y generar predicciones utilizando técnicas de machine learning.

Para ello se ha desarrollado un pipeline completo que permite:

Recopilar imágenes de tráfico desde cámaras públicas del Ayuntamiento de Madrid.

Integrar información meteorológica en tiempo real mediante la API Open-Meteo.

Procesar y limpiar los datos obtenidos.

Detectar vehículos en las imágenes mediante visión artificial.

Generar un dataset estructurado y reutilizable.

Entrenar modelos predictivos para estimar el nivel de tráfico.

Ofrecer una interfaz que permita consultar datos y realizar predicciones.

Evolución del proyecto

El desarrollo del proyecto se ha realizado en dos etapas principales.

Fase 1 – Ingeniería de datos y modelado (PCDI)

En la primera fase se desarrolló un sistema centrado en la obtención y procesamiento de datos.

Durante esta etapa se implementó:

Un pipeline ETL para recopilar datos de tráfico y meteorología.

Un sistema de detección de vehículos mediante YOLOv8.

La generación de un dataset final en formato CSV.

El entrenamiento de varios modelos de machine learning.

Una aplicación gráfica desarrollada en Python para interactuar con el sistema.

Fase 2 – Evolución arquitectónica del sistema (PCII)

En la fase actual el proyecto evoluciona hacia una arquitectura más estructurada.

Las principales mejoras incorporadas son:

Migración del almacenamiento de datos desde archivos CSV a una base de datos relacional (MariaDB).

Diseño de una estructura de base de datos con tablas relacionadas:

camara

captura

meteo

deteccion

trafico

Desarrollo de un módulo de acceso a datos (db_client.py).

Creación de una API inicial en Python para exponer funcionalidades del sistema.

Reorganización del proyecto siguiendo una arquitectura más modular.

Esta evolución permite preparar el sistema para futuras mejoras como una aplicación web completa o despliegue en entornos más cercanos a producción.

Flujo de trabajo del sistema

El funcionamiento de TrafiVision se basa en un pipeline de procesamiento de datos dividido en varias etapas.

Extracción de datos

Captura de imágenes desde cámaras públicas de tráfico.

Obtención de datos meteorológicos mediante la API Open-Meteo.

Transformación

Limpieza y normalización de los datos.

Generación de variables temporales y contextuales.

Integración de datos procedentes de distintas fuentes.

Análisis visual

Detección de vehículos en imágenes mediante YOLOv8.

Integración

Generación de un dataset estructurado.

Almacenamiento de la información en base de datos.

Modelado predictivo

Entrenamiento de modelos de clasificación supervisada.

Predicción del nivel de tráfico urbano.

Dataset

El dataset generado integra información procedente de tres fuentes principales:

Cámaras de tráfico del Ayuntamiento de Madrid

Datos meteorológicos obtenidos mediante la API Open-Meteo

Resultados de detección de vehículos generados por YOLOv8

Cada registro contiene variables como:

Fecha y hora de captura

Ubicación de la cámara

Franja horaria

Tipo de día (laborable o no laborable)

Temperatura

Condiciones meteorológicas

Número de vehículos detectados

Nivel de tráfico estimado

El dataset utilizado actualmente contiene aproximadamente 2900 registros y diversas variables temporales, meteorológicas y contextuales.

Modelado predictivo

A partir del dataset generado se han entrenado distintos modelos de clasificación supervisada:

Decision Tree

Logistic Regression

K-Nearest Neighbors

Random Forest

Naive Bayes

Los modelos basados en árboles han mostrado los mejores resultados, alcanzando precisiones cercanas al 72-73% en las pruebas realizadas.

Aplicación

El proyecto incluye una aplicación desarrollada en Python que permite interactuar con el sistema.

Entre sus funcionalidades se incluyen:

Exploración de los registros almacenados en la base de datos.

Visualización de estadísticas generales del sistema.

Consulta de información histórica.

Predicción del nivel de tráfico a partir de distintos parámetros.

Visualización del árbol de decisión cuando se utiliza este modelo.

La aplicación ha sido desarrollada utilizando CustomTkinter, lo que permite crear una interfaz moderna y fácil de utilizar.

Estructura del repositorio
TrafiVision
│
├── app/
│   ├── main.py
│   ├── ventana_principal.py
│   ├── ver_csv.py
│   ├── prediccion.py
│   ├── train_models.py
│   └── documentacion.py
│
├── api/
│   └── api.py
│
├── db/
│   ├── db_client.py
│   └── test_connection.py
│
├── models/
│
├── dataset_final(A).csv
├── dataset_final_limpio.csv
│
├── etl_camaras_madrid.py
├── etl_tiempo.py
├── limpiar_hora_dataset.py
├── merge_datasets_final.py
├── yolo_final.py
│
├── DocumentacionF1_PcII.pdf
├── README.md
└── requirements.txt
Ejecución del proyecto

Instalar dependencias:

pip install -r requirements.txt

Ejecutar el pipeline ETL:

python etl_camaras_madrid.py
python etl_tiempo.py
python yolo_final.py
python limpiar_hora_dataset.py

Entrenar los modelos:

python -m app.train_models

Ejecutar la aplicación:

python -m app.main
Tecnologías utilizadas

Python 3

Pandas

NumPy

OpenCV

Ultralytics YOLOv8

PyTorch

Scikit-learn

MariaDB / MySQL

CustomTkinter

Matplotlib

Joblib

Estado actual del proyecto

Actualmente el sistema permite:

Ejecutar un pipeline ETL completo

Generar datasets estructurados

Detectar vehículos mediante visión artificial

Entrenar modelos predictivos

Almacenar datos en base de datos relacional

Consultar información mediante aplicación interactiva

Trabajo futuro

Entre las mejoras previstas para próximas fases se incluyen:

Desarrollo de una aplicación web completa.

Implementación de un sistema de autenticación y gestión de usuarios.

Automatización del reentrenamiento de modelos.

Integración de nuevas fuentes de datos.

Despliegue del sistema en un entorno accesible desde navegador.
