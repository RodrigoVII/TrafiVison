-- ============================================================
-- TRAFIVISION - CREACION DE BASE DE DATOS
-- ============================================================
-- Este archivo crea toda la estructura de la base de datos
-- utilizada por el proyecto TrafiVision.
--
-- Incluye:
-- - Base de datos
-- - Tablas principales
-- - Relaciones entre tablas
-- - Restricciones de integridad
--
-- ============================================================


-- ------------------------------------------------------------
-- 1. Crear base de datos
-- ------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS trafivision
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE trafivision;



-- ------------------------------------------------------------
-- 2. TABLA CAMARA
-- ------------------------------------------------------------
-- Guarda información estática de cada cámara urbana

CREATE TABLE IF NOT EXISTS camara (

    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Código o identificador de la cámara
    codigo VARCHAR(50) NOT NULL,
    
    -- Coordenadas geográficas
    latitud DECIMAL(9,6),
    longitud DECIMAL(9,6),
    
    -- Distrito o zona de la ciudad
    distrito VARCHAR(100),

    -- Evita cámaras duplicadas
    UNIQUE(codigo)

) ENGINE=InnoDB;



-- ------------------------------------------------------------
-- 3. TABLA CAPTURA
-- ------------------------------------------------------------
-- Representa una captura de datos en un momento concreto
-- para una cámara determinada

CREATE TABLE IF NOT EXISTS captura (

    id INT AUTO_INCREMENT PRIMARY KEY,

    -- Relación con la cámara
    camara_id INT NOT NULL,

    -- Momento de la captura
    timestamp DATETIME NOT NULL,

    -- Variables temporales derivadas
    dia_semana VARCHAR(20),
    franja_horaria VARCHAR(20),
    es_laborable BOOLEAN,

    -- Clave foránea
    CONSTRAINT fk_captura_camara
        FOREIGN KEY (camara_id)
        REFERENCES camara(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    -- Evita duplicar capturas de la misma cámara en el mismo momento
    UNIQUE(camara_id, timestamp)

) ENGINE=InnoDB;



-- ------------------------------------------------------------
-- 4. TABLA METEO
-- ------------------------------------------------------------
-- Datos meteorológicos asociados a una captura

CREATE TABLE IF NOT EXISTS meteo (

    id INT AUTO_INCREMENT PRIMARY KEY,

    captura_id INT NOT NULL,

    temperatura FLOAT,
    precipitacion FLOAT,
    humedad FLOAT,

    CONSTRAINT fk_meteo_captura
        FOREIGN KEY (captura_id)
        REFERENCES captura(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    -- Garantiza relación 1:1
    UNIQUE(captura_id)

) ENGINE=InnoDB;



-- ------------------------------------------------------------
-- 5. TABLA DETECCION
-- ------------------------------------------------------------
-- Resultado del conteo de vehículos mediante visión artificial

CREATE TABLE IF NOT EXISTS deteccion (

    id INT AUTO_INCREMENT PRIMARY KEY,

    captura_id INT NOT NULL,

    num_vehiculos INT,

    CONSTRAINT fk_deteccion_captura
        FOREIGN KEY (captura_id)
        REFERENCES captura(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    -- Relación 1:1 con captura
    UNIQUE(captura_id)

) ENGINE=InnoDB;



-- ------------------------------------------------------------
-- 6. TABLA TRAFICO
-- ------------------------------------------------------------
-- Clasificación del nivel de tráfico

CREATE TABLE IF NOT EXISTS trafico (

    id INT AUTO_INCREMENT PRIMARY KEY,

    captura_id INT NOT NULL,

    -- Nivel de tráfico categórico
    nivel ENUM('bajo','medio','alto') NOT NULL,

    CONSTRAINT fk_trafico_captura
        FOREIGN KEY (captura_id)
        REFERENCES captura(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,

    -- Relación 1:1
    UNIQUE(captura_id)

) ENGINE=InnoDB;