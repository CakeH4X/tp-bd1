DROP TABLE IF EXISTS estado CASCADE;
DROP TABLE IF EXISTS anio CASCADE;
DROP TABLE IF EXISTS nivel_educacion CASCADE;
DROP TABLE IF EXISTS temp_table CASCADE;

COPY temp_table FROM '/Users/tmmymrtnz/Documents_Local/TP_BD/us_births_2016_2021.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

CREATE TEMPORARY TABLE temp_table (
        estado_desc         TEXT,
        estado_abr          CHAR(2),
        anio                INT,
        genero              CHAR,
        edu_descr           TEXT,
        edu_cod             INT,
        nacimientos         INT,
        m_edad_prom         FLOAT,
        avg_birth_weight    FLOAT
);

CREATE TABLE estado(
        estado_desc     TEXT NOT NULL,
        estado_abr      CHAR(2) NOT NULL,
        PRIMARY KEY(estado_abr)
);

CREATE TABLE anio(
        anio            INT NOT NULL,
        es_bisiesto     BOOLEAN NOT NULL,
        PRIMARY KEY(anio)
);

CREATE TABLE nivel_educacion(
        edu_cod         INT NOT NULL,
        edu_descr       TEXT NOT NULL,
        PRIMARY KEY(edu_cod)
);

/* CARGAMOS LOS DATOS CORRESPONDIENTES DENTRO DE LA TABLA "estado" */
INSERT INTO estado(estado_desc, estado_abr)
SELECT temp_table.estado, temp_table.estado_abbr FROM temp_table
ON CONFLICT (estado_abbr) DO NOTHING;

/* CARGAMOS LOS DATOS CORRESPONDIENTES DENTRO DE LA TABLA "anios" */
insert into anios(anio,es_bisiesto)
SELECT temp_table.anio,
        CASE
                WHEN temp_table.anio % 4 == 0 THEN TRUE
                WHEN temp_table.anio % 100 != 0 THEN TRUE
                WHEN temp_table.anio % 4 != 0 THEN TRUE
                ELSE FALSE
        END
FROM temp_table
ON CONFLICT (anio) DO NOTHING;

/* CARGAMOS LOS DATOS CORRESPONDIENTES DENTRO DE LA TABLA "nivel_educacion" */
INSERT INTO nivel_educacion(edu_code,edu_titulo)
SELECT temp_table.edu_code,temp_table.mother_edu FROM temp_table
ON CONFLICT (edu_code) DO NOTHING;

/* ------------------------------------------------------------------------ */
/* ----------------------- DECLARACION DE FUNCIONES ----------------------- */
/* -------------------------- Y DATOS DE SALIDA --------------------------- */
DROP FUNCTION IF EXISTS reporte_consolidado;
DROP FUNCTION IF EXISTS es_año_bisiesto;

CREATE year_state_record AS (
    total INT,
    avgage INT,
    minage INT,
    maxage INT,
    avgweight DECIMAL,
    minweight DECIMAL,
    maxweight decimal
);

CREATE FUNCTION year_state_stats(IN year INT) RETURNS year_state_record AS $$

DECLARE
    result year_state_record;
BEGIN
    SELECT SUM(births) as Total, AVG(m_avg_age) as AvgAge, MIN(m_avg_age) as MinAge, MAX(m_avg_age) as MaxAge, AVG(avg_birth_weight) as AvgWeight, MIN(avg_birth_weight) as MingWeight, MAX(avg_birth_weight) as MaxWeight
    FROM bruto
    GROUP BY estado_abr
    WHERE anio = year;

CREATE FUNCTION reporte_consolidado(IN n INT) RETURNS VOID AS $$



CREATE FUNCTION es_año_bisiesto(IN n INT) RETURNS BOOLEAN AS $$
