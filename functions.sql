DROP TABLE IF EXISTS estado CASCADE;
DROP TABLE IF EXISTS anio CASCADE;
DROP TABLE IF EXISTS nivel_educacion CASCADE;
DROP TABLE IF EXISTS temp_table CASCADE;

CREATE TABLE temp_table (
        estado_desc         TEXT,
        estado_abr          CHAR(2),
        anio                INT,
        genero              CHAR,
        edu_desc            TEXT,
        edu_code            INT,
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
        edu_code         INT NOT NULL,
        edu_desc       TEXT NOT NULL,
        PRIMARY KEY(edu_code)
);


CREATE OR REPLACE FUNCTION insert_estado() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT * FROM estado WHERE estado_abr = NEW.estado_abr) THEN
        INSERT INTO estado (estado_desc, estado_abr) VALUES (NEW.estado_desc, NEW.estado_abr);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_anio() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT * FROM anio WHERE anio = NEW.anio) THEN
        INSERT INTO anio (anio, es_bisiesto) VALUES (NEW.anio, ((NEW.anio % 4 = 0 AND NEW.anio % 100 != 0) OR NEW.anio % 400 = 0));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION insert_nivel_educacion() RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT * FROM nivel_educacion WHERE edu_code = NEW.edu_code) THEN
        INSERT INTO nivel_educacion (edu_code, edu_desc) VALUES (NEW.edu_code, NEW.edu_desc);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS insert_estado_trigger ON temp_table;
DROP TRIGGER IF EXISTS insert_anio_trigger ON temp_table;
DROP TRIGGER IF EXISTS insert_nivel_educacion_trigger ON temp_table;

CREATE TRIGGER insert_estado_trigger
AFTER INSERT ON temp_table
FOR EACH ROW
EXECUTE FUNCTION insert_estado();

CREATE TRIGGER insert_anio_trigger
AFTER INSERT ON temp_table
FOR EACH ROW
EXECUTE FUNCTION insert_anio();

CREATE TRIGGER insert_nivel_educacion_trigger
AFTER INSERT ON temp_table
FOR EACH ROW
EXECUTE FUNCTION insert_nivel_educacion();

COPY temp_table FROM '/Library/PostgreSQL/15/us_births_2016_2021.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

/* ------------------------------------------------------------------------ */
/* ----------------------- DECLARACION DE FUNCIONES ----------------------- */
/* -------------------------- Y DATOS DE SALIDA --------------------------- */
DROP FUNCTION IF EXISTS year_state_stats(integer);
DROP FUNCTION IF EXISTS year_gender_stats(integer);
DROP FUNCTION IF EXISTS year_education_stats(integer);
DROP FUNCTION IF EXISTS reporte_consolidado;
DROP FUNCTION IF EXISTS es_año_bisiesto;
DROP FUNCTION IF EXISTS year_calculate_stats(aYear INTEGER);
DROP FUNCTION IF EXISTS year_calculate_stats(aYear INTEGER);
DROP FUNCTION IF EXISTS ReporteConsolidado(N INTEGER);

/* FUNCION QUE DEVUELVE PARA UN year UN QUERY QUE TIENE LA INFO DE CADA ESTADO */

CREATE OR REPLACE FUNCTION year_state_stats(aYear INT)
RETURNS TABLE (
  category TEXT,
  total BIGINT,
  avg_age NUMERIC,
  min_age NUMERIC,
  max_age NUMERIC,
  avg_weight NUMERIC,
  min_weight NUMERIC,
  max_weight NUMERIC
)
AS $$
BEGIN
  RETURN QUERY
  SELECT 'State: ' || estado_desc AS category,
       SUM(nacimientos) AS total,
       ROUND(AVG(m_edad_prom)::numeric, 0) AS avg_age,
       ROUND(MIN(m_edad_prom)::numeric, 0) AS min_age,
       ROUND(MAX(m_edad_prom)::numeric, 0) AS max_age,
       ROUND(CAST(AVG(avg_birth_weight) / 1000.0 AS numeric), 3) AS avg_weight,
       ROUND(CAST(MIN(avg_birth_weight) / 1000.0 AS numeric), 3) AS min_weight,
       ROUND(CAST(MAX(avg_birth_weight) / 1000.0 AS numeric), 3) AS max_weight
FROM temp_table
WHERE anio = aYear
GROUP BY estado_desc
HAVING SUM(nacimientos) > 200000
ORDER BY estado_desc DESC;
END;
$$ LANGUAGE plpgsql;


/* FUNCION QUE DEVUELVE PARA UN year UN QUERY QUE TIENE LA INFO DE CADA GENERO */

CREATE OR REPLACE FUNCTION year_gender_stats(aYear INTEGER)
RETURNS TABLE (
  category TEXT,
  total BIGINT,
  avgage NUMERIC,
  minage NUMERIC,
  maxage NUMERIC,
  avgweight NUMERIC,
  minweight NUMERIC,
  maxweight NUMERIC
)
AS $$
BEGIN
  RETURN QUERY
  SELECT 'Gender: Male' AS category,
         SUM(nacimientos) AS total,
         ROUND(AVG(m_edad_prom)::numeric, 0) AS AvgAge,
         ROUND(MIN(m_edad_prom)::numeric, 0) AS MinAge,
         ROUND(MAX(m_edad_prom)::numeric, 0) AS MaxAge,
         ROUND(CAST(AVG(avg_birth_weight) / 1000.0 AS numeric), 3) AS AvgWeight,
         ROUND(CAST(MIN(avg_birth_weight) / 1000.0 AS numeric), 3) AS MinWeight,
         ROUND(CAST(MAX(avg_birth_weight) / 1000.0 AS numeric), 3) AS MaxWeight
  FROM temp_table
  WHERE anio = aYear AND genero = 'M'

  UNION ALL

  SELECT 'Gender: Female' AS category,
         SUM(nacimientos) AS total,
         ROUND(AVG(m_edad_prom)::numeric, 0) AS AvgAge,
         ROUND(MIN(m_edad_prom)::numeric, 0) AS MinAge,
         ROUND(MAX(m_edad_prom)::numeric, 0) AS MaxAge,
         ROUND(CAST(AVG(avg_birth_weight) / 1000.0 AS numeric), 3) AS AvgWeight,
         ROUND(CAST(MIN(avg_birth_weight) / 1000.0 AS numeric), 3) AS MinWeight,
         ROUND(CAST(MAX(avg_birth_weight) / 1000.0 AS numeric), 3) AS MaxWeight
  FROM temp_table
  WHERE anio = aYear AND genero = 'F';

  RETURN;
END;
$$ LANGUAGE plpgsql;


/* FUNCION QUE TE DEUVELVE PARA UN year UN QUERY QUE TIENE LA INFO DE LA EDUCACION */

CREATE OR REPLACE FUNCTION year_education_stats(aYear INTEGER)
RETURNS TABLE (
  category TEXT,
  total BIGINT,
  avgage NUMERIC,
  minage NUMERIC,
  maxage NUMERIC,
  avgweight NUMERIC,
  minweight NUMERIC,
  maxweight NUMERIC
)
AS $$
BEGIN
  RETURN QUERY
  SELECT edu_desc AS category,
         SUM(nacimientos) AS total,
         ROUND(AVG(m_edad_prom)::numeric, 0) AS AvgAge,
         ROUND(MIN(m_edad_prom)::numeric, 0) AS MinAge,
         ROUND(MAX(m_edad_prom)::numeric, 0) AS MaxAge,
         ROUND(CAST(AVG(avg_birth_weight) / 1000.0 AS numeric), 3) AS AvgWeight,
         ROUND(CAST(MIN(avg_birth_weight) / 1000.0 AS numeric), 3) AS MinWeight,
         ROUND(CAST(MAX(avg_birth_weight) / 1000.0 AS numeric), 3) AS MaxWeight
  FROM temp_table
  WHERE anio = aYear AND edu_code != -9
  GROUP BY edu_desc
  ORDER BY edu_desc DESC;

  RETURN;
END;
$$ LANGUAGE plpgsql;

/*FUNCION QUE DEUVELVE PARA UN YEAR EL CUMULATIVE DATA DEL FINAL*/

CREATE OR REPLACE FUNCTION year_cumulative_stats(aYear INTEGER)
RETURNS TABLE (
  category TEXT,
  total BIGINT,
  avgage NUMERIC,
  minage NUMERIC,
  maxage NUMERIC,
  avgweight NUMERIC,
  minweight NUMERIC,
  maxweight NUMERIC
)
AS $$
BEGIN
  RETURN QUERY
  SELECT
    'Aggregate' AS category,
    SUM(nacimientos) AS total,
    ROUND(AVG(m_edad_prom)::numeric, 0) AS avgage,
    ROUND(MIN(m_edad_prom)::numeric, 0) AS minage,
    ROUND(MAX(m_edad_prom)::numeric, 0) AS maxage,
    ROUND(CAST(AVG(avg_birth_weight) / 1000.0 AS numeric), 3) AS avgweight,
    ROUND(CAST(MIN(avg_birth_weight) / 1000.0 AS numeric), 3) AS minweight,
    ROUND(CAST(MAX(avg_birth_weight) / 1000.0 AS numeric), 3) AS maxweight
  FROM
    temp_table
  WHERE
    anio = aYear;
END;
$$ LANGUAGE plpgsql;

/*FUNCION QUE CALCULA LOS STATS PARA UN ANO */

CREATE OR REPLACE FUNCTION year_calculate_stats(aYear INTEGER)
RETURNS VOID
AS $$
DECLARE
    row_result RECORD;
BEGIN
    FOR row_result IN (
        SELECT *
        FROM (
            SELECT *
            FROM year_state_stats(aYear)

            UNION ALL

            SELECT *
            FROM year_gender_stats(aYear)

            UNION ALL

            SELECT *
            FROM year_education_stats(aYear)

            UNION ALL

            SELECT *
            FROM year_cumulative_stats(aYear)
        ) AS subquery
    )
    LOOP
        RAISE NOTICE '%', row_result;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


/*FUNCION QUE RECIBE UN N E IMPRIME LOS STATS DE LOS PRIMEROS N ANIOS */

CREATE OR REPLACE FUNCTION ReporteConsolidado(N INTEGER)
RETURNS VOID
AS $$
DECLARE
    year_value INTEGER;
BEGIN
    FOR year_value IN (
        SELECT anio
        FROM anio
        ORDER BY anio
        LIMIT N
    )
    LOOP
        PERFORM year_calculate_stats(year_value);
    END LOOP;
END;
$$ LANGUAGE plpgsql;


/* PARA CORRER EL COMANDO USAR LOS SIG COMANDOS */

DO $$
BEGIN
PERFORM ReporteConsolidado(2);
END;
$$ LANGUAGE PLPGSQL;
