-- BD para dwh
CREATE DATABASE dwh;

\connect dwh

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Documentación de los esquemas
COMMENT ON SCHEMA bronze IS 'Datos crudos, tal cual llegan del CSV + metadata de ingesta';
COMMENT ON SCHEMA silver IS 'Datos limpios, tipados, estandarizados y deduplicados';
COMMENT ON SCHEMA gold IS 'Modelo dimensional orientado al negocio (hechos y dimensiones)';

GRANT ALL PRIVILEGES ON DATABASE dwh TO flor;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO flor;
GRANT ALL PRIVILEGES ON SCHEMA silver TO flor;
GRANT ALL PRIVILEGES ON SCHEMA gold TO flor;