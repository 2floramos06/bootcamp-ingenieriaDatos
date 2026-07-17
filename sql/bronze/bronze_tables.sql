CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.students;

CREATE TABLE bronze.students (
    student_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    birth_date TEXT,
    enrolled_at TEXT,
    country TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

--Cambiar fechas text a date en silver