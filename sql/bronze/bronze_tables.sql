CREATE SCHEMA IF NOT EXISTS bronze;

DROP TABLE IF EXISTS bronze.students;

--TABLAS DE UNIVERSITY

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

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.professors (
    professor_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    department TEXT,
    hired_at TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.courses (
    course_id TEXT,
    code TEXT,
    name TEXT,
    credits TEXT,
    department TEXT,
    professor_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.semesters (
    semester_id TEXT,
    code TEXT,
    year TEXT,
    half TEXT,
    start_date TEXT,
    end_date TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.enrollments (
    enrollment_id TEXT,
    enrolled_at TEXT,
    status TEXT,
    student_id TEXT,
    course_id TEXT,
    semester_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.grades (
    grade_id TEXT,
    assessment TEXT,
    score TEXT,
    weight TEXT,
    graded_at TEXT,
    enrollment_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);