CREATE SCHEMA IF NOT EXISTS silver;

--UNIVERSITY
CREATE TABLE IF NOT EXISTS silver.students (
    student_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    birth_date DATE,
    enrolled_at TIMESTAMP,
    country TEXT,

    is_temporally_valid BOOLEAN NOT NULL,

    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.professors (
    professor_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    department TEXT,
    hired_at TIMESTAMP,

    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.courses (
    course_id TEXT PRIMARY KEY,
    code TEXT,
    name TEXT,
    credits INTEGER,
    department TEXT,
    professor_id TEXT,

    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.semesters (
    semester_id TEXT PRIMARY KEY,
    code TEXT,
    year INTEGER,
    half INTEGER,
    start_date DATE,
    end_date DATE,

    is_temporally_valid BOOLEAN NOT NULL,

    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.enrollments (
    enrollment_id TEXT PRIMARY KEY,
    enrolled_at TIMESTAMP,
    status TEXT,
    student_id TEXT,
    course_id TEXT,
    semester_id TEXT,

    is_repeated_combination BOOLEAN NOT NULL,

    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.grades (
    grade_id TEXT PRIMARY KEY,
    assessment TEXT,
    score NUMERIC(6,2),
    weight NUMERIC(5,4),
    total_weight NUMERIC(8,4),
    graded_at DATE,
    enrollment_id TEXT,

    is_score_valid BOOLEAN NOT NULL,
    is_weight_valid BOOLEAN NOT NULL,
    is_weight_sum_valid BOOLEAN NOT NULL,

    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

COMMENT ON TABLE silver.grades IS
'Calificaciones limpias y tipadas provenientes de bronze.grades';

COMMENT ON COLUMN silver.grades.total_weight IS
'Suma de los pesos de todas las calificaciones de la misma inscripción';

COMMENT ON COLUMN silver.grades.is_weight_sum_valid IS
'Indica si los pesos de la inscripción suman 1';
