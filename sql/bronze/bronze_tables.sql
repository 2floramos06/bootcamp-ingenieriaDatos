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

--TABLAS BILLING
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id TEXT,
    external_ref TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    created_at TEXT,
    segment TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.products (
    product_id TEXT,
    sku TEXT,
    name TEXT,
    category TEXT,
    monthly_price TEXT,
    active TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.subscriptions (
    subscription_id TEXT,
    status TEXT,
    start_date TEXT,
    end_date TEXT,
    customer_id TEXT,
    product_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.invoices (
    invoice_id TEXT,
    issued_at TEXT,
    due_at TEXT,
    total TEXT,
    status TEXT,
    currency TEXT,
    customer_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.invoice_items (
    invoice_item_id TEXT,
    quantity TEXT,
    unit_price TEXT,
    line_total TEXT,
    invoice_id TEXT,
    product_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.payments (
    payment_id TEXT,
    amount TEXT,
    paid_at TEXT,
    method TEXT,
    invoice_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

--TABLAS DE CRM
CREATE TABLE IF NOT EXISTS bronze.accounts (
    account_id TEXT,
    name TEXT,
    industry TEXT,
    country TEXT,
    annual_revenue TEXT,
    employees TEXT,
    created_at TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.contacts (
    contact_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    title TEXT,
    created_at TEXT,
    account_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.leads (
    lead_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    source TEXT,
    status TEXT,
    score TEXT,
    created_at TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.opportunities (
    opportunity_id TEXT,
    name TEXT,
    stage TEXT,
    amount TEXT,
    close_date TEXT,
    created_at TEXT,
    account_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.opportunity_contacts (
    opportunity_id TEXT,
    contact_id TEXT,
    role TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bronze.activities (
    activity_id TEXT,
    type TEXT,
    subject TEXT,
    occurred_at TEXT,
    contact_id TEXT,
    opportunity_id TEXT,

    _source_file TEXT NOT NULL,
    _ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _batch_id TEXT NOT NULL
);