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
 -- BILLING 
 CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id TEXT PRIMARY KEY,
    external_ref TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    created_at TIMESTAMP,
    segment TEXT,
    is_student_linked BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.products (
    product_id TEXT PRIMARY KEY,
    sku TEXT,
    name TEXT,
    category TEXT,
    monthly_price NUMERIC(10,2),
    active BOOLEAN,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.subscriptions (
    subscription_id TEXT PRIMARY KEY,
    status TEXT,
    start_date DATE,
    end_date DATE,
    customer_id TEXT,
    product_id TEXT,
    is_temporally_valid BOOLEAN NOT NULL,
    is_customer_linked BOOLEAN NOT NULL,
    is_product_linked BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.invoices (
    invoice_id TEXT PRIMARY KEY,
    issued_at DATE,
    due_at DATE,
    total NUMERIC(12,2),
    status TEXT,
    currency TEXT,
    customer_id TEXT,
    is_temporally_valid BOOLEAN NOT NULL,
    is_total_valid BOOLEAN NOT NULL,
    is_customer_linked BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.invoice_items (
    invoice_item_id TEXT PRIMARY KEY,
    quantity INTEGER,
    unit_price NUMERIC(12,2),
    line_total NUMERIC(14,2),
    calculated_line_total NUMERIC(14,2),
    invoice_id TEXT,
    product_id TEXT,
    is_quantity_valid BOOLEAN NOT NULL,
    is_unit_price_valid BOOLEAN NOT NULL,
    is_line_total_valid BOOLEAN NOT NULL,
    is_invoice_linked BOOLEAN NOT NULL,
    is_product_linked BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id TEXT PRIMARY KEY,
    amount NUMERIC(12,2),
    paid_at DATE,
    method TEXT,
    invoice_id TEXT,
    is_amount_valid BOOLEAN NOT NULL,
    is_method_valid BOOLEAN NOT NULL,
    is_invoice_linked BOOLEAN NOT NULL,
    is_temporally_valid BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

--CRM
CREATE TABLE IF NOT EXISTS silver.accounts (
    account_id TEXT PRIMARY KEY,
    name TEXT,
    industry TEXT,
    country TEXT,
    annual_revenue NUMERIC(16,2),
    employees INTEGER,
    created_at TIMESTAMP,
    is_annual_revenue_valid BOOLEAN NOT NULL,
    is_employees_valid BOOLEAN NOT NULL,
    is_created_at_valid BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.contacts (
    contact_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    title TEXT,
    created_at TIMESTAMP,
    account_id TEXT,
    is_email_present BOOLEAN NOT NULL,
    is_created_at_valid BOOLEAN NOT NULL,
    is_account_linked BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.leads (
    lead_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    source TEXT,
    status TEXT,
    score INTEGER,
    created_at TIMESTAMP,
    is_email_present BOOLEAN NOT NULL,
    is_source_valid BOOLEAN NOT NULL,
    is_status_valid BOOLEAN NOT NULL,
    is_score_valid BOOLEAN NOT NULL,
    is_created_at_valid BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.opportunities (
    opportunity_id TEXT PRIMARY KEY,
    name TEXT,
    stage TEXT,
    amount NUMERIC(16,2),
    close_date DATE,
    created_at TIMESTAMP,
    account_id TEXT,
    is_stage_valid BOOLEAN NOT NULL,
    is_amount_valid BOOLEAN NOT NULL,
    is_temporally_valid BOOLEAN NOT NULL,
    is_account_linked BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.opportunity_contacts (
    opportunity_id TEXT NOT NULL,
    contact_id TEXT NOT NULL,
    role TEXT,
    is_role_valid BOOLEAN NOT NULL,
    is_opportunity_linked BOOLEAN NOT NULL,
    is_contact_linked BOOLEAN NOT NULL,
    is_same_account BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (opportunity_id, contact_id)
);

CREATE TABLE IF NOT EXISTS silver.activities (
    activity_id TEXT PRIMARY KEY,
    activity_type TEXT,
    subject TEXT,
    occurred_at TIMESTAMP,
    contact_id TEXT,
    opportunity_id TEXT,
    is_type_valid BOOLEAN NOT NULL,
    is_occurred_at_valid BOOLEAN NOT NULL,
    has_contact_id BOOLEAN NOT NULL,
    has_opportunity_id BOOLEAN NOT NULL,
    has_any_reference BOOLEAN NOT NULL,
    is_contact_linked BOOLEAN NOT NULL,
    is_opportunity_linked BOOLEAN NOT NULL,
    _source_file TEXT,
    _bronze_ingested_at TIMESTAMP,
    _batch_id TEXT,
    _silver_created_at TIMESTAMP NOT NULL
);