from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine, text


# Detectar la raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = PROJECT_ROOT / "data" / "raw"

#STUDENTS_FILE = RAW_PATH / "university" / "students.csv"

DATABASE_URL = (
    "postgresql+psycopg2://flor:mundolibre@localhost:5432/dwh"
)

TABLE_CONFIG = {
    #UNIVERSITY
    "students": {
        "domain": "university",
        "file": "students.csv"
    },
    "professors": {
        "domain": "university",
        "file": "professors.csv"
    },
    "courses": {
        "domain": "university",
        "file": "courses.csv"
    },
    "semesters": {
        "domain": "university",
        "file": "semesters.csv"
    },
    "enrollments": {
        "domain": "university",
        "file": "enrollments.csv"
    },
    "grades": {
        "domain": "university",
        "file": "grades.csv"
    },

    #BILLING
    "customers": {
        "domain": "billing",
        "file": "customers.csv"
    },
    "products": {
        "domain": "billing",
        "file": "products.csv"
    },
    "subscriptions": {
        "domain": "billing",
        "file": "subscriptions.csv"
    },
    "invoices": {
        "domain": "billing",
        "file": "invoices.csv"
    },
    "invoice_items": {
        "domain": "billing",
        "file": "inovice_items.csv"
    },
    "payments":{
        "domain": "billing",
        "file": "payments.csv"
    },

    #CRM
    "accounts": {
        "domain": "crm",
        "file": "accounts.csv"
    },
    "contacts": {
        "domain": "crm",
        "file": "contacts.csv"
    },
    "leads": {
        "domain": "crm",
        "file": "leads.csv"
    },
    "opportunities": {
        "domain": "crm",
        "file": "opportunities.csv"
    },
    "opportunity_contacts": {
        "domain": "crm",
        "file": "opportunity_contacts.csv"
    },
    "activities": {
        "domain": "crm",
        "file": "activities.csv"
    }
}