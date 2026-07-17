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
        "file": "invoice_items.csv"
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

def cargar_tabla_bronze(
    engine,
    table_name,
    domain,
    file_name,
    batch_id
):
    file_path = RAW_PATH / domain / file_name

    if not file_path.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo: {file_path}"
        )

    df = pd.read_csv(
        file_path,
        dtype=str,
        keep_default_na=True
    )

    cantidad_csv = len(df)

    # Metadata de ingesta
    df["_source_file"] = file_name
    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_batch_id"] = batch_id

    with engine.begin() as connection:

        connection.execute(
            text(f'TRUNCATE TABLE bronze."{table_name}"') #Si falla el script no se crean dupplicados
        )

        df.to_sql(
            name=table_name,
            con=connection,
            schema="bronze",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

        cantidad_postgres = connection.execute(
            text(
                f'SELECT COUNT(*) '
                f'FROM bronze."{table_name}"'
            )
        ).scalar_one()

    if cantidad_csv != cantidad_postgres:
        raise ValueError(
            f"{table_name}: CSV={cantidad_csv}, "
            f"PostgreSQL={cantidad_postgres}"
        )

    return {
        "tabla": table_name,
        "archivo": file_name,
        "filas_csv": cantidad_csv,
        "filas_postgresql": cantidad_postgres,
        "estado": "OK"
    }

def main():
    engine = create_engine(DATABASE_URL)

    batch_id = str(uuid4()) #Si hay un error rastrear en que ejecución entraron 
    resultados = []

    print("Carga Bronze")
    print("Batch ID:", batch_id)
    print("--------------------------------")

    for table_name, config in TABLE_CONFIG.items():

        print(f"Cargando {table_name}...")

        resultado = cargar_tabla_bronze(
            engine=engine,
            table_name=table_name,
            domain=config["domain"],
            file_name=config["file"],
            batch_id=batch_id
        )

        resultados.append(resultado)

        print(
            f"{table_name}: "
            f"{resultado['filas_postgresql']} filas"
        )

    print("--------------------------------")
    print("Bronze finalizada")
    print("Tablas cargadas:", len(resultados))

    resumen = pd.DataFrame(resultados)
    print(resumen.to_string(index=False))


if __name__ == "__main__":
    main()