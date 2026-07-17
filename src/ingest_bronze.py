from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd
from sqlalchemy import create_engine, text


# Detectar la raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = PROJECT_ROOT / "data" / "raw"

STUDENTS_FILE = RAW_PATH / "university" / "students.csv"

DATABASE_URL = (
    "postgresql+psycopg2://flor:mundolibre@localhost:5432/dwh"
)


def main():
    if not STUDENTS_FILE.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo: {STUDENTS_FILE}"
        )

    # Identificador único de esta ejecución
    batch_id = str(uuid4())

    # Leer CSV como texto para conservar los datos de origen
    df_students = pd.read_csv(
        STUDENTS_FILE,
        dtype=str
    )

    # Agregar metadata de ingesta
    df_students["_source_file"] = STUDENTS_FILE.name
    df_students["_ingested_at"] = datetime.now(timezone.utc)
    df_students["_batch_id"] = batch_id

    engine = create_engine(DATABASE_URL)

    with engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE bronze.students")
        )

        df_students.to_sql(
            name="students",
            con=connection,
            schema="bronze",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

        cantidad_postgres = connection.execute(
            text("SELECT COUNT(*) FROM bronze.students")
        ).scalar_one()

    cantidad_csv = len(df_students)

    print("Archivo:", STUDENTS_FILE)
    print("Filas CSV:", cantidad_csv)
    print("Filas PostgreSQL:", cantidad_postgres)
    print("Batch ID:", batch_id)

    if cantidad_csv != cantidad_postgres:
        raise ValueError(
            "Los registros del CSV y Postgres no coincide"
        )

    print("Carga Bronze completa")


if __name__ == "__main__":
    main()