from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

import docker
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.errors import NotFound

JUPYTER_CONTAINER = "bootcamp_jupyter"

GOLD_SQL_DIRECTORY = Path(
    "/opt/airflow/sql/gold"
)

GOLD_EXPORT_SCRIPT = (
    "/home/jovyan/work/src/"
    "export_gold_parquet.py"
)

POSTGRES_DRIVER_PACKAGE = (
    "org.postgresql:postgresql:42.7.4"
)


EXPECTED_GOLD_COUNTS = {
    "student_academic_summary": 5000,
    "course_performance_summary": 300,
    "customer_billing_summary": 32147,
    "customer_subscription_summary": 10000,
    "account_crm_summary": 5000,
    "lead_funnel_summary": 5,
    "billing_currency_kpis": 8,
    "executive_kpis": 1,
    "vw_student_customer_360": 5000,
    "vw_course_performance_dashboard": 300,
    "vw_billing_customer_dashboard": 32147,
    "vw_crm_account_dashboard": 5000,
    "vw_lead_source_dashboard": 5,
}


ALLOWED_SQL_FILES = {
    "gold_cleanup.sql",
    "gold_tables.sql",
    "gold_kpis.sql",
    "gold_views.sql",
}

def crear_conexion_postgresql():
    """
    Crea una conexión con PostgreSQL utilizando
    las variables de entorno de Airflow.
    """
    return psycopg2.connect(
        host=os.environ["DWH_HOST"],
        port=os.environ.get(
            "DWH_PORT",
            "5432",
        ),
        dbname=os.environ.get(
            "DWH_DB",
            "dwh",
        ),
        user=os.environ["DWH_USER"],
        password=os.environ["DWH_PASSWORD"],
        connect_timeout=10,
    )


def ejecutar_archivo_sql(
    sql_file_name: str,
) -> None:
    """
    Lee y ejecuta un archivo SQL de la capa Gold.
    """
    if sql_file_name not in ALLOWED_SQL_FILES:
        raise ValueError(
            f"Archivo SQL no permitido: "
            f"{sql_file_name}"
        )

    sql_path = (
        GOLD_SQL_DIRECTORY
        / sql_file_name
    )

    if not sql_path.is_file():
        raise FileNotFoundError(
            f"No se encontró el archivo: "
            f"{sql_path}"
        )

    sql_content = sql_path.read_text(
        encoding="utf-8"
    )

    if not sql_content.strip():
        raise ValueError(
            f"El archivo {sql_file_name} "
            f"está vacío."
        )

    print(
        f"Ejecutando archivo: {sql_path}"
    )

    connection = crear_conexion_postgresql()

    try:
        # Los archivos Gold ya tienen BEGIN y COMMIT
        connection.autocommit = True

        with connection.cursor() as cursor:
            cursor.execute(sql_content)

        print(
            f"{sql_file_name} ejecutado "
            f"correctamente."
        )

    except Exception:
        print(
            f"Error ejecutando "
            f"{sql_file_name}."
        )
        raise

    finally:
        connection.close()

def limpiar_gold() -> None:
    """
    Elimina las vistas Gold que dependen de las
    tablas que serán reconstruidas.
    """
    ejecutar_archivo_sql(
        "gold_cleanup.sql"
    )

def construir_gold_tables() -> None:
    ejecutar_archivo_sql(
        "gold_tables.sql"
    )


def construir_gold_kpis() -> None:
    ejecutar_archivo_sql(
        "gold_kpis.sql"
    )


def construir_gold_views() -> None:
    ejecutar_archivo_sql(
        "gold_views.sql"
    )


def validar_gold_postgresql() -> None:
    """
    Verifica que las tablas y vistas Gold
    tengan los conteos esperados.
    """
    connection = crear_conexion_postgresql()

    errores = []

    try:
        with connection.cursor() as cursor:
            for (
                object_name,
                expected_count,
            ) in EXPECTED_GOLD_COUNTS.items():

                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM gold."{object_name}"
                    """
                )

                actual_count = (
                    cursor.fetchone()[0]
                )

                print(
                    f"gold.{object_name}: "
                    f"esperado={expected_count:,}, "
                    f"encontrado={actual_count:,}"
                )

                if (
                    actual_count
                    != expected_count
                ):
                    errores.append(
                        f"{object_name}: "
                        f"esperado={expected_count}, "
                        f"encontrado={actual_count}"
                    )

    finally:
        connection.close()

    if errores:
        raise ValueError(
            "Conteos Gold incorrectos:\n"
            + "\n".join(errores)
        )

    print(
        "Los 13 objetos Gold tienen "
        "los conteos esperados."
    )


def obtener_jupyter(
    client: docker.DockerClient,
):
    try:
        return client.containers.get(
            JUPYTER_CONTAINER
        )

    except NotFound as exc:
        raise RuntimeError(
            "No existe el contenedor "
            "bootcamp_jupyter. "
            "Debe crearse previamente "
            "con Docker Compose."
        ) from exc


def iniciar_jupyter() -> None:
    """
    Inicia el contenedor Jupyter y espera
    hasta que esté disponible.
    """
    client = docker.from_env()

    container = obtener_jupyter(
        client
    )

    container.reload()

    if container.status != "running":
        print(
            "Iniciando bootcamp_jupyter..."
        )
        container.start()

    else:
        print(
            "bootcamp_jupyter ya estaba "
            "iniciado."
        )

    for _ in range(90):
        container.reload()

        state = container.attrs.get(
            "State",
            {},
        )

        status = state.get("Status")

        health = (
            state
            .get("Health", {})
            .get("Status")
        )

        print(
            "Estado Jupyter: "
            f"status={status}, "
            f"health={health}"
        )

        if (
            status == "running"
            and health in (
                None,
                "healthy",
            )
        ):
            print("Jupyter está listo.")
            return

        if health == "unhealthy":
            raise RuntimeError(
                "bootcamp_jupyter quedó "
                "unhealthy."
            )

        time.sleep(2)

    raise TimeoutError(
        "Jupyter no estuvo listo dentro "
        "del tiempo esperado."
    )


def ejecutar_en_jupyter(
    command: str,
    description: str,
) -> None:
    """
    Ejecuta un comando dentro de Jupyter
    y envía su salida a los logs de Airflow.
    """
    client = docker.from_env()

    container = obtener_jupyter(
        client
    )

    container.reload()

    if container.status != "running":
        raise RuntimeError(
            "bootcamp_jupyter no está "
            "ejecutándose."
        )

    print(f"Iniciando: {description}")

    exec_data = client.api.exec_create(
        container.id,
        [
            "bash",
            "-lc",
            command,
        ],
        stdout=True,
        stderr=True,
    )

    exec_id = exec_data["Id"]

    for chunk in client.api.exec_start(
        exec_id,
        stream=True,
    ):
        print(
            chunk.decode(
                "utf-8",
                errors="replace",
            ),
            end="",
            flush=True,
        )

    result = client.api.exec_inspect(
        exec_id
    )

    exit_code = result.get(
        "ExitCode"
    )

    if exit_code != 0:
        raise RuntimeError(
            f"{description} terminó "
            f"con código {exit_code}."
        )

    print(
        f"{description} terminó "
        f"correctamente."
    )


def exportar_gold_parquet() -> None:
    """
    Ejecuta el script Spark que exporta
    Gold a Parquet y valida los conteos.
    """
    command = (
        "spark-submit "
        f"--packages "
        f"{POSTGRES_DRIVER_PACKAGE} "
        f"{GOLD_EXPORT_SCRIPT}"
    )

    ejecutar_en_jupyter(
        command=command,
        description=(
            "Exportación y validación "
            "Gold a Parquet"
        ),
    )


def detener_jupyter() -> None:
    """
    Detiene Jupyter incluso cuando alguna
    tarea anterior haya fallado.
    """
    client = docker.from_env()

    try:
        container = client.containers.get(
            JUPYTER_CONTAINER
        )

    except NotFound:
        print(
            "bootcamp_jupyter no existe; "
            "no hay nada que detener."
        )
        return

    container.reload()

    if container.status == "running":
        print(
            "Deteniendo "
            "bootcamp_jupyter..."
        )

        container.stop(
            timeout=30
        )

        print(
            "bootcamp_jupyter detenido."
        )

    else:
        print(
            "bootcamp_jupyter ya estaba "
            "detenido."
        )

with DAG(
    dag_id="bootcamp_gold",
    description=(
        "Construye Gold, valida PostgreSQL "
        "y exporta los resultados a Parquet."
    ),
    start_date=datetime(
        2026,
        7,
        21,
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "bootcamp",
        "gold",
        "parquet",
    ],
) as dag:
    
    task_cleanup_gold = PythonOperator(
        task_id="cleanup_gold_views",
        python_callable=limpiar_gold,
    )

    task_gold_tables = PythonOperator(
        task_id="build_gold_tables",
        python_callable=(
            construir_gold_tables
        ),
    )

    task_gold_kpis = PythonOperator(
        task_id="build_gold_kpis",
        python_callable=(
            construir_gold_kpis
        ),
    )

    task_gold_views = PythonOperator(
        task_id="build_gold_views",
        python_callable=(
            construir_gold_views
        ),
    )

    task_validate_postgresql = (
        PythonOperator(
            task_id=(
                "validate_gold_postgresql"
            ),
            python_callable=(
                validar_gold_postgresql
            ),
        )
    )

    task_start_jupyter = PythonOperator(
        task_id="start_jupyter",
        python_callable=iniciar_jupyter,
    )

    task_export_parquet = PythonOperator(
        task_id=(
            "export_and_validate_"
            "gold_parquet"
        ),
        python_callable=(
            exportar_gold_parquet
        ),
    )

    task_stop_jupyter = PythonOperator(
        task_id="stop_jupyter",
        python_callable=detener_jupyter,
        trigger_rule=(
            TriggerRule.ALL_DONE
        ),
    )

    (   
        task_cleanup_gold
        >> task_gold_tables
        >> task_gold_kpis
        >> task_gold_views
        >> task_validate_postgresql
        >> task_start_jupyter
        >> task_export_parquet
        >> task_stop_jupyter
    )