from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime

import docker
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.errors import NotFound
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


JUPYTER_CONTAINER = "bootcamp_jupyter"
BRONZE_SCRIPT = "/opt/airflow/src/ingest_bronze.py"
SILVER_SCRIPT = "/home/jovyan/work/src/transform_silver_spark.py"

EXPECTED_SILVER_COUNTS = {
    "students": 5000,
    "professors": 200,
    "courses": 300,
    "semesters": 8,
    "enrollments": 25000,
    "grades": 60000,
    "customers": 10000,
    "products": 200,
    "subscriptions": 15000,
    "invoices": 50000,
    "invoice_items": 150000,
    "payments": 80000,
    "accounts": 5000,
    "contacts": 15000,
    "leads": 2000,
    "opportunities": 3000,
    "opportunity_contacts": 6000,
    "activities": 20000,
}


def cargar_bronze() -> None:
    """Ejecuta la ingesta Bronze dentro del contenedor de Airflow."""
    print("Iniciando carga Bronze...")

    subprocess.run(
        [sys.executable, BRONZE_SCRIPT],
        cwd="/opt/airflow",
        env=os.environ.copy(),
        check=True,
    )

    print("Carga Bronze terminada correctamente.")


def obtener_jupyter(client: docker.DockerClient):
    try:
        return client.containers.get(JUPYTER_CONTAINER)
    except NotFound as exc:
        raise RuntimeError(
            "No existe el contenedor bootcamp_jupyter. "
            "Créalo una vez con Docker Compose antes de ejecutar el DAG."
        ) from exc


def iniciar_jupyter() -> None:
    """Inicia Jupyter y espera hasta que el contenedor esté listo."""
    client = docker.from_env()
    container = obtener_jupyter(client)
    container.reload()

    if container.status != "running":
        print("Iniciando bootcamp_jupyter...")
        container.start()
    else:
        print("bootcamp_jupyter ya estaba iniciado.")

    for _ in range(90):
        container.reload()

        state = container.attrs.get("State", {})
        status = state.get("Status")
        health = state.get("Health", {}).get("Status")

        print(f"Estado Jupyter: status={status}, health={health}")

        if status == "running" and health in (None, "healthy"):
            print("Jupyter está listo.")
            return

        if health == "unhealthy":
            raise RuntimeError("bootcamp_jupyter quedó unhealthy.")

        time.sleep(2)

    raise TimeoutError(
        "Jupyter no estuvo listo dentro del tiempo esperado."
    )


def ejecutar_en_jupyter(command: str, description: str) -> None:
    """Ejecuta un comando dentro de Jupyter y transmite su salida a Airflow."""
    client = docker.from_env()
    container = obtener_jupyter(client)
    container.reload()

    if container.status != "running":
        raise RuntimeError(
            "bootcamp_jupyter no está ejecutándose."
        )

    print(f"Iniciando: {description}")

    exec_data = client.api.exec_create(
        container.id,
        ["bash", "-lc", command],
        stdout=True,
        stderr=True,
    )
    exec_id = exec_data["Id"]

    for chunk in client.api.exec_start(exec_id, stream=True):
        print(
            chunk.decode("utf-8", errors="replace"),
            end="",
            flush=True,
        )

    result = client.api.exec_inspect(exec_id)
    exit_code = result.get("ExitCode")

    if exit_code != 0:
        raise RuntimeError(
            f"{description} terminó con código {exit_code}."
        )

    print(f"{description} terminó correctamente.")


def transformar_silver(domain: str) -> None:
    allowed_domains = {
        "university",
        "billing",
        "crm",
    }

    if domain not in allowed_domains:
        raise ValueError(
            f"Dominio no permitido: {domain}"
        )

    command = (
        "spark-submit "
        "--packages org.postgresql:postgresql:42.7.4 "
        f"{SILVER_SCRIPT} "
        f"--domain {domain}"
    )

    ejecutar_en_jupyter(
        command=command,
        description=f"Silver {domain}",
    )


def validar_silver() -> None:
    """Comprueba los conteos esperados de las 18 tablas Silver."""
    connection = psycopg2.connect(
        host=os.environ["DWH_HOST"],
        port=os.environ.get("DWH_PORT", "5432"),
        dbname=os.environ.get("DWH_DB", "dwh"),
        user=os.environ["DWH_USER"],
        password=os.environ["DWH_PASSWORD"],
    )

    errores = []

    try:
        with connection.cursor() as cursor:
            for table, expected in EXPECTED_SILVER_COUNTS.items():
                cursor.execute(
                    f'SELECT COUNT(*) FROM silver."{table}"'
                )
                actual = cursor.fetchone()[0]

                print(
                    f"silver.{table}: "
                    f"esperado={expected}, encontrado={actual}"
                )

                if actual != expected:
                    errores.append(
                        f"{table}: esperado={expected}, "
                        f"encontrado={actual}"
                    )
    finally:
        connection.close()

    if errores:
        raise ValueError(
            "Conteos Silver incorrectos:\n"
            + "\n".join(errores)
        )

    print(
        "Las 18 tablas Silver tienen "
        "los conteos esperados."
    )


def detener_jupyter() -> None:
    """Detiene Jupyter incluso si una tarea anterior falló."""
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
        print("Deteniendo bootcamp_jupyter...")
        container.stop(timeout=30)
        print("bootcamp_jupyter detenido.")
    else:
        print(
            "bootcamp_jupyter ya estaba detenido."
        )


with DAG(
    dag_id="bootcamp_bronze_silver",
    description=(
        "Orquesta la carga Bronze y las "
        "transformaciones Silver por dominio."
    ),
    start_date=datetime(2026, 7, 20),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bootcamp", "bronze", "silver"],
) as dag:

    task_bronze = PythonOperator(
        task_id="ingest_bronze", #task 0
        python_callable=cargar_bronze,
    )

    task_start_jupyter = PythonOperator(
        task_id="start_jupyter", #task 1
        python_callable=iniciar_jupyter,
    )

    task_silver_university = PythonOperator(
        task_id="silver_university", #task 2
        python_callable=transformar_silver,
        op_kwargs={"domain": "university"},
    )

    task_silver_billing = PythonOperator(
        task_id="silver_billing", #task 3
        python_callable=transformar_silver,
        op_kwargs={"domain": "billing"},
    )

    task_silver_crm = PythonOperator(
        task_id="silver_crm", #task 4
        python_callable=transformar_silver,
        op_kwargs={"domain": "crm"},
    )

    task_validate = PythonOperator(
        task_id="validate_silver_counts", #task 5
        python_callable=validar_silver,
    )

    task_stop_jupyter = PythonOperator(
        task_id="stop_jupyter", #task 6
        python_callable=detener_jupyter,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    task_trigger_gold = TriggerDagRunOperator(
    task_id="trigger_gold_pipeline",
    trigger_dag_id="bootcamp_gold",
    wait_for_completion=False,
    reset_dag_run=False,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (   
        task_bronze
        >> task_start_jupyter
        >> task_silver_university
        >> task_silver_billing
        >> task_silver_crm
        >> task_validate
        >> task_stop_jupyter
    )

    [
        task_validate,
        task_stop_jupyter,
    ] >> task_trigger_gold
