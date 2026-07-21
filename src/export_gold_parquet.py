from __future__ import annotations

import os

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

DWH_HOST = os.getenv("DWH_HOST", "postgres")
DWH_PORT = os.getenv("DWH_PORT", "5432")
DWH_DB = os.getenv("DWH_DB", "dwh")
DWH_USER = os.getenv("DWH_USER")
DWH_PASSWORD = os.getenv("DWH_PASSWORD")

if not DWH_USER or not DWH_PASSWORD:
    raise ValueError(
        "No se encontraron DWH_USER o DWH_PASSWORD"
    )

JDBC_URL = (
    f"jdbc:postgresql://"
    f"{DWH_HOST}:{DWH_PORT}/{DWH_DB}"
)

JDBC_PROPERTIES = {
    "user": DWH_USER,
    "password": DWH_PASSWORD,
    "driver": "org.postgresql.Driver",
}

PARQUET_BASE_PATH = os.getenv(
    "GOLD_PARQUET_BASE_PATH",
    "/home/jovyan/work/data/parquet/gold",
)

SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "local[2]",
)

SPARK_SHUFFLE_PARTITIONS = os.getenv(
    "SPARK_SHUFFLE_PARTITIONS",
    "8",
)

PARQUET_WRITE_PARTITIONS = int(
    os.getenv(
        "GOLD_PARQUET_WRITE_PARTITIONS",
        "1",
    )
)

GOLD_OBJECTS = (
    "student_academic_summary",
    "course_performance_summary",
    "customer_billing_summary",
    "customer_subscription_summary",
    "account_crm_summary",
    "lead_funnel_summary",
    "billing_currency_kpis",
    "executive_kpis",
    "vw_student_customer_360",
    "vw_course_performance_dashboard",
    "vw_billing_customer_dashboard",
    "vw_crm_account_dashboard",
    "vw_lead_source_dashboard",
)

EXPECTED_COUNTS = {
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

def crear_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("ExportGoldParquet")
        .master(SPARK_MASTER)
        .config(
            "spark.sql.shuffle.partitions",
            SPARK_SHUFFLE_PARTITIONS,
        )
        .config(
            "spark.default.parallelism",
            SPARK_SHUFFLE_PARTITIONS,
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def leer_objeto_gold(
    spark: SparkSession,
    object_name: str,
) -> DataFrame:
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"gold.{object_name}",
        properties=JDBC_PROPERTIES,
    )

def escribir_parquet(
    dataframe: DataFrame,
    object_name: str,
) -> str:
    output_path = (
        f"{PARQUET_BASE_PATH}/{object_name}"
    )

    (
        dataframe
        .coalesce(PARQUET_WRITE_PARTITIONS)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    return output_path

def exportar_objeto_gold(
    spark: SparkSession,
    object_name: str,
) -> None:
    print("\n" + "=" * 65)
    print(f"EXPORTANDO GOLD.{object_name}")
    print("=" * 65)

    dataframe = leer_objeto_gold(
        spark,
        object_name,
    )

    dataframe.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    try:
        postgres_count = dataframe.count()

        expected_count = EXPECTED_COUNTS[
            object_name
        ]

        print(
            f"Registros esperados: "
            f"{expected_count:,}"
        )

        print(
            f"Registros en PostgreSQL: "
            f"{postgres_count:,}"
        )

        if postgres_count != expected_count:
            raise ValueError(
                f"El conteo de gold.{object_name} "
                f"no coincide. "
                f"Esperado={expected_count}, "
                f"encontrado={postgres_count}"
            )

        output_path = escribir_parquet(
            dataframe,
            object_name,
        )

        parquet_count = (
            spark.read
            .parquet(output_path)
            .count()
        )

        print(
            f"Registros en Parquet: "
            f"{parquet_count:,}"
        )

        if parquet_count != postgres_count:
            raise ValueError(
                f"El Parquet de {object_name} "
                f"no coincide con PostgreSQL. "
                f"PostgreSQL={postgres_count}, "
                f"Parquet={parquet_count}"
            )

        print(
            f"Gold.{object_name} exportado "
            f"correctamente"
        )

        print(
            f"Ruta: {output_path}"
        )

    finally:
        dataframe.unpersist(
            blocking=True
        )

        spark.catalog.clearCache()

def main() -> None:
    spark = crear_spark()

    try:
        print(
            "EXPORTACIÓN GOLD A PARQUET"
        )

        print(
            f"Objetos Gold a exportar: "
            f"{len(GOLD_OBJECTS)}"
        )

        for object_name in GOLD_OBJECTS:
            exportar_objeto_gold(
                spark,
                object_name,
            )

        print(
            f"{len(GOLD_OBJECTS)} objetos Gold "
            f"fueron exportados y validados."
        )

    finally:
        spark.stop()

if __name__ == "__main__":
    main()