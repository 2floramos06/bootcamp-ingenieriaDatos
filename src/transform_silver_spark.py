import argparse
import gc
import os

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


DWH_HOST = os.getenv("DWH_HOST", "postgres")
DWH_PORT = os.getenv("DWH_PORT", "5432")
DWH_DB = os.getenv("DWH_DB", "dwh")
DWH_USER = os.getenv("DWH_USER")
DWH_PASSWORD = os.getenv("DWH_PASSWORD")

if not DWH_USER or not DWH_PASSWORD:
    raise ValueError("No se encontraron DWH_USER o DWH_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{DWH_HOST}:{DWH_PORT}/{DWH_DB}"
JDBC_PROPERTIES = {
    "user": DWH_USER,
    "password": DWH_PASSWORD,
    "driver": "org.postgresql.Driver",
}

PARQUET_BASE_PATH = os.getenv(
    "PARQUET_BASE_PATH",
    "/home/jovyan/work/data/parquet/silver",
)

SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")
JDBC_WRITE_PARTITIONS = int(os.getenv("JDBC_WRITE_PARTITIONS", "2"))
PARQUET_WRITE_PARTITIONS = int(os.getenv("PARQUET_WRITE_PARTITIONS", "2"))

ENROLLMENT_STATUSES = ("completed", "active", "failed", "dropped")
SUBSCRIPTION_STATUSES = ("active", "cancelled", "paused")
INVOICE_STATUSES = ("paid", "pending", "overdue")
PAYMENT_METHODS = ("card", "bank_transfer", "paypal", "cash")
LEAD_SOURCES = ("web", "referral", "ads", "event", "cold_call")
LEAD_STATUSES = ("new", "contacted", "qualified", "converted", "lost")
OPPORTUNITY_STAGES = (
    "prospect",
    "qualification",
    "proposal",
    "negotiation",
    "won",
    "lost",
)
OPPORTUNITY_ROLES = (
    "influencer",
    "end_user",
    "financial",
    "technical",
    "decision_maker",
)
ACTIVITY_TYPES = ("email", "call", "meeting", "note", "demo")

#Spark, lecturas y escrituras
def crear_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("TransformSilver")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def leer_tabla_bronze(spark: SparkSession, tabla: str) -> DataFrame:
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"bronze.{tabla}",
        properties=JDBC_PROPERTIES,
    )


def leer_tabla_silver(
    spark: SparkSession,
    tabla: str,
    columnas: tuple[str, ...] | None = None,
) -> DataFrame:
    df = spark.read.jdbc(
        url=JDBC_URL,
        table=f"silver.{tabla}",
        properties=JDBC_PROPERTIES,
    )
    return df.select(*columnas) if columnas else df


def escribir_tabla_silver(df_silver: DataFrame, tabla: str) -> None:
    (
        df_silver.coalesce(JDBC_WRITE_PARTITIONS)
        .write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"silver.{tabla}")
        .option("user", DWH_USER)
        .option("password", DWH_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "5000")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def escribir_tabla_parquet(df_silver: DataFrame, tabla: str) -> None:
    ruta = f"{PARQUET_BASE_PATH}/{tabla}"
    (
        df_silver.coalesce(PARQUET_WRITE_PARTITIONS)
        .write.mode("overwrite")
        .parquet(ruta)
    )
    print(f"{tabla} exportado correctamente a Parquet")
    print(f"Ruta: {ruta}")


def contar_tabla_postgresql(spark: SparkSession, tabla: str) -> int:
    consulta = f"(SELECT COUNT(*)::bigint AS total FROM silver.{tabla}) AS conteo"
    fila = spark.read.jdbc(
        url=JDBC_URL,
        table=consulta,
        properties=JDBC_PROPERTIES,
    ).first()
    return int(fila["total"])


def validar_escrituras(
    spark: SparkSession,
    tabla: str,
    total_esperado: int,
) -> None:
    total_postgresql = contar_tabla_postgresql(spark, tabla)
    ruta = f"{PARQUET_BASE_PATH}/{tabla}"
    total_parquet = spark.read.parquet(ruta).count()

    print(f"Registros esperados en {tabla}: {total_esperado}")
    print(f"Registros en silver.{tabla}: {total_postgresql}")
    print(f"Registros en Parquet {tabla}: {total_parquet}")

    if total_postgresql != total_esperado:
        raise ValueError(f"La escritura de {tabla} en PostgreSQL no coincide")
    if total_parquet != total_esperado:
        raise ValueError(f"La escritura de {tabla} en Parquet no coincide")

    print(f"{tabla} validado correctamente en PostgreSQL y Parquet")


#Transformaciones
def seleccionar_bronze(df_bronze: DataFrame, *columnas) -> DataFrame:
    return df_bronze.select(
        *columnas,
        F.col("_source_file"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_batch_id"),
    )


def finalizar_silver(df: DataFrame, columnas: list[str]) -> DataFrame:
    return (
        df.withColumn("_silver_created_at", F.current_timestamp())
        .select(
            *columnas,
            "_source_file",
            "_bronze_ingested_at",
            "_batch_id",
            "_silver_created_at",
        )
    )

def texto_vacio(columna: str):
    return F.col(columna).isNull() | (F.trim(F.col(columna)) == "")

def texto_presente(columna: str):
    return ~texto_vacio(columna)

def referencia_id(df: DataFrame, columna: str, alias: str) -> DataFrame:
    return F.broadcast(
        df.select(F.col(columna).alias(alias)).dropDuplicates([alias])
    )

def contar_huerfanos(
    df_hijo: DataFrame,
    columna_hijo: str,
    df_padre: DataFrame,
    columna_padre: str | None = None,
) -> int:
    columna_padre = columna_padre or columna_hijo

    hijos = (
        df_hijo.select(F.trim(F.col(columna_hijo)).alias("_id"))
        .filter(texto_presente("_id"))
        .distinct()
    )
    padres = (
        df_padre.select(F.trim(F.col(columna_padre)).alias("_id"))
        .filter(texto_presente("_id"))
        .distinct()
    )
    return hijos.join(F.broadcast(padres), on="_id", how="left_anti").count()

#Transformaciones UNIVERSITY
def transformar_students(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("student_id").alias("student_id"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.to_date(F.trim("birth_date")).alias("birth_date"),
        F.to_timestamp(F.trim("enrolled_at")).alias("enrolled_at"),
        F.upper(F.trim("country")).alias("country"),
    )
    df = df.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("birth_date").isNotNull()
            & F.col("enrolled_at").isNotNull()
            & (F.col("birth_date") < F.to_date("enrolled_at")),
            True,
        ).otherwise(False),
    )
    return finalizar_silver(
        df,
        [
            "student_id",
            "first_name",
            "last_name",
            "email",
            "birth_date",
            "enrolled_at",
            "country",
            "is_temporally_valid",
        ],
    )


def transformar_professors(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("professor_id").alias("professor_id"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.trim("department").alias("department"),
        F.to_timestamp(F.trim("hired_at")).alias("hired_at"),
    )
    return finalizar_silver(
        df,
        [
            "professor_id",
            "first_name",
            "last_name",
            "email",
            "department",
            "hired_at",
        ],
    )


def transformar_courses(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("course_id").alias("course_id"),
        F.upper(F.trim("code")).alias("code"),
        F.trim("name").alias("name"),
        F.trim("credits").cast("integer").alias("credits"),
        F.trim("department").alias("department"),
        F.trim("professor_id").alias("professor_id"),
    )
    return finalizar_silver(
        df,
        ["course_id", "code", "name", "credits", "department", "professor_id"],
    )


def transformar_semesters(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("semester_id").alias("semester_id"),
        F.upper(F.trim("code")).alias("code"),
        F.trim("year").cast("integer").alias("year"),
        F.trim("half").cast("integer").alias("half"),
        F.to_date(F.trim("start_date")).alias("start_date"),
        F.to_date(F.trim("end_date")).alias("end_date"),
    )
    df = df.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("start_date").isNotNull()
            & F.col("end_date").isNotNull()
            & (F.col("start_date") <= F.col("end_date")),
            True,
        ).otherwise(False),
    )
    return finalizar_silver(
        df,
        [
            "semester_id",
            "code",
            "year",
            "half",
            "start_date",
            "end_date",
            "is_temporally_valid",
        ],
    )


def transformar_enrollments(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("enrollment_id").alias("enrollment_id"),
        F.to_date(F.trim("enrolled_at")).alias("enrolled_at"),
        F.lower(F.trim("status")).alias("status"),
        F.trim("student_id").alias("student_id"),
        F.trim("course_id").alias("course_id"),
        F.trim("semester_id").alias("semester_id"),
    )
    ventana = Window.partitionBy("student_id", "course_id", "semester_id")
    df = df.withColumn("is_repeated_combination", F.count("*").over(ventana) > 1)
    return finalizar_silver(
        df,
        [
            "enrollment_id",
            "enrolled_at",
            "status",
            "student_id",
            "course_id",
            "semester_id",
            "is_repeated_combination",
        ],
    )


def transformar_grades(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("grade_id").alias("grade_id"),
        F.lower(F.trim("assessment")).alias("assessment"),
        F.trim("score").cast("decimal(6,2)").alias("score"),
        F.trim("weight").cast("decimal(5,4)").alias("weight"),
        F.to_date(F.trim("graded_at")).alias("graded_at"),
        F.trim("enrollment_id").alias("enrollment_id"),
    )

    ventana = Window.partitionBy("enrollment_id")
    df = (
        df.withColumn(
            "total_weight",
            F.round(F.sum("weight").over(ventana), 4).cast("decimal(8,4)"),
        )
        .withColumn("_grade_count", F.count(F.lit(1)).over(ventana))
        .withColumn("_weight_count", F.count("weight").over(ventana))
        .withColumn(
            "is_score_valid",
            F.col("score").isNotNull()
            & F.col("score").between(0, 100),
        )
        .withColumn(
            "is_weight_valid",
            F.col("weight").isNotNull()
            & (F.col("weight") > 0)
            & (F.col("weight") <= 1),
        )
        .withColumn(
            "is_weight_sum_one",
            (F.col("_grade_count") == F.col("_weight_count"))
            & F.col("total_weight").isNotNull()
            & (F.col("total_weight") == F.lit(1).cast("decimal(8,4)")),
        )
        .drop("_grade_count", "_weight_count")
    )

    return finalizar_silver(
        df,
        [
            "grade_id",
            "assessment",
            "score",
            "weight",
            "total_weight",
            "graded_at",
            "enrollment_id",
            "is_score_valid",
            "is_weight_valid",
            "is_weight_sum_one",
        ],
    )

#Transformaciones BILLING
def transformar_customers(
    df_bronze: DataFrame,
    df_students_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("customer_id").alias("customer_id"),
        F.trim("external_ref").alias("external_ref"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.upper(F.trim("country")).alias("country"),
        F.to_timestamp(F.trim("created_at")).alias("created_at"),
        F.lower(F.trim("segment")).alias("segment"),
    )
    estudiantes = referencia_id(df_students_silver, "student_id", "_student_id_ref")
    df = (
        df.join(estudiantes, F.col("external_ref") == F.col("_student_id_ref"), "left")
        .withColumn("is_student_linked", F.col("_student_id_ref").isNotNull())
        .drop("_student_id_ref")
    )
    return finalizar_silver(
        df,
        [
            "customer_id",
            "external_ref",
            "first_name",
            "last_name",
            "email",
            "country",
            "created_at",
            "segment",
            "is_student_linked",
        ],
    )


def transformar_products(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("product_id").alias("product_id"),
        F.upper(F.trim("sku")).alias("sku"),
        F.trim("name").alias("name"),
        F.lower(F.trim("category")).alias("category"),
        F.trim("monthly_price").cast("decimal(10,2)").alias("monthly_price"),
        F.lower(F.trim("active")).cast("boolean").alias("active"),
    )
    return finalizar_silver(
        df,
        ["product_id", "sku", "name", "category", "monthly_price", "active"],
    )


def transformar_subscriptions(
    df_bronze: DataFrame,
    df_customers_silver: DataFrame,
    df_products_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("subscription_id").alias("subscription_id"),
        F.lower(F.trim("status")).alias("status"),
        F.to_date(F.trim("start_date")).alias("start_date"),
        F.to_date(F.trim("end_date")).alias("end_date"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("product_id").alias("product_id"),
    )
    df = df.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("start_date").isNotNull()
            & F.col("end_date").isNotNull()
            & (F.col("start_date") <= F.col("end_date")),
            True,
        ).otherwise(False),
    )

    customers = referencia_id(df_customers_silver, "customer_id", "_customer_id_ref")
    products = referencia_id(df_products_silver, "product_id", "_product_id_ref")
    df = (
        df.join(customers, F.col("customer_id") == F.col("_customer_id_ref"), "left")
        .withColumn("is_customer_linked", F.col("_customer_id_ref").isNotNull())
        .drop("_customer_id_ref")
        .join(products, F.col("product_id") == F.col("_product_id_ref"), "left")
        .withColumn("is_product_linked", F.col("_product_id_ref").isNotNull())
        .drop("_product_id_ref")
    )
    return finalizar_silver(
        df,
        [
            "subscription_id",
            "status",
            "start_date",
            "end_date",
            "customer_id",
            "product_id",
            "is_temporally_valid",
            "is_customer_linked",
            "is_product_linked",
        ],
    )


def transformar_invoices(
    df_bronze: DataFrame,
    df_customers_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("invoice_id").alias("invoice_id"),
        F.to_date(F.trim("issued_at")).alias("issued_at"),
        F.to_date(F.trim("due_at")).alias("due_at"),
        F.trim("total").cast("decimal(12,2)").alias("total"),
        F.lower(F.trim("status")).alias("status"),
        F.upper(F.trim("currency")).alias("currency"),
        F.trim("customer_id").alias("customer_id"),
    )
    df = (
        df.withColumn(
            "is_temporally_valid",
            F.when(
                F.col("issued_at").isNotNull()
                & F.col("due_at").isNotNull()
                & (F.col("issued_at") <= F.col("due_at")),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "is_total_valid",
            F.col("total").isNotNull() & (F.col("total") >= 0),
        )
    )
    customers = referencia_id(df_customers_silver, "customer_id", "_customer_id_ref")
    df = (
        df.join(customers, F.col("customer_id") == F.col("_customer_id_ref"), "left")
        .withColumn("is_customer_linked", F.col("_customer_id_ref").isNotNull())
        .drop("_customer_id_ref")
    )
    return finalizar_silver(
        df,
        [
            "invoice_id",
            "issued_at",
            "due_at",
            "total",
            "status",
            "currency",
            "customer_id",
            "is_temporally_valid",
            "is_total_valid",
            "is_customer_linked",
        ],
    )


def transformar_invoice_items(
    df_bronze: DataFrame,
    df_invoices_silver: DataFrame,
    df_products_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("invoice_item_id").alias("invoice_item_id"),
        F.trim("quantity").cast("integer").alias("quantity"),
        F.trim("unit_price").cast("decimal(12,2)").alias("unit_price"),
        F.trim("line_total").cast("decimal(14,2)").alias("line_total"),
        F.trim("invoice_id").alias("invoice_id"),
        F.trim("product_id").alias("product_id"),
    )
    df = (
        df.withColumn(
            "calculated_line_total",
            F.round(F.col("quantity") * F.col("unit_price"), 2).cast("decimal(14,2)"),
        )
        .withColumn(
            "is_quantity_valid",
            F.col("quantity").isNotNull() & (F.col("quantity") > 0),
        )
        .withColumn(
            "is_unit_price_valid",
            F.col("unit_price").isNotNull() & (F.col("unit_price") >= 0),
        )
        .withColumn(
            "is_line_total_valid",
            F.coalesce(
                F.abs(F.col("line_total") - F.col("calculated_line_total"))
                <= F.lit(0.01),
                F.lit(False),
            ),
        )
    )

    invoices = referencia_id(df_invoices_silver, "invoice_id", "_invoice_id_ref")
    products = referencia_id(df_products_silver, "product_id", "_product_id_ref")
    df = (
        df.join(invoices, F.col("invoice_id") == F.col("_invoice_id_ref"), "left")
        .withColumn("is_invoice_linked", F.col("_invoice_id_ref").isNotNull())
        .drop("_invoice_id_ref")
        .join(products, F.col("product_id") == F.col("_product_id_ref"), "left")
        .withColumn("is_product_linked", F.col("_product_id_ref").isNotNull())
        .drop("_product_id_ref")
    )
    return finalizar_silver(
        df,
        [
            "invoice_item_id",
            "quantity",
            "unit_price",
            "line_total",
            "calculated_line_total",
            "invoice_id",
            "product_id",
            "is_quantity_valid",
            "is_unit_price_valid",
            "is_line_total_valid",
            "is_invoice_linked",
            "is_product_linked",
        ],
    )


def transformar_payments(
    df_bronze: DataFrame,
    df_invoices_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("payment_id").alias("payment_id"),
        F.trim("amount").cast("decimal(12,2)").alias("amount"),
        F.to_date(F.trim("paid_at")).alias("paid_at"),
        F.lower(F.trim("method")).alias("method"),
        F.trim("invoice_id").alias("invoice_id"),
    )
    df = (
        df.withColumn(
            "is_amount_valid",
            F.col("amount").isNotNull() & (F.col("amount") > 0),
        )
        .withColumn(
            "is_method_valid",
            F.col("method").isNotNull() & F.col("method").isin(*PAYMENT_METHODS),
        )
    )

    invoices = F.broadcast(
        df_invoices_silver.select(
            F.col("invoice_id").alias("_invoice_id_ref"),
            F.col("issued_at").alias("_invoice_issued_at"),
        ).dropDuplicates(["_invoice_id_ref"])
    )
    df = (
        df.join(invoices, F.col("invoice_id") == F.col("_invoice_id_ref"), "left")
        .withColumn("is_invoice_linked", F.col("_invoice_id_ref").isNotNull())
        .withColumn(
            "is_temporally_valid",
            F.when(
                F.col("paid_at").isNotNull()
                & F.col("_invoice_issued_at").isNotNull()
                & (F.col("paid_at") >= F.col("_invoice_issued_at")),
                True,
            ).otherwise(False),
        )
        .drop("_invoice_id_ref", "_invoice_issued_at")
    )
    return finalizar_silver(
        df,
        [
            "payment_id",
            "amount",
            "paid_at",
            "method",
            "invoice_id",
            "is_amount_valid",
            "is_method_valid",
            "is_invoice_linked",
            "is_temporally_valid",
        ],
    )

#Transformaciones CRM
def transformar_accounts(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("account_id").alias("account_id"),
        F.trim("name").alias("name"),
        F.lower(F.trim("industry")).alias("industry"),
        F.upper(F.trim("country")).alias("country"),
        F.trim("annual_revenue").cast("decimal(16,2)").alias("annual_revenue"),
        F.trim("employees").cast("integer").alias("employees"),
        F.to_timestamp(F.trim("created_at")).alias("created_at"),
    )
    df = (
        df.withColumn(
            "is_annual_revenue_valid",
            F.col("annual_revenue").isNotNull() & (F.col("annual_revenue") >= 0),
        )
        .withColumn(
            "is_employees_valid",
            F.col("employees").isNotNull() & (F.col("employees") >= 0),
        )
        .withColumn("is_created_at_valid", F.col("created_at").isNotNull())
    )
    return finalizar_silver(
        df,
        [
            "account_id",
            "name",
            "industry",
            "country",
            "annual_revenue",
            "employees",
            "created_at",
            "is_annual_revenue_valid",
            "is_employees_valid",
            "is_created_at_valid",
        ],
    )


def transformar_contacts(
    df_bronze: DataFrame,
    df_accounts_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("contact_id").alias("contact_id"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.trim("phone").alias("phone"),
        F.trim("title").alias("title"),
        F.to_timestamp(F.trim("created_at")).alias("created_at"),
        F.trim("account_id").alias("account_id"),
    )
    df = (
        df.withColumn("is_email_present", texto_presente("email"))
        .withColumn("is_created_at_valid", F.col("created_at").isNotNull())
    )
    accounts = referencia_id(df_accounts_silver, "account_id", "_account_id_ref")
    df = (
        df.join(accounts, F.col("account_id") == F.col("_account_id_ref"), "left")
        .withColumn("is_account_linked", F.col("_account_id_ref").isNotNull())
        .drop("_account_id_ref")
    )
    return finalizar_silver(
        df,
        [
            "contact_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "title",
            "created_at",
            "account_id",
            "is_email_present",
            "is_created_at_valid",
            "is_account_linked",
        ],
    )


def transformar_leads(df_bronze: DataFrame) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("lead_id").alias("lead_id"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.lower(F.trim("source")).alias("source"),
        F.lower(F.trim("status")).alias("status"),
        F.trim("score").cast("integer").alias("score"),
        F.to_timestamp(F.trim("created_at")).alias("created_at"),
    )
    df = (
        df.withColumn("is_email_present", texto_presente("email"))
        .withColumn(
            "is_source_valid",
            F.col("source").isNotNull() & F.col("source").isin(*LEAD_SOURCES),
        )
        .withColumn(
            "is_status_valid",
            F.col("status").isNotNull() & F.col("status").isin(*LEAD_STATUSES),
        )
        .withColumn(
            "is_score_valid",
            F.col("score").isNotNull() & F.col("score").between(0, 100),
        )
        .withColumn("is_created_at_valid", F.col("created_at").isNotNull())
    )
    return finalizar_silver(
        df,
        [
            "lead_id",
            "first_name",
            "last_name",
            "email",
            "source",
            "status",
            "score",
            "created_at",
            "is_email_present",
            "is_source_valid",
            "is_status_valid",
            "is_score_valid",
            "is_created_at_valid",
        ],
    )


def transformar_opportunities(
    df_bronze: DataFrame,
    df_accounts_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("opportunity_id").alias("opportunity_id"),
        F.trim("name").alias("name"),
        F.lower(F.trim("stage")).alias("stage"),
        F.trim("amount").cast("decimal(16,2)").alias("amount"),
        F.to_date(F.trim("close_date")).alias("close_date"),
        F.to_timestamp(F.trim("created_at")).alias("created_at"),
        F.trim("account_id").alias("account_id"),
    )
    df = (
        df.withColumn(
            "is_stage_valid",
            F.col("stage").isNotNull() & F.col("stage").isin(*OPPORTUNITY_STAGES),
        )
        .withColumn(
            "is_amount_valid",
            F.col("amount").isNotNull() & (F.col("amount") > 0),
        )
        .withColumn(
            "is_temporally_valid",
            F.when(
                F.col("created_at").isNotNull()
                & F.col("close_date").isNotNull()
                & (F.to_date("created_at") <= F.col("close_date")),
                True,
            ).otherwise(False),
        )
    )
    accounts = referencia_id(df_accounts_silver, "account_id", "_account_id_ref")
    df = (
        df.join(accounts, F.col("account_id") == F.col("_account_id_ref"), "left")
        .withColumn("is_account_linked", F.col("_account_id_ref").isNotNull())
        .drop("_account_id_ref")
    )
    return finalizar_silver(
        df,
        [
            "opportunity_id",
            "name",
            "stage",
            "amount",
            "close_date",
            "created_at",
            "account_id",
            "is_stage_valid",
            "is_amount_valid",
            "is_temporally_valid",
            "is_account_linked",
        ],
    )


def transformar_opportunity_contacts(
    df_bronze: DataFrame,
    df_opportunities_silver: DataFrame,
    df_contacts_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("opportunity_id").alias("opportunity_id"),
        F.trim("contact_id").alias("contact_id"),
        F.lower(F.trim("role")).alias("role"),
    )
    df = df.withColumn(
        "is_role_valid",
        F.col("role").isNotNull() & F.col("role").isin(*OPPORTUNITY_ROLES),
    )

    opportunities = F.broadcast(
        df_opportunities_silver.select(
            F.col("opportunity_id").alias("_opportunity_id_ref"),
            F.col("account_id").alias("_opportunity_account_id"),
        ).dropDuplicates(["_opportunity_id_ref"])
    )
    contacts = F.broadcast(
        df_contacts_silver.select(
            F.col("contact_id").alias("_contact_id_ref"),
            F.col("account_id").alias("_contact_account_id"),
        ).dropDuplicates(["_contact_id_ref"])
    )
    df = (
        df.join(
            opportunities,
            F.col("opportunity_id") == F.col("_opportunity_id_ref"),
            "left",
        )
        .withColumn(
            "is_opportunity_linked",
            F.col("_opportunity_id_ref").isNotNull(),
        )
        .join(contacts, F.col("contact_id") == F.col("_contact_id_ref"), "left")
        .withColumn("is_contact_linked", F.col("_contact_id_ref").isNotNull())
        .withColumn(
            "is_same_account",
            F.when(
                F.col("is_opportunity_linked")
                & F.col("is_contact_linked")
                & (F.col("_opportunity_account_id") == F.col("_contact_account_id")),
                True,
            ).otherwise(False),
        )
        .drop(
            "_opportunity_id_ref",
            "_contact_id_ref",
            "_opportunity_account_id",
            "_contact_account_id",
        )
    )
    return finalizar_silver(
        df,
        [
            "opportunity_id",
            "contact_id",
            "role",
            "is_role_valid",
            "is_opportunity_linked",
            "is_contact_linked",
            "is_same_account",
        ],
    )


def transformar_activities(
    df_bronze: DataFrame,
    df_contacts_silver: DataFrame,
    df_opportunities_silver: DataFrame,
) -> DataFrame:
    df = seleccionar_bronze(
        df_bronze,
        F.trim("activity_id").alias("activity_id"),
        F.lower(F.trim("type")).alias("activity_type"),
        F.trim("subject").alias("subject"),
        F.to_timestamp(F.trim("occurred_at")).alias("occurred_at"),
        F.when(F.trim("contact_id") == "", None)
        .otherwise(F.trim("contact_id"))
        .cast("string")
        .alias("contact_id"),
        F.when(F.trim("opportunity_id") == "", None)
        .otherwise(F.trim("opportunity_id"))
        .cast("string")
        .alias("opportunity_id"),
    )
    df = (
        df.withColumn(
            "is_type_valid",
            F.col("activity_type").isNotNull()
            & F.col("activity_type").isin(*ACTIVITY_TYPES),
        )
        .withColumn("is_occurred_at_valid", F.col("occurred_at").isNotNull())
        .withColumn("has_contact_id", F.col("contact_id").isNotNull())
        .withColumn("has_opportunity_id", F.col("opportunity_id").isNotNull())
        .withColumn(
            "has_any_reference",
            F.col("has_contact_id") | F.col("has_opportunity_id"),
        )
    )

    contacts = referencia_id(df_contacts_silver, "contact_id", "_contact_id_ref")
    opportunities = referencia_id(
        df_opportunities_silver,
        "opportunity_id",
        "_opportunity_id_ref",
    )
    df = (
        df.join(contacts, F.col("contact_id") == F.col("_contact_id_ref"), "left")
        .withColumn(
            "is_contact_linked",
            F.col("has_contact_id") & F.col("_contact_id_ref").isNotNull(),
        )
        .join(
            opportunities,
            F.col("opportunity_id") == F.col("_opportunity_id_ref"),
            "left",
        )
        .withColumn(
            "is_opportunity_linked",
            F.col("has_opportunity_id")
            & F.col("_opportunity_id_ref").isNotNull(),
        )
        .drop("_contact_id_ref", "_opportunity_id_ref")
    )
    return finalizar_silver(
        df,
        [
            "activity_id",
            "activity_type",
            "subject",
            "occurred_at",
            "contact_id",
            "opportunity_id",
            "is_type_valid",
            "is_occurred_at_valid",
            "has_contact_id",
            "has_opportunity_id",
            "has_any_reference",
            "is_contact_linked",
            "is_opportunity_linked",
        ],
    )

#Validaciones genéricas
Regla = tuple[str, str, object]


def contar_reglas(df: DataFrame, reglas: list[Regla]) -> dict[str, int]:
    expresiones = [F.count(F.lit(1)).alias("_total")]
    for clave, _, condicion in reglas:
        expresiones.append(
            F.coalesce(
                F.sum(F.when(condicion, 1).otherwise(0)),
                F.lit(0),
            )
            .cast("long")
            .alias(clave)
        )
    fila = df.agg(*expresiones).first().asDict()
    return {clave: int(valor or 0) for clave, valor in fila.items()}


def validar_con_reglas(
    df_bronze: DataFrame,
    df_silver: DataFrame,
    tabla: str,
    claves: str | tuple[str, ...],
    reglas: list[Regla] | None = None,
) -> int:
    claves = (claves,) if isinstance(claves, str) else claves
    reglas = reglas or []

    condicion_clave_nula = texto_vacio(claves[0])
    for clave in claves[1:]:
        condicion_clave_nula = condicion_clave_nula | texto_vacio(clave)

    reglas_completas = [
        (
            "claves_nulas",
            f"Claves nulas o vacías ({', '.join(claves)})",
            condicion_clave_nula,
        ),
        *reglas,
    ]

    total_bronze = df_bronze.count()
    estadisticas = contar_reglas(df_silver, reglas_completas)
    total_silver = estadisticas["_total"]
    duplicados = (
        df_silver.groupBy(*claves)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"VALIDACIONES DE {tabla.upper()}")

    print(f"Registros Bronze: {total_bronze}")
    print(f"Registros transformados: {total_silver}")
    print(f"Grupos duplicados por clave: {duplicados}")
    for clave, mensaje, _ in reglas_completas:
        print(f"{mensaje}: {estadisticas[clave]}")

    if total_bronze != total_silver:
        raise ValueError(f"Bronze y Silver tienen conteos diferentes en {tabla}")
    if estadisticas["claves_nulas"] > 0:
        raise ValueError(f"Existen claves nulas o vacías en {tabla}")
    if duplicados > 0:
        raise ValueError(f"Existen claves duplicadas en {tabla}")

    return total_silver

#Validaciones UNIVERSITY
def validar_students(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "students",
        "student_id",
        [
            (
                "fechas_invalidas",
                "Relaciones temporales inválidas",
                ~F.col("is_temporally_valid"),
            )
        ],
    )


def validar_professors(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "professors",
        "professor_id",
        [("hired_at_nulos", "Hired_at nulos o no convertibles", F.col("hired_at").isNull())],
    )


def validar_courses(
    df_bronze: DataFrame,
    df_silver: DataFrame,
    df_professors_silver: DataFrame,
) -> int:
    total = validar_con_reglas(
        df_bronze,
        df_silver,
        "courses",
        "course_id",
        [
            ("credits_nulos", "Credits nulos o no convertibles", F.col("credits").isNull()),
            ("credits_invalidos", "Credits menores o iguales a cero", F.col("credits") <= 0),
            ("professor_id_nulos", "Professor ID nulos o vacíos", texto_vacio("professor_id")),
        ],
    )
    huerfanos = contar_huerfanos(df_silver, "professor_id", df_professors_silver)
    print(f"Professor ID sin profesor existente: {huerfanos}")
    return total


def validar_semesters(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "semesters",
        "semester_id",
        [
            ("year_nulos", "Year nulos o no convertibles", F.col("year").isNull()),
            ("half_nulos", "Half nulos o no convertibles", F.col("half").isNull()),
            (
                "half_invalidos",
                "Half diferentes de 1 o 2",
                F.col("half").isNotNull() & ~F.col("half").isin(1, 2),
            ),
            (
                "fechas_nulas",
                "Registros con fechas nulas",
                F.col("start_date").isNull() | F.col("end_date").isNull(),
            ),
            (
                "fechas_inconsistentes",
                "Registros con relación temporal inválida",
                ~F.col("is_temporally_valid"),
            ),
        ],
    )


def validar_enrollments(
    df_bronze: DataFrame,
    df_silver: DataFrame,
    df_students_silver: DataFrame,
    df_courses_silver: DataFrame,
    df_semesters_silver: DataFrame,
) -> int:
    total = validar_con_reglas(
        df_bronze,
        df_silver,
        "enrollments",
        "enrollment_id",
        [
            ("fechas_nulas", "Enrolled_at nulos o no convertibles", F.col("enrolled_at").isNull()),
            (
                "estados_invalidos",
                "Estados inválidos",
                F.col("status").isNull() | ~F.col("status").isin(*ENROLLMENT_STATUSES),
            ),
            (
                "combinaciones_repetidas",
                "Filas en combinaciones repetidas",
                F.col("is_repeated_combination"),
            ),
        ],
    )

    students_huerfanos = contar_huerfanos(df_silver, "student_id", df_students_silver)
    courses_huerfanos = contar_huerfanos(df_silver, "course_id", df_courses_silver)
    semesters_huerfanos = contar_huerfanos(df_silver, "semester_id", df_semesters_silver)
    grupos_repetidos = (
        df_silver.groupBy("student_id", "course_id", "semester_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Students huérfanos: {students_huerfanos}")
    print(f"Courses huérfanos: {courses_huerfanos}")
    print(f"Semesters huérfanos: {semesters_huerfanos}")
    print(f"Grupos repetidos: {grupos_repetidos}")
    return total


def validar_grades(
    df_bronze: DataFrame,
    df_silver: DataFrame,
    df_enrollments_silver: DataFrame,
) -> int:
    total = validar_con_reglas(
        df_bronze,
        df_silver,
        "grades",
        "grade_id",
        [
            ("assessment_nulos", "Assessment nulos o vacíos", texto_vacio("assessment")),
            ("scores_invalidos", "Scores inválidos", ~F.col("is_score_valid")),
            ("weights_invalidos", "Weights individuales inválidos", ~F.col("is_weight_valid")),
            ("fechas_nulas", "Graded_at nulos o no convertibles", F.col("graded_at").isNull()),
            ("enrollment_id_nulos", "Enrollment ID nulos o vacíos", texto_vacio("enrollment_id")),
            (
                "filas_peso_invalido",
                "Filas cuyo peso total no suma 1",
                ~F.col("is_weight_sum_one"),
            ),
        ],
    )
    huerfanos = contar_huerfanos(df_silver, "enrollment_id", df_enrollments_silver)
    enrollments_peso_invalido = (
        df_silver.filter(~F.col("is_weight_sum_one"))
        .select("enrollment_id")
        .distinct()
        .count()
    )
    print(f"Enrollments huérfanos: {huerfanos}")
    print(f"Inscripciones cuyo peso total no suma 1: {enrollments_peso_invalido}")
    return total

#Validaciones BILLING
def validar_customers(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    total = validar_con_reglas(
        df_bronze,
        df_silver,
        "customers",
        "customer_id",
        [
            ("external_ref_nulos", "External ref nulos o vacíos", texto_vacio("external_ref")),
            ("created_at_nulos", "Created_at nulos o no convertibles", F.col("created_at").isNull()),
            ("segment_nulos", "Segment nulos o vacíos", texto_vacio("segment")),
            (
                "customers_sin_estudiante",
                "Customers sin estudiante relacionado",
                ~F.col("is_student_linked"),
            ),
        ],
    )
    duplicados = (
        df_silver.filter(texto_presente("external_ref"))
        .groupBy("external_ref")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    print(f"External ref duplicados: {duplicados}")
    return total


def validar_products(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    total = validar_con_reglas(
        df_bronze,
        df_silver,
        "products",
        "product_id",
        [
            ("sku_nulos", "SKU nulos o vacíos", texto_vacio("sku")),
            ("nombres_nulos", "Nombres nulos o vacíos", texto_vacio("name")),
            ("categorias_nulas", "Categorías nulas o vacías", texto_vacio("category")),
            (
                "precios_nulos",
                "Monthly price nulos o no convertibles",
                F.col("monthly_price").isNull(),
            ),
            (
                "precios_invalidos",
                "Monthly price menores a cero",
                F.col("monthly_price").isNotNull() & (F.col("monthly_price") < 0),
            ),
            ("active_nulos", "Active nulos o no convertibles", F.col("active").isNull()),
        ],
    )
    duplicados = (
        df_silver.filter(texto_presente("sku"))
        .groupBy("sku")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    print(f"SKU duplicados: {duplicados}")
    return total


def validar_subscriptions(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "subscriptions",
        "subscription_id",
        [
            (
                "estados_invalidos",
                "Estados inválidos",
                F.col("status").isNull() | ~F.col("status").isin(*SUBSCRIPTION_STATUSES),
            ),
            (
                "fechas_nulas",
                "Fechas nulas o no convertibles",
                F.col("start_date").isNull() | F.col("end_date").isNull(),
            ),
            (
                "fechas_inconsistentes",
                "Suscripciones con fechas inconsistentes",
                ~F.col("is_temporally_valid"),
            ),
            ("customer_id_nulos", "Customer ID nulos o vacíos", texto_vacio("customer_id")),
            ("product_id_nulos", "Product ID nulos o vacíos", texto_vacio("product_id")),
            (
                "customers_huerfanos",
                "Suscripciones sin customer relacionado",
                ~F.col("is_customer_linked"),
            ),
            (
                "products_huerfanos",
                "Suscripciones sin product relacionado",
                ~F.col("is_product_linked"),
            ),
        ],
    )


def validar_invoices(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "invoices",
        "invoice_id",
        [
            (
                "fechas_nulas",
                "Fechas nulas o no convertibles",
                F.col("issued_at").isNull() | F.col("due_at").isNull(),
            ),
            (
                "fechas_inconsistentes",
                "Facturas con fechas inconsistentes",
                ~F.col("is_temporally_valid"),
            ),
            (
                "totales_invalidos",
                "Totales nulos, negativos o no convertibles",
                ~F.col("is_total_valid"),
            ),
            (
                "estados_invalidos",
                "Estados inválidos",
                F.col("status").isNull() | ~F.col("status").isin(*INVOICE_STATUSES),
            ),
            ("monedas_nulas", "Monedas nulas o vacías", texto_vacio("currency")),
            ("customer_id_nulos", "Customer ID nulos o vacíos", texto_vacio("customer_id")),
            (
                "customers_huerfanos",
                "Facturas sin customer relacionado",
                ~F.col("is_customer_linked"),
            ),
        ],
    )


def validar_invoice_items(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "invoice_items",
        "invoice_item_id",
        [
            (
                "cantidades_invalidas",
                "Cantidades nulas, no convertibles o <= 0",
                ~F.col("is_quantity_valid"),
            ),
            (
                "precios_invalidos",
                "Precios nulos, no convertibles o negativos",
                ~F.col("is_unit_price_valid"),
            ),
            (
                "totales_linea_invalidos",
                "Líneas donde quantity * unit_price no coincide",
                ~F.col("is_line_total_valid"),
            ),
            ("invoice_id_nulos", "Invoice ID nulos o vacíos", texto_vacio("invoice_id")),
            ("product_id_nulos", "Product ID nulos o vacíos", texto_vacio("product_id")),
            (
                "invoices_huerfanas",
                "Items sin invoice relacionada",
                ~F.col("is_invoice_linked"),
            ),
            (
                "products_huerfanos",
                "Items sin product relacionado",
                ~F.col("is_product_linked"),
            ),
        ],
    )


def validar_payments(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "payments",
        "payment_id",
        [
            (
                "amounts_invalidos",
                "Amounts nulos, no convertibles o <= 0",
                ~F.col("is_amount_valid"),
            ),
            ("fechas_nulas", "Paid_at nulos o no convertibles", F.col("paid_at").isNull()),
            ("metodos_invalidos", "Métodos de pago inválidos", ~F.col("is_method_valid")),
            ("invoice_id_nulos", "Invoice ID nulos o vacíos", texto_vacio("invoice_id")),
            (
                "invoices_huerfanas",
                "Payments sin invoice relacionada",
                ~F.col("is_invoice_linked"),
            ),
            (
                "fechas_inconsistentes",
                "Payments anteriores a la emisión",
                ~F.col("is_temporally_valid"),
            ),
        ],
    )

#Validaciones CRM
def validar_accounts(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "accounts",
        "account_id",
        [
            ("nombres_nulos", "Nombres nulos o vacíos", texto_vacio("name")),
            ("industrias_nulas", "Industrias nulas o vacías", texto_vacio("industry")),
            ("paises_nulos", "Países nulos o vacíos", texto_vacio("country")),
            (
                "revenues_invalidos",
                "Annual revenue nulos, negativos o no convertibles",
                ~F.col("is_annual_revenue_valid"),
            ),
            (
                "employees_invalidos",
                "Employees nulos, negativos o no convertibles",
                ~F.col("is_employees_valid"),
            ),
            (
                "created_at_invalidos",
                "Created_at nulos o no convertibles",
                ~F.col("is_created_at_valid"),
            ),
        ],
    )


def validar_contacts(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "contacts",
        "contact_id",
        [
            ("nombres_nulos", "First name nulos o vacíos", texto_vacio("first_name")),
            ("apellidos_nulos", "Last name nulos o vacíos", texto_vacio("last_name")),
            ("emails_faltantes", "Emails nulos o vacíos", ~F.col("is_email_present")),
            ("phones_nulos", "Phones nulos o vacíos", texto_vacio("phone")),
            ("titles_nulos", "Titles nulos o vacíos", texto_vacio("title")),
            (
                "created_at_invalidos",
                "Created_at nulos o no convertibles",
                ~F.col("is_created_at_valid"),
            ),
            ("account_id_nulos", "Account ID nulos o vacíos", texto_vacio("account_id")),
            (
                "accounts_huerfanas",
                "Contacts sin account relacionada",
                ~F.col("is_account_linked"),
            ),
        ],
    )


def validar_leads(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "leads",
        "lead_id",
        [
            ("nombres_nulos", "First name nulos o vacíos", texto_vacio("first_name")),
            ("apellidos_nulos", "Last name nulos o vacíos", texto_vacio("last_name")),
            ("emails_faltantes", "Emails nulos o vacíos", ~F.col("is_email_present")),
            ("fuentes_invalidas", "Fuentes inválidas", ~F.col("is_source_valid")),
            ("estados_invalidos", "Estados inválidos", ~F.col("is_status_valid")),
            (
                "scores_invalidos",
                "Scores nulos o fuera del rango 0-100",
                ~F.col("is_score_valid"),
            ),
            (
                "fechas_invalidas",
                "Created_at nulos o no convertibles",
                ~F.col("is_created_at_valid"),
            ),
        ],
    )


def validar_opportunities(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "opportunities",
        "opportunity_id",
        [
            ("nombres_nulos", "Nombres nulos o vacíos", texto_vacio("name")),
            ("etapas_invalidas", "Etapas inválidas", ~F.col("is_stage_valid")),
            (
                "amounts_invalidos",
                "Amounts nulos, no convertibles o <= 0",
                ~F.col("is_amount_valid"),
            ),
            ("created_at_invalidos", "Created_at nulos o no convertibles", F.col("created_at").isNull()),
            ("close_date_invalidos", "Close_date nulos o no convertibles", F.col("close_date").isNull()),
            (
                "fechas_inconsistentes",
                "Created_at posterior a close_date",
                ~F.col("is_temporally_valid"),
            ),
            ("account_id_nulos", "Account ID nulos o vacíos", texto_vacio("account_id")),
            (
                "accounts_huerfanas",
                "Opportunities sin account relacionada",
                ~F.col("is_account_linked"),
            ),
        ],
    )


def validar_opportunity_contacts(
    df_bronze: DataFrame,
    df_silver: DataFrame,
) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "opportunity_contacts",
        ("opportunity_id", "contact_id"),
        [
            ("roles_invalidos", "Roles inválidos", ~F.col("is_role_valid")),
            (
                "opportunities_huerfanas",
                "Opportunities sin relación",
                ~F.col("is_opportunity_linked"),
            ),
            (
                "contacts_huerfanos",
                "Contacts sin relación",
                ~F.col("is_contact_linked"),
            ),
            (
                "misma_cuenta",
                "Relaciones de la misma cuenta",
                F.col("is_same_account"),
            ),
            (
                "distinta_cuenta",
                "Relaciones de distinta cuenta",
                ~F.col("is_same_account"),
            ),
        ],
    )


def validar_activities(df_bronze: DataFrame, df_silver: DataFrame) -> int:
    return validar_con_reglas(
        df_bronze,
        df_silver,
        "activities",
        "activity_id",
        [
            ("tipos_invalidos", "Tipos de actividad inválidos", ~F.col("is_type_valid")),
            ("subjects_vacios", "Subjects nulos o vacíos", texto_vacio("subject")),
            (
                "fechas_invalidas",
                "Occurred_at nulos o no convertibles",
                ~F.col("is_occurred_at_valid"),
            ),
            ("sin_contacto", "Actividades sin contacto", ~F.col("has_contact_id")),
            (
                "sin_oportunidad",
                "Actividades sin oportunidad",
                ~F.col("has_opportunity_id"),
            ),
            (
                "sin_ninguna_relacion",
                "Actividades sin ninguna relación",
                ~F.col("has_any_reference"),
            ),
            (
                "con_ambas_relaciones",
                "Actividades con ambas relaciones",
                F.col("has_contact_id") & F.col("has_opportunity_id"),
            ),
            (
                "contacts_huerfanos",
                "Contact IDs informados pero huérfanos",
                F.col("has_contact_id") & ~F.col("is_contact_linked"),
            ),
            (
                "opportunities_huerfanas",
                "Opportunity IDs informados pero huérfanos",
                F.col("has_opportunity_id") & ~F.col("is_opportunity_linked"),
            ),
        ],
    )

#Ejec. genérica de una tabla
def procesar_tabla(
    spark: SparkSession,
    tabla: str,
    transformador,
    validador,
    argumentos_transformacion: tuple = (),
    argumentos_validacion: tuple = (),
) -> None:
    print(f"PROCESANDO {tabla.upper()}")
    print("-" * 58)

    df_bronze = leer_tabla_bronze(spark, tabla)
    df_silver = transformador(df_bronze, *argumentos_transformacion)

    df_silver.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        total = validador(df_bronze, df_silver, *argumentos_validacion)
        escribir_tabla_silver(df_silver, tabla)
        escribir_tabla_parquet(df_silver, tabla)
        validar_escrituras(spark, tabla, total)
        print(f"{tabla} cargado correctamente")
    finally:
        df_silver.unpersist(blocking=True)
        spark.catalog.clearCache()
        del df_bronze
        del df_silver
        gc.collect()

#Ejec. por dominio
def procesar_university(spark: SparkSession) -> None:
    print("\nINICIANDO UNIVERSITY")
    procesar_tabla(spark, "students", transformar_students, validar_students)
    procesar_tabla(spark, "professors", transformar_professors, validar_professors)

    professors_ref = leer_tabla_silver(spark, "professors", ("professor_id",))
    procesar_tabla(
        spark,
        "courses",
        transformar_courses,
        validar_courses,
        argumentos_validacion=(professors_ref,),
    )
    del professors_ref

    procesar_tabla(spark, "semesters", transformar_semesters, validar_semesters)

    students_ref = leer_tabla_silver(spark, "students", ("student_id",))
    courses_ref = leer_tabla_silver(spark, "courses", ("course_id",))
    semesters_ref = leer_tabla_silver(spark, "semesters", ("semester_id",))
    procesar_tabla(
        spark,
        "enrollments",
        transformar_enrollments,
        validar_enrollments,
        argumentos_validacion=(students_ref, courses_ref, semesters_ref),
    )
    del students_ref, courses_ref, semesters_ref

    enrollments_ref = leer_tabla_silver(spark, "enrollments", ("enrollment_id",))
    procesar_tabla(
        spark,
        "grades",
        transformar_grades,
        validar_grades,
        argumentos_validacion=(enrollments_ref,),
    )
    del enrollments_ref
    print("\nUNIVERSITY CARGADO CORRECTAMENTE")


def procesar_billing(spark: SparkSession) -> None:
    print("\nINICIANDO BILLING")
    students_ref = leer_tabla_silver(spark, "students", ("student_id",))
    procesar_tabla(
        spark,
        "customers",
        transformar_customers,
        validar_customers,
        argumentos_transformacion=(students_ref,),
    )
    del students_ref

    procesar_tabla(spark, "products", transformar_products, validar_products)

    customers_ref = leer_tabla_silver(spark, "customers", ("customer_id",))
    products_ref = leer_tabla_silver(spark, "products", ("product_id",))
    procesar_tabla(
        spark,
        "subscriptions",
        transformar_subscriptions,
        validar_subscriptions,
        argumentos_transformacion=(customers_ref, products_ref),
    )
    del customers_ref, products_ref

    customers_ref = leer_tabla_silver(spark, "customers", ("customer_id",))
    procesar_tabla(
        spark,
        "invoices",
        transformar_invoices,
        validar_invoices,
        argumentos_transformacion=(customers_ref,),
    )
    del customers_ref

    invoices_ref = leer_tabla_silver(spark, "invoices", ("invoice_id",))
    products_ref = leer_tabla_silver(spark, "products", ("product_id",))
    procesar_tabla(
        spark,
        "invoice_items",
        transformar_invoice_items,
        validar_invoice_items,
        argumentos_transformacion=(invoices_ref, products_ref),
    )
    del invoices_ref, products_ref

    invoices_ref = leer_tabla_silver(
        spark,
        "invoices",
        ("invoice_id", "issued_at"),
    )
    procesar_tabla(
        spark,
        "payments",
        transformar_payments,
        validar_payments,
        argumentos_transformacion=(invoices_ref,),
    )
    del invoices_ref
    print("\nBILLING CARGADO CORRECTAMENTE")


def procesar_crm(spark: SparkSession) -> None:
    print("\nINICIANDO CRM")
    procesar_tabla(spark, "accounts", transformar_accounts, validar_accounts)

    accounts_ref = leer_tabla_silver(spark, "accounts", ("account_id",))
    procesar_tabla(
        spark,
        "contacts",
        transformar_contacts,
        validar_contacts,
        argumentos_transformacion=(accounts_ref,),
    )
    del accounts_ref

    procesar_tabla(spark, "leads", transformar_leads, validar_leads)

    accounts_ref = leer_tabla_silver(spark, "accounts", ("account_id",))
    procesar_tabla(
        spark,
        "opportunities",
        transformar_opportunities,
        validar_opportunities,
        argumentos_transformacion=(accounts_ref,),
    )
    del accounts_ref

    opportunities_ref = leer_tabla_silver(
        spark,
        "opportunities",
        ("opportunity_id", "account_id"),
    )
    contacts_ref = leer_tabla_silver(
        spark,
        "contacts",
        ("contact_id", "account_id"),
    )
    procesar_tabla(
        spark,
        "opportunity_contacts",
        transformar_opportunity_contacts,
        validar_opportunity_contacts,
        argumentos_transformacion=(opportunities_ref, contacts_ref),
    )
    del opportunities_ref, contacts_ref

    contacts_ref = leer_tabla_silver(spark, "contacts", ("contact_id",))
    opportunities_ref = leer_tabla_silver(
        spark,
        "opportunities",
        ("opportunity_id",),
    )
    procesar_tabla(
        spark,
        "activities",
        transformar_activities,
        validar_activities,
        argumentos_transformacion=(contacts_ref, opportunities_ref),
    )
    del contacts_ref, opportunities_ref
    print("\nCRM CARGADO CORRECTAMENTE")

def leer_argumentos():
    parser = argparse.ArgumentParser(description="Transformación Bronze a Silver")
    parser.add_argument(
        "--domain",
        choices=("all", "university", "billing", "crm"),
        default="all",
        help="Dominio a procesar. Por defecto procesa los tres.",
    )
    return parser.parse_args()


def main() -> None:
    argumentos = leer_argumentos()
    spark = crear_spark()

    try:
        print(f"Versión de Spark: {spark.version}")
        print(f"Master: {spark.sparkContext.master}")
        print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
        print(f"Dominio solicitado: {argumentos.domain}")

        if argumentos.domain in ("all", "university"):
            procesar_university(spark)
        if argumentos.domain in ("all", "billing"):
            procesar_billing(spark)
        if argumentos.domain in ("all", "crm"):
            procesar_crm(spark)

        print("PIPELINE SILVER FINALIZADO CORRECTAMENTE")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
