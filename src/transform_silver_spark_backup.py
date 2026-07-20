import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#Conf de postgres
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
    f"jdbc:postgresql://{DWH_HOST}:{DWH_PORT}/{DWH_DB}"
)

JDBC_PROPERTIES = {
    "user": DWH_USER,
    "password": DWH_PASSWORD,
    "driver": "org.postgresql.Driver",
}

PARQUET_BASE_PATH = os.getenv(
    "PARQUET_BASE_PATH",
    "/home/jovyan/work/data/parquet/silver",
)

#Crear Spark
def crear_spark():
    spark = (
        SparkSession.builder
        .appName("TransformSilver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def leer_tabla_bronze(spark, tabla):
    return spark.read.jdbc(
        url=JDBC_URL,
        table=f"bronze.{tabla}",
        properties=JDBC_PROPERTIES,
    )

def transformar_students(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("student_id")
        ).alias("student_id"),

        F.initcap(
            F.trim(F.col("first_name"))
        ).alias("first_name"),

        F.initcap(
            F.trim(F.col("last_name"))
        ).alias("last_name"),

        F.lower(
            F.trim(F.col("email"))
        ).alias("email"),

        F.to_date(
            F.trim(F.col("birth_date"))
        ).alias("birth_date"),

        F.to_timestamp(
            F.trim(F.col("enrolled_at"))
        ).alias("enrolled_at"),

        F.upper(
            F.trim(F.col("country"))
        ).alias("country"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("birth_date").isNull()
            | F.col("enrolled_at").isNull(),
            F.lit(False),
        )
        .when(
            F.col("birth_date")
            < F.to_date(F.col("enrolled_at")),
            F.lit(True),
        )
        .otherwise(F.lit(False)),
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "student_id",
        "first_name",
        "last_name",
        "email",
        "birth_date",
        "enrolled_at",
        "country",
        "is_temporally_valid",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_professors(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("professor_id")
        ).alias("professor_id"),

        F.initcap(
            F.trim(F.col("first_name"))
        ).alias("first_name"),

        F.initcap(
            F.trim(F.col("last_name"))
        ).alias("last_name"),

        F.lower(
            F.trim(F.col("email"))
        ).alias("email"),

        F.trim(
            F.col("department")
        ).alias("department"),

        F.to_timestamp(
            F.trim(F.col("hired_at"))
        ).alias("hired_at"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "professor_id",
        "first_name",
        "last_name",
        "email",
        "department",
        "hired_at",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_courses(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("course_id")
        ).alias("course_id"),

        F.upper(
            F.trim(F.col("code"))
        ).alias("code"),

        F.trim(
            F.col("name")
        ).alias("name"),

        F.trim(
            F.col("credits")
        ).cast("integer").alias("credits"),

        F.trim(
            F.col("department")
        ).alias("department"),

        F.trim(
            F.col("professor_id")
        ).alias("professor_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "course_id",
        "code",
        "name",
        "credits",
        "department",
        "professor_id",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_semesters(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("semester_id")
        ).alias("semester_id"),

        F.upper(
            F.trim(F.col("code"))
        ).alias("code"),

        F.trim(
            F.col("year")
        ).cast("integer").alias("year"),

        F.trim(
            F.col("half")
        ).cast("integer").alias("half"),

        F.to_date(
            F.trim(F.col("start_date"))
        ).alias("start_date"),

        F.to_date(
            F.trim(F.col("end_date"))
        ).alias("end_date"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("start_date").isNull()
            | F.col("end_date").isNull(),
            F.lit(False),
        )
        .when(
            F.col("start_date") <= F.col("end_date"),
            F.lit(True),
        )
        .otherwise(F.lit(False)),
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "semester_id",
        "code",
        "year",
        "half",
        "start_date",
        "end_date",
        "is_temporally_valid",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_enrollments(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("enrollment_id")
        ).alias("enrollment_id"),

        F.to_date(
            F.trim(F.col("enrolled_at"))
        ).alias("enrolled_at"),

        F.lower(
            F.trim(F.col("status"))
        ).alias("status"),

        F.trim(
            F.col("student_id")
        ).alias("student_id"),

        F.trim(
            F.col("course_id")
        ).alias("course_id"),

        F.trim(
            F.col("semester_id")
        ).alias("semester_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    ventana_combinacion = Window.partitionBy(
        "student_id",
        "course_id",
        "semester_id",
    )

    df_silver = df_silver.withColumn(
        "is_repeated_combination",
        F.count("*").over(ventana_combinacion) > 1,
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "enrollment_id",
        "enrolled_at",
        "status",
        "student_id",
        "course_id",
        "semester_id",
        "is_repeated_combination",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_grades(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("grade_id")
        ).alias("grade_id"),

        F.lower(
            F.trim(F.col("assessment"))
        ).alias("assessment"),

        F.trim(
            F.col("score")
        ).cast("decimal(6,2)").alias("score"),

        F.trim(
            F.col("weight")
        ).cast("decimal(5,4)").alias("weight"),

        F.to_date(
            F.trim(F.col("graded_at"))
        ).alias("graded_at"),

        F.trim(
            F.col("enrollment_id")
        ).alias("enrollment_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    ventana_enrollment = Window.partitionBy(
        "enrollment_id"
    )

    df_silver = df_silver.withColumn(
        "total_weight",
        F.round(
            F.sum("weight").over(ventana_enrollment),
            4,
        ).cast("decimal(8,4)"),
    )

    df_silver = df_silver.withColumn(
        "_grade_count",
        F.count(F.lit(1)).over(ventana_enrollment),
    )

    df_silver = df_silver.withColumn(
        "_weight_count",
        F.count("weight").over(ventana_enrollment),
    )

    # Validar que la nota esté entre 0 y 100
    df_silver = df_silver.withColumn(
        "is_score_valid",
        F.when(
            F.col("score").isNotNull()
            & (F.col("score") >= 0)
            & (F.col("score") <= 100),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    # Validar que el peso individual esté entre 0 y 1
    df_silver = df_silver.withColumn(
        "is_weight_valid",
        F.when(
            F.col("weight").isNotNull()
            & (F.col("weight") > 0)
            & (F.col("weight") <= 1),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    df_silver = df_silver.withColumn(
        "is_weight_sum_valid",
        F.when(
            (F.col("_grade_count") == F.col("_weight_count"))
            & F.col("total_weight").isNotNull()
            & (
                F.col("total_weight")
                == F.lit(1).cast("decimal(8,4)")
            ),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "grade_id",
        "assessment",
        "score",
        "weight",
        "total_weight",
        "graded_at",
        "enrollment_id",
        "is_score_valid",
        "is_weight_valid",
        "is_weight_sum_valid",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_customers(
    df_bronze,
    df_students_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("customer_id")
        ).alias("customer_id"),

        F.trim(
            F.col("external_ref")
        ).alias("external_ref"),

        F.initcap(
            F.trim(F.col("first_name"))
        ).alias("first_name"),

        F.initcap(
            F.trim(F.col("last_name"))
        ).alias("last_name"),

        F.lower(
            F.trim(F.col("email"))
        ).alias("email"),

        F.upper(
            F.trim(F.col("country"))
        ).alias("country"),

        F.to_timestamp(
            F.trim(F.col("created_at"))
        ).alias("created_at"),

        F.lower(
            F.trim(F.col("segment"))
        ).alias("segment"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_students_ref = (
        df_students_silver
        .select(
            F.col("student_id").alias(
                "_student_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_students_ref,
            df_silver["external_ref"]
            == df_students_ref["_student_id_ref"],
            how="left",
        )
        .withColumn(
            "is_student_linked",
            F.col("_student_id_ref").isNotNull(),
        )
        .drop("_student_id_ref")
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "customer_id",
        "external_ref",
        "first_name",
        "last_name",
        "email",
        "country",
        "created_at",
        "segment",
        "is_student_linked",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_products(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("product_id")
        ).alias("product_id"),

        F.upper(
            F.trim(F.col("sku"))
        ).alias("sku"),

        F.trim(
            F.col("name")
        ).alias("name"),

        F.lower(
            F.trim(F.col("category"))
        ).alias("category"),

        F.trim(
            F.col("monthly_price")
        ).cast("decimal(10,2)").alias(
            "monthly_price"
        ),

        F.lower(
            F.trim(F.col("active"))
        ).cast("boolean").alias("active"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "product_id",
        "sku",
        "name",
        "category",
        "monthly_price",
        "active",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_subscriptions(
    df_bronze,
    df_customers_silver,
    df_products_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("subscription_id")
        ).alias("subscription_id"),

        F.lower(
            F.trim(F.col("status"))
        ).alias("status"),

        F.to_date(
            F.trim(F.col("start_date"))
        ).alias("start_date"),

        F.to_date(
            F.trim(F.col("end_date"))
        ).alias("end_date"),

        F.trim(
            F.col("customer_id")
        ).alias("customer_id"),

        F.trim(
            F.col("product_id")
        ).alias("product_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("start_date").isNull()
            | F.col("end_date").isNull(),
            F.lit(False),
        )
        .when(
            F.col("start_date")
            <= F.col("end_date"),
            F.lit(True),
        )
        .otherwise(F.lit(False)),
    )

    df_customers_ref = (
        df_customers_silver
        .select(
            F.col("customer_id").alias(
                "_customer_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_customers_ref,
            df_silver["customer_id"]
            == df_customers_ref["_customer_id_ref"],
            how="left",
        )
        .withColumn(
            "is_customer_linked",
            F.col("_customer_id_ref").isNotNull(),
        )
        .drop("_customer_id_ref")
    )

    df_products_ref = (
        df_products_silver
        .select(
            F.col("product_id").alias(
                "_product_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_products_ref,
            df_silver["product_id"]
            == df_products_ref["_product_id_ref"],
            how="left",
        )
        .withColumn(
            "is_product_linked",
            F.col("_product_id_ref").isNotNull(),
        )
        .drop("_product_id_ref")
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
        "subscription_id",
        "status",
        "start_date",
        "end_date",
        "customer_id",
        "product_id",
        "is_temporally_valid",
        "is_customer_linked",
        "is_product_linked",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_invoices(
    df_bronze,
    df_customers_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("invoice_id")
        ).alias("invoice_id"),

        F.to_date(
            F.trim(F.col("issued_at"))
        ).alias("issued_at"),

        F.to_date(
            F.trim(F.col("due_at"))
        ).alias("due_at"),

        F.trim(
            F.col("total")
        ).cast("decimal(12,2)").alias("total"),

        F.lower(
            F.trim(F.col("status"))
        ).alias("status"),

        F.upper(
            F.trim(F.col("currency"))
        ).alias("currency"),

        F.trim(
            F.col("customer_id")
        ).alias("customer_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("issued_at").isNull()
            | F.col("due_at").isNull(),
            F.lit(False),
        )
        .when(
            F.col("issued_at") <= F.col("due_at"),
            F.lit(True),
        )
        .otherwise(F.lit(False)),
    )

    df_silver = df_silver.withColumn(
        "is_total_valid",
        F.when(
            F.col("total").isNotNull()
            & (F.col("total") >= 0),
            F.lit(True),
        )
        .otherwise(F.lit(False)),
    )

    df_customers_ref = (
        df_customers_silver
        .select(
            F.col("customer_id").alias(
                "_customer_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_customers_ref,
            df_silver["customer_id"]
            == df_customers_ref["_customer_id_ref"],
            how="left",
        )
        .withColumn(
            "is_customer_linked",
            F.col("_customer_id_ref").isNotNull(),
        )
        .drop("_customer_id_ref")
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
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
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_invoice_items(
    df_bronze,
    df_invoices_silver,
    df_products_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("invoice_item_id")
        ).alias("invoice_item_id"),

        F.trim(
            F.col("quantity")
        ).cast("integer").alias("quantity"),

        F.trim(
            F.col("unit_price")
        ).cast("decimal(12,2)").alias(
            "unit_price"
        ),

        F.trim(
            F.col("line_total")
        ).cast("decimal(14,2)").alias(
            "line_total"
        ),

        F.trim(
            F.col("invoice_id")
        ).alias("invoice_id"),

        F.trim(
            F.col("product_id")
        ).alias("product_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "calculated_line_total",
        F.round(
            F.col("quantity")
            * F.col("unit_price"),
            2,
        ).cast("decimal(14,2)"),
    )

    df_silver = df_silver.withColumn(
        "is_quantity_valid",
        F.col("quantity").isNotNull()
        & (F.col("quantity") > 0),
    )

    df_silver = df_silver.withColumn(
        "is_unit_price_valid",
        F.col("unit_price").isNotNull()
        & (F.col("unit_price") >= 0),
    )

    df_silver = df_silver.withColumn(
        "is_line_total_valid",
        F.when(
            F.col("line_total").isNull()
            | F.col("calculated_line_total").isNull(),
            F.lit(False),
        )
        .when(
            F.abs(
                F.col("line_total")
                - F.col("calculated_line_total")
            ) <= F.lit(0.01),
            F.lit(True),
        )
        .otherwise(F.lit(False)),
    )

    df_invoices_ref = (
        df_invoices_silver
        .select(
            F.col("invoice_id").alias(
                "_invoice_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_invoices_ref,
            df_silver["invoice_id"]
            == df_invoices_ref["_invoice_id_ref"],
            how="left",
        )
        .withColumn(
            "is_invoice_linked",
            F.col("_invoice_id_ref").isNotNull(),
        )
        .drop("_invoice_id_ref")
    )

    df_products_ref = (
        df_products_silver
        .select(
            F.col("product_id").alias(
                "_product_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_products_ref,
            df_silver["product_id"]
            == df_products_ref["_product_id_ref"],
            how="left",
        )
        .withColumn(
            "is_product_linked",
            F.col("_product_id_ref").isNotNull(),
        )
        .drop("_product_id_ref")
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
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
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_payments(
    df_bronze,
    df_invoices_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("payment_id")
        ).alias("payment_id"),

        F.trim(
            F.col("amount")
        ).cast("decimal(12,2)").alias("amount"),

        F.to_date(
            F.trim(F.col("paid_at"))
        ).alias("paid_at"),

        F.lower(
            F.trim(F.col("method"))
        ).alias("method"),

        F.trim(
            F.col("invoice_id")
        ).alias("invoice_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_amount_valid",
        F.col("amount").isNotNull()
        & (F.col("amount") > 0),
    )

    df_silver = df_silver.withColumn(
        "is_method_valid",
        F.col("method").isNotNull()
        & F.col("method").isin(
            "card",
            "bank_transfer",
            "paypal",
            "cash",
        ),
    )

    df_invoices_ref = (
        df_invoices_silver
        .select(
            F.col("invoice_id").alias(
                "_invoice_id_ref"
            ),
            F.col("issued_at").alias(
                "_invoice_issued_at"
            ),
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_invoices_ref,
            df_silver["invoice_id"]
            == df_invoices_ref["_invoice_id_ref"],
            how="left",
        )
        .withColumn(
            "is_invoice_linked",
            F.col("_invoice_id_ref").isNotNull(),
        )
    )

    df_silver = df_silver.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("paid_at").isNull()
            | F.col("_invoice_issued_at").isNull(),
            F.lit(False),
        )
        .when(
            F.col("paid_at")
            >= F.col("_invoice_issued_at"),
            F.lit(True),
        )
        .otherwise(F.lit(False)),
    )

    df_silver = (
        df_silver
        .drop(
            "_invoice_id_ref",
            "_invoice_issued_at",
        )
        .withColumn(
            "_silver_created_at",
            F.current_timestamp(),
        )
    )

    return df_silver.select(
        "payment_id",
        "amount",
        "paid_at",
        "method",
        "invoice_id",
        "is_amount_valid",
        "is_method_valid",
        "is_invoice_linked",
        "is_temporally_valid",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_accounts(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("account_id")
        ).alias("account_id"),

        F.trim(
            F.col("name")
        ).alias("name"),

        F.lower(
            F.trim(F.col("industry"))
        ).alias("industry"),

        F.upper(
            F.trim(F.col("country"))
        ).alias("country"),

        F.trim(
            F.col("annual_revenue")
        ).cast("decimal(16,2)").alias(
            "annual_revenue"
        ),

        F.trim(
            F.col("employees")
        ).cast("integer").alias("employees"),

        F.to_timestamp(
            F.trim(F.col("created_at"))
        ).alias("created_at"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_annual_revenue_valid",
        F.col("annual_revenue").isNotNull()
        & (F.col("annual_revenue") >= 0),
    )

    df_silver = df_silver.withColumn(
        "is_employees_valid",
        F.col("employees").isNotNull()
        & (F.col("employees") >= 0),
    )

    df_silver = df_silver.withColumn(
        "is_created_at_valid",
        F.col("created_at").isNotNull(),
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
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
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_contacts(
    df_bronze,
    df_accounts_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("contact_id")
        ).alias("contact_id"),

        F.initcap(
            F.trim(F.col("first_name"))
        ).alias("first_name"),

        F.initcap(
            F.trim(F.col("last_name"))
        ).alias("last_name"),

        F.lower(
            F.trim(F.col("email"))
        ).alias("email"),

        F.trim(
            F.col("phone")
        ).alias("phone"),

        F.trim(
            F.col("title")
        ).alias("title"),

        F.to_timestamp(
            F.trim(F.col("created_at"))
        ).alias("created_at"),

        F.trim(
            F.col("account_id")
        ).alias("account_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_email_present",
        F.col("email").isNotNull()
        & (F.trim(F.col("email")) != ""),
    )

    df_silver = df_silver.withColumn(
        "is_created_at_valid",
        F.col("created_at").isNotNull(),
    )

    df_accounts_ref = (
        df_accounts_silver
        .select(
            F.col("account_id").alias(
                "_account_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_accounts_ref,
            df_silver["account_id"]
            == df_accounts_ref["_account_id_ref"],
            how="left",
        )
        .withColumn(
            "is_account_linked",
            F.col("_account_id_ref").isNotNull(),
        )
        .drop("_account_id_ref")
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
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
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_leads(df_bronze):
    df_silver = df_bronze.select(
        F.trim(
            F.col("lead_id")
        ).alias("lead_id"),

        F.initcap(
            F.trim(F.col("first_name"))
        ).alias("first_name"),

        F.initcap(
            F.trim(F.col("last_name"))
        ).alias("last_name"),

        F.lower(
            F.trim(F.col("email"))
        ).alias("email"),

        F.lower(
            F.trim(F.col("source"))
        ).alias("source"),

        F.lower(
            F.trim(F.col("status"))
        ).alias("status"),

        F.trim(
            F.col("score")
        ).cast("integer").alias("score"),

        F.to_timestamp(
            F.trim(F.col("created_at"))
        ).alias("created_at"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_email_present",
        F.col("email").isNotNull()
        & (F.trim(F.col("email")) != ""),
    )

    df_silver = df_silver.withColumn(
        "is_source_valid",
        F.col("source").isNotNull()
        & F.col("source").isin(
            "web",
            "referral",
            "ads",
            "event",
            "cold_call",
        ),
    )

    df_silver = df_silver.withColumn(
        "is_status_valid",
        F.col("status").isNotNull()
        & F.col("status").isin(
            "new",
            "contacted",
            "qualified",
            "converted",
            "lost",
        ),
    )

    df_silver = df_silver.withColumn(
        "is_score_valid",
        F.col("score").isNotNull()
        & (F.col("score") >= 0)
        & (F.col("score") <= 100),
    )

    df_silver = df_silver.withColumn(
        "is_created_at_valid",
        F.col("created_at").isNotNull(),
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
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
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_opportunities(
    df_bronze,
    df_accounts_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("opportunity_id")
        ).alias("opportunity_id"),

        F.trim(
            F.col("name")
        ).alias("name"),

        F.lower(
            F.trim(F.col("stage"))
        ).alias("stage"),

        F.trim(
            F.col("amount")
        ).cast("decimal(16,2)").alias("amount"),

        F.to_date(
            F.trim(F.col("close_date"))
        ).alias("close_date"),

        F.to_timestamp(
            F.trim(F.col("created_at"))
        ).alias("created_at"),

        F.trim(
            F.col("account_id")
        ).alias("account_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_stage_valid",
        F.col("stage").isNotNull()
        & F.col("stage").isin(
            "prospect",
            "qualification",
            "proposal",
            "negotiation",
            "won",
            "lost",
        ),
    )

    df_silver = df_silver.withColumn(
        "is_amount_valid",
        F.col("amount").isNotNull()
        & (F.col("amount") > 0),
    )

    df_silver = df_silver.withColumn(
        "is_temporally_valid",
        F.when(
            F.col("created_at").isNull()
            | F.col("close_date").isNull(),
            F.lit(False),
        )
        .when(
            F.to_date(F.col("created_at"))
            <= F.col("close_date"),
            F.lit(True),
        )
        .otherwise(F.lit(False)),
    )

    df_accounts_ref = (
        df_accounts_silver
        .select(
            F.col("account_id").alias(
                "_account_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_accounts_ref,
            df_silver["account_id"]
            == df_accounts_ref["_account_id_ref"],
            how="left",
        )
        .withColumn(
            "is_account_linked",
            F.col("_account_id_ref").isNotNull(),
        )
        .drop("_account_id_ref")
    )

    df_silver = df_silver.withColumn(
        "_silver_created_at",
        F.current_timestamp(),
    )

    return df_silver.select(
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
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_opportunity_contacts(
    df_bronze,
    df_opportunities_silver,
    df_contacts_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("opportunity_id")
        ).alias("opportunity_id"),

        F.trim(
            F.col("contact_id")
        ).alias("contact_id"),

        F.lower(
            F.trim(F.col("role"))
        ).alias("role"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_role_valid",
        F.col("role").isNotNull()
        & F.col("role").isin(
            "influencer",
            "end_user",
            "financial",
            "technical",
            "decision_maker",
        ),
    )

    df_opportunities_ref = (
        df_opportunities_silver
        .select(
            F.col("opportunity_id").alias(
                "_opportunity_id_ref"
            ),
            F.col("account_id").alias(
                "_opportunity_account_id"
            ),
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_opportunities_ref,
            df_silver["opportunity_id"]
            == df_opportunities_ref[
                "_opportunity_id_ref"
            ],
            how="left",
        )
        .withColumn(
            "is_opportunity_linked",
            F.col(
                "_opportunity_id_ref"
            ).isNotNull(),
        )
    )

    df_contacts_ref = (
        df_contacts_silver
        .select(
            F.col("contact_id").alias(
                "_contact_id_ref"
            ),
            F.col("account_id").alias(
                "_contact_account_id"
            ),
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_contacts_ref,
            df_silver["contact_id"]
            == df_contacts_ref["_contact_id_ref"],
            how="left",
        )
        .withColumn(
            "is_contact_linked",
            F.col("_contact_id_ref").isNotNull(),
        )
    )

    df_silver = df_silver.withColumn(
        "is_same_account",
        F.when(
            F.col("is_opportunity_linked")
            & F.col("is_contact_linked")
            & (
                F.col("_opportunity_account_id")
                == F.col("_contact_account_id")
            ),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    df_silver = (
        df_silver
        .drop(
            "_opportunity_id_ref",
            "_contact_id_ref",
            "_opportunity_account_id",
            "_contact_account_id",
        )
        .withColumn(
            "_silver_created_at",
            F.current_timestamp(),
        )
    )

    return df_silver.select(
        "opportunity_id",
        "contact_id",
        "role",
        "is_role_valid",
        "is_opportunity_linked",
        "is_contact_linked",
        "is_same_account",
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def transformar_activities(
    df_bronze,
    df_contacts_silver,
    df_opportunities_silver,
):
    df_silver = df_bronze.select(
        F.trim(
            F.col("activity_id")
        ).alias("activity_id"),

        F.lower(
            F.trim(F.col("type"))
        ).alias("activity_type"),

        F.trim(
            F.col("subject")
        ).alias("subject"),

        F.to_timestamp(
            F.trim(F.col("occurred_at"))
        ).alias("occurred_at"),

        F.when(
            F.trim(F.col("contact_id")) == "",
            F.lit(None).cast("string"),
        )
        .otherwise(
            F.trim(F.col("contact_id"))
        )
        .alias("contact_id"),

        F.when(
            F.trim(F.col("opportunity_id")) == "",
            F.lit(None).cast("string"),
        )
        .otherwise(
            F.trim(F.col("opportunity_id"))
        )
        .alias("opportunity_id"),

        F.col("_source_file"),

        F.col("_ingested_at").alias(
            "_bronze_ingested_at"
        ),

        F.col("_batch_id"),
    )

    df_silver = df_silver.withColumn(
        "is_type_valid",
        F.col("activity_type").isNotNull()
        & F.col("activity_type").isin(
            "email",
            "call",
            "meeting",
            "note",
            "demo",
        ),
    )

    df_silver = df_silver.withColumn(
        "is_occurred_at_valid",
        F.col("occurred_at").isNotNull(),
    )

    df_silver = df_silver.withColumn(
        "has_contact_id",
        F.col("contact_id").isNotNull(),
    )

    df_silver = df_silver.withColumn(
        "has_opportunity_id",
        F.col("opportunity_id").isNotNull(),
    )

    df_silver = df_silver.withColumn(
        "has_any_reference",
        F.col("has_contact_id")
        | F.col("has_opportunity_id"),
    )

    df_contacts_ref = (
        df_contacts_silver
        .select(
            F.col("contact_id").alias(
                "_contact_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_contacts_ref,
            df_silver["contact_id"]
            == df_contacts_ref["_contact_id_ref"],
            how="left",
        )
        .withColumn(
            "is_contact_linked",
            F.col("has_contact_id")
            & F.col("_contact_id_ref").isNotNull(),
        )
    )

    df_opportunities_ref = (
        df_opportunities_silver
        .select(
            F.col("opportunity_id").alias(
                "_opportunity_id_ref"
            )
        )
        .distinct()
    )

    df_silver = (
        df_silver
        .join(
            df_opportunities_ref,
            df_silver["opportunity_id"]
            == df_opportunities_ref[
                "_opportunity_id_ref"
            ],
            how="left",
        )
        .withColumn(
            "is_opportunity_linked",
            F.col("has_opportunity_id")
            & F.col(
                "_opportunity_id_ref"
            ).isNotNull(),
        )
    )

    df_silver = (
        df_silver
        .drop(
            "_contact_id_ref",
            "_opportunity_id_ref",
        )
        .withColumn(
            "_silver_created_at",
            F.current_timestamp(),
        )
    )

    return df_silver.select(
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
        "_source_file",
        "_bronze_ingested_at",
        "_batch_id",
        "_silver_created_at",
    )

def validar_students(
    df_bronze,
    df_silver,
):
    return validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="students",
        columna_pk="student_id",
    )

def validar_professors(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="professors",
        columna_pk="professor_id",
    )

    hired_at_nulos = (
        df_silver
        .filter(F.col("hired_at").isNull())
        .count()
    )

    print(
        "Hired_at nulos o no convertibles:",
        hired_at_nulos,
    )

    return total_silver

def validar_courses(
    df_bronze,
    df_silver,
    df_professors_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="courses",
        columna_pk="course_id",
    )

    credits_nulos = (
        df_silver
        .filter(F.col("credits").isNull())
        .count()
    )

    credits_invalidos = (
        df_silver
        .filter(F.col("credits") <= 0)
        .count()
    )

    professor_id_nulos = (
        df_silver
        .filter(
            F.col("professor_id").isNull()
            | (F.trim(F.col("professor_id")) == "")
        )
        .count()
    )

    profesores_huerfanos = (
        df_silver
        .select("professor_id")
        .filter(
            F.col("professor_id").isNotNull()
            & (F.trim(F.col("professor_id")) != "")
        )
        .distinct()
        .join(
            df_professors_silver
            .select("professor_id")
            .distinct(),
            on="professor_id",
            how="left_anti",
        )
        .count()
    )

    print("Credits nulos o no convertibles:", credits_nulos)
    print("Credits menores o iguales a cero:", credits_invalidos)
    print("Professor ID nulos o vacíos:", professor_id_nulos)
    print(
        "Professor ID sin profesor existente:",
        profesores_huerfanos,
    )

    return total_silver

def validar_semesters(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="semesters",
        columna_pk="semester_id",
    )

    year_nulos = (
        df_silver
        .filter(F.col("year").isNull())
        .count()
    )

    half_nulos = (
        df_silver
        .filter(F.col("half").isNull())
        .count()
    )

    half_invalidos = (
        df_silver
        .filter(
            F.col("half").isNotNull()
            & ~F.col("half").isin(1, 2)
        )
        .count()
    )

    fechas_nulas = (
        df_silver
        .filter(
            F.col("start_date").isNull()
            | F.col("end_date").isNull()
        )
        .count()
    )

    fechas_inconsistentes = (
        df_silver
        .filter(
            F.col("start_date").isNotNull()
            & F.col("end_date").isNotNull()
            & (
                F.col("start_date")
                > F.col("end_date")
            )
        )
        .count()
    )

    print("Year nulos o no convertibles:", year_nulos)
    print("Half nulos o no convertibles:", half_nulos)
    print("Half diferentes de 1 o 2:", half_invalidos)
    print("Registros con fechas nulas:", fechas_nulas)
    print(
        "Registros con relación temporal inválida:",
        fechas_inconsistentes,
    )

    return total_silver

def validar_enrollments(
    df_bronze,
    df_silver,
    df_students_silver,
    df_courses_silver,
    df_semesters_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="enrollments",
        columna_pk="enrollment_id",
    )

    fechas_nulas = (
        df_silver
        .filter(F.col("enrolled_at").isNull())
        .count()
    )

    estados_invalidos = (
        df_silver
        .filter(
            F.col("status").isNull()
            | ~F.col("status").isin(
                "completed",
                "active",
                "failed",
                "dropped",
            )
        )
        .count()
    )

    students_huerfanos = (
        df_silver
        .select("student_id")
        .filter(F.col("student_id").isNotNull()
                & (F.trim(F.col("student_id")) !="")
                )
        .distinct()
        .join(
            df_students_silver
            .select("student_id")
            .distinct(),
            on="student_id",
            how="left_anti",
        )
        .count()
    )

    courses_huerfanos = (
        df_silver
        .select("course_id")
        .filter(F.col("course_id").isNotNull())
        .distinct()
        .join(
            df_courses_silver
            .select("course_id")
            .distinct(),
            on="course_id",
            how="left_anti",
        )
        .count()
    )

    semesters_huerfanos = (
        df_silver
        .select("semester_id")
        .filter(F.col("semester_id").isNotNull())
        .distinct()
        .join(
            df_semesters_silver
            .select("semester_id")
            .distinct(),
            on="semester_id",
            how="left_anti",
        )
        .count()
    )

    combinaciones_repetidas = (
        df_silver
        .filter(F.col("is_repeated_combination"))
        .count()
    )

    grupos_repetidos = (
        df_silver
        .groupBy(
            "student_id",
            "course_id",
            "semester_id",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("Enrolled_at nulos o no convertibles:", fechas_nulas)
    print("Estados inválidos:", estados_invalidos)
    print("Students huérfanos:", students_huerfanos)
    print("Courses huérfanos:", courses_huerfanos)
    print("Semesters huérfanos:", semesters_huerfanos)
    print("Filas en combinaciones repetidas:", combinaciones_repetidas)
    print("Grupos repetidos:", grupos_repetidos)

    return total_silver

def validar_grades(
    df_bronze,
    df_silver,
    df_enrollments_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="grades",
        columna_pk="grade_id",
    )

    assessments_nulos = (
        df_silver
        .filter(
            F.col("assessment").isNull()
            | (F.trim(F.col("assessment")) == "")
        )
        .count()
    )

    scores_invalidos = (
        df_silver
        .filter(~F.col("is_score_valid"))
        .count()
    )

    weights_invalidos = (
        df_silver
        .filter(~F.col("is_weight_valid"))
        .count()
    )

    fechas_nulas = (
        df_silver
        .filter(F.col("graded_at").isNull())
        .count()
    )

    enrollment_id_nulos = (
        df_silver
        .filter(
            F.col("enrollment_id").isNull()
            | (F.trim(F.col("enrollment_id")) == "")
        )
        .count()
    )

    enrollments_huerfanos = (
        df_silver
        .select("enrollment_id")
        .filter(
            F.col("enrollment_id").isNotNull()
            & (F.trim(F.col("enrollment_id")) != "")
        )
        .distinct()
        .join(
            df_enrollments_silver
            .select("enrollment_id")
            .distinct(),
            on="enrollment_id",
            how="left_anti",
        )
        .count()
    )

    filas_peso_total_invalido = (
        df_silver
        .filter(~F.col("is_weight_sum_valid"))
        .count()
    )

    enrollments_peso_total_invalido = (
        df_silver
        .filter(~F.col("is_weight_sum_valid"))
        .select("enrollment_id")
        .distinct()
        .count()
    )

    print("Assessment nulos o vacíos:", assessments_nulos)
    print("Scores inválidos:", scores_invalidos)
    print("Weights individuales inválidos:", weights_invalidos)
    print("Graded_at nulos o no convertibles:", fechas_nulas)
    print("Enrollment ID nulos o vacíos:", enrollment_id_nulos)
    print("Enrollments huérfanos:", enrollments_huerfanos)
    print(
        "Filas cuyo peso total no suma 1:",
        filas_peso_total_invalido,
    )
    print(
        "Inscripciones cuyo peso total no suma 1:",
        enrollments_peso_total_invalido,
    )

    return total_silver

def validar_customers(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="customers",
        columna_pk="customer_id",
    )

    external_ref_nulos = (
        df_silver
        .filter(
            F.col("external_ref").isNull()
            | (
                F.trim(F.col("external_ref"))
                == ""
            )
        )
        .count()
    )

    external_ref_duplicados = (
        df_silver
        .filter(
            F.col("external_ref").isNotNull()
        )
        .groupBy("external_ref")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    created_at_nulos = (
        df_silver
        .filter(
            F.col("created_at").isNull()
        )
        .count()
    )

    segment_nulos = (
        df_silver
        .filter(
            F.col("segment").isNull()
            | (
                F.trim(F.col("segment"))
                == ""
            )
        )
        .count()
    )

    customers_sin_estudiante = (
        df_silver
        .filter(
            ~F.col("is_student_linked")
        )
        .count()
    )

    print("External ref nulos o vacíos:", external_ref_nulos)
    print(
        "External ref duplicados:",
        external_ref_duplicados,
    )
    print(
        "Created_at nulos o no convertibles:",
        created_at_nulos,
    )
    print("Segment nulos o vacíos:", segment_nulos)
    print(
        "Customers sin estudiante relacionado:",
        customers_sin_estudiante,
    )

    return total_silver

def validar_products(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="products",
        columna_pk="product_id",
    )

    sku_nulos = (
        df_silver
        .filter(
            F.col("sku").isNull()
            | (F.trim(F.col("sku")) == "")
        )
        .count()
    )

    sku_duplicados = (
        df_silver
        .filter(
            F.col("sku").isNotNull()
            & (F.trim(F.col("sku")) != "")
        )
        .groupBy("sku")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    nombres_nulos = (
        df_silver
        .filter(
            F.col("name").isNull()
            | (F.trim(F.col("name")) == "")
        )
        .count()
    )

    categorias_nulas = (
        df_silver
        .filter(
            F.col("category").isNull()
            | (F.trim(F.col("category")) == "")
        )
        .count()
    )

    precios_nulos = (
        df_silver
        .filter(
            F.col("monthly_price").isNull()
        )
        .count()
    )

    precios_invalidos = (
        df_silver
        .filter(
            F.col("monthly_price").isNotNull()
            & (F.col("monthly_price") < 0)
        )
        .count()
    )

    active_nulos = (
        df_silver
        .filter(
            F.col("active").isNull()
        )
        .count()
    )

    print("SKU nulos o vacíos:", sku_nulos)
    print("SKU duplicados:", sku_duplicados)
    print("Nombres nulos o vacíos:", nombres_nulos)
    print("Categorías nulas o vacías:", categorias_nulas)
    print(
        "Monthly price nulos o no convertibles:",
        precios_nulos,
    )
    print(
        "Monthly price menores a cero:",
        precios_invalidos,
    )
    print(
        "Active nulos o no convertibles:",
        active_nulos,
    )

    return total_silver

def validar_subscriptions(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="subscriptions",
        columna_pk="subscription_id",
    )

    estados_invalidos = (
        df_silver
        .filter(
            F.col("status").isNull()
            | ~F.col("status").isin(
                "active",
                "cancelled",
                "paused",
            )
        )
        .count()
    )

    fechas_nulas = (
        df_silver
        .filter(
            F.col("start_date").isNull()
            | F.col("end_date").isNull()
        )
        .count()
    )

    fechas_inconsistentes = (
        df_silver
        .filter(
            ~F.col("is_temporally_valid")
        )
        .count()
    )

    customer_id_nulos = (
        df_silver
        .filter(
            F.col("customer_id").isNull()
            | (
                F.trim(F.col("customer_id"))
                == ""
            )
        )
        .count()
    )

    product_id_nulos = (
        df_silver
        .filter(
            F.col("product_id").isNull()
            | (
                F.trim(F.col("product_id"))
                == ""
            )
        )
        .count()
    )

    customers_huerfanos = (
        df_silver
        .filter(
            ~F.col("is_customer_linked")
        )
        .count()
    )

    products_huerfanos = (
        df_silver
        .filter(
            ~F.col("is_product_linked")
        )
        .count()
    )

    print("Estados inválidos:", estados_invalidos)
    print("Fechas nulas o no convertibles:", fechas_nulas)
    print(
        "Suscripciones con fechas inconsistentes:",
        fechas_inconsistentes,
    )
    print(
        "Customer ID nulos o vacíos:",
        customer_id_nulos,
    )
    print(
        "Product ID nulos o vacíos:",
        product_id_nulos,
    )
    print(
        "Suscripciones sin customer relacionado:",
        customers_huerfanos,
    )
    print(
        "Suscripciones sin product relacionado:",
        products_huerfanos,
    )

    return total_silver

def validar_invoices(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="invoices",
        columna_pk="invoice_id",
    )

    fechas_nulas = (
        df_silver
        .filter(
            F.col("issued_at").isNull()
            | F.col("due_at").isNull()
        )
        .count()
    )

    fechas_inconsistentes = (
        df_silver
        .filter(
            ~F.col("is_temporally_valid")
        )
        .count()
    )

    totales_invalidos = (
        df_silver
        .filter(
            ~F.col("is_total_valid")
        )
        .count()
    )

    estados_invalidos = (
        df_silver
        .filter(
            F.col("status").isNull()
            | ~F.col("status").isin(
                "paid",
                "pending",
                "overdue",
            )
        )
        .count()
    )

    monedas_nulas = (
        df_silver
        .filter(
            F.col("currency").isNull()
            | (F.trim(F.col("currency")) == "")
        )
        .count()
    )

    customer_id_nulos = (
        df_silver
        .filter(
            F.col("customer_id").isNull()
            | (F.trim(F.col("customer_id")) == "")
        )
        .count()
    )

    customers_huerfanos = (
        df_silver
        .filter(
            ~F.col("is_customer_linked")
        )
        .count()
    )

    print("Fechas nulas o no convertibles:", fechas_nulas)
    print(
        "Facturas con fechas inconsistentes:",
        fechas_inconsistentes,
    )
    print(
        "Totales nulos, negativos o no convertibles:",
        totales_invalidos,
    )
    print("Estados inválidos:", estados_invalidos)
    print("Monedas nulas o vacías:", monedas_nulas)
    print(
        "Customer ID nulos o vacíos:",
        customer_id_nulos,
    )
    print(
        "Facturas sin customer relacionado:",
        customers_huerfanos,
    )

    return total_silver

def validar_invoice_items(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="invoice_items",
        columna_pk="invoice_item_id",
    )

    cantidades_invalidas = (
        df_silver
        .filter(
            ~F.col("is_quantity_valid")
        )
        .count()
    )

    precios_invalidos = (
        df_silver
        .filter(
            ~F.col("is_unit_price_valid")
        )
        .count()
    )

    totales_linea_invalidos = (
        df_silver
        .filter(
            ~F.col("is_line_total_valid")
        )
        .count()
    )

    invoice_id_nulos = (
        df_silver
        .filter(
            F.col("invoice_id").isNull()
            | (F.trim(F.col("invoice_id")) == "")
        )
        .count()
    )

    product_id_nulos = (
        df_silver
        .filter(
            F.col("product_id").isNull()
            | (F.trim(F.col("product_id")) == "")
        )
        .count()
    )

    invoices_huerfanas = (
        df_silver
        .filter(
            ~F.col("is_invoice_linked")
        )
        .count()
    )

    products_huerfanos = (
        df_silver
        .filter(
            ~F.col("is_product_linked")
        )
        .count()
    )

    print(
        "Cantidades nulas, no convertibles o <= 0:",
        cantidades_invalidas,
    )
    print(
        "Precios nulos, no convertibles o negativos:",
        precios_invalidos,
    )
    print(
        "Líneas donde quantity * unit_price no coincide:",
        totales_linea_invalidos,
    )
    print(
        "Invoice ID nulos o vacíos:",
        invoice_id_nulos,
    )
    print(
        "Product ID nulos o vacíos:",
        product_id_nulos,
    )
    print(
        "Items sin invoice relacionada:",
        invoices_huerfanas,
    )
    print(
        "Items sin product relacionado:",
        products_huerfanos,
    )

    return total_silver

def validar_payments(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="payments",
        columna_pk="payment_id",
    )

    amounts_invalidos = (
        df_silver
        .filter(
            ~F.col("is_amount_valid")
        )
        .count()
    )

    fechas_nulas = (
        df_silver
        .filter(
            F.col("paid_at").isNull()
        )
        .count()
    )

    metodos_invalidos = (
        df_silver
        .filter(
            ~F.col("is_method_valid")
        )
        .count()
    )

    invoice_id_nulos = (
        df_silver
        .filter(
            F.col("invoice_id").isNull()
            | (F.trim(F.col("invoice_id")) == "")
        )
        .count()
    )

    invoices_huerfanas = (
        df_silver
        .filter(
            ~F.col("is_invoice_linked")
        )
        .count()
    )

    fechas_inconsistentes = (
        df_silver
        .filter(
            ~F.col("is_temporally_valid")
        )
        .count()
    )

    print(
        "Amounts nulos, no convertibles o <= 0:",
        amounts_invalidos,
    )
    print(
        "Paid_at nulos o no convertibles:",
        fechas_nulas,
    )
    print(
        "Métodos de pago inválidos:",
        metodos_invalidos,
    )
    print(
        "Invoice ID nulos o vacíos:",
        invoice_id_nulos,
    )
    print(
        "Payments sin invoice relacionada:",
        invoices_huerfanas,
    )
    print(
        "Payments anteriores a la emisión:",
        fechas_inconsistentes,
    )

    return total_silver

def validar_accounts(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="accounts",
        columna_pk="account_id",
    )

    nombres_nulos = (
        df_silver
        .filter(
            F.col("name").isNull()
            | (F.trim(F.col("name")) == "")
        )
        .count()
    )

    industrias_nulas = (
        df_silver
        .filter(
            F.col("industry").isNull()
            | (F.trim(F.col("industry")) == "")
        )
        .count()
    )

    paises_nulos = (
        df_silver
        .filter(
            F.col("country").isNull()
            | (F.trim(F.col("country")) == "")
        )
        .count()
    )

    revenues_invalidos = (
        df_silver
        .filter(
            ~F.col("is_annual_revenue_valid")
        )
        .count()
    )

    employees_invalidos = (
        df_silver
        .filter(
            ~F.col("is_employees_valid")
        )
        .count()
    )

    created_at_invalidos = (
        df_silver
        .filter(
            ~F.col("is_created_at_valid")
        )
        .count()
    )

    print("Nombres nulos o vacíos:", nombres_nulos)
    print("Industrias nulas o vacías:", industrias_nulas)
    print("Países nulos o vacíos:", paises_nulos)
    print(
        "Annual revenue nulos, negativos o no convertibles:",
        revenues_invalidos,
    )
    print(
        "Employees nulos, negativos o no convertibles:",
        employees_invalidos,
    )
    print(
        "Created_at nulos o no convertibles:",
        created_at_invalidos,
    )

    return total_silver

def validar_contacts(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="contacts",
        columna_pk="contact_id",
    )

    nombres_nulos = (
        df_silver
        .filter(
            F.col("first_name").isNull()
            | (F.trim(F.col("first_name")) == "")
        )
        .count()
    )

    apellidos_nulos = (
        df_silver
        .filter(
            F.col("last_name").isNull()
            | (F.trim(F.col("last_name")) == "")
        )
        .count()
    )

    emails_faltantes = (
        df_silver
        .filter(
            ~F.col("is_email_present")
        )
        .count()
    )

    phones_nulos = (
        df_silver
        .filter(
            F.col("phone").isNull()
            | (F.trim(F.col("phone")) == "")
        )
        .count()
    )

    titles_nulos = (
        df_silver
        .filter(
            F.col("title").isNull()
            | (F.trim(F.col("title")) == "")
        )
        .count()
    )

    created_at_invalidos = (
        df_silver
        .filter(
            ~F.col("is_created_at_valid")
        )
        .count()
    )

    account_id_nulos = (
        df_silver
        .filter(
            F.col("account_id").isNull()
            | (F.trim(F.col("account_id")) == "")
        )
        .count()
    )

    accounts_huerfanas = (
        df_silver
        .filter(
            ~F.col("is_account_linked")
        )
        .count()
    )

    print("First name nulos o vacíos:", nombres_nulos)
    print("Last name nulos o vacíos:", apellidos_nulos)
    print("Emails nulos o vacíos:", emails_faltantes)
    print("Phones nulos o vacíos:", phones_nulos)
    print("Titles nulos o vacíos:", titles_nulos)
    print(
        "Created_at nulos o no convertibles:",
        created_at_invalidos,
    )
    print(
        "Account ID nulos o vacíos:",
        account_id_nulos,
    )
    print(
        "Contacts sin account relacionada:",
        accounts_huerfanas,
    )

    return total_silver

def validar_leads(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="leads",
        columna_pk="lead_id",
    )

    nombres_nulos = (
        df_silver
        .filter(
            F.col("first_name").isNull()
            | (F.trim(F.col("first_name")) == "")
        )
        .count()
    )

    apellidos_nulos = (
        df_silver
        .filter(
            F.col("last_name").isNull()
            | (F.trim(F.col("last_name")) == "")
        )
        .count()
    )

    emails_faltantes = (
        df_silver
        .filter(
            ~F.col("is_email_present")
        )
        .count()
    )

    fuentes_invalidas = (
        df_silver
        .filter(
            ~F.col("is_source_valid")
        )
        .count()
    )

    estados_invalidos = (
        df_silver
        .filter(
            ~F.col("is_status_valid")
        )
        .count()
    )

    scores_invalidos = (
        df_silver
        .filter(
            ~F.col("is_score_valid")
        )
        .count()
    )

    fechas_invalidas = (
        df_silver
        .filter(
            ~F.col("is_created_at_valid")
        )
        .count()
    )

    print(
        "First name nulos o vacíos:",
        nombres_nulos,
    )
    print(
        "Last name nulos o vacíos:",
        apellidos_nulos,
    )
    print(
        "Emails nulos o vacíos:",
        emails_faltantes,
    )
    print(
        "Fuentes inválidas:",
        fuentes_invalidas,
    )
    print(
        "Estados inválidos:",
        estados_invalidos,
    )
    print(
        "Scores nulos o fuera del rango 0-100:",
        scores_invalidos,
    )
    print(
        "Created_at nulos o no convertibles:",
        fechas_invalidas,
    )

    return total_silver

def validar_opportunities(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="opportunities",
        columna_pk="opportunity_id",
    )

    nombres_nulos = (
        df_silver
        .filter(
            F.col("name").isNull()
            | (F.trim(F.col("name")) == "")
        )
        .count()
    )

    etapas_invalidas = (
        df_silver
        .filter(
            ~F.col("is_stage_valid")
        )
        .count()
    )

    amounts_invalidos = (
        df_silver
        .filter(
            ~F.col("is_amount_valid")
        )
        .count()
    )

    created_at_invalidos = (
        df_silver
        .filter(
            F.col("created_at").isNull()
        )
        .count()
    )

    close_date_invalidos = (
        df_silver
        .filter(
            F.col("close_date").isNull()
        )
        .count()
    )

    fechas_inconsistentes = (
        df_silver
        .filter(
            ~F.col("is_temporally_valid")
        )
        .count()
    )

    account_id_nulos = (
        df_silver
        .filter(
            F.col("account_id").isNull()
            | (F.trim(F.col("account_id")) == "")
        )
        .count()
    )

    accounts_huerfanas = (
        df_silver
        .filter(
            ~F.col("is_account_linked")
        )
        .count()
    )

    print(
        "Nombres nulos o vacíos:",
        nombres_nulos,
    )
    print(
        "Etapas inválidas:",
        etapas_invalidas,
    )
    print(
        "Amounts nulos, no convertibles o <= 0:",
        amounts_invalidos,
    )
    print(
        "Created_at nulos o no convertibles:",
        created_at_invalidos,
    )
    print(
        "Close_date nulos o no convertibles:",
        close_date_invalidos,
    )
    print(
        "Created_at posterior a close_date:",
        fechas_inconsistentes,
    )
    print(
        "Account ID nulos o vacíos:",
        account_id_nulos,
    )
    print(
        "Opportunities sin account relacionada:",
        accounts_huerfanas,
    )

    return total_silver

def validar_opportunity_contacts(
    df_bronze,
    df_silver,
):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    claves_nulas = (
        df_silver
        .filter(
            F.col("opportunity_id").isNull()
            | (
                F.trim(
                    F.col("opportunity_id")
                ) == ""
            )
            | F.col("contact_id").isNull()
            | (
                F.trim(
                    F.col("contact_id")
                ) == ""
            )
        )
        .count()
    )

    combinaciones_repetidas = (
        df_silver
        .groupBy(
            "opportunity_id",
            "contact_id",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    roles_invalidos = (
        df_silver
        .filter(
            ~F.col("is_role_valid")
        )
        .count()
    )

    opportunities_huerfanas = (
        df_silver
        .filter(
            ~F.col("is_opportunity_linked")
        )
        .count()
    )

    contacts_huerfanos = (
        df_silver
        .filter(
            ~F.col("is_contact_linked")
        )
        .count()
    )

    misma_cuenta = (
        df_silver
        .filter(
            F.col("is_same_account")
        )
        .count()
    )

    distinta_cuenta = (
        df_silver
        .filter(
            ~F.col("is_same_account")
        )
        .count()
    )

    print("Filas Bronze:", total_bronze)
    print("Filas Silver:", total_silver)
    print(
        "Claves compuestas nulas o vacías:",
        claves_nulas,
    )
    print(
        "Combinaciones repetidas:",
        combinaciones_repetidas,
    )
    print("Roles inválidos:", roles_invalidos)
    print(
        "Opportunities sin relación:",
        opportunities_huerfanas,
    )
    print(
        "Contacts sin relación:",
        contacts_huerfanos,
    )
    print(
        "Relaciones de la misma cuenta:",
        misma_cuenta,
    )
    print(
        "Relaciones de distinta cuenta:",
        distinta_cuenta,
    )

    if total_bronze != total_silver:
        raise ValueError(
            "La cantidad de filas Bronze y Silver "
            "no coincide para opportunity_contacts."
        )

    if claves_nulas > 0:
        raise ValueError(
            "Existen claves nulas o vacías en "
            "opportunity_contacts."
        )

    if combinaciones_repetidas > 0:
        raise ValueError(
            "Existen combinaciones duplicadas en "
            "opportunity_contacts."
        )

    return total_silver

def validar_activities(
    df_bronze,
    df_silver,
):
    total_silver = validar_basico(
        df_bronze=df_bronze,
        df_silver=df_silver,
        tabla="activities",
        columna_pk="activity_id",
    )

    tipos_invalidos = (
        df_silver
        .filter(
            ~F.col("is_type_valid")
        )
        .count()
    )

    subjects_vacios = (
        df_silver
        .filter(
            F.col("subject").isNull()
            | (F.trim(F.col("subject")) == "")
        )
        .count()
    )

    fechas_invalidas = (
        df_silver
        .filter(
            ~F.col("is_occurred_at_valid")
        )
        .count()
    )

    sin_contacto = (
        df_silver
        .filter(
            ~F.col("has_contact_id")
        )
        .count()
    )

    sin_oportunidad = (
        df_silver
        .filter(
            ~F.col("has_opportunity_id")
        )
        .count()
    )

    sin_ninguna_relacion = (
        df_silver
        .filter(
            ~F.col("has_any_reference")
        )
        .count()
    )

    con_ambas_relaciones = (
        df_silver
        .filter(
            F.col("has_contact_id")
            & F.col("has_opportunity_id")
        )
        .count()
    )

    contacts_huerfanos = (
        df_silver
        .filter(
            F.col("has_contact_id")
            & ~F.col("is_contact_linked")
        )
        .count()
    )

    opportunities_huerfanas = (
        df_silver
        .filter(
            F.col("has_opportunity_id")
            & ~F.col("is_opportunity_linked")
        )
        .count()
    )

    print(
        "Tipos de actividad inválidos:",
        tipos_invalidos,
    )
    print(
        "Subjects nulos o vacíos:",
        subjects_vacios,
    )
    print(
        "Occurred_at nulos o no convertibles:",
        fechas_invalidas,
    )
    print(
        "Actividades sin contacto:",
        sin_contacto,
    )
    print(
        "Actividades sin oportunidad:",
        sin_oportunidad,
    )
    print(
        "Actividades sin ninguna relación:",
        sin_ninguna_relacion,
    )
    print(
        "Actividades con ambas relaciones:",
        con_ambas_relaciones,
    )
    print(
        "Contact IDs informados pero huérfanos:",
        contacts_huerfanos,
    )
    print(
        "Opportunity IDs informados pero huérfanos:",
        opportunities_huerfanas,
    )

    return total_silver

def escribir_tabla_silver(df_silver, tabla):
    (
        df_silver.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"silver.{tabla}")
        .option("user", DWH_USER)
        .option("password", DWH_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

def escribir_tabla_parquet(
    df_silver,
    tabla,
):
    ruta_parquet = (
        f"{PARQUET_BASE_PATH}/{tabla}"
    )

    (
        df_silver.write
        .mode("overwrite")
        .parquet(ruta_parquet)
    )

    print(
        f"{tabla} exportado correctamente a Parquet"
    )

    print(
        f"Ruta: {ruta_parquet}"
    )

def validar_parquet(
    spark,
    tabla,
    total_esperado,
):
    ruta_parquet = (
        f"{PARQUET_BASE_PATH}/{tabla}"
    )

    df_parquet = spark.read.parquet(
        ruta_parquet
    )

    total_parquet = df_parquet.count()

    print(
        f"Registros esperados en Parquet {tabla}:",
        total_esperado,
    )

    print(
        f"Registros encontrados en Parquet {tabla}:",
        total_parquet,
    )

    if total_parquet != total_esperado:
        raise ValueError(
            f"El Parquet de {tabla} no coincide"
        )

    print(
        f"{tabla} validado correctamente en Parquet"
    )

#Validar en postgres
def validar_basico(
    df_bronze,
    df_silver,
    tabla,
    columna_pk,
):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    ids_nulos_o_vacios = (
        df_silver
        .filter(
            F.col(columna_pk).isNull()
            | (
                F.trim(F.col(columna_pk))
                == ""
            )
        )
        .count()
    )

    ids_duplicados = (
        df_silver
        .groupBy(columna_pk)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("--------------------------------")
    print(f"VALIDACIONES BÁSICAS DE {tabla.upper()}")
    print("--------------------------------")
    print("Registros Bronze:", total_bronze)
    print("Registros transformados:", total_silver)
    print(f"{columna_pk} nulos o vacíos:", ids_nulos_o_vacios)
    print(f"{columna_pk} duplicados:", ids_duplicados)

    if total_bronze != total_silver:
        raise ValueError(
            f"Bronze y Silver tienen conteos diferentes en {tabla}"
        )

    if ids_nulos_o_vacios > 0:
        raise ValueError(
            f"Existen {columna_pk} nulos o vacíos"
        )

    if ids_duplicados > 0:
        raise ValueError(
            f"Existen {columna_pk} duplicados"
        )

    return total_silver

def validar_escritura(
    spark,
    tabla,
    total_esperado,
):
    df_postgresql = spark.read.jdbc(
        url=JDBC_URL,
        table=f"silver.{tabla}",
        properties=JDBC_PROPERTIES,
    )

    total_postgresql = df_postgresql.count()

    print(
        f"Registros esperados en {tabla}:",
        total_esperado,
    )

    print(
        f"Registros en silver.{tabla}:",
        total_postgresql,
    )

    if total_postgresql != total_esperado:
        raise ValueError(
            f"La escritura de {tabla} en Silver no coincide"
        )

    print(
        f"{tabla} cargado correctamente en Silver"
    )

def main():
    spark = crear_spark()

    try:
        print("Versión de Spark:", spark.version)
        print("\nLeyendo bronze.students...")

        df_students_bronze = leer_tabla_bronze(
            spark,
            "students",
        )

        print("Transformando students...")

        df_students_silver = transformar_students(
            df_students_bronze
        )

        total_students = validar_students(
            df_students_bronze,
            df_students_silver,
        )

        print("Escribiendo silver.students...")

        escribir_tabla_silver(
            df_students_silver,
            "students",
        )

        validar_escritura(
            spark,
            "students",
            total_students,
        )

        print("Exportando studens a parquet")

        escribir_tabla_parquet(
            df_students_silver,
            "students",
        )

        validar_parquet(
            spark,
            "students",
            total_students,
        )

        print("\nLeyendo bronze.professors...")

        df_professors_bronze = leer_tabla_bronze(
            spark,
            "professors",
        )

        print("Transformando professors...")

        df_professors_silver = transformar_professors(
            df_professors_bronze
        )

        total_professors = validar_professors(
            df_professors_bronze,
            df_professors_silver,
        )

        print("Escribiendo silver.professors...")

        escribir_tabla_silver(
            df_professors_silver,
            "professors",
        )

        validar_escritura(
            spark,
            "professors",
            total_professors,
        )
        
        print("Exportando professors a parquet")

        escribir_tabla_parquet(
            df_professors_silver,
            "professors",
        )

        validar_parquet(
            spark,
            "professors",
            total_professors,
        )

        print("\nLeyendo bronze.courses...")

        df_courses_bronze = leer_tabla_bronze(
            spark,
            "courses",
        )

        print("Transformando courses...")

        df_courses_silver = transformar_courses(
            df_courses_bronze
        )

        total_courses = validar_courses(
            df_courses_bronze,
            df_courses_silver,
            df_professors_silver,
        )

        print("Escribiendo silver.courses...")

        escribir_tabla_silver(
            df_courses_silver,
            "courses",
        )

        validar_escritura(
            spark,
            "courses",
            total_courses,
        )

        print("Exportando courses a parquet")

        escribir_tabla_parquet(
            df_courses_silver,
            "courses",
        )

        validar_parquet(
            spark,
            "courses",
            total_courses,
        )

        print("\nLeyendo bronze.semesters...")

        df_semesters_bronze = leer_tabla_bronze(
            spark,
            "semesters",
        )

        print("Transformando semesters...")

        df_semesters_silver = transformar_semesters(
            df_semesters_bronze
        )

        total_semesters = validar_semesters(
            df_semesters_bronze,
            df_semesters_silver,
        )

        print("Escribiendo silver.semesters...")

        escribir_tabla_silver(
            df_semesters_silver,
            "semesters",
        )

        validar_escritura(
            spark,
            "semesters",
            total_semesters,
        )

        print("Exportando semesters a parquet")

        escribir_tabla_parquet(
            df_semesters_silver,
            "semesters",
        )

        validar_parquet(
            spark,
            "semesters",
            total_semesters,
        )

        print("\nLeyendo bronze.enrollments...")

        df_enrollments_bronze = leer_tabla_bronze(
            spark,
            "enrollments",
        )

        print("Transformando enrollments...")

        df_enrollments_silver = transformar_enrollments(
            df_enrollments_bronze
        )

        total_enrollments = validar_enrollments(
            df_enrollments_bronze,
            df_enrollments_silver,
            df_students_silver,
            df_courses_silver,
            df_semesters_silver,
        )

        print("Escribiendo silver.enrollments...")

        escribir_tabla_silver(
            df_enrollments_silver,
            "enrollments",
        )

        validar_escritura(
            spark,
            "enrollments",
            total_enrollments,
        )

        print("Exportando enrollments a parquet")

        escribir_tabla_parquet(
            df_enrollments_silver,
            "enrollments",
        )

        validar_parquet(
            spark,
            "enrollments",
            total_enrollments,
        )

        print("\nLeyendo bronze.grades...")

        df_grades_bronze = leer_tabla_bronze(
            spark,
            "grades",
        )

        print("Transformando grades...")

        df_grades_silver = transformar_grades(
            df_grades_bronze
        )

        total_grades = validar_grades(
            df_grades_bronze,
            df_grades_silver,
            df_enrollments_silver,
        )

        print("Escribiendo silver.grades...")

        escribir_tabla_silver(
            df_grades_silver,
            "grades",
        )

        validar_escritura(
            spark,
            "grades",
            total_grades,
        )

        print("Exportando grades a parquet")

        escribir_tabla_parquet(
            df_grades_silver,
            "grades",
        )

        validar_parquet(
            spark,
            "grades",
            total_grades,
        )

        print("\nUniversity cargado correctamente.")

        #Billing
        print("INICIANDO BILLING")

        print("\nLeyendo bronze.customers...")

        df_customers_bronze = leer_tabla_bronze(
            spark,
            "customers",
        )

        print("Transformando customers...")

        df_customers_silver = transformar_customers(
            df_customers_bronze,
            df_students_silver,
        )

        total_customers = validar_customers(
            df_customers_bronze,
            df_customers_silver,
        )

        print("Escribiendo silver.customers...")

        escribir_tabla_silver(
            df_customers_silver,
            "customers",
        )

        validar_escritura(
            spark,
            "customers",
            total_customers,
        )

        print("Exportando customers a Parquet...")

        escribir_tabla_parquet(
            df_customers_silver,
            "customers",
        )

        validar_parquet(
            spark,
            "customers",
            total_customers,
        )

        print(
            "\nCustomers cargado correctamente."
        )

        print("\nLeyendo bronze.products...")

        df_products_bronze = leer_tabla_bronze(
            spark,
            "products",
        )

        print("Transformando products...")

        df_products_silver = transformar_products(
            df_products_bronze
        )

        total_products = validar_products(
            df_products_bronze,
            df_products_silver,
        )

        print("Escribiendo silver.products...")

        escribir_tabla_silver(
            df_products_silver,
            "products",
        )

        validar_escritura(
            spark,
            "products",
            total_products,
        )

        print("Exportando products a Parquet...")

        escribir_tabla_parquet(
            df_products_silver,
            "products",
        )

        validar_parquet(
            spark,
            "products",
            total_products,
        )

        print(
            "\nProducts cargado correctamente."
        )

        print("\nLeyendo bronze.subscriptions...")

        df_subscriptions_bronze = leer_tabla_bronze(
            spark,
            "subscriptions",
        )

        print("Transformando subscriptions...")

        df_subscriptions_silver = (
            transformar_subscriptions(
                df_subscriptions_bronze,
                df_customers_silver,
                df_products_silver,
            )
        )

        total_subscriptions = validar_subscriptions(
            df_subscriptions_bronze,
            df_subscriptions_silver,
        )

        print(
            "Escribiendo silver.subscriptions..."
        )

        escribir_tabla_silver(
            df_subscriptions_silver,
            "subscriptions",
        )

        validar_escritura(
            spark,
            "subscriptions",
            total_subscriptions,
        )

        print(
            "Exportando subscriptions a Parquet..."
        )

        escribir_tabla_parquet(
            df_subscriptions_silver,
            "subscriptions",
        )

        validar_parquet(
            spark,
            "subscriptions",
            total_subscriptions,
        )

        print(
            "\nSubscriptions cargado correctamente."
        )

        print("\nLeyendo bronze.invoices...")

        df_invoices_bronze = leer_tabla_bronze(
            spark,
            "invoices",
        )

        print("Transformando invoices...")

        df_invoices_silver = transformar_invoices(
            df_invoices_bronze,
            df_customers_silver,
        )

        total_invoices = validar_invoices(
            df_invoices_bronze,
            df_invoices_silver,
        )

        print("Escribiendo silver.invoices...")

        escribir_tabla_silver(
            df_invoices_silver,
            "invoices",
        )

        validar_escritura(
            spark,
            "invoices",
            total_invoices,
        )

        print("Exportando invoices a Parquet...")

        escribir_tabla_parquet(
            df_invoices_silver,
            "invoices",
        )

        validar_parquet(
            spark,
            "invoices",
            total_invoices,
        )

        print(
            "\nInvoices cargado correctamente."
        )

        print("\nLeyendo bronze.invoice_items...")

        df_invoice_items_bronze = leer_tabla_bronze(
            spark,
            "invoice_items",
        )

        print("Transformando invoice_items...")

        df_invoice_items_silver = (
            transformar_invoice_items(
                df_invoice_items_bronze,
                df_invoices_silver,
                df_products_silver,
            )
        )

        total_invoice_items = validar_invoice_items(
            df_invoice_items_bronze,
            df_invoice_items_silver,
        )

        print(
            "Escribiendo silver.invoice_items..."
        )

        escribir_tabla_silver(
            df_invoice_items_silver,
            "invoice_items",
        )

        validar_escritura(
            spark,
            "invoice_items",
            total_invoice_items,
        )

        print(
            "Exportando invoice_items a Parquet..."
        )

        escribir_tabla_parquet(
            df_invoice_items_silver,
            "invoice_items",
        )

        validar_parquet(
            spark,
            "invoice_items",
            total_invoice_items,
        )

        print(
            "\nInvoice items cargado correctamente."
        )

        print("\nLeyendo bronze.payments...")

        df_payments_bronze = leer_tabla_bronze(
            spark,
            "payments",
        )

        print("Transformando payments...")

        df_payments_silver = transformar_payments(
            df_payments_bronze,
            df_invoices_silver,
        )

        total_payments = validar_payments(
            df_payments_bronze,
            df_payments_silver,
        )

        print("Escribiendo silver.payments...")

        escribir_tabla_silver(
            df_payments_silver,
            "payments",
        )

        validar_escritura(
            spark,
            "payments",
            total_payments,
        )

        print("Exportando payments a Parquet...")

        escribir_tabla_parquet(
            df_payments_silver,
            "payments",
        )

        validar_parquet(
            spark,
            "payments",
            total_payments,
        )

        print(
            "\nPayments cargado correctamente."
        )

        print(
            "\nBilling cargado correctamente."
        )

        print("\n====================================")
        print("INICIANDO CRM")
        print("====================================")

        print("\nLeyendo bronze.accounts...")

        df_accounts_bronze = leer_tabla_bronze(
            spark,
            "accounts",
        )

        print("Transformando accounts...")

        df_accounts_silver = transformar_accounts(
            df_accounts_bronze
        )

        total_accounts = validar_accounts(
            df_accounts_bronze,
            df_accounts_silver,
        )

        print("Escribiendo silver.accounts...")

        escribir_tabla_silver(
            df_accounts_silver,
            "accounts",
        )

        validar_escritura(
            spark,
            "accounts",
            total_accounts,
        )

        print("Exportando accounts a Parquet...")

        escribir_tabla_parquet(
            df_accounts_silver,
            "accounts",
        )

        validar_parquet(
            spark,
            "accounts",
            total_accounts,
        )

        print(
            "\nAccounts cargado correctamente."
        )

        print("\nLeyendo bronze.contacts...")

        df_contacts_bronze = leer_tabla_bronze(
            spark,
            "contacts",
        )

        print("Transformando contacts...")

        df_contacts_silver = transformar_contacts(
            df_contacts_bronze,
            df_accounts_silver,
        )

        total_contacts = validar_contacts(
            df_contacts_bronze,
            df_contacts_silver,
        )

        print("Escribiendo silver.contacts...")

        escribir_tabla_silver(
            df_contacts_silver,
            "contacts",
        )

        validar_escritura(
            spark,
            "contacts",
            total_contacts,
        )

        print("Exportando contacts a Parquet...")

        escribir_tabla_parquet(
            df_contacts_silver,
            "contacts",
        )

        validar_parquet(
            spark,
            "contacts",
            total_contacts,
        )

        print(
            "\nContacts cargado correctamente."
        )

        print("\nLeyendo bronze.leads...")

        df_leads_bronze = leer_tabla_bronze(
            spark,
            "leads",
        )

        print("Transformando leads...")

        df_leads_silver = transformar_leads(
            df_leads_bronze
        )

        total_leads = validar_leads(
            df_leads_bronze,
            df_leads_silver,
        )

        print("Escribiendo silver.leads...")

        escribir_tabla_silver(
            df_leads_silver,
            "leads",
        )

        validar_escritura(
            spark,
            "leads",
            total_leads,
        )

        print("Exportando leads a Parquet...")

        escribir_tabla_parquet(
            df_leads_silver,
            "leads",
        )

        validar_parquet(
            spark,
            "leads",
            total_leads,
        )

        print(
            "\nLeads cargado correctamente."
        )

        print("\nLeyendo bronze.opportunities...")

        df_opportunities_bronze = leer_tabla_bronze(
            spark,
            "opportunities",
        )

        print("Transformando opportunities...")

        df_opportunities_silver = (
            transformar_opportunities(
                df_opportunities_bronze,
                df_accounts_silver,
            )
        )

        total_opportunities = validar_opportunities(
            df_opportunities_bronze,
            df_opportunities_silver,
        )

        print(
            "Escribiendo silver.opportunities..."
        )

        escribir_tabla_silver(
            df_opportunities_silver,
            "opportunities",
        )

        validar_escritura(
            spark,
            "opportunities",
            total_opportunities,
        )

        print(
            "Exportando opportunities a Parquet..."
        )

        escribir_tabla_parquet(
            df_opportunities_silver,
            "opportunities",
        )

        validar_parquet(
            spark,
            "opportunities",
            total_opportunities,
        )

        print(
            "\nOpportunities cargado correctamente."
        )

        print(
            "\nLeyendo bronze.opportunity_contacts..."
        )

        df_opportunity_contacts_bronze = (
            leer_tabla_bronze(
                spark,
                "opportunity_contacts",
            )
        )

        print(
            "Transformando opportunity_contacts..."
        )

        df_opportunity_contacts_silver = (
            transformar_opportunity_contacts(
                df_opportunity_contacts_bronze,
                df_opportunities_silver,
                df_contacts_silver,
            )
        )

        total_opportunity_contacts = (
            validar_opportunity_contacts(
                df_opportunity_contacts_bronze,
                df_opportunity_contacts_silver,
            )
        )

        print(
            "Escribiendo "
            "silver.opportunity_contacts..."
        )

        escribir_tabla_silver(
            df_opportunity_contacts_silver,
            "opportunity_contacts",
        )

        validar_escritura(
            spark,
            "opportunity_contacts",
            total_opportunity_contacts,
        )

        print(
            "Exportando opportunity_contacts "
            "a Parquet..."
        )

        escribir_tabla_parquet(
            df_opportunity_contacts_silver,
            "opportunity_contacts",
        )

        validar_parquet(
            spark,
            "opportunity_contacts",
            total_opportunity_contacts,
        )

        print(
            "\nOpportunity contacts cargado "
            "correctamente."
        )

        print("\nLeyendo bronze.activities...")

        df_activities_bronze = leer_tabla_bronze(
            spark,
            "activities",
        )

        print("Transformando activities...")

        df_activities_silver = transformar_activities(
            df_activities_bronze,
            df_contacts_silver,
            df_opportunities_silver,
        )

        total_activities = validar_activities(
            df_activities_bronze,
            df_activities_silver,
        )

        print("Escribiendo silver.activities...")

        escribir_tabla_silver(
            df_activities_silver,
            "activities",
        )

        validar_escritura(
            spark,
            "activities",
            total_activities,
        )

        print("Exportando activities a Parquet...")

        escribir_tabla_parquet(
            df_activities_silver,
            "activities",
        )

        validar_parquet(
            spark,
            "activities",
            total_activities,
        )

        print(
            "\nActivities cargado correctamente."
        )

        print(
            "\n===================================="
        )
        print(
            "CRM CARGADO CORRECTAMENTE"
        )
        print(
            "===================================="
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()