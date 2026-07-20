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

#Crear Spark
def crear_spark():
    spark = (
        SparkSession.builder
        .appName("TransformSilverUniversity")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.4",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark

def leer_students_bronze(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="bronze.students",
        properties=JDBC_PROPERTIES,
    )

def leer_professors_bronze(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="bronze.professors",
        properties=JDBC_PROPERTIES,
    )

def leer_courses_bronze(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="bronze.courses",
        properties=JDBC_PROPERTIES,
    )

def leer_semesters_bronze(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="bronze.semesters",
        properties=JDBC_PROPERTIES,
    )

def leer_enrollments_bronze(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="bronze.enrollments",
        properties=JDBC_PROPERTIES,
    )

def leer_grades_bronze(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="bronze.grades",
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

    # Ventana: todas las notas de una misma inscripción
    ventana_enrollment = Window.partitionBy(
        "enrollment_id"
    )

    # Sumar los pesos por inscripción
    df_silver = df_silver.withColumn(
        "total_weight",
        F.round(
            F.sum("weight").over(ventana_enrollment),
            4,
        ).cast("decimal(8,4)"),
    )

    # Contar cuántas notas existen y cuántos pesos pudieron convertirse
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

    # Validar que todos los pesos existan y sumen exactamente 1
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

def validar_students(df_bronze, df_silver):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    ids_nulos = (
        df_silver
        .filter(F.col("student_id").isNull())
        .count()
    )

    ids_duplicados = (
        df_silver
        .groupBy("student_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print("--------------------------------")
    print("VALIDACIONES DE STUDENTS")
    print("--------------------------------")
    print("Registros Bronze:", total_bronze)
    print("Registros transformados:", total_silver)
    print("Student ID nulos:", ids_nulos)
    print("Student ID duplicados:", ids_duplicados)

    if total_bronze != total_silver:
        raise ValueError(
            "Bronze y Silver tienen conteos diferentes"
        )

    if ids_nulos > 0:
        raise ValueError(
            "Existen student_id nulos"
        )

    if ids_duplicados > 0:
        raise ValueError(
            "Existen student_id duplicados"
        )

    return total_silver

def validar_professors(df_bronze, df_silver):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    ids_nulos = (
        df_silver
        .filter(F.col("professor_id").isNull())
        .count()
    )

    ids_duplicados = (
        df_silver
        .groupBy("professor_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    hired_at_nulos = (
        df_silver
        .filter(F.col("hired_at").isNull())
        .count()
    )

    print("--------------------------------")
    print("VALIDACIONES DE PROFESSORS")
    print("--------------------------------")
    print("Registros Bronze:", total_bronze)
    print("Registros transformados:", total_silver)
    print("Professor ID nulos:", ids_nulos)
    print("Professor ID duplicados:", ids_duplicados)
    print(
        "Hired_at nulos o no convertibles:",
        hired_at_nulos,
    )

    if total_bronze != total_silver:
        raise ValueError(
            "Bronze y Silver tienen conteos diferentes en professors"
        )

    if ids_nulos > 0:
        raise ValueError(
            "Existen professor_id nulos"
        )

    if ids_duplicados > 0:
        raise ValueError(
            "Existen professor_id duplicados"
        )

    return total_silver

def validar_courses(
    df_bronze,
    df_silver,
    df_professors_silver,
):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    ids_nulos = (
        df_silver
        .filter(F.col("course_id").isNull())
        .count()
    )

    ids_duplicados = (
        df_silver
        .groupBy("course_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
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
        .filter(F.col("professor_id").isNull())
        .count()
    )

    profesores_huerfanos = (
        df_silver
        .select("professor_id")
        .filter(F.col("professor_id").isNotNull())
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

    print("--------------------------------")
    print("VALIDACIONES DE COURSES")
    print("--------------------------------")
    print("Registros Bronze:", total_bronze)
    print("Registros transformados:", total_silver)
    print("Course ID nulos:", ids_nulos)
    print("Course ID duplicados:", ids_duplicados)
    print("Credits nulos o no convertibles:", credits_nulos)
    print("Credits menores o iguales a cero:", credits_invalidos)
    print("Professor ID nulos:", professor_id_nulos)
    print("Professor ID sin profesor existente:", profesores_huerfanos)

    if total_bronze != total_silver:
        raise ValueError(
            "Bronze y Silver tienen conteos diferentes en courses"
        )

    if ids_nulos > 0:
        raise ValueError(
            "Existen course_id nulos"
        )

    if ids_duplicados > 0:
        raise ValueError(
            "Existen course_id duplicados"
        )

    return total_silver

def validar_semesters(df_bronze, df_silver):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    ids_nulos = (
        df_silver
        .filter(F.col("semester_id").isNull())
        .count()
    )

    ids_duplicados = (
        df_silver
        .groupBy("semester_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
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
        & (F.col("start_date") > F.col("end_date"))
    )
    .count()
    )

    print("--------------------------------")
    print("VALIDACIONES DE SEMESTERS")
    print("--------------------------------")
    print("Registros Bronze:", total_bronze)
    print("Registros transformados:", total_silver)
    print("Semester ID nulos:", ids_nulos)
    print("Semester ID duplicados:", ids_duplicados)
    print("Year nulos o no convertibles:", year_nulos)
    print("Half nulos o no convertibles:", half_nulos)
    print("Half diferentes de 1 o 2:", half_invalidos)
    print("Registros con fechas nulas:", fechas_nulas)
    print(
        "Registros con relación temporal inválida:",
        fechas_inconsistentes,
    )

    if total_bronze != total_silver:
        raise ValueError(
            "Bronze y Silver tienen conteos diferentes en semesters"
        )

    if ids_nulos > 0:
        raise ValueError(
            "Existen semester_id nulos"
        )

    if ids_duplicados > 0:
        raise ValueError(
            "Existen semester_id duplicados"
        )

    return total_silver

def validar_enrollments(
    df_bronze,
    df_silver,
    df_students_silver,
    df_courses_silver,
    df_semesters_silver,
):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    ids_nulos = (
        df_silver
        .filter(F.col("enrollment_id").isNull())
        .count()
    )

    ids_duplicados = (
        df_silver
        .groupBy("enrollment_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
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
        .filter(F.col("student_id").isNotNull())
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

    print("--------------------------------")
    print("VALIDACIONES DE ENROLLMENTS")
    print("--------------------------------")
    print("Registros Bronze:", total_bronze)
    print("Registros transformados:", total_silver)
    print("Enrollment ID nulos:", ids_nulos)
    print("Enrollment ID duplicados:", ids_duplicados)
    print("Enrolled_at nulos o no convertibles:", fechas_nulas)
    print("Estados inválidos:", estados_invalidos)
    print("Student ID sin estudiante existente:", students_huerfanos)
    print("Course ID sin curso existente:", courses_huerfanos)
    print("Semester ID sin semestre existente:", semesters_huerfanos)
    print("Filas en combinaciones repetidas:", combinaciones_repetidas)
    print("Grupos repetidos:", grupos_repetidos)

    if total_bronze != total_silver:
        raise ValueError(
            "Bronze y Silver tienen conteos diferentes en enrollments"
        )

    if ids_nulos > 0:
        raise ValueError(
            "Existen enrollment_id nulos"
        )

    if ids_duplicados > 0:
        raise ValueError(
            "Existen enrollment_id duplicados"
        )

    return total_silver

def validar_grades(
    df_bronze,
    df_silver,
    df_enrollments_silver,
):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    ids_nulos = (
        df_silver
        .filter(F.col("grade_id").isNull())
        .count()
    )

    ids_duplicados = (
        df_silver
        .groupBy("grade_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assessments_nulos = (
        df_silver
        .filter(F.col("assessment").isNull())
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
        .filter(F.col("enrollment_id").isNull())
        .count()
    )

    enrollments_huerfanos = (
        df_silver
        .select("enrollment_id")
        .filter(F.col("enrollment_id").isNotNull())
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

    print("--------------------------------")
    print("VALIDACIONES DE GRADES")
    print("--------------------------------")
    print("Registros Bronze:", total_bronze)
    print("Registros transformados:", total_silver)
    print("Grade ID nulos:", ids_nulos)
    print("Grade ID duplicados:", ids_duplicados)
    print("Assessment nulos:", assessments_nulos)
    print("Scores inválidos:", scores_invalidos)
    print("Weights individuales inválidos:", weights_invalidos)
    print("Graded_at nulos o no convertibles:", fechas_nulas)
    print("Enrollment ID nulos:", enrollment_id_nulos)
    print(
        "Enrollment ID sin inscripción existente:",
        enrollments_huerfanos,
    )
    print(
        "Filas cuyo peso total no suma 1:",
        filas_peso_total_invalido,
    )
    print(
        "Inscripciones cuyo peso total no suma 1:",
        enrollments_peso_total_invalido,
    )

    if total_bronze != total_silver:
        raise ValueError(
            "Bronze y Silver tienen conteos diferentes en grades"
        )

    if ids_nulos > 0:
        raise ValueError(
            "Existen grade_id nulos"
        )

    if ids_duplicados > 0:
        raise ValueError(
            "Existen grade_id duplicados"
        )

    return total_silver

def escribir_students_silver(df_silver):
    (
        df_silver.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.students")
        .option("user", DWH_USER)
        .option("password", DWH_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

def escribir_professors_silver(df_silver):
    (
        df_silver.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.professors")
        .option("user", DWH_USER)
        .option("password", DWH_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

def escribir_courses_silver(df_silver):
    (
        df_silver.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.courses")
        .option("user", DWH_USER)
        .option("password", DWH_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

def escribir_semesters_silver(df_silver):
    (
        df_silver.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.semesters")
        .option("user", DWH_USER)
        .option("password", DWH_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

def escribir_enrollments_silver(df_silver):
    (
        df_silver.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.enrollments")
        .option("user", DWH_USER)
        .option("password", DWH_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

def escribir_grades_silver(df_silver):
    (
        df_silver.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "silver.grades")
        .option("user", DWH_USER)
        .option("password", DWH_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )

#Validar en postgres
def validar_escritura_students(spark, total_esperado):
    df_postgresql = spark.read.jdbc(
        url=JDBC_URL,
        table="silver.students",
        properties=JDBC_PROPERTIES,
    )

    total_postgresql = df_postgresql.count()

    print("Registros esperados:", total_esperado)
    print("Registros en silver.students:", total_postgresql)

    if total_postgresql != total_esperado:
        raise ValueError(
            "La escritura en Silver no coincide"
        )

    print("Students cargado correctamente en Silver")

def validar_escritura_professors(
    spark,
    total_esperado,
):
    df_postgresql = spark.read.jdbc(
        url=JDBC_URL,
        table="silver.professors",
        properties=JDBC_PROPERTIES,
    )

    total_postgresql = df_postgresql.count()

    print(
        "Registros esperados en professors:",
        total_esperado,
    )

    print(
        "Registros en silver.professors:",
        total_postgresql,
    )

    if total_postgresql != total_esperado:
        raise ValueError(
            "La escritura de professors en Silver no coincide"
        )

    print(
        "Professors cargado correctamente en Silver"
    )

def validar_escritura_courses(
    spark,
    total_esperado,
):
    df_postgresql = spark.read.jdbc(
        url=JDBC_URL,
        table="silver.courses",
        properties=JDBC_PROPERTIES,
    )

    total_postgresql = df_postgresql.count()

    print(
        "Registros esperados en courses:",
        total_esperado,
    )

    print(
        "Registros en silver.courses:",
        total_postgresql,
    )

    if total_postgresql != total_esperado:
        raise ValueError(
            "La escritura de courses en Silver no coincide"
        )

    print(
        "Courses cargado correctamente en Silver"
    )

def validar_escritura_semesters(
    spark,
    total_esperado,
):
    df_postgresql = spark.read.jdbc(
        url=JDBC_URL,
        table="silver.semesters",
        properties=JDBC_PROPERTIES,
    )

    total_postgresql = df_postgresql.count()

    print(
        "Registros esperados en semesters:",
        total_esperado,
    )

    print(
        "Registros en silver.semesters:",
        total_postgresql,
    )

    if total_postgresql != total_esperado:
        raise ValueError(
            "La escritura de semesters en Silver no coincide"
        )

    print(
        "Semesters cargado correctamente en Silver"
    )

def validar_escritura_enrollments(
    spark,
    total_esperado,
):
    df_postgresql = spark.read.jdbc(
        url=JDBC_URL,
        table="silver.enrollments",
        properties=JDBC_PROPERTIES,
    )

    total_postgresql = df_postgresql.count()

    print(
        "Registros esperados en enrollments:",
        total_esperado,
    )

    print(
        "Registros en silver.enrollments:",
        total_postgresql,
    )

    if total_postgresql != total_esperado:
        raise ValueError(
            "La escritura de enrollments en Silver no coincide"
        )

    print(
        "Enrollments cargado correctamente en Silver"
    )

def validar_escritura_grades(
    spark,
    total_esperado,
):
    df_postgresql = spark.read.jdbc(
        url=JDBC_URL,
        table="silver.grades",
        properties=JDBC_PROPERTIES,
    )

    total_postgresql = df_postgresql.count()

    print(
        "Registros esperados en grades:",
        total_esperado,
    )

    print(
        "Registros en silver.grades:",
        total_postgresql,
    )

    if total_postgresql != total_esperado:
        raise ValueError(
            "La escritura de grades en Silver no coincide"
        )

    print(
        "Grades cargado correctamente en Silver"
    )

def main():
    spark = crear_spark()

    try:
        print("Versión de Spark:", spark.version)
        print("\nLeyendo bronze.students...")

        df_students_bronze = leer_students_bronze(
            spark
        )

        print("Transformando students")

        df_students_silver = transformar_students(
            df_students_bronze
        )

        total_students = validar_students(
            df_students_bronze,
            df_students_silver,
        )

        print("Escribiendo silver.students")

        escribir_students_silver(
            df_students_silver
        )

        validar_escritura_students(
            spark,
            total_students,
        )

        print("\nLeyendo bronze.professors")

        df_professors_bronze = (
            leer_professors_bronze(spark)
        )

        print("Transformando professors")

        df_professors_silver = (
            transformar_professors(
                df_professors_bronze
            )
        )

        total_professors = validar_professors(
            df_professors_bronze,
            df_professors_silver,
        )

        print("Escribiendo silver.professors")

        escribir_professors_silver(
            df_professors_silver
        )

        validar_escritura_professors(
            spark,
            total_professors,
        )

        print("\nLeyendo bronze.courses")

        df_courses_bronze = leer_courses_bronze(
            spark
        )

        print("Transformando courses")

        df_courses_silver = transformar_courses(
            df_courses_bronze
        )

        total_courses = validar_courses(
            df_courses_bronze,
            df_courses_silver,
            df_professors_silver,
        )

        print("Escribiendo silver.courses")

        escribir_courses_silver(
            df_courses_silver
        )

        validar_escritura_courses(
            spark,
            total_courses,
        )

        print("\nLeyendo bronze.semesters")

        df_semesters_bronze = leer_semesters_bronze(
            spark
        )

        print("Transformando semesters")

        df_semesters_silver = transformar_semesters(
            df_semesters_bronze
        )

        total_semesters = validar_semesters(
            df_semesters_bronze,
            df_semesters_silver,
        )

        print("Escribiendo silver.semesters")

        escribir_semesters_silver(
            df_semesters_silver
        )

        validar_escritura_semesters(
            spark,
            total_semesters,
        )

        print("\nLeyendo bronze.enrollments")

        df_enrollments_bronze = leer_enrollments_bronze(
            spark
        )

        print("Transformando enrollments")

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

        print("Escribiendo silver.enrollments")

        escribir_enrollments_silver(
            df_enrollments_silver
        )

        validar_escritura_enrollments(
            spark,
            total_enrollments,
        )

        print("\nLeyendo bronze.grades...")

        df_grades_bronze = leer_grades_bronze(
            spark
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

        escribir_grades_silver(
            df_grades_silver
        )

        validar_escritura_grades(
            spark,
            total_grades,
        )

    finally:
        spark.stop()

if __name__ == "__main__":
    main()