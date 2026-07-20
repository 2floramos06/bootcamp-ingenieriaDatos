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
        .appName("TransformSilverUniversity")
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

    finally:
        spark.stop()


if __name__ == "__main__":
    main()