CREATE SCHEMA IF NOT EXISTS gold;

--UNIVERSITY
--students, enrollments, grades : vision por estudiante
DROP TABLE IF EXISTS gold.student_academic_summary;

CREATE TABLE gold.student_academic_summary AS
WITH grades_by_enrollment AS (
    SELECT
        enrollment_id,

        COUNT(*) AS grade_records,

        BOOL_AND(is_score_valid)
            AS all_scores_valid,

        BOOL_AND(is_weight_valid)
            AS all_weights_valid,

        ROUND(
            SUM(weight)::numeric,
            4
        ) AS observed_weight_sum,

        CASE
            WHEN BOOL_AND(is_score_valid)
             AND BOOL_AND(is_weight_valid)
             AND SUM(weight) > 0
            THEN ROUND(
                (
                    SUM(score * weight)
                    / SUM(weight)
                )::numeric,
                2
            )
            ELSE NULL
        END AS weighted_average_observed,

        BOOL_AND(is_weight_sum_one)
            AS is_weight_sum_one

    FROM silver.grades

    GROUP BY enrollment_id
),

enrollments_by_student AS (
    SELECT
        e.student_id,

        COUNT(*) AS total_enrollments,

        COUNT(*) FILTER (
            WHERE e.status = 'completed'
        ) AS completed_enrollments,

        COUNT(*) FILTER (
            WHERE e.status = 'active'
        ) AS active_enrollments,

        COUNT(*) FILTER (
            WHERE e.status = 'failed'
        ) AS failed_enrollments,

        COUNT(*) FILTER (
            WHERE e.status = 'dropped'
        ) AS dropped_enrollments,

        COUNT(*) FILTER (
            WHERE e.is_repeated_combination
        ) AS repeated_combination_enrollments,

        COUNT(*) FILTER (
            WHERE g.enrollment_id IS NOT NULL
        ) AS enrollments_with_grades,

        COUNT(*) FILTER (
            WHERE g.enrollment_id IS NOT NULL
              AND g.is_weight_sum_one
        ) AS valid_weight_enrollments,

        COUNT(*) FILTER (
            WHERE g.enrollment_id IS NOT NULL
            AND g.is_weight_sum_one
        ) AS weight_sum_one_enrollments,

        COUNT(*) FILTER (
            WHERE g.enrollment_id IS NOT NULL
            AND NOT g.is_weight_sum_one
        ) AS weight_sum_not_one_enrollments,

        COUNT(*) FILTER (
            WHERE e.status IN ('completed', 'failed')
            AND g.weighted_average_observed IS NOT NULL
        ) AS enrollments_with_observed_average,

        ROUND(
            AVG(g.weighted_average_observed) FILTER (
                WHERE e.status IN ('completed', 'failed')
                    AND g.weighted_average_observed IS NOT NULL
                ),
                2
            ) AS average_weighted_observed_score
            
    FROM silver.enrollments AS e

    LEFT JOIN grades_by_enrollment AS g
        ON e.enrollment_id = g.enrollment_id

    GROUP BY e.student_id
)

SELECT
    s.student_id,

    CONCAT_WS(
        ' ',
        s.first_name,
        s.last_name
    ) AS student_name,

    s.email,
    s.country,
    s.enrolled_at,

    s.is_temporally_valid
        AS is_student_temporally_valid,

    COALESCE(
        e.total_enrollments,
        0
    ) AS total_enrollments,

    COALESCE(
        e.completed_enrollments,
        0
    ) AS completed_enrollments,

    COALESCE(
        e.active_enrollments,
        0
    ) AS active_enrollments,

    COALESCE(
        e.failed_enrollments,
        0
    ) AS failed_enrollments,

    COALESCE(
        e.dropped_enrollments,
        0
    ) AS dropped_enrollments,

    COALESCE(
        e.repeated_combination_enrollments,
        0
    ) AS repeated_combination_enrollments,

    COALESCE(
        e.enrollments_with_grades,
        0
    ) AS enrollments_with_grades,

    COALESCE(
        e.weight_sum_one_enrollments,
        0
    ) AS weight_sum_one_enrollments,

    COALESCE(
        e.weight_sum_not_one_enrollments,
        0
    ) AS weight_sum_not_one_enrollments,

    COALESCE(
        e.enrollments_with_observed_average,
        0
    ) AS enrollments_with_observed_average,

    e.average_weighted_observed_score,

    CURRENT_TIMESTAMP AS gold_created_at

FROM silver.students AS s

LEFT JOIN enrollments_by_student AS e
    ON s.student_id = e.student_id;

ALTER TABLE gold.student_academic_summary
ADD CONSTRAINT pk_student_academic_summary
PRIMARY KEY (student_id);

--professors, enrollments, semesters, grades: rendimiento academico por curso 
DROP TABLE IF EXISTS gold.course_performance_summary;

CREATE TABLE gold.course_performance_summary AS
WITH grades_by_enrollment AS (
    SELECT
        enrollment_id,

        COUNT(*) AS grade_records,

        ROUND(
            SUM(weight)::numeric,
            4
        ) AS observed_weight_sum,

        CASE
            WHEN BOOL_AND(is_score_valid)
             AND BOOL_AND(is_weight_valid)
             AND SUM(weight) > 0
            THEN ROUND(
                (
                    SUM(score * weight)
                    / SUM(weight)
                )::numeric,
                2
            )
            ELSE NULL
        END AS weighted_average_observed

    FROM silver.grades

    GROUP BY enrollment_id
),

course_metrics AS (
    SELECT
        e.course_id,

        COUNT(*) AS total_enrollments,

        COUNT(DISTINCT e.student_id)
            AS distinct_students,

        COUNT(DISTINCT e.semester_id)
            AS semesters_offered,

        COUNT(*) FILTER (
            WHERE e.status = 'completed'
        ) AS completed_enrollments,

        COUNT(*) FILTER (
            WHERE e.status = 'active'
        ) AS active_enrollments,

        COUNT(*) FILTER (
            WHERE e.status = 'failed'
        ) AS failed_enrollments,

        COUNT(*) FILTER (
            WHERE e.status = 'dropped'
        ) AS dropped_enrollments,

        COUNT(*) FILTER (
            WHERE e.is_repeated_combination
        ) AS repeated_combination_enrollments,

        COUNT(*) FILTER (
            WHERE g.enrollment_id IS NOT NULL
        ) AS enrollments_with_grades,

        COUNT(*) FILTER (
            WHERE e.status IN ('completed', 'failed')
              AND g.weighted_average_observed IS NOT NULL
        ) AS enrollments_with_observed_average,

        ROUND(
            AVG(g.weighted_average_observed) FILTER (
                WHERE e.status IN ('completed', 'failed')
                  AND g.weighted_average_observed IS NOT NULL
            ),
            2
        ) AS average_weighted_observed_score,

        MIN(s.year) AS first_year_offered,
        MAX(s.year) AS last_year_offered,

        COUNT(*) FILTER (
            WHERE NOT s.is_temporally_valid
        ) AS temporally_invalid_semester_enrollments

    FROM silver.enrollments AS e

    LEFT JOIN grades_by_enrollment AS g
        ON e.enrollment_id = g.enrollment_id

    LEFT JOIN silver.semesters AS s
        ON e.semester_id = s.semester_id

    GROUP BY e.course_id
)

SELECT
    c.course_id,
    c.code AS course_code,
    c.name AS course_name,
    c.credits,
    c.department,

    c.professor_id,

    CONCAT_WS(
        ' ',
        p.first_name,
        p.last_name
    ) AS professor_name,

    p.department AS professor_department,

    COALESCE(
        m.total_enrollments,
        0
    ) AS total_enrollments,

    COALESCE(
        m.distinct_students,
        0
    ) AS distinct_students,

    COALESCE(
        m.semesters_offered,
        0
    ) AS semesters_offered,

    COALESCE(
        m.completed_enrollments,
        0
    ) AS completed_enrollments,

    COALESCE(
        m.active_enrollments,
        0
    ) AS active_enrollments,

    COALESCE(
        m.failed_enrollments,
        0
    ) AS failed_enrollments,

    COALESCE(
        m.dropped_enrollments,
        0
    ) AS dropped_enrollments,

    COALESCE(
        m.repeated_combination_enrollments,
        0
    ) AS repeated_combination_enrollments,

    COALESCE(
        m.enrollments_with_grades,
        0
    ) AS enrollments_with_grades,

    COALESCE(
        m.enrollments_with_observed_average,
        0
    ) AS enrollments_with_observed_average,

    m.average_weighted_observed_score,
    m.first_year_offered,
    m.last_year_offered,

    COALESCE(
        m.temporally_invalid_semester_enrollments,
        0
    ) AS temporally_invalid_semester_enrollments,

    CASE
        WHEN COALESCE(m.total_enrollments, 0) > 0
        THEN ROUND(
            (
                m.completed_enrollments::numeric
                / m.total_enrollments
            ) * 100,
            2
        )
        ELSE NULL
    END AS completion_rate_pct,

    CASE
        WHEN COALESCE(m.total_enrollments, 0) > 0
        THEN ROUND(
            (
                m.failed_enrollments::numeric
                / m.total_enrollments
            ) * 100,
            2
        )
        ELSE NULL
    END AS failure_rate_pct,

    CASE
        WHEN COALESCE(m.total_enrollments, 0) > 0
        THEN ROUND(
            (
                m.dropped_enrollments::numeric
                / m.total_enrollments
            ) * 100,
            2
        )
        ELSE NULL
    END AS dropout_rate_pct,

    CURRENT_TIMESTAMP AS gold_created_at

FROM silver.courses AS c

LEFT JOIN silver.professors AS p
    ON c.professor_id = p.professor_id

LEFT JOIN course_metrics AS m
    ON c.course_id = m.course_id;

ALTER TABLE gold.course_performance_summary
ADD CONSTRAINT pk_course_performance_summary
PRIMARY KEY (course_id);