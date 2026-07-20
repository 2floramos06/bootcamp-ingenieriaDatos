CREATE SCHEMA IF NOT EXISTS gold;

--UNIVERSITY
--students
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