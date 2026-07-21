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

--BILLING
--customers, invoices: resumen financiero por cliente y moneda
DROP TABLE IF EXISTS gold.customer_billing_summary;

CREATE TABLE gold.customer_billing_summary AS
WITH invoice_items_by_invoice AS (
    SELECT
        invoice_id,

        COUNT(*) AS item_count,

        ROUND(
            SUM(line_total)::numeric,
            2
        ) AS items_reported_total,

        ROUND(
            SUM(calculated_line_total)::numeric,
            2
        ) AS items_calculated_total,

        BOOL_AND(is_quantity_valid)
            AS all_quantities_valid,

        BOOL_AND(is_unit_price_valid)
            AS all_unit_prices_valid,

        BOOL_AND(is_line_total_valid)
            AS all_line_totals_valid

    FROM silver.invoice_items

    GROUP BY invoice_id
),

payments_by_invoice AS (
    SELECT
        invoice_id,

        COUNT(*) AS payment_count,

        ROUND(
            SUM(amount)::numeric,
            2
        ) AS amount_paid,

        MIN(paid_at) AS first_payment_date,
        MAX(paid_at) AS last_payment_date,

        COUNT(DISTINCT method)
            AS payment_method_count

    FROM silver.payments

    WHERE is_amount_valid
      AND is_invoice_linked

    GROUP BY invoice_id
),

invoice_detail AS (
    SELECT
        i.invoice_id,
        i.customer_id,
        i.currency,
        i.status,
        i.issued_at,
        i.due_at,

        i.total AS invoice_total,

        COALESCE(
            ii.item_count,
            0
        ) AS item_count,

        ii.items_reported_total,
        ii.items_calculated_total,

        COALESCE(
            p.payment_count,
            0
        ) AS payment_count,

        COALESCE(
            p.amount_paid,
            0
        ) AS amount_paid,

        p.first_payment_date,
        p.last_payment_date,
        p.payment_method_count,

        CASE
            WHEN ii.invoice_id IS NULL
            THEN TRUE
            ELSE FALSE
        END AS is_without_items,

        CASE
            WHEN ii.invoice_id IS NULL
            THEN NULL

            WHEN ABS(
                i.total
                - ii.items_reported_total
            ) <= 0.01
            THEN TRUE

            ELSE FALSE
        END AS does_invoice_match_items,

        CASE
            WHEN ii.invoice_id IS NULL
            THEN NULL

            ELSE ROUND(
                (
                    i.total
                    - ii.items_reported_total
                )::numeric,
                2
            )
        END AS invoice_item_difference,

        ROUND(
            (
                i.total
                - COALESCE(p.amount_paid, 0)
            )::numeric,
            2
        ) AS net_balance_amount,

        ROUND(
            GREATEST(
                i.total
                - COALESCE(p.amount_paid, 0),
                0
        )::numeric,
        2
        ) AS outstanding_amount,

        ROUND(
            GREATEST(
                COALESCE(p.amount_paid, 0)
                - i.total,
                0
            )::numeric,
            2
        ) AS overpaid_amount,

        CASE
            WHEN COALESCE(p.amount_paid, 0)
                > i.total + 0.01
                THEN 'overpaid'

            WHEN ABS(
                COALESCE(p.amount_paid, 0)
                - i.total
            ) <= 0.01
            THEN 'exactly_paid'

            ELSE 'underpaid'        
        END AS payment_balance_status,

        CASE
            WHEN i.status = 'paid'
                AND COALESCE(p.amount_paid, 0)
                    < i.total - 0.01
            THEN TRUE
            ELSE FALSE
        END AS is_paid_status_underpaid,

        CASE
            WHEN i.status IN ('pending', 'overdue')
                AND COALESCE(p.amount_paid, 0)
                    >= i.total - 0.01
            THEN TRUE
            ELSE FALSE
        END AS is_open_status_fully_paid

    FROM silver.invoices AS i

    LEFT JOIN invoice_items_by_invoice AS ii
        ON i.invoice_id = ii.invoice_id

    LEFT JOIN payments_by_invoice AS p
        ON i.invoice_id = p.invoice_id
)

SELECT
    c.customer_id,
    c.external_ref,

    CONCAT_WS(
        ' ',
        c.first_name,
        c.last_name
    ) AS customer_name,

    c.email,
    c.country,
    c.segment,
    c.is_student_linked,

    d.currency,

    COUNT(*) AS invoice_count,

    COUNT(*) FILTER (
        WHERE d.status = 'paid'
    ) AS paid_invoice_count,

    COUNT(*) FILTER (
        WHERE d.status = 'pending'
    ) AS pending_invoice_count,

    COUNT(*) FILTER (
        WHERE d.status = 'overdue'
    ) AS overdue_invoice_count,

    ROUND(
        SUM(d.invoice_total)::numeric,
        2
    ) AS total_invoiced,

    ROUND(
        SUM(d.amount_paid)::numeric,
        2
    ) AS total_paid,


    ROUND(
        SUM(d.net_balance_amount)::numeric,
        2
    ) AS net_balance_amount,

    ROUND(
        SUM(d.overpaid_amount)::numeric,
        2
    ) AS overpaid_amount,

    COUNT(*) FILTER (
        WHERE d.payment_balance_status = 'overpaid'
    ) AS overpaid_invoice_count,

    COUNT(*) FILTER (
        WHERE d.payment_balance_status = 'exactly_paid'
    ) AS exactly_paid_invoice_count,

    COUNT(*) FILTER (
        WHERE d.payment_balance_status = 'underpaid'
    ) AS underpaid_invoice_count,

    COUNT(*) FILTER (
        WHERE d.is_paid_status_underpaid
    ) AS paid_status_but_underpaid_count,

    COUNT(*) FILTER (
        WHERE d.is_open_status_fully_paid
    ) AS open_status_but_fully_paid_count,

    ROUND(
        SUM(d.outstanding_amount)::numeric,
        2
    ) AS outstanding_amount,

    SUM(d.payment_count)
        AS payment_count,

    COUNT(*) FILTER (
        WHERE d.payment_count > 0
    ) AS invoices_with_payment,

    COUNT(*) FILTER (
        WHERE d.payment_count = 0
    ) AS invoices_without_payment,

    COUNT(*) FILTER (
        WHERE d.is_without_items
    ) AS invoices_without_items,

    COUNT(*) FILTER (
        WHERE d.does_invoice_match_items
    ) AS invoices_matching_item_total,

    COUNT(*) FILTER (
        WHERE d.does_invoice_match_items = FALSE
    ) AS invoices_not_matching_item_total,

    ROUND(
        SUM(
            ABS(d.invoice_item_difference)
        ) FILTER (
            WHERE d.invoice_item_difference
                  IS NOT NULL
        )::numeric,
        2
    ) AS total_absolute_item_difference,

    MIN(d.issued_at)
        AS first_invoice_date,

    MAX(d.issued_at)
        AS last_invoice_date,

    MAX(d.last_payment_date)
        AS last_payment_date,

    CURRENT_TIMESTAMP AS gold_created_at

FROM silver.customers AS c

INNER JOIN invoice_detail AS d
    ON c.customer_id = d.customer_id

GROUP BY
    c.customer_id,
    c.external_ref,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.segment,
    c.is_student_linked,
    d.currency;

ALTER TABLE gold.customer_billing_summary
ADD CONSTRAINT pk_customer_billing_summary
PRIMARY KEY (customer_id, currency);

--Customers, subscriptions: resumen de suscripciones por cliente
DROP TABLE IF EXISTS gold.customer_subscription_summary;

CREATE TABLE gold.customer_subscription_summary AS
WITH subscription_metrics AS (
    SELECT
        s.customer_id,

        COUNT(*) AS total_subscriptions,

        COUNT(*) FILTER (
            WHERE s.status = 'active'
        ) AS active_subscriptions,

        COUNT(*) FILTER (
            WHERE s.status = 'paused'
        ) AS paused_subscriptions,

        COUNT(*) FILTER (
            WHERE s.status = 'cancelled'
        ) AS cancelled_subscriptions,

        COUNT(DISTINCT s.product_id)
            AS distinct_products,

        COUNT(DISTINCT s.product_id) FILTER (
            WHERE s.status = 'active'
        ) AS active_distinct_products,

        COUNT(*) FILTER (
            WHERE NOT s.is_temporally_valid
        ) AS temporally_invalid_subscriptions,

        COUNT(*) FILTER (
            WHERE p.active
        ) AS subscriptions_to_active_products,

        COUNT(*) FILTER (
            WHERE NOT p.active
        ) AS subscriptions_to_inactive_products,

        COUNT(*) FILTER (
            WHERE s.status = 'active'
                AND NOT p.active
        ) AS active_subscriptions_to_inactive_products,

        MIN(s.start_date)
            AS first_subscription_date,

        MAX(s.start_date)
            AS latest_subscription_start_date,

        MAX(s.end_date)
            AS latest_subscription_end_date

    FROM silver.subscriptions AS s

    LEFT JOIN silver.products AS p
        ON s.product_id = p.product_id

    GROUP BY s.customer_id
)

SELECT
    c.customer_id,
    c.external_ref,

    CONCAT_WS(
        ' ',
        c.first_name,
        c.last_name
    ) AS customer_name,

    c.email,
    c.country,
    c.segment,
    c.is_student_linked,

    COALESCE(
        m.total_subscriptions,
        0
    ) AS total_subscriptions,

    COALESCE(
        m.active_subscriptions,
        0
    ) AS active_subscriptions,

    COALESCE(
        m.paused_subscriptions,
        0
    ) AS paused_subscriptions,

    COALESCE(
        m.cancelled_subscriptions,
        0
    ) AS cancelled_subscriptions,

    COALESCE(
        m.distinct_products,
        0
    ) AS distinct_products,

    COALESCE(
        m.active_distinct_products,
        0
    ) AS active_distinct_products,

    COALESCE(
        m.temporally_invalid_subscriptions,
        0
    ) AS temporally_invalid_subscriptions,

    COALESCE(
        m.subscriptions_to_active_products,
        0
    ) AS subscriptions_to_active_products,

    COALESCE(
        m.subscriptions_to_inactive_products,
        0
    ) AS subscriptions_to_inactive_products,

    COALESCE(
        m.active_subscriptions_to_inactive_products,
        0
    ) AS active_subscriptions_to_inactive_products,

    m.first_subscription_date,
    m.latest_subscription_start_date,
    m.latest_subscription_end_date,

    CURRENT_TIMESTAMP AS gold_created_at

FROM silver.customers AS c

LEFT JOIN subscription_metrics AS m
    ON c.customer_id = m.customer_id;

ALTER TABLE gold.customer_subscription_summary
ADD CONSTRAINT pk_customer_subscription_summary
PRIMARY KEY (customer_id);

--CRM
--account, contacts: resumen comercial por cuenta
-- ==========================================================
-- GOLD CRM
-- Resumen comercial por cuenta
-- Una fila por cuenta
-- ==========================================================

DROP TABLE IF EXISTS gold.account_crm_summary;

CREATE TABLE gold.account_crm_summary AS
WITH contact_metrics AS (
    SELECT
        account_id,

        COUNT(*) AS total_contacts,

        COUNT(*) FILTER (
            WHERE is_email_present
        ) AS contacts_with_email,

        COUNT(*) FILTER (
            WHERE NOT is_email_present
        ) AS contacts_without_email,

        MIN(created_at) AS first_contact_created_at,
        MAX(created_at) AS latest_contact_created_at

    FROM silver.contacts

    WHERE is_account_linked

    GROUP BY account_id
),

opportunity_metrics AS (
    SELECT
        account_id,

        COUNT(*) AS total_opportunities,

        COUNT(*) FILTER (
            WHERE stage = 'prospect'
        ) AS prospect_opportunities,

        COUNT(*) FILTER (
            WHERE stage = 'qualification'
        ) AS qualification_opportunities,

        COUNT(*) FILTER (
            WHERE stage = 'proposal'
        ) AS proposal_opportunities,

        COUNT(*) FILTER (
            WHERE stage = 'negotiation'
        ) AS negotiation_opportunities,

        COUNT(*) FILTER (
            WHERE stage = 'won'
        ) AS won_opportunities,

        COUNT(*) FILTER (
            WHERE stage = 'lost'
        ) AS lost_opportunities,

        COUNT(*) FILTER (
            WHERE stage NOT IN ('won', 'lost')
        ) AS open_opportunities,

        COUNT(*) FILTER (
            WHERE NOT is_temporally_valid
        ) AS temporally_invalid_opportunities,

        ROUND(
            SUM(amount) FILTER (
                WHERE is_amount_valid
            ),
            2
        ) AS total_crm_opportunity_amount,

        ROUND(
            SUM(amount) FILTER (
                WHERE stage = 'won'
                  AND is_amount_valid
            ),
            2
        ) AS won_crm_amount,

        ROUND(
            SUM(amount) FILTER (
                WHERE stage = 'lost'
                  AND is_amount_valid
            ),
            2
        ) AS lost_crm_amount,

        ROUND(
            SUM(amount) FILTER (
                WHERE stage NOT IN ('won', 'lost')
                  AND is_amount_valid
            ),
            2
        ) AS open_pipeline_crm_amount,

        ROUND(
            AVG(amount) FILTER (
                WHERE is_amount_valid
            ),
            2
        ) AS average_crm_opportunity_amount,

        MIN(created_at) AS first_opportunity_created_at,
        MAX(created_at) AS latest_opportunity_created_at,
        MIN(close_date) AS earliest_close_date,
        MAX(close_date) AS latest_close_date

    FROM silver.opportunities

    WHERE is_account_linked

    GROUP BY account_id
),

opportunity_contact_metrics AS (
    SELECT
        o.account_id,

        COUNT(*) AS opportunity_contact_relationships,

        COUNT(*) FILTER (
            WHERE oc.is_same_account
        ) AS same_account_relationships,

        COUNT(*) FILTER (
            WHERE NOT oc.is_same_account
        ) AS different_account_relationships,

        COUNT(DISTINCT oc.opportunity_id)
            AS opportunities_with_contacts,

        COUNT(DISTINCT oc.contact_id)
            AS distinct_contacts_in_opportunities,

        COUNT(*) FILTER (
            WHERE oc.role = 'decision_maker'
        ) AS decision_maker_relationships,

        COUNT(*) FILTER (
            WHERE oc.role = 'end_user'
        ) AS end_user_relationships,

        COUNT(*) FILTER (
            WHERE oc.role = 'financial'
        ) AS financial_relationships,

        COUNT(*) FILTER (
            WHERE oc.role = 'influencer'
        ) AS influencer_relationships,

        COUNT(*) FILTER (
            WHERE oc.role = 'technical'
        ) AS technical_relationships

    FROM silver.opportunity_contacts AS oc

    INNER JOIN silver.opportunities AS o
        ON oc.opportunity_id = o.opportunity_id

    WHERE oc.is_opportunity_linked
      AND oc.is_contact_linked

    GROUP BY o.account_id
),

activity_assignment AS (
    SELECT
        a.activity_id,
        a.activity_type,
        a.occurred_at,

        CASE
            WHEN a.has_opportunity_id
             AND a.is_opportunity_linked
                THEN o.account_id

            WHEN a.has_contact_id
             AND a.is_contact_linked
                THEN c.account_id

            ELSE NULL
        END AS assigned_account_id,

        CASE
            WHEN a.has_contact_id
             AND a.has_opportunity_id
                THEN 'contact_and_opportunity'

            WHEN a.has_contact_id
             AND NOT a.has_opportunity_id
                THEN 'contact_only'

            WHEN NOT a.has_contact_id
             AND a.has_opportunity_id
                THEN 'opportunity_only'

            ELSE 'no_reference'
        END AS reference_type,

        CASE
            WHEN a.has_contact_id
             AND a.has_opportunity_id
             AND a.is_contact_linked
             AND a.is_opportunity_linked
             AND c.account_id = o.account_id
                THEN TRUE

            ELSE FALSE
        END AS both_references_same_account,

        CASE
            WHEN a.has_contact_id
             AND a.has_opportunity_id
             AND a.is_contact_linked
             AND a.is_opportunity_linked
             AND c.account_id IS DISTINCT FROM o.account_id
                THEN TRUE

            ELSE FALSE
        END AS both_references_different_account

    FROM silver.activities AS a

    LEFT JOIN silver.contacts AS c
        ON a.contact_id = c.contact_id

    LEFT JOIN silver.opportunities AS o
        ON a.opportunity_id = o.opportunity_id
),

activity_metrics AS (
    SELECT
        assigned_account_id AS account_id,

        COUNT(*) AS total_activities,

        COUNT(*) FILTER (
            WHERE activity_type = 'call'
        ) AS call_activities,

        COUNT(*) FILTER (
            WHERE activity_type = 'demo'
        ) AS demo_activities,

        COUNT(*) FILTER (
            WHERE activity_type = 'email'
        ) AS email_activities,

        COUNT(*) FILTER (
            WHERE activity_type = 'meeting'
        ) AS meeting_activities,

        COUNT(*) FILTER (
            WHERE activity_type = 'note'
        ) AS note_activities,

        COUNT(*) FILTER (
            WHERE reference_type = 'contact_only'
        ) AS contact_only_activities,

        COUNT(*) FILTER (
            WHERE reference_type = 'opportunity_only'
        ) AS opportunity_only_activities,

        COUNT(*) FILTER (
            WHERE reference_type = 'contact_and_opportunity'
        ) AS contact_and_opportunity_activities,

        COUNT(*) FILTER (
            WHERE both_references_same_account
        ) AS activities_with_same_account_references,

        COUNT(*) FILTER (
            WHERE both_references_different_account
        ) AS activities_with_different_account_references,

        MIN(occurred_at) AS first_activity_at,
        MAX(occurred_at) AS latest_activity_at

    FROM activity_assignment

    WHERE assigned_account_id IS NOT NULL

    GROUP BY assigned_account_id
)

SELECT
    a.account_id,
    a.name AS account_name,
    a.industry,
    a.country,
    a.annual_revenue,
    a.employees,
    a.created_at AS account_created_at,

    a.is_annual_revenue_valid,
    a.is_employees_valid,
    a.is_created_at_valid,

    COALESCE(c.total_contacts, 0)
        AS total_contacts,

    COALESCE(c.contacts_with_email, 0)
        AS contacts_with_email,

    COALESCE(c.contacts_without_email, 0)
        AS contacts_without_email,

    c.first_contact_created_at,
    c.latest_contact_created_at,

    COALESCE(o.total_opportunities, 0)
        AS total_opportunities,

    COALESCE(o.prospect_opportunities, 0)
        AS prospect_opportunities,

    COALESCE(o.qualification_opportunities, 0)
        AS qualification_opportunities,

    COALESCE(o.proposal_opportunities, 0)
        AS proposal_opportunities,

    COALESCE(o.negotiation_opportunities, 0)
        AS negotiation_opportunities,

    COALESCE(o.won_opportunities, 0)
        AS won_opportunities,

    COALESCE(o.lost_opportunities, 0)
        AS lost_opportunities,

    COALESCE(o.open_opportunities, 0)
        AS open_opportunities,

    COALESCE(o.temporally_invalid_opportunities, 0)
        AS temporally_invalid_opportunities,

    COALESCE(o.total_crm_opportunity_amount, 0)
        AS total_crm_opportunity_amount,

    COALESCE(o.won_crm_amount, 0)
        AS won_crm_amount,

    COALESCE(o.lost_crm_amount, 0)
        AS lost_crm_amount,

    COALESCE(o.open_pipeline_crm_amount, 0)
        AS open_pipeline_crm_amount,

    o.average_crm_opportunity_amount,

    CASE
        WHEN COALESCE(
            o.won_opportunities
            + o.lost_opportunities,
            0
        ) > 0
        THEN ROUND(
            (
                o.won_opportunities::numeric
                / (
                    o.won_opportunities
                    + o.lost_opportunities
                )
            ) * 100,
            2
        )
        ELSE NULL
    END AS closed_opportunity_win_rate_pct,

    o.first_opportunity_created_at,
    o.latest_opportunity_created_at,
    o.earliest_close_date,
    o.latest_close_date,

    COALESCE(
        oc.opportunity_contact_relationships,
        0
    ) AS opportunity_contact_relationships,

    COALESCE(
        oc.same_account_relationships,
        0
    ) AS same_account_relationships,

    COALESCE(
        oc.different_account_relationships,
        0
    ) AS different_account_relationships,

    COALESCE(
        oc.opportunities_with_contacts,
        0
    ) AS opportunities_with_contacts,

    COALESCE(
        oc.distinct_contacts_in_opportunities,
        0
    ) AS distinct_contacts_in_opportunities,

    COALESCE(
        oc.decision_maker_relationships,
        0
    ) AS decision_maker_relationships,

    COALESCE(
        oc.end_user_relationships,
        0
    ) AS end_user_relationships,

    COALESCE(
        oc.financial_relationships,
        0
    ) AS financial_relationships,

    COALESCE(
        oc.influencer_relationships,
        0
    ) AS influencer_relationships,

    COALESCE(
        oc.technical_relationships,
        0
    ) AS technical_relationships,

    COALESCE(ac.total_activities, 0)
        AS total_activities,

    COALESCE(ac.call_activities, 0)
        AS call_activities,

    COALESCE(ac.demo_activities, 0)
        AS demo_activities,

    COALESCE(ac.email_activities, 0)
        AS email_activities,

    COALESCE(ac.meeting_activities, 0)
        AS meeting_activities,

    COALESCE(ac.note_activities, 0)
        AS note_activities,

    COALESCE(ac.contact_only_activities, 0)
        AS contact_only_activities,

    COALESCE(ac.opportunity_only_activities, 0)
        AS opportunity_only_activities,

    COALESCE(
        ac.contact_and_opportunity_activities,
        0
    ) AS contact_and_opportunity_activities,

    COALESCE(
        ac.activities_with_same_account_references,
        0
    ) AS activities_with_same_account_references,

    COALESCE(
        ac.activities_with_different_account_references,
        0
    ) AS activities_with_different_account_references,

    ac.first_activity_at,
    ac.latest_activity_at,

    CURRENT_TIMESTAMP AS gold_created_at

FROM silver.accounts AS a

LEFT JOIN contact_metrics AS c
    ON a.account_id = c.account_id

LEFT JOIN opportunity_metrics AS o
    ON a.account_id = o.account_id

LEFT JOIN opportunity_contact_metrics AS oc
    ON a.account_id = oc.account_id

LEFT JOIN activity_metrics AS ac
    ON a.account_id = ac.account_id;

ALTER TABLE gold.account_crm_summary
ADD CONSTRAINT pk_account_crm_summary
PRIMARY KEY (account_id);

--Leads: Distribucion de leads por fuente
DROP TABLE IF EXISTS gold.lead_funnel_summary;

CREATE TABLE gold.lead_funnel_summary AS
SELECT
    source,

    COUNT(*) AS total_leads,

    COUNT(*) FILTER (
        WHERE status = 'new'
    ) AS new_leads,

    COUNT(*) FILTER (
        WHERE status = 'contacted'
    ) AS contacted_leads,

    COUNT(*) FILTER (
        WHERE status = 'qualified'
    ) AS qualified_leads,

    COUNT(*) FILTER (
        WHERE status = 'converted'
    ) AS converted_leads,

    COUNT(*) FILTER (
        WHERE status = 'lost'
    ) AS lost_leads,

    COUNT(*) FILTER (
        WHERE is_email_present
    ) AS leads_with_email,

    COUNT(*) FILTER (
        WHERE NOT is_email_present
    ) AS leads_without_email,

    ROUND(
        AVG(score) FILTER (
            WHERE is_score_valid
        ),
        2
    ) AS average_lead_score,

    MIN(score) FILTER (
        WHERE is_score_valid
    ) AS minimum_lead_score,

    MAX(score) FILTER (
        WHERE is_score_valid
    ) AS maximum_lead_score,

    CASE
        WHEN COUNT(*) > 0
        THEN ROUND(
            (
                COUNT(*) FILTER (
                    WHERE status = 'converted'
                )
            )::numeric
            / COUNT(*) * 100,
            2
        )
        ELSE NULL
    END AS conversion_rate_pct,

    CASE
        WHEN COUNT(*) > 0
        THEN ROUND(
            (
                COUNT(*) FILTER (
                    WHERE status IN (
                        'qualified',
                        'converted'
                    )
                )
            )::numeric
            / COUNT(*) * 100,
            2
        )
        ELSE NULL
    END AS qualified_or_converted_rate_pct,

    MIN(created_at) AS first_lead_created_at,
    MAX(created_at) AS latest_lead_created_at,

    COUNT(*) FILTER (
        WHERE NOT is_source_valid
           OR NOT is_status_valid
           OR NOT is_score_valid
           OR NOT is_created_at_valid
    ) AS leads_with_quality_issue,

    CURRENT_TIMESTAMP AS gold_created_at

FROM silver.leads

GROUP BY source;

ALTER TABLE gold.lead_funnel_summary
ADD CONSTRAINT pk_lead_funnel_summary
PRIMARY KEY (source);