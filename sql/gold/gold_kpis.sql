BEGIN;
DROP TABLE IF EXISTS gold.billing_currency_kpis;

--Billing por moneda
CREATE TABLE gold.billing_currency_kpis AS
SELECT
    currency,

    COUNT(DISTINCT customer_id)
        AS customers,

    SUM(invoice_count)::bigint
        AS total_invoices,

    SUM(paid_invoice_count)::bigint
        AS paid_invoices,

    SUM(pending_invoice_count)::bigint
        AS pending_invoices,

    SUM(overdue_invoice_count)::bigint
        AS overdue_invoices,

    SUM(payment_count)::bigint
        AS total_payments,

    ROUND(
        SUM(total_invoiced)::numeric,
        2
    ) AS total_invoiced,

    ROUND(
        SUM(total_paid)::numeric,
        2
    ) AS total_paid,

    ROUND(
        SUM(net_balance_amount)::numeric,
        2
    ) AS net_balance_amount,

    ROUND(
        SUM(outstanding_amount)::numeric,
        2
    ) AS outstanding_amount,

    ROUND(
        SUM(overpaid_amount)::numeric,
        2
    ) AS overpaid_amount,

    SUM(overpaid_invoice_count)::bigint
        AS overpaid_invoices,

    SUM(exactly_paid_invoice_count)::bigint
        AS exactly_paid_invoices,

    SUM(underpaid_invoice_count)::bigint
        AS underpaid_invoices,

    SUM(paid_status_but_underpaid_count)::bigint
        AS paid_status_but_underpaid,

    SUM(open_status_but_fully_paid_count)::bigint
        AS open_status_but_fully_paid,

    SUM(invoices_without_items)::bigint
        AS invoices_without_items,

    SUM(invoices_matching_item_total)::bigint
        AS invoices_matching_item_total,

    SUM(invoices_not_matching_item_total)::bigint
        AS invoices_not_matching_item_total,

    CASE
        WHEN SUM(invoice_count) > 0
        THEN ROUND(
            (
                SUM(paid_invoice_count)::numeric
                / SUM(invoice_count)
            ) * 100,
            2
        )
        ELSE NULL
    END AS paid_invoice_rate_pct,

    CASE
        WHEN SUM(invoice_count) > 0
        THEN ROUND(
            (
                SUM(overdue_invoice_count)::numeric
                / SUM(invoice_count)
            ) * 100,
            2
        )
        ELSE NULL
    END AS overdue_invoice_rate_pct,

    CASE
        WHEN SUM(invoice_count) > 0
        THEN ROUND(
            (
                SUM(overpaid_invoice_count)::numeric
                / SUM(invoice_count)
            ) * 100,
            2
        )
        ELSE NULL
    END AS overpaid_invoice_rate_pct,

    CASE
        WHEN SUM(invoice_count) > 0
        THEN ROUND(
            (
                SUM(underpaid_invoice_count)::numeric
                / SUM(invoice_count)
            ) * 100,
            2
        )
        ELSE NULL
    END AS underpaid_invoice_rate_pct,

    CASE
        WHEN SUM(
            invoices_matching_item_total
            + invoices_not_matching_item_total
        ) > 0
        THEN ROUND(
            (
                SUM(invoices_not_matching_item_total)::numeric
                / SUM(
                    invoices_matching_item_total
                    + invoices_not_matching_item_total
                )
            ) * 100,
            2
        )
        ELSE NULL
    END AS invoice_item_mismatch_rate_pct,

    CURRENT_TIMESTAMP AS kpi_created_at

FROM gold.customer_billing_summary

GROUP BY currency;

ALTER TABLE gold.billing_currency_kpis
ADD CONSTRAINT pk_billing_currency_kpis
PRIMARY KEY (currency);

--Ejecutivos
DROP TABLE IF EXISTS gold.executive_kpis;

CREATE TABLE gold.executive_kpis AS
WITH university_students AS (
    SELECT
        COUNT(*) AS total_students,

        SUM(total_enrollments)::bigint
            AS total_enrollments,

        SUM(completed_enrollments)::bigint
            AS completed_enrollments,

        SUM(active_enrollments)::bigint
            AS active_enrollments,

        SUM(failed_enrollments)::bigint
            AS failed_enrollments,

        SUM(dropped_enrollments)::bigint
            AS dropped_enrollments,

        SUM(enrollments_with_grades)::bigint
            AS enrollments_with_grades,

        COUNT(*) FILTER (
            WHERE average_weighted_observed_score
                  IS NOT NULL
        ) AS students_with_observed_average,

        ROUND(
            AVG(average_weighted_observed_score)
            FILTER (
                WHERE average_weighted_observed_score
                      IS NOT NULL
            ),
            2
        ) AS average_student_observed_score

    FROM gold.student_academic_summary
),

university_courses AS (
    SELECT
        COUNT(*) AS total_courses
    FROM gold.course_performance_summary
),

billing AS (
    SELECT
        COUNT(DISTINCT customer_id)
            AS customers_with_invoices,

        COUNT(DISTINCT currency)
            AS billing_currencies,

        SUM(invoice_count)::bigint
            AS total_invoices,

        SUM(paid_invoice_count)::bigint
            AS paid_invoices,

        SUM(pending_invoice_count)::bigint
            AS pending_invoices,

        SUM(overdue_invoice_count)::bigint
            AS overdue_invoices,

        SUM(payment_count)::bigint
            AS total_payments,

        SUM(invoices_without_items)::bigint
            AS invoices_without_items,

        SUM(invoices_not_matching_item_total)::bigint
            AS invoices_not_matching_item_total,

        SUM(overpaid_invoice_count)::bigint
            AS overpaid_invoices,

        SUM(exactly_paid_invoice_count)::bigint
            AS exactly_paid_invoices,

        SUM(underpaid_invoice_count)::bigint
            AS underpaid_invoices,

        SUM(paid_status_but_underpaid_count)::bigint
            AS paid_status_but_underpaid

    FROM gold.customer_billing_summary
),

subscriptions AS (
    SELECT
        COUNT(*) AS total_customers,

        COUNT(*) FILTER (
            WHERE total_subscriptions = 0
        ) AS customers_without_subscriptions,

        SUM(total_subscriptions)::bigint
            AS total_subscriptions,

        SUM(active_subscriptions)::bigint
            AS active_subscriptions,

        SUM(paused_subscriptions)::bigint
            AS paused_subscriptions,

        SUM(cancelled_subscriptions)::bigint
            AS cancelled_subscriptions,

        SUM(
            temporally_invalid_subscriptions
        )::bigint
            AS temporally_invalid_subscriptions,

        SUM(
            active_subscriptions_to_inactive_products
        )::bigint
            AS active_subscriptions_on_inactive_products

    FROM gold.customer_subscription_summary
),

crm_accounts AS (
    SELECT
        COUNT(*) AS total_accounts,

        SUM(total_contacts)::bigint
            AS total_contacts,

        SUM(total_opportunities)::bigint
            AS total_opportunities,

        SUM(won_opportunities)::bigint
            AS won_opportunities,

        SUM(lost_opportunities)::bigint
            AS lost_opportunities,

        SUM(open_opportunities)::bigint
            AS open_opportunities,

        SUM(
            temporally_invalid_opportunities
        )::bigint
            AS temporally_invalid_opportunities,

        SUM(opportunity_contact_relationships)::bigint
            AS opportunity_contact_relationships,

        SUM(same_account_relationships)::bigint
            AS same_account_relationships,

        SUM(different_account_relationships)::bigint
            AS different_account_relationships,

        SUM(total_activities)::bigint
            AS assigned_activities,

        ROUND(
            SUM(total_crm_opportunity_amount)::numeric,
            2
        ) AS total_crm_opportunity_amount,

        ROUND(
            SUM(won_crm_amount)::numeric,
            2
        ) AS won_crm_amount,

        ROUND(
            SUM(open_pipeline_crm_amount)::numeric,
            2
        ) AS open_pipeline_crm_amount

    FROM gold.account_crm_summary
),

leads AS (
    SELECT
        COUNT(*) AS lead_sources,

        SUM(total_leads)::bigint
            AS total_leads,

        SUM(converted_leads)::bigint
            AS converted_leads,

        SUM(qualified_leads)::bigint
            AS qualified_leads,

        SUM(lost_leads)::bigint
            AS lost_leads,

        SUM(leads_with_quality_issue)::bigint
            AS leads_with_quality_issue

    FROM gold.lead_funnel_summary
),

activities AS (
    SELECT
        COUNT(*) AS total_activities,

        COUNT(*) FILTER (
            WHERE NOT has_any_reference
        ) AS unassigned_activities

    FROM silver.activities
)

SELECT
    'current'::text AS snapshot_id,

    -- UNIVERSITY
    us.total_students,
    uc.total_courses,
    us.total_enrollments,
    us.completed_enrollments,
    us.active_enrollments,
    us.failed_enrollments,
    us.dropped_enrollments,
    us.enrollments_with_grades,
    us.students_with_observed_average,
    us.average_student_observed_score,

    CASE
        WHEN us.total_enrollments > 0
        THEN ROUND(
            (
                us.completed_enrollments::numeric
                / us.total_enrollments
            ) * 100,
            2
        )
        ELSE NULL
    END AS enrollment_completion_rate_pct,

    CASE
        WHEN us.total_enrollments > 0
        THEN ROUND(
            (
                us.failed_enrollments::numeric
                / us.total_enrollments
            ) * 100,
            2
        )
        ELSE NULL
    END AS enrollment_failure_rate_pct,

    CASE
        WHEN us.total_enrollments > 0
        THEN ROUND(
            (
                us.dropped_enrollments::numeric
                / us.total_enrollments
            ) * 100,
            2
        )
        ELSE NULL
    END AS enrollment_dropout_rate_pct,

    -- BILLING
    s.total_customers,

    b.customers_with_invoices,

    (
        s.total_customers
        - b.customers_with_invoices
    ) AS customers_without_invoices,

    b.billing_currencies,
    b.total_invoices,
    b.paid_invoices,
    b.pending_invoices,
    b.overdue_invoices,
    b.total_payments,
    b.invoices_without_items,
    b.invoices_not_matching_item_total,
    b.overpaid_invoices,
    b.exactly_paid_invoices,
    b.underpaid_invoices,
    b.paid_status_but_underpaid,

    CASE
        WHEN b.total_invoices > 0
        THEN ROUND(
            (
                b.paid_invoices::numeric
                / b.total_invoices
            ) * 100,
            2
        )
        ELSE NULL
    END AS paid_invoice_status_rate_pct,

    CASE
        WHEN b.paid_invoices > 0
        THEN ROUND(
            (
                b.paid_status_but_underpaid::numeric
                / b.paid_invoices
            ) * 100,
            2
        )
        ELSE NULL
    END AS paid_status_underpaid_rate_pct,

    s.total_subscriptions,
    s.active_subscriptions,
    s.paused_subscriptions,
    s.cancelled_subscriptions,
    s.temporally_invalid_subscriptions,
    s.active_subscriptions_on_inactive_products,
    s.customers_without_subscriptions,

    CASE
        WHEN s.total_subscriptions > 0
        THEN ROUND(
            (
                s.active_subscriptions::numeric
                / s.total_subscriptions
            ) * 100,
            2
        )
        ELSE NULL
    END AS active_subscription_rate_pct,

    -- CRM
    crm.total_accounts,
    crm.total_contacts,
    crm.total_opportunities,
    crm.won_opportunities,
    crm.lost_opportunities,
    crm.open_opportunities,
    crm.temporally_invalid_opportunities,
    crm.opportunity_contact_relationships,
    crm.same_account_relationships,
    crm.different_account_relationships,

    crm.total_crm_opportunity_amount,
    crm.won_crm_amount,
    crm.open_pipeline_crm_amount,

    CASE
        WHEN (
            crm.won_opportunities
            + crm.lost_opportunities
        ) > 0
        THEN ROUND(
            (
                crm.won_opportunities::numeric
                / (
                    crm.won_opportunities
                    + crm.lost_opportunities
                )
            ) * 100,
            2
        )
        ELSE NULL
    END AS closed_opportunity_win_rate_pct,

    l.lead_sources,
    l.total_leads,
    l.qualified_leads,
    l.converted_leads,
    l.lost_leads,
    l.leads_with_quality_issue,

    CASE
        WHEN l.total_leads > 0
        THEN ROUND(
            (
                l.converted_leads::numeric
                / l.total_leads
            ) * 100,
            2
        )
        ELSE NULL
    END AS lead_conversion_rate_pct,

    a.total_activities,
    crm.assigned_activities,
    a.unassigned_activities,

    CURRENT_TIMESTAMP AS kpi_created_at

FROM university_students AS us
CROSS JOIN university_courses AS uc
CROSS JOIN billing AS b
CROSS JOIN subscriptions AS s
CROSS JOIN crm_accounts AS crm
CROSS JOIN leads AS l
CROSS JOIN activities AS a;

ALTER TABLE gold.executive_kpis
ADD CONSTRAINT pk_executive_kpis
PRIMARY KEY (snapshot_id);

COMMIT;