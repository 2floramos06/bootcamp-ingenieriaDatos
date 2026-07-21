BEGIN;

--Vista total de estudiante + cliente
DROP VIEW IF EXISTS gold.vw_student_customer_360;

CREATE VIEW gold.vw_student_customer_360 AS
WITH billing_counts_by_customer AS (
    SELECT
        customer_id,

        COUNT(DISTINCT currency)
            AS billing_currency_count,

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

        SUM(overpaid_invoice_count)::bigint
            AS overpaid_invoices,

        SUM(underpaid_invoice_count)::bigint
            AS underpaid_invoices,

        SUM(paid_status_but_underpaid_count)::bigint
            AS paid_status_but_underpaid,

        SUM(invoices_without_items)::bigint
            AS invoices_without_items,

        SUM(invoices_not_matching_item_total)::bigint
            AS invoices_not_matching_item_total

    FROM gold.customer_billing_summary

    GROUP BY customer_id
)

SELECT
    s.student_id,
    s.student_name,
    s.email AS student_email,
    s.country AS student_country,
    s.enrolled_at AS university_enrolled_at,

    s.total_enrollments,
    s.completed_enrollments,
    s.active_enrollments,
    s.failed_enrollments,
    s.dropped_enrollments,
    s.enrollments_with_grades,
    s.average_weighted_observed_score,

    cs.customer_id,
    cs.customer_name,
    cs.email AS customer_email,
    cs.country AS customer_country,
    cs.segment,
    cs.is_student_linked,

    cs.total_subscriptions,
    cs.active_subscriptions,
    cs.paused_subscriptions,
    cs.cancelled_subscriptions,
    cs.temporally_invalid_subscriptions,
    cs.active_subscriptions_to_inactive_products,

    COALESCE(b.billing_currency_count, 0)
        AS billing_currency_count,

    COALESCE(b.total_invoices, 0)
        AS total_invoices,

    COALESCE(b.paid_invoices, 0)
        AS paid_invoices,

    COALESCE(b.pending_invoices, 0)
        AS pending_invoices,

    COALESCE(b.overdue_invoices, 0)
        AS overdue_invoices,

    COALESCE(b.total_payments, 0)
        AS total_payments,

    COALESCE(b.overpaid_invoices, 0)
        AS overpaid_invoices,

    COALESCE(b.underpaid_invoices, 0)
        AS underpaid_invoices,

    COALESCE(b.paid_status_but_underpaid, 0)
        AS paid_status_but_underpaid,

    COALESCE(b.invoices_without_items, 0)
        AS invoices_without_items,

    COALESCE(b.invoices_not_matching_item_total, 0)
        AS invoices_not_matching_item_total,

    (
        s.failed_enrollments > 0
    ) AS has_failed_enrollment,

    (
        s.dropped_enrollments > 0
    ) AS has_dropped_enrollment,

    (
        cs.active_subscriptions > 0
    ) AS has_active_subscription,

    (
        COALESCE(b.total_invoices, 0) > 0
    ) AS has_invoices,

    (
        COALESCE(b.overpaid_invoices, 0) > 0
        OR COALESCE(b.underpaid_invoices, 0) > 0
        OR COALESCE(
            b.paid_status_but_underpaid,
            0
        ) > 0
    ) AS has_billing_reconciliation_issue

FROM gold.student_academic_summary AS s

LEFT JOIN gold.customer_subscription_summary AS cs
    ON s.student_id = cs.external_ref

LEFT JOIN billing_counts_by_customer AS b
    ON cs.customer_id = b.customer_id;

--Vista de rendimiento por curso
DROP VIEW IF EXISTS gold.vw_course_performance_dashboard;

CREATE VIEW gold.vw_course_performance_dashboard AS
SELECT
    course_id,
    course_code,
    course_name,
    credits,
    department,
    professor_id,
    professor_name,
    professor_department,

    total_enrollments,
    distinct_students,
    semesters_offered,

    completed_enrollments,
    active_enrollments,
    failed_enrollments,
    dropped_enrollments,

    repeated_combination_enrollments,
    enrollments_with_grades,
    enrollments_with_observed_average,

    average_weighted_observed_score,

    first_year_offered,
    last_year_offered,

    completion_rate_pct,
    failure_rate_pct,
    dropout_rate_pct,

    temporally_invalid_semester_enrollments,

    (
        failed_enrollments > 0
    ) AS has_failed_enrollments,

    (
        dropped_enrollments > 0
    ) AS has_dropped_enrollments,

    (
        temporally_invalid_semester_enrollments > 0
    ) AS has_temporal_issue

FROM gold.course_performance_summary;

--Vista cliente + moneda
DROP VIEW IF EXISTS gold.vw_billing_customer_dashboard;

CREATE VIEW gold.vw_billing_customer_dashboard AS
SELECT
    customer_id,
    external_ref,
    customer_name,
    email,
    country,
    segment,
    is_student_linked,
    currency,

    invoice_count,
    paid_invoice_count,
    pending_invoice_count,
    overdue_invoice_count,

    total_invoiced,
    total_paid,
    net_balance_amount,
    outstanding_amount,
    overpaid_amount,

    payment_count,
    invoices_with_payment,
    invoices_without_payment,

    overpaid_invoice_count,
    exactly_paid_invoice_count,
    underpaid_invoice_count,
    paid_status_but_underpaid_count,
    open_status_but_fully_paid_count,

    invoices_without_items,
    invoices_matching_item_total,
    invoices_not_matching_item_total,
    total_absolute_item_difference,

    first_invoice_date,
    last_invoice_date,
    last_payment_date,

    CASE
        WHEN total_invoiced > 0
        THEN ROUND(
            (
                total_paid
                / total_invoiced
            ) * 100,
            2
        )
        ELSE NULL
    END AS payment_coverage_pct,

    (
        overpaid_invoice_count > 0
        OR underpaid_invoice_count > 0
        OR paid_status_but_underpaid_count > 0
    ) AS has_payment_reconciliation_issue,

    (
        invoices_without_items > 0
        OR invoices_not_matching_item_total > 0
    ) AS has_invoice_item_issue

FROM gold.customer_billing_summary;

--Vista comercial por cuenta
DROP VIEW IF EXISTS gold.vw_crm_account_dashboard;

CREATE VIEW gold.vw_crm_account_dashboard AS
SELECT
    account_id,
    account_name,
    industry,
    country,
    annual_revenue,
    employees,
    account_created_at,

    total_contacts,
    contacts_with_email,
    contacts_without_email,

    total_opportunities,
    prospect_opportunities,
    qualification_opportunities,
    proposal_opportunities,
    negotiation_opportunities,
    won_opportunities,
    lost_opportunities,
    open_opportunities,

    total_crm_opportunity_amount,
    won_crm_amount,
    lost_crm_amount,
    open_pipeline_crm_amount,
    average_crm_opportunity_amount,

    closed_opportunity_win_rate_pct,

    temporally_invalid_opportunities,

    opportunity_contact_relationships,
    same_account_relationships,
    different_account_relationships,
    opportunities_with_contacts,
    distinct_contacts_in_opportunities,

    decision_maker_relationships,
    end_user_relationships,
    financial_relationships,
    influencer_relationships,
    technical_relationships,

    total_activities,
    call_activities,
    demo_activities,
    email_activities,
    meeting_activities,
    note_activities,

    first_activity_at,
    latest_activity_at,

    (
        open_opportunities > 0
    ) AS has_open_pipeline,

    (
        temporally_invalid_opportunities > 0
    ) AS has_temporally_invalid_opportunity,

    (
        different_account_relationships > 0
    ) AS has_cross_account_contact_relationship

FROM gold.account_crm_summary;

--Vista de leads
DROP VIEW IF EXISTS gold.vw_lead_source_dashboard;

CREATE VIEW gold.vw_lead_source_dashboard AS
SELECT
    source,

    total_leads,
    new_leads,
    contacted_leads,
    qualified_leads,
    converted_leads,
    lost_leads,

    leads_with_email,
    leads_without_email,

    average_lead_score,
    minimum_lead_score,
    maximum_lead_score,

    conversion_rate_pct,
    qualified_or_converted_rate_pct,

    first_lead_created_at,
    latest_lead_created_at,

    leads_with_quality_issue,

    (
        leads_with_quality_issue > 0
    ) AS has_quality_issue

FROM gold.lead_funnel_summary;

COMMIT;