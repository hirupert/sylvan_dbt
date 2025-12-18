{{ config(materialized='table') }}

with deal_base as (

    select
        deal_id,
        deal_name,
        pipeline_id,
        stage_id,
        booking_date,
        contract_start_date,
        contract_end_date,
        contract_term_months,
        free_trial_duration,
        free_trial_placement,
        payment_terms,
        billing_frequency,
        renewal_date,
        amount,
        mrr,
        arr,
        close_date,
        primary_associated_company,
        order_form_url,
        usage_data
    from {{ ref('stg_deal') }}
    where pipeline_id = '820348987'

),

deal_company_link as (
    select deal_id, company_id
    from (
        select
            deal_id,
            company_id,
            type_id,
            row_number() over (partition by deal_id order by type_id desc) as rn
        from {{ ref('stg_deal_company') }}
    )
    where rn = 1
),

company_dim as (
    select
        id as company_id,
        name as company_name,
        website as company_website,
        industry as company_industry,
        city as company_city,
        state as company_state,
        country as company_country,
        zip as company_zip,
        address as company_address,
        domain as company_domain,
        lifecycle_stage as company_lifecycle_stage,
        phone as company_phone,
        revenue_range as company_revenue_range
    from {{ ref('stg_company') }}
),

pipeline_dim as (
    select pipeline_id, label as pipeline_label
    from {{ ref('stg_deal_pipeline') }}
),

stage_dim as (
    select
        stage_id,
        label as stage_label,
        is_closed as is_closed_stage
    from {{ ref('stg_deal_pipeline_stage') }}
),

contact_agg as (
    select
        dc.deal_id,
        listagg(distinct c.id::string, ', ') within group (order by c.id::string) as contact_ids
    from {{ ref('stg_deal_contact') }} dc
    join {{ ref('stg_contact') }} c
        on dc.contact_id = c.id
    group by dc.deal_id
),

line_item_agg as (
    select
        ld.deal_id,
        sum(li.amount) as line_amount_total,
        sum(li.arr) as line_arr_total,
        sum(li.mrr) as line_mrr_total
    from {{ ref('stg_line_item_deal') }} ld
    join {{ ref('stg_line_item') }} li
        on ld.line_item_id = li.id
    group by ld.deal_id
),

deal_enriched as (
    select
        d.*,
        dp.pipeline_label,
        ds.stage_label,
        ds.is_closed_stage,
        coalesce(d.primary_associated_company, dcl.company_id) as company_id,
        c.company_name,
        ca.contact_ids,
        li.line_amount_total,
        li.line_arr_total,
        li.line_mrr_total
    from deal_base d
    left join deal_company_link dcl on d.deal_id = dcl.deal_id
    left join company_dim c on c.company_id = coalesce(d.primary_associated_company, dcl.company_id)
    left join contact_agg ca on ca.deal_id = d.deal_id
    left join pipeline_dim dp on dp.pipeline_id = d.pipeline_id
    left join stage_dim ds on ds.stage_id = d.stage_id
    left join line_item_agg li on li.deal_id = d.deal_id
),

deal_with_subscription as (
    select
        d.*,
        case
            when d.booking_date is null then null
            when coalesce(d.free_trial_duration,0) > 0
             and upper(coalesce(d.free_trial_placement,'')) = 'BEGINNING'
                then dateadd(month, d.free_trial_duration, d.booking_date)
            else d.booking_date
        end as subscription_start_date,

        case
            when d.booking_date is null
              or d.contract_term_months is null then null
            when coalesce(d.free_trial_duration,0) > 0
             and upper(coalesce(d.free_trial_placement,'')) = 'END'
                then dateadd(month, d.contract_term_months - d.free_trial_duration, d.booking_date)
            else dateadd(month, d.contract_term_months, d.booking_date)
        end as subscription_end_date
    from deal_enriched d
),

invoice_counts as (
    select
        associated_hubspot_deal_id as deal_id,
        count(case when paid_timestamp is not null then 1 end) as invoices_raised_so_far,
        count(case when sent_timestamp is not null and paid_timestamp is null then 1 end) as invoices_due
    from {{ source('airtable_sylvan_customer_management', 'INVOICES') }}
    group by associated_hubspot_deal_id
),

final as (
    select
        dws.*,
        case
            when upper(dws.billing_frequency) = 'ANNUAL' then 1
            when upper(dws.billing_frequency) = 'SEMI-ANNUAL' then 2
            when upper(dws.billing_frequency) = 'MONTHLY'
                then dws.contract_term_months - coalesce(dws.free_trial_duration,0)
            else 0
        end as invoices_to_raise,

        case
            when upper(dws.billing_frequency) in ('ANNUAL','SEMI-ANNUAL','MONTHLY')
                then dws.amount /
                     nullif(
                        case
                            when upper(dws.billing_frequency) = 'ANNUAL' then 1
                            when upper(dws.billing_frequency) = 'SEMI-ANNUAL' then 2
                            when upper(dws.billing_frequency) = 'MONTHLY'
                                then dws.contract_term_months - coalesce(dws.free_trial_duration,0)
                        end, 0)
            else null
        end as invoice_amount,

        coalesce(ic.invoices_raised_so_far,0) as invoices_raised_so_far,
        coalesce(ic.invoices_due,0) as invoices_due,

        case
            when coalesce(ic.invoices_raised_so_far,0) >=
                 case
                     when upper(dws.billing_frequency) = 'ANNUAL' then 1
                     when upper(dws.billing_frequency) = 'SEMI-ANNUAL' then 2
                     when upper(dws.billing_frequency) = 'MONTHLY'
                        then dws.contract_term_months - coalesce(dws.free_trial_duration,0)
                 end
                then null
            when upper(dws.billing_frequency) = 'ANNUAL'
                then dws.subscription_start_date
            when upper(dws.billing_frequency) = 'SEMI-ANNUAL'
                then dateadd(month, 6 * coalesce(ic.invoices_raised_so_far,0), dws.subscription_start_date)
            when upper(dws.billing_frequency) = 'MONTHLY'
                then dateadd(month, coalesce(ic.invoices_raised_so_far,0), dws.subscription_start_date)
        end as next_invoice_to_raise
    from deal_with_subscription dws
    left join invoice_counts ic on dws.deal_id = ic.deal_id
)

select * from final
