# Mobile App Analytics Data Model

## 1. Modelling Approach

The gold layer uses a dimensional star schema.

The model is based on three business processes:

1. Revenue
2. App Performance
3. Subscriptions

Each business process is represented by a fact table. Shared dimensions provide common business context such as source system, app, product, country, report type, and date.

## 2. Conceptual Model

Data originates from Apple App Store and Google Play. These source systems provide reports about mobile apps, including revenue, app performance, and subscription activity.

The main business entities are:

- Source System
- App
- Product / SKU / Subscription Plan
- Country / Region
- Time
- Revenue
- App Performance
- Subscription

## 3. Logical Model

### Dimension Tables

- dim_source_system
- dim_report_type
- dim_country
- dim_app
- dim_product
- dim_date

### Fact Tables

- fact_revenue
- fact_app_performance
- fact_subscriptions

## 4. Star Schema Design

The gold layer follows a star schema pattern.

The fact tables contain measurable business events or metrics. The dimension tables describe the business context used for filtering, grouping, and reporting.

## 5. Fact Table Grain

### fact_revenue

One row represents revenue for a specific source system, app, product, country, report type, and reporting period.

### fact_app_performance

One row represents app performance metrics for a specific source system, app, country, report type, and reporting period.

### fact_subscriptions

One row represents subscription metrics for a specific source system, app, subscription SKU, country, report type, and reporting period.

## 6. Gold Layer Tables Implemented

### Dimensions

- mobile_app_analytics_gold.dim_source_system
- mobile_app_analytics_gold.dim_report_type
- mobile_app_analytics_gold.dim_country
- mobile_app_analytics_gold.dim_app
- mobile_app_analytics_gold.dim_product
- mobile_app_analytics_gold.dim_date

### Facts

- mobile_app_analytics_gold.fact_revenue
- mobile_app_analytics_gold.fact_app_performance
- mobile_app_analytics_gold.fact_subscriptions

## 7. Gold Layer S3 Structure

```text
gold/dim_source_system/
gold/dim_report_type/
gold/dim_country/
gold/dim_app/
gold/dim_product/
gold/dim_date/

gold/fact_revenue/
gold/fact_app_performance/
gold/fact_subscriptions/