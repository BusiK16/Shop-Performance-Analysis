# Shop Performance Case Study — How Is the Shop Performing?

## 1. Project Overview

This project investigates the performance of an online shop that sells electronics, accessories, wearables, home office items, stationery and gaming products.

The business case asks for a clear picture of shop performance from **January 2024 to June 2026**, using simple metrics, clear visualisations and practical recommendations for the Head of Operations.

The project follows the BrightLearn Beginner Data Analysis case study and uses four related raw datasets:

- `orders` — order-line information
- `customers` — customer information
- `payments` — payment attempts and payment status
- `products` — product information and unit prices

The case study requires the datasets to be inspected, cleaned, combined and analysed before presenting the findings in a dashboard or short presentation.

## 2. Business Objective

The main objective is to answer:

> **How is the shop performing, and what actions should the Head of Operations take based on the data?**

The analysis focuses on revenue, order volume, trends, products, categories, cities, customer segments, cancellations/returns, payment failures and the relationship between discounts and order performance.

## 3. Business Questions

The analysis is designed to answer the following questions from the case study:

1. How much revenue did the shop make, from how many orders?
2. What is the average order value?
3. Is revenue growing, shrinking or remaining relatively flat month by month?
4. Are there seasonal peaks?
5. Which products and categories generate the most revenue?
6. Which products and categories sell the most units?
7. Which cities and customer segments are most valuable?
8. What share of orders are cancelled or returned?
9. What share of payments fail?
10. Do some payment methods fail more often than others?
11. Do bigger discounts lead to bigger orders, or do they mainly reduce revenue?

## 4. Data Model

The raw tables are connected through the following keys:

```text
customers
    │
    │ CustomerID
    ▼
 orders ────────────── ProductID ──────────────► products
    │
    │ OrderID
    ▼
 payments
```

The case study specifies that:

- `orders` links to `customers` through `CustomerID`
- `orders` links to `products` through `ProductID`
- `orders` links to `payments` through `OrderID`

The main analytical table is therefore based on the order-level data enriched with customer, product and payment information.

## 5. Data Preparation

The case study requires the following preparation steps:

### Data understanding
- Inspect the number of rows and columns.
- Understand what one row represents.
- Identify numeric, text and date fields.
- Identify what business question each table can answer.

### Data cleaning
- Check and handle missing values.
- Check for duplicate orders/rows.
- Check for values that do not make business sense.
- Standardise inconsistent text values.
- Check that customer and product relationships are valid.

### Data integration
The order data is combined with:

- Product information such as product name, category and unit price.
- Customer information such as city and customer segment.
- Payment information such as payment status.

### Derived fields

The case study specifies the revenue calculation:

**Revenue = Quantity × UnitPrice × (1 − Discount)**

Additional analytical fields include:

- `Year`
- `Month`
- `Revenue`

The treatment of cancelled, returned and unpaid orders should be explicitly stated in the final analysis so that the revenue KPI is traceable and reproducible.

## 6. Current Cleaned Dataset

The supplied cleaned Excel table currently contains:

| Metric | Current result |
|---|---:|
| Rows | 49,885 |
| Columns | 17 |
| Date range | 1 Jan 2024 – 30 Jun 2026 |
| Products | 20 |
| Cities | 11 |
| Product categories | 6 |
| Customer segments | New, Regular, VIP, Unknown |
| Duplicate rows | 0 |
| Missing values in final table | 0 |

The final analytical table contains the following columns:

`OrderID`, `CustomerID`, `OrderDate`, `ProductID`, `Quantity`, `Discount`, `PaymentMethod`, `Status`, `ProductName`, `Category`, `UnitPrice`, `City`, `CustomerSegment`, `PaymentStatus`, `Revenue`, `Year`, `Month`

The current cleaned table also contains standardised `Unknown` values for a small number of records in fields such as payment method and customer segment. These should be explained as part of the cleaning decisions.

## 7. Key Data Quality Snapshot

The current cleaned table contains:

- **45,891 Completed** orders
- **2,464 Cancelled** orders
- **1,530 Returned** orders
- **46,455 Paid** payment records
- **1,924 Failed** payment records
- **1,506 Refunded** payment records

Payment methods represented include Gateway, CardToCard, Wallet, Cash and Unknown.

Product categories represented include Accessories, Electronics, Stationery, Home Office, Wearables and Gaming.

## 8. Tools Used

The project uses:

- **Python / Pandas** for data cleaning, exploration and analysis.
- **Excel** for pivot tables, charts, KPIs and dashboard presentation.
- **Databricks** as the notebook environment for the analytical workflow.
- **Canva** for project planning and workflow visualisation.
- **GitHub** can be used to version-control the notebook, README and project files.

## 9. Databricks Notebook

The main analysis notebook is available here:

[Open the Databricks notebook](https://dbc-d91ada9f-3156.cloud.databricks.com/editor/notebooks/1548068512847615?o=7474659081296765)

## 10. Analysis Workflow

The project follows this workflow:

1. **Understand the business problem**
2. **Inspect the four raw datasets**
3. **Profile data quality**
4. **Clean the raw data**
5. **Join the four datasets**
6. **Create calculated fields**
7. **Validate the cleaned analytical table**
8. **Perform exploratory analysis**
9. **Answer the business questions**
10. **Create KPIs and visualisations**
11. **Identify the three most important findings**
12. **Develop three practical recommendations**
13. **Build the final dashboard/presentation**
14. **Document data-quality decisions**

## 11. Recommended Dashboard / Presentation Structure

A simple final presentation can follow this structure:

### Page/Slide 1 — Executive Summary
- Total revenue
- Total orders
- Average order value
- Three most important findings

### Page/Slide 2 — Revenue & Order Performance
- Monthly revenue trend
- Monthly order trend
- Growth/decline interpretation

### Page/Slide 3 — Product & Category Performance
- Top products by revenue
- Top products by units sold
- Category comparison

### Page/Slide 4 — Customer & City Performance
- Revenue by city
- Revenue by customer segment
- Identify the most valuable markets/customers

### Page/Slide 5 — Operations & Payments
- Cancelled and returned order share
- Payment failure rate
- Payment method comparison

### Page/Slide 6 — Discount Analysis
- Discount ranges
- Order quantity/order size
- Revenue by discount range
- Conclusion on whether larger discounts are beneficial

### Page/Slide 7 — Recommendations
Three practical actions directly supported by the analysis.

## 12. Important Analytical Principle

Every calculation and visual should answer a business question.

The final story should be written for the **Head of Operations**, not for another data analyst. Each chart should have a clear title and a short “So what?” explanation.

The final recommendations should be:

- Specific
- Actionable
- Supported by the analysis
- Relevant to shop operations
- Traceable to the cleaned working dataset

## 13. Project Deliverables

The final project should contain:

- Cleaned analytical dataset
- Cleaning decisions / data-quality notes
- Databricks notebook
- Excel dashboard or presentation
- Three key findings
- Three recommendations
- README documentation
- Project plan / Gantt chart

## 14. Data Traceability

Every number presented in the final dashboard or presentation should be traceable back to the cleaned working table and the calculations used in the notebook.

