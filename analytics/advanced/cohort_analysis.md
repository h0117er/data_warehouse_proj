# Cohort analysis and Retention

<img src = 'docs/cohort_analysis.png' >

```sql
WITH first_purchase AS (
    SELECT
            customer_key, 
            DATE_TRUNC('month',MIN(order_date)) as cohort_month
      FROM PORTFOLIO.DEV_GOLD.FACT_SALES
     GROUP BY 1 )

, activity AS (
    SELECT 
            t1.customer_key,
            t2.cohort_month,
            date_trunc('month', t1.order_date) as order_month,
            datediff('month', cohort_month, order_month) as month_index,
            t1.sales_amount
      FROM PORTFOLIO.DEV_GOLD.FACT_SALES t1
      LEFT JOIN first_purchase t2 ON t1.customer_key = t2.customer_key )

SELECT 
        cohort_month,
        month_index,
        COUNT(DISTINCT customer_key) as active_user, 
        SUM(sales_amount) as total_rev,
        total_rev / active_user as rev_per_user
FROM activity
GROUP BY 1,2
ORDER BY 1,2;
```
