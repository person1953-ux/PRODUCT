PDF-READY CONTENT (COPY EVERYTHING BELOW)
# MySQL Commands vs Power BI Measures (DAX)

## Purpose
This reference helps SQL users translate common MySQL queries into equivalent
Power BI DAX measures.  
**Key difference:** SQL returns tables; DAX measures return values based on context.

---

## Basic Aggregation

**MySQL**
```sql
SELECT SUM(sales) FROM orders;


Power BI (DAX Measure)

Total Sales :=
SUM ( Orders[Sales] )

Count Rows

MySQL

SELECT COUNT(*) FROM orders;


Power BI

Order Count :=
COUNTROWS ( Orders )

GROUP BY

MySQL

SELECT region, SUM(sales)
FROM orders
GROUP BY region;


Power BI

Total Sales :=
SUM ( Orders[Sales] )


Grouping is handled automatically by the visual’s fields.

WHERE Filter

MySQL

SELECT SUM(sales)
FROM orders
WHERE region = 'East';


Power BI

Sales East :=
CALCULATE (
    SUM ( Orders[Sales] ),
    Orders[Region] = "East"
)

Multiple Conditions

MySQL

SELECT SUM(sales)
FROM orders
WHERE region = 'East'
  AND year = 2025;


Power BI

Sales East 2025 :=
CALCULATE (
    SUM ( Orders[Sales] ),
    Orders[Region] = "East",
    Orders[Year] = 2025
)

CASE WHEN

MySQL

CASE
  WHEN sales > 1000 THEN 'High'
  ELSE 'Low'
END


Power BI

Sales Category :=
IF (
    Orders[Sales] > 1000,
    "High",
    "Low"
)

DISTINCT COUNT

MySQL

SELECT COUNT(DISTINCT customer_id)
FROM orders;


Power BI

Distinct Customers :=
DISTINCTCOUNT ( Orders[CustomerID] )

JOIN Tables

MySQL

SELECT c.name, SUM(o.sales)
FROM orders o
JOIN customers c ON o.cust_id = c.id
GROUP BY c.name;


Power BI

Total Sales :=
SUM ( Orders[Sales] )


Relationships replace JOINs.

HAVING

MySQL

SELECT region, SUM(sales)
FROM orders
GROUP BY region
HAVING SUM(sales) > 10000;


Power BI

High Sales Regions :=
CALCULATE (
    [Total Sales],
    FILTER (
        VALUES ( Orders[Region] ),
        [Total Sales] > 10000
    )
)

TOP N

MySQL

SELECT region, SUM(sales)
FROM orders
GROUP BY region
ORDER BY SUM(sales) DESC
LIMIT 5;


Power BI

Top 5 Regions :=
TOPN (
    5,
    VALUES ( Orders[Region] ),
    [Total Sales],
    DESC
)

Year-to-Date (YTD)

MySQL

SELECT SUM(sales)
FROM orders
WHERE order_date BETWEEN
  '2025-01-01' AND CURRENT_DATE();


Power BI

Sales YTD :=
TOTALYTD (
    [Total Sales],
    'Date'[Date]
)

Mental Model Summary

SQL: Select rows → GROUP BY → return table

DAX: Evaluate measure → filter context → return value

SQL shapes the data.
DAX interprets the data.


---

## 🧾 HOW TO SAVE THIS AS A PDF (FAST)

### 🔹 Option 1: Pandoc (Recommended)
```bash
pandoc mysql_to_powerbi.md -o mysql_to_powerbi.pdf

🔹 Option 2: VS Code

Paste content into mysql_to_powerbi.md

Install Markdown PDF

Right-click → Export as PDF

🔹 Option 3: Typora

Paste → File → Export → PDF

If you want next

I can:

✅ Optimize layout for one physical page

📘 Add an answer key / explanations

🧠 Align mappings to Bloom’s Taxonomy

🎓 Convert this into a training slide deck

Just tell me 👍