-- Monthly net and gross revenue, including returns as negative net revenue.
SELECT date_trunc('month', sale_date)::date AS month, SUM(net_revenue) AS net_revenue,
       SUM(gross_revenue) AS gross_revenue, SUM(return_value) AS return_value
FROM daily_sales GROUP BY 1 ORDER BY 1;

-- Highest net revenue products.
SELECT stock_code, description, net_quantity, net_revenue
FROM product_sales ORDER BY net_revenue DESC LIMIT 20;

-- Country performance excluding the United Kingdom for an international view.
SELECT country, net_revenue, invoice_count, customer_count
FROM country_sales WHERE country <> 'United Kingdom' ORDER BY net_revenue DESC;

-- Daily revenue trend for one market.
SELECT sale_date, net_revenue, invoice_count
FROM daily_sales WHERE country = 'United Kingdom' ORDER BY sale_date;
