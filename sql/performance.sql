-- Refresh planner statistics before inspecting plans after a pipeline load.
ANALYZE retail_line_items;
ANALYZE daily_sales;
ANALYZE product_sales;
ANALYZE country_sales;

-- The sale_date,country index should be considered for this selective range.
EXPLAIN (ANALYZE, BUFFERS)
SELECT sale_date, net_revenue, invoice_count
FROM daily_sales
WHERE country = 'United Kingdom'
  AND sale_date >= DATE '2011-10-01'
  AND sale_date < DATE '2011-11-01'
ORDER BY sale_date;

-- Inspect the product dimension primary key/index and actual row estimate.
EXPLAIN (ANALYZE, BUFFERS)
SELECT stock_code, description, net_quantity, net_revenue
FROM product_sales
WHERE stock_code = '85123A';

-- Inspect the underlying fact lookup used when refreshing daily groups.
EXPLAIN (ANALYZE, BUFFERS)
SELECT sale_date, country, SUM(line_amount)
FROM retail_line_items
WHERE sale_date >= DATE '2011-10-01'
  AND sale_date < DATE '2011-11-01'
GROUP BY sale_date, country;

-- Capture this output before and after any index or query change. Index use on
-- tiny tables is planner-dependent and a sequential scan can be optimal.
