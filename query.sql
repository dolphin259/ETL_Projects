SELECT 
    d.month,
    SUM(f.total_amount) AS total_revenue
FROM 
    fact_orders f
JOIN 
    dim_dates d ON f.order_date_key = d.date_key
GROUP BY 
    d.month
ORDER BY 
    d.month;
SELECT 
    f.order_status,
    COUNT(f.order_id) AS order_count
FROM 
    fact_orders f
GROUP BY 
    f.order_status
ORDER BY 
    order_count DESC;
SELECT 
    p.product_category_name,
    count(f.order_id) AS total_sold
FROM 
    fact_orders f
JOIN 
    dim_products p ON f.product_key = p.product_id 
GROUP BY 
    p.product_category_name
ORDER BY 
    total_sold DESC;

-- doanh thu theo khu vuc
SELECT 
    g.geolocation_city,
    SUM(f.total_amount) AS total_revenue
FROM 
    fact_orders f
JOIN 
    dim_geolocation g ON f.geolocation_key = g.geolocation_key
GROUP BY 
    g.geolocation_city
ORDER BY 
    total_revenue DESC;