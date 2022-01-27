CREATE TABLE IF NOT EXISTS top_price(
    price Float64,
    supplier String,
    start DateTime,
    end   DateTime
) engine TinyLog;

SELECT supplier, SUM(price) AS total
FROM top_price
WHERE start > '2022-01-26 10:53:00'
GROUP BY supplier
ORDER BY total DESC;