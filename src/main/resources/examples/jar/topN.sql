SELECT supplier, sum(price) AS total
FROM top_price
WHERE start > '2022-01-26 10:53:00'
GROUP BY supplier
ORDER BY total DESC;