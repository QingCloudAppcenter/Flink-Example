create table if not exists top_price(
    price Float64,
    supplier String,
    start DateTime,
    end   DateTime
) engine TinyLog;

select supplier, SUM(price) as total
from top_price
where start > '2022-01-26 10:53:00'
group by supplier
order by total desc;