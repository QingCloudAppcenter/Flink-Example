--创建表clickhouse_key_words
CREATE TABLE clickhouse_key_words(
    province String,
    sex      String,
    word     String,
    stt      DateTime,
    edt      DateTime,
    ct       Int64
)engine TinyLog;

--统计每省前3热点词汇
select *
from (select province, groupArray(3)(word) as keywords, groupArray(3)(total_count) as keywords_counts
      from (
            select word, province, sum(ct) as total_count
            from clickhouse_key_words
            where stt > '2022-01-21 00:00:00'
              and edt < '2022-01-21 23:59:59'
            group by word, province
            order by province, total_count desc)
      group by province
      order by province) ARRAY JOIN keywords, keywords_counts;

--统计女性热点词汇前10
select word, sum(ct) as total_count
from clickhouse_key_words
where sex = '女'
group by word
order by total_count desc
limit 10;

