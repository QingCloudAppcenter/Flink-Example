```shell
mvn clean package
```

# kafka 写入假数据
```shell
flink run -d -p 1 -c examples.app.MockData topN-demo.jar --kafka.brokers localhost:9092 --kafka.topics demo001
```

# 读取kafka 计算窗口内消费top3写入clickhouse
```shell
flink run -d -p 1 -c examples.app.TopN topN-demo.jar --kafka.brokers localhost:9092 --kafka.topics demo001 --kafka.group.id test01 --clickhouse.url jdbc:clickhouse://localhost:8123/pk --clickhouse.username default --clickhouse.password default --use.sql true
```