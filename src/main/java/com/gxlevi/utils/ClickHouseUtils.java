package com.gxlevi.utils;

import com.gxlevi.bean.TransientSink;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtils {
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    public static <T> SinkFunction<T> getSink(String url, String username, String password, String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        final Field[] fields = t.getClass().getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            final Field field = fields[i];
                            field.setAccessible(true);
                            final TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null) {
                                offset++;
                                continue;
                            }
                            final Object value = field.get(t);
                            preparedStatement.setObject(i + 1 - offset, value);
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(3000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(CLICKHOUSE_DRIVER)
                        .withUrl(url)
                        .withUsername(username)
                        .withPassword(password)
                        .build());

    }
}
