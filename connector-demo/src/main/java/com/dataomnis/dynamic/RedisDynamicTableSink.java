package com.dataomnis.dynamic;

import com.dataomnis.api.RedisOutputFormat;
import com.dataomnis.api.RedisSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

public class RedisDynamicTableSink implements DynamicTableSink {
    private final String redisIp;

    private final int redisPort;

    private final String redisPassword;

    private final String redisValueType;

    private final int ttl;

    private final TableSchema tableSchema;

    public RedisDynamicTableSink(String redisIp,
                                 int redisPort,
                                 String redisPassword,
                                 String redisValueType,
                                 TableSchema tableSchema,
                                 int ttl) {
        this.redisIp = redisIp;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.redisValueType = redisValueType;
        this.tableSchema = tableSchema;
        this.ttl = ttl;
    }

    private RedisOutputFormat<?> newFormat() {
        return RedisOutputFormat.builder()
                .setRedisIp(redisIp)
                .setRedisPort(redisPort)
                .setRedisPassword(redisPassword)
                .setRedisValueType(redisValueType)
                .setTtl(ttl)
                .build();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SinkFunction sinkFunction = new RedisSinkFunction<>(newFormat());
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(
                redisIp, redisPort, redisPassword, redisValueType, tableSchema, ttl);
    }

    @Override
    public String asSummaryString() {
        return "redis sink";
    }

    public static Builder builder(){
        return new Builder();
    }

    public static class Builder {

        private String redisIp;

        private int redisPort;

        private String redisPassword;

        private String redisValueType;

        private int ttl;

        private TableSchema tableSchema;

        public Builder setRedisIp(String redisIp) {
            this.redisIp = redisIp;
            return this;
        }

        public Builder setRedisPort(int redisPort) {
            this.redisPort = redisPort;
            return this;
        }

        public Builder setRedisPassword(String redisPassword) {
            this.redisPassword = redisPassword;
            return this;
        }

        public Builder setRedisValueType(String redisValueType) {
            this.redisValueType = redisValueType;
            return this;
        }

        public Builder setTableSchema(TableSchema tableSchema) {
            this.tableSchema = tableSchema;
            return this;
        }

        public Builder setTtl(int ttl) {
            this.ttl = ttl;
            return this;
        }

        public RedisDynamicTableSink build() {
            return new RedisDynamicTableSink(
                    redisIp, redisPort, redisPassword, redisValueType, tableSchema, ttl);
        }
    }
}
