package com.dataomnis.dynamic;

import com.dataomnis.api.RedisLookupFunction;
import com.dataomnis.api.util.RedisWriteStrategy;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

public class RedisDynamicTableSource implements LookupTableSource, ScanTableSource {
    private final String[] fieldNames;
    private final DataType[] fieldTypes;
    private final String ip;
    private final int port;
    private final String redisPassword;
    private final String redisValueType;
    private final boolean openCache;
    private final int cacheMaxSize;
    private final String keyAppendCharacter;
    private final boolean writeOpen;
    private final RedisWriteStrategy redisWriteStrategy;

    public RedisDynamicTableSource(
            String[] fieldNames,
            DataType[] fieldTypes,
            String ip,
            int port,
            String redisPassword,
            String redisValueType,
            boolean openCache,
            int cacheMaxSize,
            String keyAppendCharacter,
            boolean writeOpen,
            RedisWriteStrategy redisWriteStrategy) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.ip = ip;
        this.port = port;
        this.redisPassword = redisPassword;
        this.redisValueType = redisValueType;
        this.openCache = openCache;
        this.cacheMaxSize = cacheMaxSize;
        this.keyAppendCharacter = keyAppendCharacter;
        this.writeOpen = writeOpen;
        this.redisWriteStrategy = redisWriteStrategy;
        if (writeOpen && RedisWriteStrategy.ALWAYS.getCode().equals(redisWriteStrategy) && openCache) {
            throw new FlinkRuntimeException("when key.strategy is 'always',should not open cache.");
        }
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            final int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(innerKeyArr.length == 1, "redis only support non-nested look up keys");
            keyNames[i] = fieldNames[innerKeyArr[0]];
        }
        RedisLookupFunction lookupFunction =
                RedisLookupFunction.builder()
                        .setIp(ip)
                        .setPort(port)
                        .setRedisPassword(redisPassword)
                        .setRedisValueType(redisValueType)
                        .setFieldNames(fieldNames)
                        .setFieldTypes(fieldTypes)
                        .setRedisJoinKeys(keyNames)
                        .setCacheMaxSize(cacheMaxSize)
                        .setOpenCache(openCache)
                        .setKeyAppendCharacter(keyAppendCharacter)
                        .setWriteOpen(writeOpen)
                        .setRedisWriteStrategy(redisWriteStrategy)
                        .build();
        return TableFunctionProvider.of(lookupFunction);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(
                fieldNames,
                fieldTypes,
                ip,
                port,
                redisPassword,
                redisValueType,
                openCache,
                cacheMaxSize,
                keyAppendCharacter,
                writeOpen,
                redisWriteStrategy);
    }

    @Override
    public String asSummaryString() {
        return "redis source";
    }

    public static class Builder {
        private String[] fieldNames;

        private DataType[] fieldTypes;

        private String ip;

        private int port;

        private String redisPassword;

        private String redisValueType;

        private boolean openCache;

        private int cacheMaxSize;

        private String keyAppendCharacter;

        private boolean writeOpen;

        private RedisWriteStrategy redisWriteStrategy;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(DataType[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
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

        public Builder setOpenCache(boolean openCache) {
            this.openCache = openCache;
            return this;
        }

        public Builder setCacheMaxSize(int cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder setKeyAppendCharacter(String keyAppendCharacter) {
            this.keyAppendCharacter = keyAppendCharacter;
            return this;
        }

        public Builder setWriteOpen(boolean writeOpen) {
            this.writeOpen = writeOpen;
            return this;
        }

        public Builder setRedisWriteStrategy(RedisWriteStrategy redisWriteStrategy) {
            this.redisWriteStrategy = redisWriteStrategy;
            return this;
        }

        public RedisDynamicTableSource build() {
            return new RedisDynamicTableSource(
                    fieldNames,
                    fieldTypes,
                    ip,
                    port,
                    redisPassword,
                    redisValueType,
                    openCache,
                    cacheMaxSize,
                    keyAppendCharacter,
                    writeOpen,
                    redisWriteStrategy);
        }
    }

}
