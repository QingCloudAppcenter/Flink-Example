package com.dataomnis.dynamic;

import com.dataomnis.api.util.RedisWriteStrategy;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.dataomnis.dynamic.RedisOptions.*;

public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        helper.validateExcept("No");
        return RedisDynamicTableSink.builder()
                .setRedisIp(tableOptions.get(HOSTNAME))
                .setRedisPort(tableOptions.get(PORT))
                .setRedisPassword(tableOptions.get(PASSWORD))
                .setRedisValueType(tableOptions.get(TYPE))
                .setTtl(tableOptions.get(TTL))
                .setTableSchema(context.getCatalogTable().getSchema())
                .build();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig readableConfig = helper.getOptions();
        helper.validateExcept("No");
        return RedisDynamicTableSource.builder()
                .setIp(readableConfig.get(HOSTNAME))
                .setPort(readableConfig.get(PORT))
                .setRedisPassword(readableConfig.get(PASSWORD))
                .setRedisValueType(readableConfig.get(TYPE))
                .setWriteOpen(readableConfig.get(WRITE_OPEN))
                .setRedisWriteStrategy(
                        RedisWriteStrategy.get(readableConfig.get(STORE_KEY_STRATEGY)))
                .setOpenCache(readableConfig.get(OPEN_CACHE))
                .setKeyAppendCharacter(readableConfig.get(KEY_APPEND_CHARACTER))
                .setCacheMaxSize(readableConfig.get(CACHE_SIZE))
                .setFieldNames(context.getCatalogTable().getSchema().getFieldNames())
                .setFieldTypes(context.getCatalogTable().getSchema().getFieldDataTypes())
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return "redis";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(PASSWORD);
        options.add(TYPE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(WRITE_OPEN);
        options.add(STORE_KEY_STRATEGY);
        options.add(KEY_APPEND_CHARACTER);
        options.add(OPEN_CACHE);
        options.add(CACHE_SIZE);
        options.add(TTL);
        return options;
    }
}
