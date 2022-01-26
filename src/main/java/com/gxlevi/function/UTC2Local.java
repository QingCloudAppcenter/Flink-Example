package com.gxlevi.function;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class UTC2Local extends ScalarFunction {
    public Timestamp eval(Timestamp time) {
        long timestamp = time.getTime() + 28800000;
        return new Timestamp(timestamp);
    }
}
