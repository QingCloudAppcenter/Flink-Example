package com.dataomnis.example;

import org.apache.flink.table.functions.ScalarFunction;

public class UdfDemo extends ScalarFunction {
    public String eval(int sex){
        if (sex==1){
            return "男";
        }else {
            return "女";
        }
    }
}
