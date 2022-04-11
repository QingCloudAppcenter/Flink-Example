package com.dataomnis.example.utils;


import com.dataomnis.example.app.MockData;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public class ArgsUtils {
    public static ParameterTool getTool(String[] args) throws URISyntaxException, IOException {
        ParameterTool tool;
        if (StringUtils.isNotBlank(System.getenv("LOCAL")) && System.getenv("LOCAL").equalsIgnoreCase("true")) {
            final URL fileURL = MockData.class.getClassLoader().getResource("local.properties");
            assert fileURL != null;
            tool = ParameterTool.fromPropertiesFile(new File(fileURL.toURI()));
        } else {
            tool = ParameterTool.fromArgs(args);
        }
        return tool;
    }
}
