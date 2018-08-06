package com.ofco.spark.examples.java.util;

import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;

public class Utils {
    public static Map<String, String> getParams(String args[]) {
        Map<String, String> params = new HashMap<>();
        params.put("master", "local[*]");

        Option inputOption = new Option("input", true, "Specify input file");
        inputOption.setRequired(true);

        Option outputOption = new Option("output", true, "Specify input file");
        outputOption.setRequired(true);

        Option masterOption = new Option("master", true, "Specify spark master");

        Options options = new Options();
        options.addOption(masterOption);
        options.addOption(inputOption);
        options.addOption(outputOption);

        CommandLineParser parser = new PosixParser();

        try {
            CommandLine line = parser.parse(options, args);
            for (Option option : line.getOptions()) {
                String key = option.getOpt();
                String value = option.getValue();
                params.put(key, value);
            }
        } catch (ParseException e) {
            throw new IllegalArgumentException("Usage: --master <spark master> --file <file>", e);
        }

        return params;
    }
}
