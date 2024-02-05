package com.xboshy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static String processTemplate(String filename, HashMap<String, String> dict) throws Exception {
        Velocity.init();

        VelocityContext context = new VelocityContext();

        for(Map.Entry<String, String> e : dict.entrySet())
            context.put(e.getKey(), e.getValue());

        File file = new File(filename);
        FileReader fr = new FileReader(file);
        BufferedReader template = new BufferedReader(fr);
        StringWriter writer = new StringWriter();

        Velocity.evaluate(context, writer, filename, template);

        return writer.toString();
    }

    public static void main(String[] args) throws Exception{
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "yyyy-MM-dd HH:mm:ss:SSS [Z]");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showLogName", "false");

        Logger logger = LoggerFactory.getLogger(Main.class);

        Options options = new Options();
        options.addRequiredOption("e", "extract", true, "Extract YAML");
        options.addRequiredOption("t", "transform", true, "Transform YAML");
        options.addRequiredOption("l", "load", true, "Load YAML");
        Option option = new Option("v", "var", true, "Variable");
        option.hasArgs();
        option.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(option);

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse( options, args);

        String extractYAML = cmd.getOptionValue("e");
        String transformYAML = cmd.getOptionValue("t");
        String loadYAML = cmd.getOptionValue("l");


        HashMap<String, String> varsMap = new HashMap<>();
        if(cmd.hasOption("v")) {
            String[] variables = cmd.getOptionValues("v");

            if ((variables.length % 2) != 0)
                throw new Exception("Use: -v var val");

            for (int i = 0; i < variables.length; i += 2)
                varsMap.put(variables[i], variables[i + 1]);
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();

        Source source = mapper.readValue(processTemplate(extractYAML, varsMap), Source.class);
        Processor processor = mapper.readValue(processTemplate(transformYAML, varsMap), Processor.class);
        Sink sink = mapper.readValue(processTemplate(loadYAML, varsMap), Sink.class);

        logger.info(">>>>> EXTRACT.process <<<<<");
        source.execute(processor);
        if (source.getState() != State.SUCCESS) {
            logger.warn("EXTRACT.process failed.");
            throw new Exception("Exit");
        }

        logger.info(">>>>> TRANSFORM.process <<<<<");
        processor.execute();
        if (processor.getState() != State.SUCCESS) {
            logger.warn("TRANSFORM.process failed.");
            throw new Exception("Exit");
        }

        logger.info(">>>>> LOAD.process <<<<<");
        sink.execute(processor);
        if (sink.getState() != State.SUCCESS) {
            logger.warn("LOAD.process failed.");
            throw new Exception("Exit");
        }

        try {
            logger.info(">>>>> EXTRACT.clean <<<<<");
            source.clean();
        } catch (Exception e) {
            logger.warn("EXTRACT.clean step failed.");
        }

        try {
            logger.info(">>>>> TRANSFORM.clean <<<<<");
            processor.clean();
        } catch (Exception e) {
            logger.warn("TRANSFORM.clean step failed.");
        }

        try {
            logger.info(">>>>> LOAD.clean <<<<<");
            sink.clean();
        } catch (Exception e) {
            logger.warn("LOAD.clean failed.");
        }
    }
}
