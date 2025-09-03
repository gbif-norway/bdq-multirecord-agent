package org.bdq.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bdq.cli.model.BDQRequest;
import org.bdq.cli.model.BDQResponse;
import org.bdq.cli.executor.BDQTestExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * CLI entry point for BDQ validation
 * Usage: java -jar bdq-cli.jar [--input=input.json] [--output=output.json]
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    
    public static void main(String[] args) {
        String inputFile = null;
        String outputFile = null;
        
        // Parse command line arguments
        for (String arg : args) {
            if (arg.startsWith("--input=")) {
                inputFile = arg.substring("--input=".length());
            } else if (arg.startsWith("--output=")) {
                outputFile = arg.substring("--output=".length());
            } else if (arg.equals("--help") || arg.equals("-h")) {
                printUsage();
                System.exit(0);
            }
        }
        
        try {
            // Read input
            BDQRequest request;
            if (inputFile != null) {
                request = readInputFromFile(inputFile);
            } else {
                request = readInputFromStdin();
            }
            
            // Execute tests
            BDQTestExecutor executor = new BDQTestExecutor();
            BDQResponse response = executor.executeTests(request);
            
            // Write output
            if (outputFile != null) {
                writeOutputToFile(response, outputFile);
            } else {
                writeOutputToStdout(response);
            }
            
            System.exit(0);
            
        } catch (Exception e) {
            logger.error("Error executing BDQ tests", e);
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
    
    private static BDQRequest readInputFromFile(String inputFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(inputFile), BDQRequest.class);
    }
    
    private static BDQRequest readInputFromStdin() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(System.in, BDQRequest.class);
    }
    
    private static void writeOutputToFile(BDQResponse response, String outputFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(outputFile), response);
    }
    
    private static void writeOutputToStdout(BDQResponse response) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, response);
    }
    
    private static void printUsage() {
        System.out.println("BDQ CLI - Biodiversity Data Quality Validation");
        System.out.println();
        System.out.println("Usage: java -jar bdq-cli.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --input=FILE     Input JSON file (default: stdin)");
        System.out.println("  --output=FILE    Output JSON file (default: stdout)");
        System.out.println("  --help, -h       Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar bdq-cli.jar --input=data.json --output=results.json");
        System.out.println("  cat data.json | java -jar bdq-cli.jar > results.json");
    }
}
