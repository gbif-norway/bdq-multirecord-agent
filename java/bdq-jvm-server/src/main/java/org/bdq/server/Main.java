package org.bdq.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for BDQ JVM server
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    
    public static void main(String[] args) {
        String socketPath = "/tmp/bdq_jvm.sock";
        
        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            if ("--socket".equals(args[i]) && i + 1 < args.length) {
                socketPath = args[i + 1];
                i++; // Skip next argument
            } else if (args[i].startsWith("--socket=")) {
                socketPath = args[i].substring("--socket=".length());
            }
        }
        
        logger.info("Starting BDQ Server with socket: {}", socketPath);
        
        BDQServer server = new BDQServer(socketPath);
        
        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received, stopping server...");
            try {
                server.stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));
        
        try {
            server.start();
        } catch (Exception e) {
            logger.error("Server error", e);
            System.exit(1);
        }
    }
}
