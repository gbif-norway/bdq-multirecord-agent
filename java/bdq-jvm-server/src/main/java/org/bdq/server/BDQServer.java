package org.bdq.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bdq.server.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/**
 * BDQ server that listens on Unix domain socket and processes JSON-RPC requests
 */
public class BDQServer {
    private static final Logger logger = LoggerFactory.getLogger(BDQServer.class);
    
    private final String socketPath;
    private final ObjectMapper objectMapper;
    private final BDQTestExecutor testExecutor;
    private final ExecutorService threadPool;
    
    private volatile boolean running = false;
    private ServerSocketChannel serverChannel;
    
    public BDQServer(String socketPath) {
        this.socketPath = socketPath;
        this.objectMapper = new ObjectMapper();
        this.testExecutor = new BDQTestExecutor();
        
        // Create thread pool sized to available CPUs
        int threadCount = Math.max(2, Runtime.getRuntime().availableProcessors());
        this.threadPool = Executors.newFixedThreadPool(threadCount);
        
        logger.info("BDQ Server initialized with socket path: {}, thread pool size: {}", 
                   socketPath, threadCount);
    }
    
    public void start() throws IOException {
        // Remove existing socket file if present
        Path socketFile = Paths.get(socketPath);
        if (Files.exists(socketFile)) {
            Files.delete(socketFile);
        }
        
        // Create Unix domain socket server
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(socketPath);
        serverChannel = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
        serverChannel.bind(address);
        
        running = true;
        logger.info("BDQ Server started on {}", socketPath);
        
        // Accept connections in a loop
        while (running) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                threadPool.submit(() -> handleClient(clientChannel));
            } catch (IOException e) {
                if (running) {
                    logger.error("Error accepting client connection", e);
                }
            }
        }
    }
    
    public void stop() throws IOException {
        running = false;
        if (serverChannel != null && serverChannel.isOpen()) {
            serverChannel.close();
        }
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Clean up socket file
        try {
            Files.deleteIfExists(Paths.get(socketPath));
        } catch (IOException e) {
            logger.warn("Could not delete socket file: {}", e.getMessage());
        }
        
        logger.info("BDQ Server stopped");
    }
    
    private void handleClient(SocketChannel clientChannel) {
        logger.debug("Client connected");
        
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientChannel.socket().getInputStream()));
             PrintWriter writer = new PrintWriter(
                new OutputStreamWriter(clientChannel.socket().getOutputStream()), true)) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    String response = processRequest(line);
                    writer.println(response);
                } catch (Exception e) {
                    logger.error("Error processing request", e);
                    // Send error response
                    BDQResponse errorResponse = new BDQResponse("unknown", 
                        Collections.emptyMap(), 
                        Collections.singletonList(new BDQResponse.TestError("unknown", e.getMessage())));
                    writer.println(objectMapper.writeValueAsString(errorResponse));
                }
            }
            
        } catch (IOException e) {
            logger.debug("Client disconnected: {}", e.getMessage());
        } finally {
            try {
                clientChannel.close();
            } catch (IOException e) {
                logger.debug("Error closing client channel", e);
            }
        }
    }
    
    private String processRequest(String requestJson) throws Exception {
        logger.debug("Processing request: {}", requestJson);
        
        // Handle special commands
        if (requestJson.contains("\"health\":true")) {
            return "{\"ok\":true}";
        }
        
        // Handle warmup command
        if (requestJson.contains("\"warmup\":true")) {
            return handleWarmup(requestJson);
        }
        
        // Handle test execution request
        BDQRequest request = objectMapper.readValue(requestJson, BDQRequest.class);
        return handleTestRequest(request);
    }
    
    private String handleWarmup(String requestJson) throws Exception {
        // Parse warmup request with test mappings
        WarmupRequest warmupRequest = objectMapper.readValue(requestJson, WarmupRequest.class);
        
        // Convert to internal format and set in executor
        Map<String, TestMapping> mappings = new HashMap<>();
        for (WarmupRequest.TestMappingRequest tmr : warmupRequest.getTestMappings()) {
            TestMapping mapping = new TestMapping(
                tmr.getTestId(), tmr.getLibrary(), tmr.getJavaClass(), tmr.getJavaMethod(),
                tmr.getActedUpon(), tmr.getConsulted(), tmr.getParameters(), tmr.getTestType()
            );
            // Set default parameters if provided
            if (tmr.getDefaultParameters() != null) {
                mapping.setDefaultParameters(tmr.getDefaultParameters());
            }
            mappings.put(tmr.getTestId(), mapping);
        }
        
        testExecutor.setTestMappings(mappings);
        testExecutor.warmup();
        
        return "{\"warmupComplete\":true,\"testsLoaded\":" + mappings.size() + "}";
    }
    
    private String handleTestRequest(BDQRequest request) throws Exception {
        Map<String, BDQResponse.TestResults> results = new HashMap<>();
        List<BDQResponse.TestError> errors = new ArrayList<>();
        
        // Process each test in parallel using threadPool
        List<Future<?>> futures = new ArrayList<>();
        for (BDQRequest.TestRequest testRequest : request.getTests()) {
            futures.add(threadPool.submit(() -> {
                try {
                    List<BDQResponse.TupleResult> tupleResults = testExecutor.executeTest(
                        testRequest.getTestId(),
                        testRequest.getActedUpon(),
                        testRequest.getConsulted(),
                        testRequest.getParameters(),
                        testRequest.getTuples()
                    );
                    synchronized (results) {
                        results.put(testRequest.getTestId(), new BDQResponse.TestResults(tupleResults));
                    }
                } catch (Exception e) {
                    logger.error("Error executing test {}: {}", testRequest.getTestId(), e.getMessage());
                    synchronized (errors) {
                        errors.add(new BDQResponse.TestError(testRequest.getTestId(), e.getMessage()));
                    }
                }
            }));
        }
        for (Future<?> f : futures) {
            try { f.get(); } catch (Exception ignored) {}
        }
        
        BDQResponse response = new BDQResponse(request.getRequestId(), results, errors);
        return objectMapper.writeValueAsString(response);
    }
    
    // Helper class for warmup request
    public static class WarmupRequest {
        private boolean warmup;
        private List<TestMappingRequest> testMappings;
        
        public boolean isWarmup() { return warmup; }
        public void setWarmup(boolean warmup) { this.warmup = warmup; }
        
        public List<TestMappingRequest> getTestMappings() { return testMappings; }
        public void setTestMappings(List<TestMappingRequest> testMappings) { this.testMappings = testMappings; }
        
        public static class TestMappingRequest {
            private String testId;
            private String library;
            private String javaClass;
            private String javaMethod;
            private List<String> actedUpon;
            private List<String> consulted;
            private List<String> parameters;
            private String testType;
            private Map<String, String> defaultParameters;
            
            // Getters and setters
            public String getTestId() { return testId; }
            public void setTestId(String testId) { this.testId = testId; }
            
            public String getLibrary() { return library; }
            public void setLibrary(String library) { this.library = library; }
            
            public String getJavaClass() { return javaClass; }
            public void setJavaClass(String javaClass) { this.javaClass = javaClass; }
            
            public String getJavaMethod() { return javaMethod; }
            public void setJavaMethod(String javaMethod) { this.javaMethod = javaMethod; }
            
            public List<String> getActedUpon() { return actedUpon; }
            public void setActedUpon(List<String> actedUpon) { this.actedUpon = actedUpon; }
            
            public List<String> getConsulted() { return consulted; }
            public void setConsulted(List<String> consulted) { this.consulted = consulted; }
            
            public List<String> getParameters() { return parameters; }
            public void setParameters(List<String> parameters) { this.parameters = parameters; }
            
            public String getTestType() { return testType; }
            public void setTestType(String testType) { this.testType = testType; }
            
            public Map<String, String> getDefaultParameters() { return defaultParameters; }
            public void setDefaultParameters(Map<String, String> defaultParameters) { this.defaultParameters = defaultParameters; }
        }
    }
}
