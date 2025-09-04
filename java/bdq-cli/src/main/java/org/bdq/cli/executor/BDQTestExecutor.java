package org.bdq.cli.executor;

import org.bdq.cli.model.BDQRequest;
import org.bdq.cli.model.BDQResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executes BDQ tests using reflection on the FilteredPush libraries
 */
public class BDQTestExecutor {
    private static final Logger logger = LoggerFactory.getLogger(BDQTestExecutor.class);
    
    // Cache for resolved methods to avoid repeated reflection
    private final Map<String, Method> methodCache = new ConcurrentHashMap<>();
    private final Map<String, Object> instanceCache = new ConcurrentHashMap<>();
    
    // Test mappings - would be loaded from TG2_tests.csv
    private final Map<String, TestMapping> testMappings = new HashMap<>();
    
    public BDQTestExecutor() {
        // Initialize with some basic test mappings
        // In a real implementation, this would be loaded from TG2_tests.csv
        initializeTestMappings();
    }
    
    private void initializeTestMappings() {
        // Load test mappings from TG2_tests.csv
        loadTestMappingsFromCSV();
    }
    
    private void loadTestMappingsFromCSV() {
        String[] possiblePaths = {
            "TG2_tests.csv",
            "/opt/bdq/TG2_tests.csv",
            "/app/TG2_tests.csv",
            "bdq-spec/tg2/core/TG2_tests.csv"
        };
        
        for (String csvPath : possiblePaths) {
            try (java.io.InputStream is = getClass().getClassLoader().getResourceAsStream("TG2_tests.csv")) {
                if (is != null) {
                    csvPath = "classpath:TG2_tests.csv";
                    parseCSVFromInputStream(is);
                    logger.info("Loaded {} test mappings from classpath TG2_tests.csv", testMappings.size());
                    return;
                }
            } catch (Exception e) {
                logger.debug("Could not load TG2_tests.csv from classpath: {}", e.getMessage());
            }
            
            try {
                java.nio.file.Path path = java.nio.file.Paths.get(csvPath);
                if (java.nio.file.Files.exists(path)) {
                    parseCSVFromFile(csvPath);
                    logger.info("Loaded {} test mappings from {}", testMappings.size(), csvPath);
                    return;
                }
            } catch (Exception e) {
                logger.debug("Could not load TG2_tests.csv from {}: {}", csvPath, e.getMessage());
            }
        }
        
        // Fallback to hardcoded critical mappings if CSV not found
        logger.warn("TG2_tests.csv not found, using fallback hardcoded mappings");
        loadFallbackMappings();
    }
    
    private void parseCSVFromInputStream(java.io.InputStream is) throws Exception {
        org.apache.commons.csv.CSVParser parser = org.apache.commons.csv.CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .parse(new java.io.InputStreamReader(is));
            
        for (org.apache.commons.csv.CSVRecord record : parser) {
            TestMapping mapping = parseCSVRecord(record);
            if (mapping != null) {
                testMappings.put(mapping.testId, mapping);
            }
        }
    }
    
    private void parseCSVFromFile(String csvPath) throws Exception {
        org.apache.commons.csv.CSVParser parser = org.apache.commons.csv.CSVFormat.DEFAULT
            .withFirstRecordAsHeader()
            .parse(java.nio.file.Files.newBufferedReader(java.nio.file.Paths.get(csvPath)));
            
        for (org.apache.commons.csv.CSVRecord record : parser) {
            TestMapping mapping = parseCSVRecord(record);
            if (mapping != null) {
                testMappings.put(mapping.testId, mapping);
            }
        }
    }
    
    private TestMapping parseCSVRecord(org.apache.commons.csv.CSVRecord record) {
        try {
            String testId = record.get("Label").trim();
            if (testId.isEmpty()) {
                return null;
            }
            
            String actedUponStr = record.get("InformationElement:ActedUpon");
            String consultedStr = record.get("InformationElement:Consulted");
            String sourceLink = record.get("Link to Specification Source Code");
            
            // Parse library and class from source code URL
            String[] libraryAndClass = parseSourceLink(sourceLink);
            if (libraryAndClass == null) {
                logger.debug("Could not determine library/class for test {}", testId);
                return null;
            }
            
            String library = libraryAndClass[0];
            String javaClass = libraryAndClass[1];
            String javaMethod = deriveMethodName(testId);
            String testType = determineTestType(testId);
            
            List<String> actedUpon = parseFieldList(actedUponStr);
            List<String> consulted = parseFieldList(consultedStr);
            
            return new TestMapping(testId, library, javaClass, javaMethod, actedUpon, consulted, Collections.emptyMap(), testType);
            
        } catch (Exception e) {
            logger.debug("Error parsing CSV record: {}", e.getMessage());
            return null;
        }
    }
    
    private String[] parseSourceLink(String sourceLink) {
        if (sourceLink == null || sourceLink.trim().isEmpty()) {
            return null;
        }
        
        // Identify library from URL path
        String library = null;
        if (sourceLink.contains("geo_ref_qc")) {
            library = "geo_ref_qc";
        } else if (sourceLink.contains("event_date_qc")) {
            library = "event_date_qc";  
        } else if (sourceLink.contains("sci_name_qc")) {
            library = "sci_name_qc";
        } else if (sourceLink.contains("rec_occur_qc")) {
            library = "rec_occur_qc";
        } else {
            return null;
        }
        
        // Extract class name from URL (e.g., DwCGeoRefDQ.java -> DwCGeoRefDQ)
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("/([A-Z][a-zA-Z0-9_]+)\\.java");
        java.util.regex.Matcher matcher = pattern.matcher(sourceLink);
        if (!matcher.find()) {
            return null;
        }
        
        String className = matcher.group(1);
        String fullClassName = getFullClassName(library, className);
        
        return new String[]{library, fullClassName};
    }
    
    private String getFullClassName(String library, String className) {
        // Standard package patterns for each library
        String packageName;
        switch (library) {
            case "geo_ref_qc":
                packageName = "org.filteredpush.qc.georeference";
                break;
            case "event_date_qc":
                packageName = "org.filteredpush.qc.date";
                break;
            case "sci_name_qc":
                packageName = "org.filteredpush.qc.sciname";
                break;
            case "rec_occur_qc":
                packageName = "org.filteredpush.qc.metadata";
                break;
            default:
                packageName = "org.filteredpush.qc.unknown";
        }
        
        return packageName + "." + className;
    }
    
    private String deriveMethodName(String testId) {
        // Convert VALIDATION_COUNTRY_FOUND -> validationCountryFound
        String[] parts = testId.split("_");
        if (parts.length < 2) {
            return testId.toLowerCase();
        }
        
        StringBuilder methodName = new StringBuilder(parts[0].toLowerCase());
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i].toLowerCase();
            methodName.append(Character.toUpperCase(part.charAt(0)));
            if (part.length() > 1) {
                methodName.append(part.substring(1));
            }
        }
        
        return methodName.toString();
    }
    
    private String determineTestType(String testId) {
        if (testId.startsWith("VALIDATION_")) {
            return "VALIDATION";
        } else if (testId.startsWith("AMENDMENT_")) {
            return "AMENDMENT";
        } else if (testId.startsWith("MEASURE_")) {
            return "MEASURE";
        } else if (testId.startsWith("ISSUE_")) {
            return "ISSUE";
        } else {
            return "UNKNOWN";
        }
    }
    
    private List<String> parseFieldList(String fieldValue) {
        if (fieldValue == null || fieldValue.trim().isEmpty()) {
            return Collections.emptyList();
        }
        
        return Arrays.asList(fieldValue.split(","))
            .stream()
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(java.util.stream.Collectors.toList());
    }
    
    private void loadFallbackMappings() {
        // Critical fallback mappings if CSV parsing fails
        testMappings.put("VALIDATION_COUNTRY_FOUND", 
            new TestMapping("VALIDATION_COUNTRY_FOUND", "geo_ref_qc", 
                "org.filteredpush.qc.georeference.DwCGeoRefDQ", 
                "validationCountryFound", 
                Arrays.asList("dwc:country"), 
                Collections.emptyList(), 
                Collections.emptyMap(), 
                "VALIDATION"));
                
        testMappings.put("VALIDATION_OCCURRENCEID_NOTEMPTY", 
            new TestMapping("VALIDATION_OCCURRENCEID_NOTEMPTY", "rec_occur_qc", 
                "org.filteredpush.qc.metadata.DwCMetadataDQ", 
                "validationOccurrenceidNotempty", 
                Arrays.asList("dwc:occurrenceID"), 
                Collections.emptyList(), 
                Collections.emptyMap(), 
                "VALIDATION"));
                
        testMappings.put("VALIDATION_BASISOFRECORD_NOTEMPTY", 
            new TestMapping("VALIDATION_BASISOFRECORD_NOTEMPTY", "rec_occur_qc", 
                "org.filteredpush.qc.metadata.DwCMetadataDQ", 
                "validationBasisofrecordNotempty", 
                Arrays.asList("dwc:basisOfRecord"), 
                Collections.emptyList(), 
                Collections.emptyMap(), 
                "VALIDATION"));
    }
    
    public BDQResponse executeTests(BDQRequest request) {
        Map<String, BDQResponse.TestResults> results = new HashMap<>();
        List<BDQResponse.TestError> errors = new ArrayList<>();
        
        for (BDQRequest.TestRequest testRequest : request.getTests()) {
            try {
                List<BDQResponse.TupleResult> tupleResults = executeTest(
                    testRequest.getTestId(),
                    testRequest.getActedUpon(),
                    testRequest.getConsulted(),
                    testRequest.getParameters(),
                    testRequest.getTuples()
                );
                results.put(testRequest.getTestId(), new BDQResponse.TestResults(tupleResults));
            } catch (Exception e) {
                logger.error("Error executing test {}: {}", testRequest.getTestId(), e.getMessage());
                errors.add(new BDQResponse.TestError(testRequest.getTestId(), e.getMessage()));
            }
        }
        
        return new BDQResponse(request.getRequestId(), results, errors);
    }
    
    private List<BDQResponse.TupleResult> executeTest(String testId, List<String> actedUpon, 
                                                    List<String> consulted, Map<String, String> parameters,
                                                    List<List<String>> tuples) {
        TestMapping mapping = testMappings.get(testId);
        if (mapping == null) {
            logger.warn("No mapping found for test {}", testId);
            return Collections.emptyList();
        }
        
        try {
            Method method = getOrResolveMethod(mapping);
            Object instance = getOrCreateInstance(mapping.getJavaClass());
            
            List<BDQResponse.TupleResult> results = new ArrayList<>();
            
            for (int i = 0; i < tuples.size(); i++) {
                List<String> tuple = tuples.get(i);
                try {
                    BDQResponse.TupleResult result = executeSingleTuple(method, instance, testId,
                        actedUpon, consulted, parameters, tuple, i);
                    results.add(result);
                } catch (Exception e) {
                    logger.error("Error executing test {} for tuple {}: {}", testId, i, e.getMessage(), e);
                    results.add(new BDQResponse.TupleResult(i, "INTERNAL_PREREQUISITES_NOT_MET", null, 
                        "Execution error: " + e.getMessage()));
                }
            }
            
            return results;
            
        } catch (Exception e) {
            logger.error("Error setting up test {}: {}", testId, e.getMessage());
            return Collections.emptyList();
        }
    }
    
    private BDQResponse.TupleResult executeSingleTuple(Method method, Object instance, String testId,
                                                     List<String> actedUpon, List<String> consulted,
                                                     Map<String, String> parameters, List<String> tuple, int tupleIndex) 
                                                     throws Exception {
        
        // Build arguments for method invocation based on method signature
        Object[] args = new Object[method.getParameterCount()];
        Class<?>[] paramTypes = method.getParameterTypes();
        
        int argIndex = 0;
        
        // First, add actedUpon values
        for (int i = 0; i < actedUpon.size() && i < tuple.size() && argIndex < args.length; i++) {
            if (paramTypes[argIndex] == String.class) {
                args[argIndex] = tuple.get(i);
                argIndex++;
            }
        }
        
        // Add consulted values if needed
        for (int i = 0; i < consulted.size() && argIndex < args.length; i++) {
            if (paramTypes[argIndex] == String.class) {
                args[argIndex] = consulted.get(i);
                argIndex++;
            }
        }
        
        // Add parameters if needed
        if (parameters != null && argIndex < args.length) {
            if (paramTypes[argIndex] == Map.class) {
                args[argIndex] = parameters;
                argIndex++;
            }
        }
        
        // Fill remaining parameters with null if needed
        while (argIndex < args.length) {
            args[argIndex] = null;
            argIndex++;
        }
        
        // Execute the method
        Object result = method.invoke(instance, args);
        
        // Extract values from DQResponse object
        try {
            // Use reflection to extract values from DQResponse
            Class<?> resultClass = result.getClass();
            
            // Get the ResultState
            Object resultState = resultClass.getMethod("getResultState").invoke(result);
            String status = (String) resultState.getClass().getMethod("getLabel").invoke(resultState);
            
            // Get the comment
            String comment = (String) resultClass.getMethod("getComment").invoke(result);
            if (comment == null) {
                comment = "Test executed";
            }
            
            // Get the actual result value
            Object value = resultClass.getMethod("getValue").invoke(result);
            String resultValue;
            
            if (value != null) {
                // Extract the label from the value (ComplianceValue, AmendmentValue, etc.)
                try {
                    resultValue = (String) value.getClass().getMethod("getLabel").invoke(value);
                } catch (Exception e) {
                    // If getLabel fails, try toString or use the object directly
                    resultValue = value.toString();
                }
            } else {
                resultValue = null;
            }
            
            return new BDQResponse.TupleResult(tupleIndex, status, resultValue, comment);
            
        } catch (Exception e) {
            logger.error("Error extracting DQResponse values: {}", e.getMessage());
            // Fallback to simple toString
            return new BDQResponse.TupleResult(tupleIndex, "INTERNAL_PREREQUISITES_NOT_MET", 
                result.toString(), "Error extracting result: " + e.getMessage());
        }
    }
    
    private Method getOrResolveMethod(TestMapping mapping) throws Exception {
        String cacheKey = mapping.getJavaClass() + "#" + mapping.getJavaMethod();
        
        Method method = methodCache.get(cacheKey);
        if (method == null) {
            Class<?> clazz = Class.forName(mapping.getJavaClass());
            method = clazz.getMethod(mapping.getJavaMethod(), String.class);
            methodCache.put(cacheKey, method);
        }
        
        return method;
    }
    
    private Object getOrCreateInstance(String className) throws Exception {
        Object instance = instanceCache.get(className);
        if (instance == null) {
            Class<?> clazz = Class.forName(className);
            instance = clazz.getDeclaredConstructor().newInstance();
            instanceCache.put(className, instance);
        }
        
        return instance;
    }
    
    private static class TestMapping {
        private final String testId;
        private final String library;
        private final String javaClass;
        private final String javaMethod;
        private final List<String> actedUpon;
        private final List<String> consulted;
        private final Map<String, String> parameters;
        private final String testType;
        
        public TestMapping(String testId, String library, String javaClass, String javaMethod,
                         List<String> actedUpon, List<String> consulted, 
                         Map<String, String> parameters, String testType) {
            this.testId = testId;
            this.library = library;
            this.javaClass = javaClass;
            this.javaMethod = javaMethod;
            this.actedUpon = actedUpon;
            this.consulted = consulted;
            this.parameters = parameters;
            this.testType = testType;
        }
        
        public String getTestId() { return testId; }
        public String getLibrary() { return library; }
        public String getJavaClass() { return javaClass; }
        public String getJavaMethod() { return javaMethod; }
        public List<String> getActedUpon() { return actedUpon; }
        public List<String> getConsulted() { return consulted; }
        public Map<String, String> getParameters() { return parameters; }
        public String getTestType() { return testType; }
    }
}
