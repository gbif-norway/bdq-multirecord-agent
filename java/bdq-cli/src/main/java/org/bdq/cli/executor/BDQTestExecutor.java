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
        // This is a placeholder - in reality, you'd parse TG2_tests.csv
        // For now, we'll add some basic mappings
        testMappings.put("VALIDATION_COUNTRY_FOUND", 
            new TestMapping("VALIDATION_COUNTRY_FOUND", "rec_occur_qc", 
                "org.filteredpush.qc.metadata.DwCMetadataDQ", 
                "validationCountryFound", 
                Arrays.asList("dwc:country"), 
                Collections.emptyList(), 
                Collections.emptyMap(), 
                "VALIDATION"));
        
        // Add occurrence tests
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
