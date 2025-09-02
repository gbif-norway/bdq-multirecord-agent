package org.bdq.server;

import org.bdq.server.model.TestMapping;
import org.bdq.server.model.BDQResponse.TupleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
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
    // Per-test, per-tuple LRU caches
    private final Map<String, LinkedHashMap<List<String>, TupleResult>> perTestTupleCache = new ConcurrentHashMap<>();
    private final int tupleCacheMaxEntries = 5000; // bounded size per test
    
    // Test mappings provided during warmup
    private Map<String, TestMapping> testMappings = new HashMap<>();
    
    public void setTestMappings(Map<String, TestMapping> mappings) {
        this.testMappings = mappings;
        logger.info("Loaded {} test mappings", mappings.size());
    }
    
    /**
     * Execute a test for a list of tuples
     */
    public List<TupleResult> executeTest(String testId, List<String> actedUpon, 
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
            
            List<TupleResult> results = new ArrayList<>();
            
            for (int i = 0; i < tuples.size(); i++) {
                List<String> tuple = tuples.get(i);
                try {
                    // Attempt cache lookup first
                    TupleResult cached = getFromTupleCache(testId, tuple);
                    TupleResult result;
                    if (cached != null) {
                        // Return a copy with correct tupleIndex
                        result = new TupleResult(i, cached.getStatus(), cached.getResult(), cached.getComment());
                    } else {
                        result = executeSingleTuple(method, instance, testId,
                            actedUpon, consulted, parameters, tuple, i);
                        putIntoTupleCache(testId, tuple, new TupleResult(0, result.getStatus(), result.getResult(), result.getComment()));
                    }
                    results.add(result);
                } catch (Exception e) {
                    logger.error("Error executing test {} for tuple {}: {}", testId, i, e.getMessage(), e);
                    String stackTrace = getStackTrace(e);
                    results.add(new TupleResult(i, "INTERNAL_PREREQUISITES_NOT_MET", null, 
                                              "Execution error: " + e.getMessage() + " (see logs for stack trace)"));
                }
            }
            
            return results;
            
        } catch (Exception e) {
            logger.error("Error setting up test {}: {}", testId, e.getMessage());
            return Collections.emptyList();
        }
    }
    
    private TupleResult executeSingleTuple(Method method, Object instance, String testId,
                                         List<String> actedUpon, List<String> consulted,
                                         Map<String, String> parameters, List<String> tuple, int tupleIndex) 
                                         throws Exception {
        
        // Build arguments for method invocation based on method signature
        Object[] args = new Object[method.getParameterCount()];
        Class<?>[] paramTypes = method.getParameterTypes();
        
        int argIndex = 0;
        
        // First, add actedUpon values (usually String parameters with @ActedUpon annotations)
        for (int i = 0; i < actedUpon.size() && i < tuple.size() && argIndex < args.length; i++) {
            if (paramTypes[argIndex] == String.class) {
                args[argIndex] = tuple.get(i);
                argIndex++;
            }
        }
        
        // Then add consulted values if any
        for (int i = 0; i < consulted.size() && argIndex < args.length; i++) {
            if (paramTypes[argIndex] == String.class) {
                args[argIndex] = ""; // Default empty value for consulted parameters
                argIndex++;
            }
        }
        
        // Finally, add parameters object if method expects it (some methods take parameters)
        if (argIndex < args.length && Map.class.isAssignableFrom(paramTypes[argIndex])) {
            args[argIndex] = parameters != null ? parameters : new HashMap<String, String>();
            argIndex++;
        }
        
        // Execute the method with individual parameters
        Object result = method.invoke(instance, args);
        
        // Parse the result - BDQ methods typically return DQResponse objects
        return parseMethodResult(result, tupleIndex);
    }
    
    private TupleResult parseMethodResult(Object result, int tupleIndex) {
        if (result == null) {
            return new TupleResult(tupleIndex, "INTERNAL_PREREQUISITES_NOT_MET", null, "Method returned null");
        }
        
        try {
            // BDQ methods return DQResponse objects with getResultState(), getComment(), etc.
            // Use reflection to extract the common fields
            Class<?> resultClass = result.getClass();
            
            // Get status/result state
            String status = "INTERNAL_PREREQUISITES_NOT_MET";
            try {
                Method getResultState = resultClass.getMethod("getResultState");
                Object resultState = getResultState.invoke(result);
                if (resultState != null) {
                    status = resultState.toString();
                }
            } catch (Exception e) {
                logger.debug("Could not get result state: {}", e.getMessage());
            }
            
            // Get comment
            String comment = "";
            try {
                Method getComment = resultClass.getMethod("getComment");
                Object commentObj = getComment.invoke(result);
                if (commentObj != null) {
                    comment = commentObj.toString();
                }
            } catch (Exception e) {
                logger.debug("Could not get comment: {}", e.getMessage());
            }
            
            // Get result value (for validations) or amended values (for amendments)
            Object resultValue = null;
            try {
                // Try different method names that BDQ libraries use
                String[] resultMethods = {"getResult", "getValue", "getAmendedValue"};
                for (String methodName : resultMethods) {
                    try {
                        Method getResult = resultClass.getMethod(methodName);
                        resultValue = getResult.invoke(result);
                        break;
                    } catch (NoSuchMethodException ignored) {
                        // Try next method name
                    }
                }
            } catch (Exception e) {
                logger.debug("Could not get result value: {}", e.getMessage());
            }
            
            return new TupleResult(tupleIndex, status, resultValue, comment);
            
        } catch (Exception e) {
            logger.error("Error parsing method result: {}", e.getMessage());
            return new TupleResult(tupleIndex, "INTERNAL_PREREQUISITES_NOT_MET", null, 
                                 "Error parsing result: " + e.getMessage());
        }
    }
    
    private Method getOrResolveMethod(TestMapping mapping) throws Exception {
        String cacheKey = mapping.getJavaClass() + "#" + mapping.getJavaMethod();
        
        return methodCache.computeIfAbsent(cacheKey, key -> {
            try {
                Class<?> clazz = Class.forName(mapping.getJavaClass());
                
                // Look for method with the matching name and appropriate parameters
                Method[] methods = clazz.getMethods();
                Method bestMatch = null;
                
                for (Method method : methods) {
                    if (method.getName().equals(mapping.getJavaMethod())) {
                        // Prefer methods that match the expected parameter count
                        int expectedParams = mapping.getActedUpon().size() + mapping.getConsulted().size();
                        
                        if (method.getParameterCount() == expectedParams ||
                            method.getParameterCount() == expectedParams + 1) { // +1 for parameters
                            bestMatch = method;
                            break;
                        } else if (bestMatch == null) {
                            // Keep first match as fallback
                            bestMatch = method;
                        }
                    }
                }
                
                if (bestMatch != null) {
                    logger.info("Resolved method {} with {} parameters", key, bestMatch.getParameterCount());
                    return bestMatch;
                }
                
                throw new NoSuchMethodException("Method not found: " + key);
                
            } catch (Exception e) {
                logger.error("Failed to resolve method {}: {}", key, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }
    
    private Object getOrCreateInstance(String className) throws Exception {
        return instanceCache.computeIfAbsent(className, key -> {
            try {
                Class<?> clazz = Class.forName(key);
                Object instance = clazz.getDeclaredConstructor().newInstance();
                logger.info("Created instance of {}", key);
                return instance;
            } catch (Exception e) {
                logger.error("Failed to create instance of {}: {}", key, e.getMessage());
                throw new RuntimeException(e);
            }
        });
    }
    
    /**
     * Warmup method to pre-load and validate all test mappings
     */
    public void warmup() {
        logger.info("Starting warmup for {} test mappings", testMappings.size());
        
        int successful = 0;
        int failed = 0;
        
        for (TestMapping mapping : testMappings.values()) {
            try {
                getOrResolveMethod(mapping);
                getOrCreateInstance(mapping.getJavaClass());
                successful++;
            } catch (Exception e) {
                logger.warn("Warmup failed for test {}: {}", mapping.getTestId(), e.getMessage());
                failed++;
            }
        }
        
        logger.info("Warmup completed: {} successful, {} failed", successful, failed);
    }

    // LRU cache helpers
    private TupleResult getFromTupleCache(String testId, List<String> tuple) {
        LinkedHashMap<List<String>, TupleResult> cache = perTestTupleCache.get(testId);
        if (cache == null) return null;
        synchronized (cache) {
            return cache.get(tuple);
        }
    }

    private void putIntoTupleCache(String testId, List<String> tuple, TupleResult result) {
        LinkedHashMap<List<String>, TupleResult> cache = perTestTupleCache.computeIfAbsent(testId, k -> new LinkedHashMap<List<String>, TupleResult>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<List<String>, TupleResult> eldest) {
                return size() > tupleCacheMaxEntries;
            }
        });
        synchronized (cache) {
            cache.put(tuple, result);
        }
    }
    
    /**
     * Get stack trace as string for error reporting
     */
    private String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}
