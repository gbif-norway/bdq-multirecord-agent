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
    
    public BDQTestExecutor() {
        // No initialization needed - method info comes from Python
        logger.info("BDQ Test Executor initialized - using method info from request");
    }
    
    public BDQResponse executeTests(BDQRequest request) {
        Map<String, BDQResponse.TestResults> results = new HashMap<>();
        List<BDQResponse.TestError> errors = new ArrayList<>();
        
        for (BDQRequest.TestRequest testRequest : request.getTests()) {
            try {
                List<BDQResponse.TupleResult> tupleResults = executeTest(
                    testRequest.getTestId(),
                    testRequest.getJavaClass(),
                    testRequest.getJavaMethod(),
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
    
    private List<BDQResponse.TupleResult> executeTest(String testId, String javaClass, String javaMethod,
                                                    List<String> actedUpon, List<String> consulted, 
                                                    Map<String, String> parameters, List<List<String>> tuples) {
        // Method info provided directly from Python - no lookup needed
        logger.debug("Executing test {} using {}#{}", testId, javaClass, javaMethod);
        
        try {
            Method method = getOrResolveMethod(javaClass, javaMethod);
            Object instance = getOrCreateInstance(javaClass);
            
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
        
        logger.debug("Mapping arguments for {}: {} params, tuple size {}, acted upon {}, consulted {}", 
            testId, paramTypes.length, tuple.size(), actedUpon.size(), consulted.size());
        
        int argIndex = 0;
        
        // Map actedUpon values to method parameters
        for (int i = 0; i < actedUpon.size() && i < tuple.size() && argIndex < args.length; i++) {
            if (paramTypes[argIndex] == String.class) {
                args[argIndex] = tuple.get(i);
                logger.debug("Mapping actedUpon[{}] '{}' to parameter[{}]", i, tuple.get(i), argIndex);
                argIndex++;
            } else {
                logger.warn("Parameter[{}] is not String type for actedUpon value: {}", argIndex, paramTypes[argIndex]);
                break;
            }
        }
        
        // Map consulted values if needed (these come after actedUpon in the tuple)
        int consultedStartIndex = actedUpon.size();
        for (int i = 0; i < consulted.size() && argIndex < args.length; i++) {
            int tupleIndex2 = consultedStartIndex + i;
            if (tupleIndex2 < tuple.size() && paramTypes[argIndex] == String.class) {
                args[argIndex] = tuple.get(tupleIndex2);
                logger.debug("Mapping consulted[{}] '{}' to parameter[{}]", i, tuple.get(tupleIndex2), argIndex);
                argIndex++;
            } else if (paramTypes[argIndex] == String.class) {
                // If no consulted value available, use empty string or null
                args[argIndex] = "";
                logger.debug("No consulted value available, using empty string for parameter[{}]", argIndex);
                argIndex++;
            }
        }
        
        // Handle additional parameters (like bdq:sourceAuthority)
        while (argIndex < args.length) {
            if (paramTypes[argIndex] == String.class) {
                // Check if we have parameter values to use
                String paramValue = null;
                if (parameters != null && !parameters.isEmpty()) {
                    // Try common parameter names
                    paramValue = parameters.get("bdq:sourceAuthority");
                    if (paramValue == null) {
                        paramValue = parameters.values().iterator().next(); // Use first available parameter
                    }
                }
                args[argIndex] = paramValue != null ? paramValue : "";
                logger.debug("Mapping parameter '{}' to parameter[{}]", paramValue, argIndex);
                argIndex++;
            } else if (paramTypes[argIndex] == Map.class) {
                args[argIndex] = parameters != null ? parameters : new HashMap<>();
                logger.debug("Mapping parameters Map to parameter[{}]", argIndex);
                argIndex++;
            } else {
                // For other types, use null
                args[argIndex] = null;
                logger.debug("Using null for unknown parameter type {} at index {}", paramTypes[argIndex], argIndex);
                argIndex++;
            }
        }
        
        // Execute the method
        logger.debug("Invoking {} with args: {}", method.getName(), Arrays.toString(args));
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
    
    private Method getOrResolveMethod(String javaClass, String javaMethod) throws Exception {
        String cacheKey = javaClass + "#" + javaMethod;
        
        Method method = methodCache.get(cacheKey);
        if (method == null) {
            Class<?> clazz = Class.forName(javaClass);
            
            // Try to find the method by name - don't hardcode parameter types
            Method[] methods = clazz.getMethods();
            for (Method m : methods) {
                if (m.getName().equals(javaMethod)) {
                    method = m;
                    logger.debug("Found method {} with {} parameters: {}", 
                        javaMethod, m.getParameterCount(), 
                        Arrays.toString(m.getParameterTypes()));
                    break;
                }
            }
            
            if (method == null) {
                throw new NoSuchMethodException("Method " + javaMethod + " not found in class " + javaClass);
            }
            
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
}
