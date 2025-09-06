package org.bdq.py4j;

import py4j.GatewayServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Py4J Gateway Server for BDQ test execution
 * This runs as a separate JVM process and provides BDQ test execution via Py4J
 */
public class BDQGateway {
    private static final Logger logger = LoggerFactory.getLogger(BDQGateway.class);
    
    // Cache for resolved methods to avoid repeated reflection
    private final Map<String, Method> methodCache = new ConcurrentHashMap<>();
    private final Map<String, Object> instanceCache = new ConcurrentHashMap<>();
    
    public BDQGateway() {
        logger.info("🚀 BDQ Py4J Gateway initialized");
    }
    
    /**
     * Execute a single BDQ test
     */
    public Map<String, Object> executeTest(String testId, String javaClass, String javaMethod,
                                         List<String> actedUpon, List<String> consulted,
                                         Map<String, String> parameters, List<List<String>> tuples) {
        
        Map<String, Object> result = new HashMap<>();
        List<Map<String, Object>> tupleResults = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        
        try {
            logger.debug("Executing test {} using {}#{}", testId, javaClass, javaMethod);
            
            Method method = getOrResolveMethod(javaClass, javaMethod);
            Object instance = getOrCreateInstance(javaClass);
            
            for (int i = 0; i < tuples.size(); i++) {
                List<String> tuple = tuples.get(i);
                try {
                    Map<String, Object> tupleResult = executeSingleTuple(method, instance, testId,
                        actedUpon, consulted, parameters, tuple, i);
                    tupleResults.add(tupleResult);
                } catch (Exception e) {
                    logger.error("Error executing test {} for tuple {}: {}", testId, i, e.getMessage());
                    Map<String, Object> errorResult = new HashMap<>();
                    errorResult.put("tuple_index", i);
                    errorResult.put("status", "INTERNAL_PREREQUISITES_NOT_MET");
                    errorResult.put("result", null);
                    errorResult.put("comment", "Execution error: " + e.getMessage());
                    tupleResults.add(errorResult);
                }
            }
            
        } catch (Exception e) {
            logger.error("Error setting up test {}: {}", testId, e.getMessage());
            errors.add("Setup error: " + e.getMessage());
        }
        
        result.put("test_id", testId);
        result.put("tuple_results", tupleResults);
        result.put("errors", errors);
        
        return result;
    }
    
    private Method getOrResolveMethod(String javaClass, String javaMethod) throws Exception {
        String cacheKey = javaClass + "#" + javaMethod;
        
        if (methodCache.containsKey(cacheKey)) {
            return methodCache.get(cacheKey);
        }
        
        Class<?> clazz = Class.forName(javaClass);
        
        // Try to find method with String parameters (most common case)
        Class<?>[] paramTypes = new Class<?>[10]; // Max 10 parameters
        for (int i = 0; i < paramTypes.length; i++) {
            paramTypes[i] = String.class;
        }
        
        Method method = null;
        for (int paramCount = 1; paramCount <= 10; paramCount++) {
            try {
                Class<?>[] actualParamTypes = Arrays.copyOf(paramTypes, paramCount);
                method = clazz.getMethod(javaMethod, actualParamTypes);
                break;
            } catch (NoSuchMethodException e) {
                // Try next parameter count
            }
        }
        
        if (method == null) {
            throw new NoSuchMethodException("Could not find method " + javaMethod + " in class " + javaClass);
        }
        
        methodCache.put(cacheKey, method);
        return method;
    }
    
    private Object getOrCreateInstance(String javaClass) throws Exception {
        if (instanceCache.containsKey(javaClass)) {
            return instanceCache.get(javaClass);
        }
        
        Class<?> clazz = Class.forName(javaClass);
        Object instance = clazz.newInstance();
        instanceCache.put(javaClass, instance);
        
        return instance;
    }
    
    private Map<String, Object> executeSingleTuple(Method method, Object instance, String testId,
                                                 List<String> actedUpon, List<String> consulted,
                                                 Map<String, String> parameters, List<String> tuple, int tupleIndex) 
                                                 throws Exception {
        
        // Build arguments for method invocation
        Object[] args = new Object[method.getParameterCount()];
        Class<?>[] paramTypes = method.getParameterTypes();
        
        logger.debug("Mapping arguments for {}: {} params, tuple size {}", 
            testId, paramTypes.length, tuple.size());
        
        int argIndex = 0;
        
        // Map actedUpon values to method parameters
        for (int i = 0; i < actedUpon.size() && i < tuple.size() && argIndex < args.length; i++) {
            if (paramTypes[argIndex] == String.class) {
                args[argIndex] = tuple.get(i);
                argIndex++;
            } else {
                logger.warn("Parameter[{}] is not String type for actedUpon value: {}", argIndex, paramTypes[argIndex]);
                break;
            }
        }
        
        // Map consulted values if needed
        int consultedStartIndex = actedUpon.size();
        for (int i = 0; i < consulted.size() && argIndex < args.length; i++) {
            int tupleIndex2 = consultedStartIndex + i;
            if (tupleIndex2 < tuple.size() && paramTypes[argIndex] == String.class) {
                args[argIndex] = tuple.get(tupleIndex2);
                argIndex++;
            } else if (paramTypes[argIndex] == String.class) {
                args[argIndex] = "";
                argIndex++;
            }
        }
        
        // Handle additional parameters
        while (argIndex < args.length) {
            if (paramTypes[argIndex] == String.class) {
                String paramValue = null;
                if (parameters != null && !parameters.isEmpty()) {
                    paramValue = parameters.get("bdq:sourceAuthority");
                    if (paramValue == null) {
                        paramValue = parameters.values().iterator().next();
                    }
                }
                args[argIndex] = paramValue != null ? paramValue : "";
                argIndex++;
            } else if (paramTypes[argIndex] == Map.class) {
                args[argIndex] = parameters != null ? parameters : new HashMap<>();
                argIndex++;
            } else {
                args[argIndex] = null;
                argIndex++;
            }
        }
        
        // Execute the method
        logger.debug("Invoking {} with {} args", method.getName(), args.length);
        Object result = method.invoke(instance, args);
        
        // Extract values from DQResponse object
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("tuple_index", tupleIndex);
        
        try {
            // Get the ResultState
            Object resultState = result.getClass().getMethod("getResultState").invoke(result);
            String status = (String) resultState.getClass().getMethod("getLabel").invoke(resultState);
            resultMap.put("status", status);
            
            // Get the comment
            String comment = (String) result.getClass().getMethod("getComment").invoke(result);
            if (comment == null) {
                comment = "Test executed";
            }
            resultMap.put("comment", comment);
            
            // Get the actual result value
            Object value = result.getClass().getMethod("getValue").invoke(result);
            String resultValue = null;
            
            if (value != null) {
                try {
                    resultValue = (String) value.getClass().getMethod("getLabel").invoke(value);
                } catch (Exception e) {
                    resultValue = value.toString();
                }
            }
            resultMap.put("result", resultValue);
            
        } catch (Exception e) {
            logger.error("Error extracting result info: {}", e.getMessage());
            resultMap.put("status", "INTERNAL_PREREQUISITES_NOT_MET");
            resultMap.put("result", null);
            resultMap.put("comment", "Result extraction error: " + e.getMessage());
        }
        
        return resultMap;
    }
    
    /**
     * Health check method
     */
    public String healthCheck() {
        return "BDQ Py4J Gateway is running";
    }
    
    /**
     * Get Java version
     */
    public String getJavaVersion() {
        return System.getProperty("java.version");
    }

    /**
     * Discover available BDQ methods keyed by their annotation label across known classes.
     * Returns a Map: label -> { library, class_name, method_name, annotation_type, annotation_label }
     */
    public Map<String, Map<String, String>> getAvailableMethodsByLabel() {
        Map<String, Map<String, String>> out = new HashMap<>();
        // Classes to scan
        String[] classNames = new String[] {
            "org.filteredpush.qc.metadata.DwCMetadataDQ",
            "org.filteredpush.qc.metadata.DwCMetadataDQDefaults",
            "org.filteredpush.qc.georeference.DwCGeoRefDQ",
            "org.filteredpush.qc.georeference.DwCGeoRefDQDefaults",
            "org.filteredpush.qc.date.DwCEventDQ",
            "org.filteredpush.qc.date.DwCEventDQDefaults",
            "org.filteredpush.qc.date.DwCOtherDateDQ",
            "org.filteredpush.qc.date.DwCOtherDateDQDefaults",
            "org.filteredpush.qc.sciname.DwCSciNameDQ",
            "org.filteredpush.qc.sciname.DwCSciNameDQDefaults"
        };

        Set<String> accepted = new HashSet<>(Arrays.asList("Validation","Amendment","Issue","Measure"));

        for (String className : classNames) {
            try {
                Class<?> clazz = Class.forName(className);
                for (Method m : clazz.getMethods()) {
                    for (java.lang.annotation.Annotation ann : m.getAnnotations()) {
                        String annType = ann.annotationType().getSimpleName();
                        if (!accepted.contains(annType)) continue;
                        try {
                            // All target annotations expose label()
                            Method labelMethod = ann.annotationType().getMethod("label");
                            Object labelObj = labelMethod.invoke(ann);
                            if (labelObj == null) continue;
                            String label = labelObj.toString();
                            if (label.isEmpty()) continue;

                            String fqnClass = clazz.getName();
                            String methodName = m.getName();
                            String pkg = clazz.getPackage().getName();
                            String lib = pkg.substring(pkg.lastIndexOf('.') + 1);
                            if ("metadata".equals(lib)) lib = "rec_occur_qc";
                            else if ("georeference".equals(lib)) lib = "geo_ref_qc";
                            else if ("date".equals(lib)) lib = "event_date_qc";
                            else if ("sciname".equals(lib)) lib = "sci_name_qc";

                            Map<String, String> candidate = new HashMap<>();
                            candidate.put("library", lib);
                            candidate.put("class_name", fqnClass);
                            candidate.put("method_name", methodName);
                            candidate.put("annotation_type", annType);
                            candidate.put("annotation_label", label);

                            Map<String, String> prev = out.get(label);
                            if (prev == null) {
                                out.put(label, candidate);
                            } else {
                                // Prefer non-String suffixed method names if duplicates
                                String prevName = prev.get("method_name");
                                if (prevName != null && prevName.endsWith("String") && !methodName.endsWith("String")) {
                                    out.put(label, candidate);
                                }
                            }
                        } catch (Exception ignore) {
                            // If label() missing, skip
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Discovery error in {}: {}", className, e.getMessage());
            }
        }
        return out;
    }

    /**
     * Find single method info by label.
     */
    public Map<String, String> findMethodByLabel(String label) {
        Map<String, Map<String, String>> all = getAvailableMethodsByLabel();
        Map<String, String> m = all.get(label);
        return m != null ? m : new HashMap<>();
    }
    
    public static void main(String[] args) {
        try {
            BDQGateway gateway = new BDQGateway();
            int port = 25333; // Fixed port for Py4J gateway
            GatewayServer server = new GatewayServer(gateway, port);
            server.start();
            
            // Log startup info to stderr
            System.err.println("🚀 BDQ Py4J Gateway started on port " + port);
            System.err.println("Java version: " + System.getProperty("java.version"));
            
            // Keep the server running
            Thread.currentThread().join();
            
        } catch (Exception e) {
            logger.error("Failed to start BDQ Py4J Gateway", e);
            System.exit(1);
        }
    }
}
