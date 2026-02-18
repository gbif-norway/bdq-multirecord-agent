package com.bdq.api.controller;

import com.bdq.api.model.RunTestRequest;
import com.bdq.api.model.TestInfo;
import com.bdq.api.model.ValidationResponse;
import com.bdq.api.runner.ParameterDefaultsProvider;
import com.bdq.api.runner.TestRegistry;
import com.bdq.api.util.FFDQMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.datakurator.ffdq.annotations.ActedUpon;
import org.datakurator.ffdq.annotations.Consulted;
// Avoid name clash with java.lang.reflect.Parameter by referencing fully-qualified name when needed
import org.datakurator.ffdq.api.DQResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

@RestController
@RequestMapping("/api/v1/tests")
@Tag(name = "BDQ Tests", description = "List and run BDQ tests implemented by FilteredPush libraries")
public class TestsController {

    @Autowired
    private TestRegistry registry;

    @Autowired
    private ParameterDefaultsProvider parameterDefaults;

    @GetMapping
    @Operation(summary = "List discovered tests (from FilteredPush libraries)")
    public List<TestInfo> listTests() {
        return registry.list();
    }

    @PostMapping("/run")
    @Operation(summary = "Run a test by BDQ label or GUID with provided parameters")
    public ValidationResponse runTest(@RequestBody RunTestRequest req) throws Exception {
        return runSingle(req);
    }

    @PostMapping("/run/batch")
    @Operation(summary = "Run multiple tests; returns results in input order")
    public List<ValidationResponse> runBatch(@RequestBody List<RunTestRequest> requests) throws Exception {
        if (requests == null) return Collections.emptyList();

        int size = requests.size();
        if (size == 0) return Collections.emptyList();

        int maxThreads = Math.min(Math.max(Runtime.getRuntime().availableProcessors(), 2), 8);
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(maxThreads);
        try {
            List<java.util.concurrent.CompletableFuture<ValidationResponse>> futures = new ArrayList<>(size);
            for (RunTestRequest r : requests) {
                futures.add(java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                    try {
                        return runSingle(r);
                    } catch (Exception e) {
                        // Do not fail whole batch; map error to a response shape
                        String msg = (e.getMessage() != null) ? e.getMessage() : e.getClass().getSimpleName();
                        return new ValidationResponse("INTERNAL_PREREQUISITES_NOT_MET", "", msg);
                    }
                }, pool));
            }
            List<ValidationResponse> out = new ArrayList<>(size);
            for (java.util.concurrent.CompletableFuture<ValidationResponse> f : futures) {
                out.add(f.join());
            }
            return out;
        } finally {
            pool.shutdown();
        }
    }

    private ValidationResponse runSingle(RunTestRequest req) throws Exception {
        if (req == null || req.getId() == null || req.getId().isEmpty()) {
            throw new BadRequest("Missing id (BDQ label or GUID)");
        }
        Map<String, String> params = new HashMap<>(Optional.ofNullable(req.getParams()).orElse(Collections.emptyMap()));

        TestRegistry.Entry entry = registry.find(req.getId())
                .orElseThrow(() -> new NotFound("Unknown test id or guid: " + req.getId()));
        Method m = entry.method;

        parameterDefaults.applyDefaults(m, params);

        Object[] args = new Object[m.getParameterCount()];
        Annotation[][] paramAnns = m.getParameterAnnotations();
        Class<?>[] paramTypes = m.getParameterTypes();

        for (int i = 0; i < paramAnns.length; i++) {
            String key = null;
            for (Annotation a : paramAnns[i]) {
                if (a instanceof ActedUpon) { key = ((ActedUpon) a).value(); break; }
                if (a instanceof Consulted) { key = ((Consulted) a).value(); break; }
                if (a instanceof org.datakurator.ffdq.annotations.Parameter) {
                    key = ((org.datakurator.ffdq.annotations.Parameter) a).name();
                    break;
                }
            }
            String value = (key != null) ? params.get(key) : null;

            // Bind only strings directly; pass null for non-String to allow defaults in FP methods
            if (paramTypes[i] == String.class) {
                args[i] = value;
            } else {
                args[i] = null;
            }
        }

        Object result = m.invoke(null, args);
        if (!(result instanceof DQResponse)) {
            throw new IllegalStateException("Test did not return a DQResponse");
        }
        @SuppressWarnings("unchecked")
        DQResponse<?> dq = (DQResponse<?>) result;
        return FFDQMapper.toValidationResponse(dq);
    }

    @ResponseStatus(HttpStatus.BAD_REQUEST)
    private static class BadRequest extends RuntimeException { BadRequest(String m) { super(m); } }
    @ResponseStatus(HttpStatus.NOT_FOUND)
    private static class NotFound extends RuntimeException { NotFound(String m) { super(m); } }
}
