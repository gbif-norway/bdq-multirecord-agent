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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/v1/tests")
@Tag(name = "BDQ Tests", description = "List and run BDQ tests implemented by FilteredPush libraries")
public class TestsController {

    private static final Logger log = LoggerFactory.getLogger(TestsController.class);
    private static final long SLOW_ITEM_WARN_MS = 5000L;

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

        long batchStartNs = System.nanoTime();
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        String primaryTestId = requests.get(0) != null ? requests.get(0).getId() : null;
        boolean homogeneousIds = true;
        for (RunTestRequest r : requests) {
            String currentId = (r != null) ? r.getId() : null;
            if (!Objects.equals(primaryTestId, currentId)) {
                homogeneousIds = false;
                break;
            }
        }

        int availableCpus = Math.max(Runtime.getRuntime().availableProcessors(), 1);
        int maxThreads = Math.min(Math.max(availableCpus, 1), 8);
        log.info(
                "runBatch start: requestId={}, size={}, threads={}, primaryTestId={}, homogeneousIds={}",
                requestId,
                size,
                maxThreads,
                primaryTestId,
                homogeneousIds
        );

        if (size == 1) {
            RunTestRequest single = requests.get(0);
            String singleTestId = (single != null) ? single.getId() : null;
            long singleStartNs = System.nanoTime();
            try {
                ValidationResponse response = runSingle(single);
                long singleDurationMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - singleStartNs);
                log.info(
                        "runBatch complete: requestId={}, size=1, failed=0, durationMs={}, testId={}",
                        requestId,
                        singleDurationMs,
                        singleTestId
                );
                return List.of(response);
            } catch (Exception e) {
                String msg = (e.getMessage() != null) ? e.getMessage() : e.getClass().getSimpleName();
                long singleDurationMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - singleStartNs);
                log.error(
                        "runBatch item failed: requestId={}, index=0, testId={}, errorType={}, message={}",
                        requestId,
                        singleTestId,
                        e.getClass().getSimpleName(),
                        msg
                );
                log.info(
                        "runBatch complete: requestId={}, size=1, failed=1, durationMs={}, testId={}",
                        requestId,
                        singleDurationMs,
                        singleTestId
                );
                return List.of(new ValidationResponse("INTERNAL_PREREQUISITES_NOT_MET", "", msg));
            }
        }

        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(maxThreads);
        try {
            List<java.util.concurrent.CompletableFuture<ValidationResponse>> futures = new ArrayList<>(size);
            AtomicInteger completedCount = new AtomicInteger(0);
            for (int i = 0; i < size; i++) {
                final int itemIndex = i;
                final RunTestRequest r = requests.get(i);
                futures.add(java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                    String itemTestId = (r != null) ? r.getId() : null;
                    long itemStartNs = System.nanoTime();
                    try {
                        ValidationResponse response = runSingle(r);
                        long itemDurationMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - itemStartNs);
                        if (itemDurationMs >= SLOW_ITEM_WARN_MS) {
                            log.warn(
                                    "runBatch slow item: requestId={}, index={}, testId={}, durationMs={}, status={}, result={}",
                                    requestId,
                                    itemIndex,
                                    itemTestId,
                                    itemDurationMs,
                                    response != null ? response.getStatus() : null,
                                    response != null ? response.getResult() : null
                            );
                        }
                        return response;
                    } catch (Exception e) {
                        // Do not fail whole batch; map error to a response shape
                        String msg = (e.getMessage() != null) ? e.getMessage() : e.getClass().getSimpleName();
                        log.error(
                                "runBatch item failed: requestId={}, index={}, testId={}, errorType={}, message={}",
                                requestId,
                                itemIndex,
                                itemTestId,
                                e.getClass().getSimpleName(),
                                msg
                        );
                        return new ValidationResponse("INTERNAL_PREREQUISITES_NOT_MET", "", msg);
                    } finally {
                        int done = completedCount.incrementAndGet();
                        if (done == size || done % 500 == 0) {
                            log.info("runBatch progress: requestId={}, completed={}/{}", requestId, done, size);
                        }
                    }
                }, pool));
            }
            List<ValidationResponse> out = new ArrayList<>(size);
            for (java.util.concurrent.CompletableFuture<ValidationResponse> f : futures) {
                out.add(f.join());
            }

            long failed = out.stream()
                    .filter(Objects::nonNull)
                    .filter(r -> "INTERNAL_PREREQUISITES_NOT_MET".equals(r.getStatus()))
                    .count();
            long batchDurationMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStartNs);
            log.info(
                    "runBatch complete: requestId={}, size={}, failed={}, durationMs={}",
                    requestId,
                    size,
                    failed,
                    batchDurationMs
            );
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
