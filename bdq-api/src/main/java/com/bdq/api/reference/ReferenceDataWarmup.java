package com.bdq.api.reference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Order(0)
public class ReferenceDataWarmup implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ReferenceDataWarmup.class);

    private final ReferenceDataManager manager;
    private final ReferenceDataProperties properties;
    private final LocalReferenceDataRepository repository;
    private final PointCountryResolver pointCountryResolver;

    public ReferenceDataWarmup(ReferenceDataManager manager,
                               ReferenceDataProperties properties,
                               LocalReferenceDataRepository repository,
                               PointCountryResolver pointCountryResolver) {
        this.manager = manager;
        this.properties = properties;
        this.repository = repository;
        this.pointCountryResolver = pointCountryResolver;
    }

    @Override
    public void run(ApplicationArguments args) {
        Duration timeout = properties.getWarmupTimeout();
        CompletableFuture<Map<String, Path>> future = CompletableFuture.supplyAsync(manager::ensureAll);
        try {
            Map<String, Path> hydrated = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Reference data warmup complete; {} dataset(s) hydrated", hydrated.size());
            repository.warmup();
            pointCountryResolver.rebuildIndex();
        } catch (TimeoutException e) {
            future.cancel(true);
            log.warn("Reference data warmup exceeded timeout of {} seconds. Some datasets may hydrate lazily.", timeout.toSeconds());
        } catch (Exception e) {
            log.error("Reference data warmup failed", e);
        }
    }
}
