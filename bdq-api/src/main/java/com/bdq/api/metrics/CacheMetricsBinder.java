package com.bdq.api.metrics;

import com.github.benmanes.caffeine.cache.Cache;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

@Component
public class CacheMetricsBinder {

    private static final Logger log = LoggerFactory.getLogger(CacheMetricsBinder.class);

    private final CacheManager cacheManager;
    private final MeterRegistry meterRegistry;

    public CacheMetricsBinder(CacheManager cacheManager, MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    public void bind() {
        for (String cacheName : cacheManager.getCacheNames()) {
            org.springframework.cache.Cache cache = cacheManager.getCache(cacheName);
            if (cache instanceof CaffeineCache caffeineCache) {
                Object nativeCache = caffeineCache.getNativeCache();
                if (nativeCache instanceof Cache<?, ?> delegate) {
                    CaffeineCacheMetrics.monitor(meterRegistry, delegate, cacheName);
                    log.debug("Registered cache metrics for {}", cacheName);
                }
            }
        }
    }
}
