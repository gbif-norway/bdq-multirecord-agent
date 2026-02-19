package com.bdq.api.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

@Component
public class ExternalResponseCache {

    private static final Logger log = LoggerFactory.getLogger(ExternalResponseCache.class);

    private final CacheManager cacheManager;

    public ExternalResponseCache(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    public <K, V> V compute(String cacheName, K key, Supplier<V> valueLoader) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache == null) {
            log.warn("Cache '{}' not configured; falling back to uncached load", cacheName);
            return valueLoader.get();
        }
        return cache.get(key, () -> {
            V value = valueLoader.get();
            if (value == null) {
                log.debug("Cache '{}' miss for key {}; loader returned null", cacheName, key);
            }
            return value;
        });
    }

    public void evict(String cacheName, Object key) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            cache.evictIfPresent(key);
        }
    }

    public void clear(String cacheName) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache != null) {
            cache.clear();
        }
    }

    public long size(String cacheName) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache == null) {
            return 0L;
        }
        Object nativeCache = cache.getNativeCache();
        if (nativeCache instanceof com.github.benmanes.caffeine.cache.Cache<?, ?> caffeineCache) {
            return caffeineCache.estimatedSize();
        }
        return 0L;
    }

    public Optional<com.github.benmanes.caffeine.cache.Cache<?, ?>> asCaffeine(String cacheName) {
        Cache cache = cacheManager.getCache(cacheName);
        if (cache == null) {
            return Optional.empty();
        }
        Object nativeCache = cache.getNativeCache();
        if (nativeCache instanceof com.github.benmanes.caffeine.cache.Cache<?, ?> caffeineCache) {
            return Optional.of(caffeineCache);
        }
        return Optional.empty();
    }
}
