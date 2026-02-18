package com.bdq.api.config;

import com.bdq.api.reference.ReferenceDataProperties;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(ReferenceDataProperties.class)
public class ReferenceDataConfig {

    @Bean
    public CacheManager cacheManager(ReferenceDataProperties properties) {
        Map<String, ReferenceDataProperties.CacheProperties> cacheConfigs = properties.getCaches();
        List<Cache> caches = new ArrayList<>();
        if (cacheConfigs != null) {
            cacheConfigs.forEach((name, cfg) -> {
                Duration ttl = cfg.getExpireAfter();
                Caffeine<Object, Object> builder = Caffeine.newBuilder()
                        .recordStats()
                        .maximumSize(cfg.getMaximumSize());
                if (ttl != null && !ttl.isZero()) {
                    builder.expireAfterWrite(ttl);
                }
                caches.add(new CaffeineCache(name, builder.build()));
            });
        }
        SimpleCacheManager manager = new SimpleCacheManager();
        manager.setCaches(caches);
        return manager;
    }
}
