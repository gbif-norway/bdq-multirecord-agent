package com.bdq.api.http;

import com.bdq.api.reference.ReferenceDataProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.ResponseCache;
import java.nio.file.Path;

@Component
public class HttpCacheInitializer {

    private static final Logger log = LoggerFactory.getLogger(HttpCacheInitializer.class);

    private final ReferenceDataProperties properties;
    private final ObjectMapper objectMapper;

    public HttpCacheInitializer(ReferenceDataProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    void registerCache() {
        if (!properties.getHttpCache().isEnabled()) {
            return;
        }
        if (ResponseCache.getDefault() instanceof OfflineResponseCache) {
            return;
        }
        Path cacheDir = Path.of(properties.getRootDir(), properties.getHttpCache().getDirectory());
        try {
            OfflineResponseCache cache = new OfflineResponseCache(cacheDir, properties.getHttpCache().getTtl(), objectMapper);
            cache.registerProvider(new GbifResponseProvider());
            ResponseCache.setDefault(cache);
            log.info("HTTP response cache initialised at {}", cacheDir);
        } catch (IOException e) {
            log.warn("Unable to initialise HTTP response cache at {}: {}", cacheDir, e.getMessage());
        } catch (Throwable t) {
            log.warn("Response caching not available: {}", t.getMessage());
        }
    }
}
