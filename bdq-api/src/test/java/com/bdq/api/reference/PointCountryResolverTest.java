package com.bdq.api.reference;

import com.bdq.api.cache.CacheNames;
import com.bdq.api.cache.ExternalResponseCache;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.core.io.DefaultResourceLoader;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PointCountryResolverTest {

    @TempDir
    Path tempDir;

    private PointCountryResolver resolver;

    @BeforeEach
    void setUp() throws IOException {
        ReferenceDataProperties properties = new ReferenceDataProperties();
        properties.setRootDir(tempDir.toString());
        properties.setWarmupTimeout(Duration.ofSeconds(5));
        properties.getSpatial().setGridSizeDegrees(0.1d);

        ReferenceDataProperties.DatasetProperties boundaries = new ReferenceDataProperties.DatasetProperties();
        boundaries.setSource("classpath:reference-data/natural-earth-admin0.geojson");
        boundaries.setChecksum("45f41865adec4f86602c2cd05c0e29cd8b437614bf2f5b5a863d12463202cae4");
        boundaries.setFilename("natural-earth-admin0.geojson");
        boundaries.setFormat(DatasetFormat.GEOJSON);
        boundaries.setWarmup(true);

        Map<String, ReferenceDataProperties.DatasetProperties> datasets = new HashMap<>();
        datasets.put("natural-earth-admin0", boundaries);
        properties.setDatasets(datasets);

        ReferenceDataProperties.CacheProperties cacheProps = new ReferenceDataProperties.CacheProperties();
        cacheProps.setMaximumSize(1000);
        cacheProps.setExpireAfter(Duration.ofHours(1));
        properties.setCaches(Map.of(CacheNames.POINT_TO_COUNTRY, cacheProps));

        DefaultResourceLoader resourceLoader = new DefaultResourceLoader();
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        ReferenceDataManager manager = new ReferenceDataManager(properties, resourceLoader, objectMapper);

        SimpleCacheManager cacheManager = new SimpleCacheManager();
        cacheManager.setCaches(List.of(new CaffeineCache(CacheNames.POINT_TO_COUNTRY,
                Caffeine.newBuilder().maximumSize(10_000).build())));
        cacheManager.afterPropertiesSet();

        resolver = new PointCountryResolver(manager, properties, objectMapper, new ExternalResponseCache(cacheManager));
        resolver.rebuildIndex();
    }

    @Test
    void resolvesUnitedStatesWithinPolygon() {
        assertThat(resolver.resolveCountry(40.7128, -74.0060)).contains("US");
    }

    @Test
    void resolvesCanadaWithinPolygon() {
        assertThat(resolver.resolveCountry(51.05, -114.07)).contains("CA");
    }

    @Test
    void returnsEmptyWhenOutsideKnownPolygons() {
        assertThat(resolver.resolveCountry(-10.0, -140.0)).isEmpty();
    }
}
