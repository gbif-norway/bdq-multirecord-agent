package com.bdq.api.reference;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

@Validated
@ConfigurationProperties(prefix = "reference-data")
public class ReferenceDataProperties {

    @NotNull
    private String rootDir = System.getProperty("user.home") + "/.bdq/reference-data";

    @NotNull
    private Duration warmupTimeout = Duration.ofSeconds(90);

    private Map<String, DatasetProperties> datasets = new LinkedHashMap<>();

    private Map<String, CacheProperties> caches = new LinkedHashMap<>();

    private SpatialProperties spatial = new SpatialProperties();
    private HttpCacheProperties httpCache = new HttpCacheProperties();

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public Duration getWarmupTimeout() {
        return warmupTimeout;
    }

    public void setWarmupTimeout(Duration warmupTimeout) {
        this.warmupTimeout = warmupTimeout;
    }

    public Map<String, DatasetProperties> getDatasets() {
        return datasets;
    }

    public void setDatasets(Map<String, DatasetProperties> datasets) {
        this.datasets = datasets;
    }

    public Map<String, CacheProperties> getCaches() {
        return caches;
    }

    public void setCaches(Map<String, CacheProperties> caches) {
        this.caches = caches;
    }

    public SpatialProperties getSpatial() {
        return spatial;
    }

    public void setSpatial(SpatialProperties spatial) {
        this.spatial = spatial;
    }

    public HttpCacheProperties getHttpCache() {
        return httpCache;
    }

    public void setHttpCache(HttpCacheProperties httpCache) {
        this.httpCache = httpCache;
    }

    public static class DatasetProperties {
        @NotBlank
        private String source;
        @NotBlank
        private String checksum;
        @NotBlank
        private String filename;
        @NotNull
        private DatasetFormat format = DatasetFormat.JSON;
        private boolean warmup = false;

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getChecksum() {
            return checksum;
        }

        public void setChecksum(String checksum) {
            this.checksum = checksum;
        }

        public String getFilename() {
            return filename;
        }

        public void setFilename(String filename) {
            this.filename = filename;
        }

        public DatasetFormat getFormat() {
            return format;
        }

        public void setFormat(DatasetFormat format) {
            this.format = format;
        }

        public boolean isWarmup() {
            return warmup;
        }

        public void setWarmup(boolean warmup) {
            this.warmup = warmup;
        }
    }

    public static class CacheProperties {
        @Min(1)
        private long maximumSize = 10_000;
        @NotNull
        private Duration expireAfter = Duration.ofHours(24);

        public CacheProperties() {}

        public CacheProperties(@DefaultValue("10000") long maximumSize,
                               @DefaultValue("PT24H") Duration expireAfter) {
            this.maximumSize = maximumSize;
            this.expireAfter = expireAfter;
        }

        public long getMaximumSize() {
            return maximumSize;
        }

        public void setMaximumSize(long maximumSize) {
            this.maximumSize = maximumSize;
        }

        public Duration getExpireAfter() {
            return expireAfter;
        }

        public void setExpireAfter(Duration expireAfter) {
            this.expireAfter = expireAfter;
        }
    }

    public static class SpatialProperties {
        private double gridSizeDegrees = 0.1d;

        public double getGridSizeDegrees() {
            return gridSizeDegrees;
        }

        public void setGridSizeDegrees(double gridSizeDegrees) {
            this.gridSizeDegrees = gridSizeDegrees;
        }
    }

    public static class HttpCacheProperties {
        private boolean enabled = true;
        private Duration ttl = Duration.ofHours(12);
        private String directory = "http-cache";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }

        public String getDirectory() {
            return directory;
        }

        public void setDirectory(String directory) {
            this.directory = directory;
        }
    }
}
