package com.bdq.api.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.CacheRequest;
import java.net.CacheResponse;
import java.net.ResponseCache;
import java.net.URI;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class OfflineResponseCache extends ResponseCache {

    private static final Logger log = LoggerFactory.getLogger(OfflineResponseCache.class);

    private final Path cacheDir;
    private final Duration ttl;
    private final ObjectMapper mapper;
    private final CopyOnWriteArrayList<OfflineResponseProvider> providers = new CopyOnWriteArrayList<>();

    public OfflineResponseCache(Path cacheDir, Duration ttl, ObjectMapper mapper) throws IOException {
        this.cacheDir = cacheDir;
        this.ttl = ttl;
        this.mapper = mapper;
        Files.createDirectories(cacheDir);
    }

    public void registerProvider(OfflineResponseProvider provider) {
        if (provider != null) {
            providers.add(provider);
        }
    }

    @Override
    public CacheResponse get(URI uri, String rqstMethod, Map<String, List<String>> rqstHeaders) throws IOException {
        for (OfflineResponseProvider provider : providers) {
            Optional<ResponseContent> provided = provider.resolve(uri, rqstMethod, rqstHeaders);
            if (provided.isPresent()) {
                ResponseContent rc = provided.get();
                return new MemoryCacheResponse(rc.headers(), rc.body());
            }
        }
        Path payload = bodyPath(uri, rqstMethod);
        Path meta = metadataPath(uri, rqstMethod);
        if (!Files.exists(payload) || !Files.exists(meta)) {
            return null;
        }
        CacheMetadata metadata = readMetadata(meta);
        if (metadata == null) {
            return null;
        }
        if (ttl != null && !ttl.isZero()) {
            Instant expiresAt = metadata.timestamp().plus(ttl);
            if (Instant.now().isAfter(expiresAt)) {
                log.debug("Cached response for {} expired; ignoring", uri);
                return null;
            }
        }
        return new StoredCacheResponse(payload, metadata);
    }

    @Override
    public CacheRequest put(URI uri, URLConnection conn) throws IOException {
        if (!"GET".equalsIgnoreCase(conn.getURL().getProtocol())) {
            // only cache http(s) GET for now
        }
        String method = "GET";
        if (conn instanceof java.net.HttpURLConnection http) {
            method = Optional.ofNullable(http.getRequestMethod()).orElse("GET");
        }
        if (!"GET".equalsIgnoreCase(method)) {
            return null;
        }
        Path payload = bodyPath(uri, method);
        Path meta = metadataPath(uri, method);
        Files.createDirectories(payload.getParent());
        Files.createDirectories(meta.getParent());
        Map<String, List<String>> headers = conn.getHeaderFields();
        CacheMetadata metadata = new CacheMetadata(Instant.now(), Optional.ofNullable(headers).orElseGet(HashMap::new));
        return new FileCacheRequest(payload, meta, metadata, mapper);
    }

    private Path bodyPath(URI uri, String method) {
        return cacheDir.resolve(hashKey(uri, method) + ".body");
    }

    private Path metadataPath(URI uri, String method) {
        return cacheDir.resolve(hashKey(uri, method) + ".meta.json");
    }

    private String hashKey(URI uri, String method) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String key = method + "::" + uri.toString();
            byte[] hash = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private CacheMetadata readMetadata(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            return mapper.readValue(in, CacheMetadata.class);
        } catch (IOException e) {
            log.debug("Failed to read cache metadata {}: {}", path, e.getMessage());
            return null;
        }
    }

    private record CacheMetadata(Instant timestamp, Map<String, List<String>> headers) {
    }

    private static class StoredCacheResponse extends CacheResponse {

        private final Path payload;
        private final CacheMetadata metadata;

        private StoredCacheResponse(Path payload, CacheMetadata metadata) {
            this.payload = payload;
            this.metadata = metadata;
        }

        @Override
        public Map<String, List<String>> getHeaders() throws IOException {
            return metadata.headers();
        }

        @Override
        public InputStream getBody() throws IOException {
            return Files.newInputStream(payload);
        }
    }

    private static class FileCacheRequest extends CacheRequest {

        private final Path payloadPath;
        private final Path metadataPath;
        private final CacheMetadata metadata;
        private final ObjectMapper mapper;

        private boolean aborted = false;

        private FileCacheRequest(Path payloadPath, Path metadataPath, CacheMetadata metadata, ObjectMapper mapper) {
            this.payloadPath = payloadPath;
            this.metadataPath = metadataPath;
            this.metadata = metadata;
            this.mapper = mapper;
        }

        @Override
        public OutputStream getBody() throws IOException {
            final OutputStream delegate = Files.newOutputStream(payloadPath);
            return new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    delegate.write(b);
                }

                @Override
                public void write(byte[] b) throws IOException {
                    delegate.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    delegate.write(b, off, len);
                }

                @Override
                public void flush() throws IOException {
                    delegate.flush();
                }

                @Override
                public void close() throws IOException {
                    try {
                        delegate.close();
                        if (!aborted) {
                            mapper.writerWithDefaultPrettyPrinter().writeValue(metadataPath.toFile(), metadata);
                        }
                    } finally {
                        if (aborted) {
                            Files.deleteIfExists(payloadPath);
                            Files.deleteIfExists(metadataPath);
                        }
                    }
                }
            };
        }

        @Override
        public void abort() {
            aborted = true;
        }
    }

    private static class MemoryCacheResponse extends CacheResponse {

        private final Map<String, List<String>> headers;
        private final byte[] body;

        private MemoryCacheResponse(Map<String, List<String>> headers, byte[] body) {
            this.headers = headers != null ? headers : Map.of();
            this.body = body != null ? body : new byte[0];
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return headers;
        }

        @Override
        public InputStream getBody() {
            return new java.io.ByteArrayInputStream(body);
        }
    }

    public interface OfflineResponseProvider {
        Optional<ResponseContent> resolve(URI uri, String method, Map<String, List<String>> headers) throws IOException;
    }

    public record ResponseContent(Map<String, List<String>> headers, byte[] body) {}
}
