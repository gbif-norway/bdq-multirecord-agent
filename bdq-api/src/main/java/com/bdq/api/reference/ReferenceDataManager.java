package com.bdq.api.reference;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ReferenceDataManager {

    private static final Logger log = LoggerFactory.getLogger(ReferenceDataManager.class);

    private final ReferenceDataProperties properties;
    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;

    private final Path rootDir;
    private final Map<String, DatasetHandle> datasetCache = new ConcurrentHashMap<>();

    public ReferenceDataManager(ReferenceDataProperties properties,
                                ResourceLoader resourceLoader,
                                ObjectMapper objectMapper) throws IOException {
        this.properties = properties;
        this.resourceLoader = resourceLoader;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.rootDir = Path.of(properties.getRootDir());
        Files.createDirectories(rootDir);
    }

    public Path ensureDataset(String name) {
        ReferenceDataProperties.DatasetProperties dataset = getDataset(name);
        DatasetHandle handle = datasetCache.computeIfAbsent(name, k -> new DatasetHandle());
        synchronized (handle) {
            try {
                Path target = rootDir.resolve(dataset.getFilename());
                if (Files.exists(target) && checksumMatches(target, dataset.getChecksum())) {
                    handle.path = target;
                    return target;
                }
                log.info("Reference dataset '{}' missing or outdated; refreshing from {}", name, dataset.getSource());
                Path downloaded = fetchDataset(name, dataset);
                String actualChecksum = computeChecksum(downloaded);
                if (!dataset.getChecksum().equalsIgnoreCase(actualChecksum)) {
                    throw new IllegalStateException("Checksum mismatch for dataset " + name + ": expected "
                            + dataset.getChecksum() + " but found " + actualChecksum);
                }
                writeMetadata(name, dataset, downloaded, actualChecksum);
                handle.path = downloaded;
                return downloaded;
            } catch (IOException e) {
                handle.path = null;
                throw new IllegalStateException("Unable to hydrate dataset " + name, e);
            }
        }
    }

    public Map<String, Path> ensureAll() {
        if (properties.getDatasets().isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Path> resolved = new ConcurrentHashMap<>();
        properties.getDatasets().keySet().forEach(name -> {
            Path path = ensureDataset(name);
            resolved.put(name, path);
        });
        return resolved;
    }

    public void refreshAll(boolean force) {
        Set<String> names = properties.getDatasets().keySet();
        for (String name : names) {
            refreshDataset(name, force);
        }
    }

    public void refreshDataset(String name, boolean force) {
        ReferenceDataProperties.DatasetProperties dataset = getDataset(name);
        DatasetHandle handle = datasetCache.computeIfAbsent(name, k -> new DatasetHandle());
        synchronized (handle) {
            try {
                Path target = rootDir.resolve(dataset.getFilename());
                if (Files.exists(target) && force) {
                    Files.delete(target);
                    Path metadata = metadataPath(target);
                    Files.deleteIfExists(metadata);
                }
                handle.path = null;
                ensureDataset(name);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to refresh dataset " + name, e);
            }
        }
    }

    public Optional<Path> getDatasetPath(String name) {
        DatasetHandle handle = datasetCache.get(name);
        if (handle != null && handle.path != null) {
            return Optional.of(handle.path);
        }
        if (!properties.getDatasets().containsKey(name)) {
            return Optional.empty();
        }
        Path target = rootDir.resolve(properties.getDatasets().get(name).getFilename());
        if (Files.exists(target)) {
            datasetCache.putIfAbsent(name, new DatasetHandle(target));
            return Optional.of(target);
        }
        return Optional.empty();
    }

    public <T> T readJsonDataset(String name, Class<T> type) {
        Path path = ensureDataset(name);
        try {
            return objectMapper.readValue(path.toFile(), type);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse dataset " + name + " as " + type.getSimpleName(), e);
        }
    }

    public <T> T readJsonDataset(String name, TypeReference<T> type) {
        Path path = ensureDataset(name);
        try {
            return objectMapper.readValue(path.toFile(), type);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse dataset " + name + " with supplied type reference", e);
        }
    }

    private ReferenceDataProperties.DatasetProperties getDataset(String name) {
        ReferenceDataProperties.DatasetProperties dataset = properties.getDatasets().get(name);
        if (dataset == null) {
            throw new IllegalArgumentException("Unknown reference dataset: " + name);
        }
        return dataset;
    }

    private Path fetchDataset(String name, ReferenceDataProperties.DatasetProperties dataset) throws IOException {
        String source = dataset.getSource();
        Resource resource = resourceLoader.getResource(source);
        Path target = rootDir.resolve(dataset.getFilename());
        Files.createDirectories(target.getParent());

        if (resource.exists() && resource.isReadable() && !isRemote(source)) {
            try (InputStream in = resource.getInputStream();
                 OutputStream out = Files.newOutputStream(target)) {
                FileCopyUtils.copy(in, out);
            }
            return target;
        }

        if (!isRemote(source)) {
            throw new IOException("Resource " + source + " not found or unreadable");
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(source))
                .GET()
                .build();
        Path tempFile = Files.createTempFile("bdq-reference-", ".tmp");
        try {
            HttpResponse<Path> response = httpClient.send(request, HttpResponse.BodyHandlers.ofFile(tempFile));
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING);
                return target;
            }
            throw new IOException("Unexpected HTTP status " + response.statusCode() + " when downloading " + source);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while downloading " + source, e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private boolean checksumMatches(Path path, String expected) throws IOException {
        String actual = computeChecksum(path);
        return expected.equalsIgnoreCase(actual);
    }

    private String computeChecksum(Path path) throws IOException {
        MessageDigest digest = getDigest();
        try (InputStream in = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private MessageDigest getDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest not available", e);
        }
    }

    private void writeMetadata(String name,
                               ReferenceDataProperties.DatasetProperties dataset,
                               Path downloaded,
                               String actualChecksum) throws IOException {
        DatasetMetadata metadata = new DatasetMetadata(
                name,
                dataset.getSource(),
                dataset.getChecksum(),
                actualChecksum,
                Instant.now(),
                Files.size(downloaded)
        );
        Path metadataPath = metadataPath(downloaded);
        objectMapper.writerWithDefaultPrettyPrinter()
                .writeValue(metadataPath.toFile(), metadata);
    }

    private Path metadataPath(Path dataPath) {
        return dataPath.resolveSibling(dataPath.getFileName() + ".metadata.json");
    }

    private boolean isRemote(String source) {
        String lower = source.toLowerCase();
        return lower.startsWith("http://") || lower.startsWith("https://");
    }

    private static final class DatasetHandle {
        private Path path;

        private DatasetHandle() {
        }

        private DatasetHandle(Path path) {
            this.path = Objects.requireNonNull(path);
        }
    }
}
