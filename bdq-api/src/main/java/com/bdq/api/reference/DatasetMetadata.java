package com.bdq.api.reference;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;

public class DatasetMetadata {
    private String name;
    private String source;
    private String expectedChecksum;
    private String actualChecksum;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant downloadedAt;
    private long sizeBytes;

    public DatasetMetadata() {
    }

    public DatasetMetadata(String name, String source, String expectedChecksum, String actualChecksum, Instant downloadedAt, long sizeBytes) {
        this.name = name;
        this.source = source;
        this.expectedChecksum = expectedChecksum;
        this.actualChecksum = actualChecksum;
        this.downloadedAt = downloadedAt;
        this.sizeBytes = sizeBytes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getExpectedChecksum() {
        return expectedChecksum;
    }

    public void setExpectedChecksum(String expectedChecksum) {
        this.expectedChecksum = expectedChecksum;
    }

    public String getActualChecksum() {
        return actualChecksum;
    }

    public void setActualChecksum(String actualChecksum) {
        this.actualChecksum = actualChecksum;
    }

    public Instant getDownloadedAt() {
        return downloadedAt;
    }

    public void setDownloadedAt(Instant downloadedAt) {
        this.downloadedAt = downloadedAt;
    }

    public long getSizeBytes() {
        return sizeBytes;
    }

    public void setSizeBytes(long sizeBytes) {
        this.sizeBytes = sizeBytes;
    }
}
