package com.bdq.api.reference;

import com.bdq.api.reference.model.IsoCountry;
import com.bdq.api.reference.model.IsoCountryDataset;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.DefaultResourceLoader;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReferenceDataManagerTest {

    private static final String ISO_DATASET = "iso-3166-country-codes";

    @TempDir
    Path tempDir;

    private ReferenceDataManager manager;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws IOException {
        ReferenceDataProperties properties = new ReferenceDataProperties();
        properties.setRootDir(tempDir.toString());
        properties.setWarmupTimeout(Duration.ofSeconds(5));

        ReferenceDataProperties.DatasetProperties dataset = new ReferenceDataProperties.DatasetProperties();
        dataset.setSource("classpath:reference-data/iso-3166.json");
        dataset.setChecksum("234b8fec32dc2addc6a2b4b015636fdf6d4fde782f02a7efc2f4d29ebd715f8b");
        dataset.setFilename("iso-3166.json");
        dataset.setFormat(DatasetFormat.JSON);
        dataset.setWarmup(true);

        Map<String, ReferenceDataProperties.DatasetProperties> datasets = new HashMap<>();
        datasets.put(ISO_DATASET, dataset);
        properties.setDatasets(datasets);

        DefaultResourceLoader resourceLoader = new DefaultResourceLoader();
        objectMapper = new ObjectMapper().findAndRegisterModules();
        manager = new ReferenceDataManager(properties, resourceLoader, objectMapper);
    }

    @Test
    void ensureDatasetCopiesResourceAndWritesMetadata() {
        Path datasetPath = manager.ensureDataset(ISO_DATASET);
        assertThat(datasetPath).exists();
        assertThat(datasetPath.getFileName().toString()).isEqualTo("iso-3166.json");

        Path metadata = datasetPath.resolveSibling(datasetPath.getFileName() + ".metadata.json");
        assertThat(metadata).exists();
    }

    @Test
    void readJsonDatasetReturnsParsedRecords() {
        manager.ensureDataset(ISO_DATASET);
        IsoCountryDataset dataset = manager.readJsonDataset(ISO_DATASET, IsoCountryDataset.class);
        assertThat(dataset.getCountries()).isNotEmpty();
        assertThat(dataset.getCountries().get(0).name()).isNotBlank();
    }
}
