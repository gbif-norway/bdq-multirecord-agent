package com.bdq.api.metrics;

import com.bdq.api.reference.ReferenceDataManager;
import com.bdq.api.reference.ReferenceDataProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Component
public class ReferenceDataHealthIndicator implements HealthIndicator {

    private final ReferenceDataManager manager;
    private final ReferenceDataProperties properties;

    public ReferenceDataHealthIndicator(ReferenceDataManager manager, ReferenceDataProperties properties) {
        this.manager = manager;
        this.properties = properties;
    }

    @Override
    public Health health() {
        Map<String, Object> details = new HashMap<>();
        boolean allPresent = true;
        for (Map.Entry<String, ReferenceDataProperties.DatasetProperties> entry : properties.getDatasets().entrySet()) {
            String name = entry.getKey();
            Optional<Path> path = manager.getDatasetPath(name);
            boolean present = path.isPresent();
            details.put(name, present ? path.get().toString() : "missing");
            if (!present) {
                allPresent = false;
            }
        }
        if (allPresent) {
            return Health.up().withDetails(details).build();
        }
        return Health.down().withDetails(details).build();
    }
}
