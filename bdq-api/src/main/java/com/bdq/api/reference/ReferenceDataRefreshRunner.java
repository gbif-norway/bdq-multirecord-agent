package com.bdq.api.reference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Order(-10)
public class ReferenceDataRefreshRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ReferenceDataRefreshRunner.class);

    private final ReferenceDataManager manager;
    private final ReferenceDataProperties properties;
    private final LocalReferenceDataRepository repository;
    private final PointCountryResolver pointCountryResolver;

    public ReferenceDataRefreshRunner(ReferenceDataManager manager,
                                      ReferenceDataProperties properties,
                                      LocalReferenceDataRepository repository,
                                      PointCountryResolver pointCountryResolver) {
        this.manager = manager;
        this.properties = properties;
        this.repository = repository;
        this.pointCountryResolver = pointCountryResolver;
    }

    @Override
    public void run(ApplicationArguments args) {
        Set<String> datasetNames = properties.getDatasets().keySet();
        if (datasetNames.isEmpty()) {
            return;
        }

        Collection<String> refreshFlags = args.getOptionValues("refresh-reference-data");
        Collection<String> refreshNames = args.getOptionValues("reference-data.refresh");
        boolean refreshAll = args.containsOption("refresh-reference-data")
                && (refreshFlags == null || refreshFlags.isEmpty());

        if (refreshAll) {
            log.info("Refreshing all reference datasets (forced download requested)");
            manager.refreshAll(true);
            repository.warmup();
            pointCountryResolver.rebuildIndex();
            return;
        }

        List<String> targets = Stream.concat(valuesOrEmpty(refreshFlags).stream(),
                                             valuesOrEmpty(refreshNames).stream())
                .flatMap(flag -> Stream.of(flag.split(",")))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .distinct()
                .collect(Collectors.toList());

        if (targets.isEmpty()) {
            return;
        }

        for (String target : targets) {
            if (!datasetNames.contains(target)) {
                log.warn("Requested refresh for unknown dataset '{}'. Known datasets: {}", target, datasetNames);
                continue;
            }
            log.info("Refreshing reference dataset '{}'", target);
            manager.refreshDataset(target, true);
            repository.loadDataset(target);
            if ("natural-earth-admin0".equals(target)) {
                pointCountryResolver.rebuildIndex();
            }
        }
    }

    private List<String> valuesOrEmpty(Collection<String> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream()
                .map(v -> v == null ? "" : v)
                .collect(Collectors.toList());
    }
}
