package com.bdq.api.reference;

import com.bdq.api.reference.model.GbifBackboneDataset;
import com.bdq.api.reference.model.GbifBackboneEntry;
import com.bdq.api.reference.model.GbifVocabularyDataset;
import com.bdq.api.reference.model.GbifVocabularyItem;
import com.bdq.api.reference.model.IanaTimeZoneDataset;
import com.bdq.api.reference.model.IanaTimeZoneEntry;
import com.bdq.api.reference.model.IsoCountry;
import com.bdq.api.reference.model.IsoCountryDataset;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Component
public class LocalReferenceDataRepository {

    private static final Logger log = LoggerFactory.getLogger(LocalReferenceDataRepository.class);

    private final ReferenceDataManager dataManager;
    private final ReferenceDataProperties properties;

    private final AtomicReference<List<IsoCountry>> isoCountries = new AtomicReference<>(List.of());
    private final AtomicReference<Map<String, IsoCountry>> isoByAlpha2 = new AtomicReference<>(Map.of());
    private final AtomicReference<Map<String, IsoCountry>> isoByAlpha3 = new AtomicReference<>(Map.of());

    private final AtomicReference<Map<String, List<GbifVocabularyItem>>> vocabularies = new AtomicReference<>(Map.of());
    private final AtomicReference<GbifBackboneDataset> backboneDataset = new AtomicReference<>();
    private final AtomicReference<Map<String, GbifBackboneEntry>> backboneByCanonical = new AtomicReference<>(Map.of());
    private final AtomicReference<IanaTimeZoneDataset> timeZoneDataset = new AtomicReference<>();

    public LocalReferenceDataRepository(ReferenceDataManager dataManager,
                                        ReferenceDataProperties properties) {
        this.dataManager = dataManager;
        this.properties = properties;
    }

    public void warmup() {
        properties.getDatasets().forEach((name, cfg) -> {
            if (cfg.isWarmup()) {
                try {
                    loadDataset(name);
                } catch (Exception ex) {
                    log.warn("Failed to warm dataset '{}': {}", name, ex.getMessage());
                }
            }
        });
    }

    public void loadDataset(String name) {
        switch (name) {
            case "iso-3166-country-codes" -> loadIsoCountries();
            case "gbif-vocabularies" -> loadGbifVocabularies();
            case "gbif-backbone-taxonomy" -> loadBackbone();
            case "iana-time-zones" -> loadTimeZones();
            default -> {
                // do nothing for now
            }
        }
    }

    public Optional<IsoCountry> findCountryByAlpha2(String alpha2) {
        if (alpha2 == null) return Optional.empty();
        Map<String, IsoCountry> map = isoByAlpha2.get();
        if (map.isEmpty()) {
            loadIsoCountries();
            map = isoByAlpha2.get();
        }
        return Optional.ofNullable(map.get(alpha2.toUpperCase()));
    }

    public Optional<IsoCountry> findCountryByAlpha3(String alpha3) {
        if (alpha3 == null) return Optional.empty();
        Map<String, IsoCountry> map = isoByAlpha3.get();
        if (map.isEmpty()) {
            loadIsoCountries();
            map = isoByAlpha3.get();
        }
        return Optional.ofNullable(map.get(alpha3.toUpperCase()));
    }

    public List<IsoCountry> getAllCountries() {
        List<IsoCountry> list = isoCountries.get();
        if (list.isEmpty()) {
            loadIsoCountries();
            list = isoCountries.get();
        }
        return list;
    }

    public Optional<List<GbifVocabularyItem>> getVocabulary(String vocabName) {
        if (vocabName == null) return Optional.empty();
        Map<String, List<GbifVocabularyItem>> map = vocabularies.get();
        if (map.isEmpty()) {
            loadGbifVocabularies();
            map = vocabularies.get();
        }
        return Optional.ofNullable(map.get(vocabName));
    }

    public Optional<GbifBackboneEntry> findBackboneByCanonical(String canonicalName) {
        if (canonicalName == null) return Optional.empty();
        Map<String, GbifBackboneEntry> map = backboneByCanonical.get();
        if (map.isEmpty()) {
            loadBackbone();
            map = backboneByCanonical.get();
        }
        return Optional.ofNullable(map.get(canonicalName.toLowerCase()));
    }

    public List<GbifBackboneEntry> getAllBackboneEntries() {
        GbifBackboneDataset dataset = backboneDataset.get();
        if (dataset == null || dataset.getRanks() == null) {
            loadBackbone();
            dataset = backboneDataset.get();
        }
        if (dataset == null || dataset.getRanks() == null) {
            return List.of();
        }
        return List.copyOf(dataset.getRanks());
    }

    public Optional<IanaTimeZoneDataset> getTimeZones() {
        IanaTimeZoneDataset dataset = timeZoneDataset.get();
        if (dataset == null) {
            loadTimeZones();
            dataset = timeZoneDataset.get();
        }
        return Optional.ofNullable(dataset);
    }

    private void loadIsoCountries() {
        IsoCountryDataset dataset = dataManager.readJsonDataset(
                "iso-3166-country-codes",
                IsoCountryDataset.class
        );
        List<IsoCountry> countries = Optional.ofNullable(dataset.getCountries()).orElse(List.of());
        Map<String, IsoCountry> byAlpha2 = countries.stream()
                .filter(country -> country.alpha2() != null)
                .collect(Collectors.toMap(country -> country.alpha2().toUpperCase(), country -> country, (a, b) -> a, ConcurrentHashMap::new));
        Map<String, IsoCountry> byAlpha3 = countries.stream()
                .filter(country -> country.alpha3() != null)
                .collect(Collectors.toMap(country -> country.alpha3().toUpperCase(), country -> country, (a, b) -> a, ConcurrentHashMap::new));

        isoCountries.set(List.copyOf(countries));
        isoByAlpha2.set(byAlpha2);
        isoByAlpha3.set(byAlpha3);
        log.info("Loaded {} ISO 3166 country codes into memory", countries.size());
    }

    private void loadGbifVocabularies() {
        GbifVocabularyDataset dataset = dataManager.readJsonDataset(
                "gbif-vocabularies",
                GbifVocabularyDataset.class
        );
        Map<String, List<GbifVocabularyItem>> map = Optional.ofNullable(dataset.getVocabularies())
                .orElse(Collections.emptyMap())
                .entrySet()
                .stream()
                .collect(Collectors.toConcurrentMap(
                        entry -> entry.getKey().toLowerCase(),
                        entry -> List.copyOf(entry.getValue())
                ));
        vocabularies.set(map);
        log.info("Loaded GBIF vocabularies {} into memory", map.keySet());
    }

    private void loadBackbone() {
        GbifBackboneDataset dataset = dataManager.readJsonDataset(
                "gbif-backbone-taxonomy",
                GbifBackboneDataset.class
        );
        backboneDataset.set(dataset);
        Map<String, GbifBackboneEntry> byCanonical = Optional.ofNullable(dataset.getRanks())
                .orElse(List.of())
                .stream()
                .filter(entry -> entry.canonicalName() != null)
                .collect(Collectors.toConcurrentMap(
                        entry -> entry.canonicalName().toLowerCase(),
                        entry -> entry,
                        (a, b) -> a
                ));
        backboneByCanonical.set(byCanonical);
        log.info("Loaded {} GBIF backbone entries", byCanonical.size());
    }

    private void loadTimeZones() {
        IanaTimeZoneDataset dataset = dataManager.readJsonDataset(
                "iana-time-zones",
                IanaTimeZoneDataset.class
        );
        timeZoneDataset.set(dataset);
        int count = Optional.ofNullable(dataset.getZones()).map(List::size).orElse(0);
        log.info("Loaded {} IANA time zones", count);
    }
}
