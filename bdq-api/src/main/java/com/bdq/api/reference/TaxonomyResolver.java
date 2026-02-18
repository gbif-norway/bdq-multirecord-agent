package com.bdq.api.reference;

import com.bdq.api.cache.CacheNames;
import com.bdq.api.cache.ExternalResponseCache;
import com.bdq.api.reference.model.GbifBackboneEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

@Component
public class TaxonomyResolver {

    private static final Logger log = LoggerFactory.getLogger(TaxonomyResolver.class);

    private final LocalReferenceDataRepository repository;
    private final ExternalResponseCache cache;

    public TaxonomyResolver(LocalReferenceDataRepository repository, ExternalResponseCache cache) {
        this.repository = repository;
        this.cache = cache;
    }

    public Optional<GbifBackboneEntry> resolveLocal(String canonicalName) {
        if (canonicalName == null || canonicalName.isBlank()) {
            return Optional.empty();
        }
        return repository.findBackboneByCanonical(canonicalName);
    }

    public <T> T resolveGbifWithCache(String canonicalName, Supplier<T> remoteLookup) {
        if (canonicalName == null || canonicalName.isBlank()) {
            return null;
        }
        String key = canonicalName.toLowerCase(Locale.ROOT);
        return cache.compute(CacheNames.GBIF_TAXONOMY, key, () -> {
            log.debug("Cache miss for GBIF taxonomy '{}'; invoking remote loader", canonicalName);
            return remoteLookup.get();
        });
    }
}
