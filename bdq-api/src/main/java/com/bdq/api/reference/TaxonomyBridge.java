package com.bdq.api.reference;

import com.bdq.api.reference.model.GbifBackboneEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class TaxonomyBridge {

    private static final AtomicReference<TaxonomyResolver> RESOLVER = new AtomicReference<>();
    private static final AtomicReference<LocalReferenceDataRepository> REPOSITORY = new AtomicReference<>();
    private static final AtomicReference<ObjectMapper> MAPPER = new AtomicReference<>();

    private TaxonomyBridge() {}

    public static void setResolver(TaxonomyResolver resolver) {
        RESOLVER.set(resolver);
    }

    public static void setRepository(LocalReferenceDataRepository repository) {
        REPOSITORY.set(repository);
    }

    public static void setObjectMapper(ObjectMapper mapper) {
        MAPPER.set(mapper);
    }

    public static Optional<TaxonomyResolver> resolver() {
        return Optional.ofNullable(RESOLVER.get());
    }

    public static Optional<LocalReferenceDataRepository> repository() {
        return Optional.ofNullable(REPOSITORY.get());
    }

    public static Optional<ObjectMapper> mapper() {
        return Optional.ofNullable(MAPPER.get());
    }

    public static Optional<GbifBackboneEntry> findByCanonical(String canonical) {
        return resolver().flatMap(r -> r.resolveLocal(canonical));
    }

    public static List<GbifBackboneEntry> allEntries() {
        return repository().map(LocalReferenceDataRepository::getAllBackboneEntries).orElse(List.of());
    }
}
