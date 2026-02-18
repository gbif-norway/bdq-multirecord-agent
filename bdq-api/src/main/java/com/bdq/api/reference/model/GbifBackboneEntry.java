package com.bdq.api.reference.model;

public record GbifBackboneEntry(
        String kingdom,
        String phylum,
        String clazz,
        String order,
        String family,
        String genus,
        String canonicalName
) {
    public String canonicalAuthorityKey() {
        return canonicalName != null ? canonicalName.toLowerCase() : "";
    }
}
