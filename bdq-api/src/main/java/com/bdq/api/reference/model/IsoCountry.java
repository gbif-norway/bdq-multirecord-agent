package com.bdq.api.reference.model;

public record IsoCountry(
        String alpha2,
        String alpha3,
        String numeric,
        String name,
        String region,
        String subRegion
) {
}
