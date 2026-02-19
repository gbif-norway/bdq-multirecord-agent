package com.bdq.api.reference.model;

import org.locationtech.jts.geom.Geometry;

public class CountryGeometry {
    private String isoA2;
    private String name;
    private Geometry geometry;

    public CountryGeometry(String isoA2, String name, Geometry geometry) {
        this.isoA2 = isoA2;
        this.name = name;
        this.geometry = geometry;
    }

    public String getIsoA2() {
        return isoA2;
    }

    public String getName() {
        return name;
    }

    public Geometry getGeometry() {
        return geometry;
    }
}
