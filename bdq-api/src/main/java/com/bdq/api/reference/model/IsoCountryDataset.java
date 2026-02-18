package com.bdq.api.reference.model;

import java.util.List;
import java.util.Map;

public class IsoCountryDataset {
    private List<IsoCountry> countries;
    private Map<String, Object> metadata;

    public List<IsoCountry> getCountries() {
        return countries;
    }

    public void setCountries(List<IsoCountry> countries) {
        this.countries = countries;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
