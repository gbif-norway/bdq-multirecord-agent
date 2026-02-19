package com.bdq.api.reference.model;

import java.util.List;
import java.util.Map;

public class IanaTimeZoneDataset {

    private List<IanaTimeZoneEntry> zones;
    private Map<String, Object> metadata;

    public List<IanaTimeZoneEntry> getZones() {
        return zones;
    }

    public void setZones(List<IanaTimeZoneEntry> zones) {
        this.zones = zones;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
