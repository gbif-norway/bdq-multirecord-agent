package com.bdq.api.reference.model;

import java.util.List;
import java.util.Map;

public class GbifBackboneDataset {

    private List<GbifBackboneEntry> ranks;
    private Map<String, Object> metadata;

    public List<GbifBackboneEntry> getRanks() {
        return ranks;
    }

    public void setRanks(List<GbifBackboneEntry> ranks) {
        this.ranks = ranks;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
