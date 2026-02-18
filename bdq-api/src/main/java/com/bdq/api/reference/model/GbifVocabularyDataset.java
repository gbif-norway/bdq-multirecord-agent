package com.bdq.api.reference.model;

import java.util.List;
import java.util.Map;

public class GbifVocabularyDataset {

    private Map<String, List<GbifVocabularyItem>> vocabularies;
    private Map<String, Object> metadata;

    public Map<String, List<GbifVocabularyItem>> getVocabularies() {
        return vocabularies;
    }

    public void setVocabularies(Map<String, List<GbifVocabularyItem>> vocabularies) {
        this.vocabularies = vocabularies;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
