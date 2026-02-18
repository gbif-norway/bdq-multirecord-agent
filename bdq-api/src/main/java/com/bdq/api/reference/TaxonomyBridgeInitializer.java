package com.bdq.api.reference;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class TaxonomyBridgeInitializer {

    public TaxonomyBridgeInitializer(TaxonomyResolver resolver,
                                     LocalReferenceDataRepository repository,
                                     ObjectMapper mapper) {
        TaxonomyBridge.setResolver(resolver);
        TaxonomyBridge.setRepository(repository);
        TaxonomyBridge.setObjectMapper(mapper);
    }
}
