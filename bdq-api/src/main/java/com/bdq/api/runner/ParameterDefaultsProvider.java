package com.bdq.api.runner;

import org.datakurator.ffdq.annotations.Parameter;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

@Component
public class ParameterDefaultsProvider {

    private static final String NE_ADMIN0 = "NE_10M_ADMIN_0_COUNTRIES";
    private static final String NE_LAND_UNION = "Union of NaturalEarth 10m-physical-vectors for Land and NaturalEarth Minor Islands";
    private static final String GBIF_BACKBONE = "GBIF Backbone Taxonomy";

    public void applyDefaults(Method method, Map<String, String> params) {
        Annotation[][] paramAnnotations = method.getParameterAnnotations();
        for (Annotation[] annotations : paramAnnotations) {
            for (Annotation annotation : annotations) {
                if (annotation instanceof Parameter parameter) {
                    String key = parameter.name();
                    if (key == null || key.isBlank()) {
                        continue;
                    }
                    if (params.containsKey(key)) {
                        continue;
                    }
                    String defaultValue = resolveDefault(method, key);
                    if (defaultValue != null) {
                        params.put(key, defaultValue);
                    }
                }
            }
        }
    }

    private String resolveDefault(Method method, String key) {
        String declaringClassName = method.getDeclaringClass().getName();
        if ("bdq:geospatialLand".equals(key)) {
            return NE_LAND_UNION;
        }
        if ("bdq:assumptionOnUnknownBiome".equals(key)) {
            return "noassumption";
        }
        if ("bdq:spatialBufferInMeters".equals(key)) {
            return "3000";
        }
        if ("bdq:taxonIsMarine".equals(key)) {
            return "World Register of Marine Species (WoRMS)";
        }
        if ("bdq:sourceAuthority".equals(key)) {
            if (declaringClassName.contains(".georeference.")) {
                return NE_ADMIN0;
            }
            if (declaringClassName.contains(".sciname.")) {
                return GBIF_BACKBONE;
            }
            if (declaringClassName.contains(".metadata.")) {
                return NE_ADMIN0;
            }
        }
        return null;
    }
}
