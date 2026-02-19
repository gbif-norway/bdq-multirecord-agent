package com.bdq.api.reference.model;

import java.util.List;

public record IanaTimeZoneEntry(
        List<String> ids,
        String description,
        String abbr,
        Double offset
) {
}
