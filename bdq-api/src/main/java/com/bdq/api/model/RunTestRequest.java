package com.bdq.api.model;

import java.util.Map;

public class RunTestRequest {
    private String id;     // BDQ label or GUID
    private Map<String, String> params; // keys like "dwc:eventDate", "bdq:sourceAuthority", etc.

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Map<String, String> getParams() { return params; }
    public void setParams(Map<String, String> params) { this.params = params; }
}

