package com.bdq.api.model;

import java.util.List;

public class TestInfo {
    private String id;           // BDQ label (e.g., VALIDATION_...)
    private String guid;         // @Provides value (GUID/URN)
    private String type;         // Validation | Amendment | Measure | Issue | Unknown
    private String className;
    private String methodName;
    private List<String> actedUpon;
    private List<String> consulted;
    private List<String> parameters;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getGuid() { return guid; }
    public void setGuid(String guid) { this.guid = guid; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }
    public String getMethodName() { return methodName; }
    public void setMethodName(String methodName) { this.methodName = methodName; }
    public List<String> getActedUpon() { return actedUpon; }
    public void setActedUpon(List<String> actedUpon) { this.actedUpon = actedUpon; }
    public List<String> getConsulted() { return consulted; }
    public void setConsulted(List<String> consulted) { this.consulted = consulted; }
    public List<String> getParameters() { return parameters; }
    public void setParameters(List<String> parameters) { this.parameters = parameters; }
}

