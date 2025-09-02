package org.bdq.server.model;

import java.util.List;

public class TestMapping {
    private String testId;
    private String library;
    private String javaClass;
    private String javaMethod;
    private List<String> actedUpon;
    private List<String> consulted;
    private List<String> parameters;
    private String testType;
    
    public TestMapping() {}
    
    public TestMapping(String testId, String library, String javaClass, String javaMethod,
                      List<String> actedUpon, List<String> consulted, List<String> parameters, String testType) {
        this.testId = testId;
        this.library = library;
        this.javaClass = javaClass;
        this.javaMethod = javaMethod;
        this.actedUpon = actedUpon;
        this.consulted = consulted;
        this.parameters = parameters;
        this.testType = testType;
    }
    
    public String getTestId() { return testId; }
    public void setTestId(String testId) { this.testId = testId; }
    
    public String getLibrary() { return library; }
    public void setLibrary(String library) { this.library = library; }
    
    public String getJavaClass() { return javaClass; }
    public void setJavaClass(String javaClass) { this.javaClass = javaClass; }
    
    public String getJavaMethod() { return javaMethod; }
    public void setJavaMethod(String javaMethod) { this.javaMethod = javaMethod; }
    
    public List<String> getActedUpon() { return actedUpon; }
    public void setActedUpon(List<String> actedUpon) { this.actedUpon = actedUpon; }
    
    public List<String> getConsulted() { return consulted; }
    public void setConsulted(List<String> consulted) { this.consulted = consulted; }
    
    public List<String> getParameters() { return parameters; }
    public void setParameters(List<String> parameters) { this.parameters = parameters; }
    
    public String getTestType() { return testType; }
    public void setTestType(String testType) { this.testType = testType; }
}
