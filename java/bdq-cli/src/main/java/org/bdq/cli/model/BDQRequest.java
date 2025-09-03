package org.bdq.cli.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class BDQRequest {
    @JsonProperty("requestId")
    private String requestId;
    
    @JsonProperty("tests")
    private List<TestRequest> tests;
    
    public BDQRequest() {}
    
    public BDQRequest(String requestId, List<TestRequest> tests) {
        this.requestId = requestId;
        this.tests = tests;
    }
    
    public String getRequestId() {
        return requestId;
    }
    
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }
    
    public List<TestRequest> getTests() {
        return tests;
    }
    
    public void setTests(List<TestRequest> tests) {
        this.tests = tests;
    }
    
    public static class TestRequest {
        @JsonProperty("testId")
        private String testId;
        
        @JsonProperty("actedUpon")
        private List<String> actedUpon;
        
        @JsonProperty("consulted")
        private List<String> consulted;
        
        @JsonProperty("parameters")
        private Map<String, String> parameters;
        
        @JsonProperty("tuples")
        private List<List<String>> tuples;
        
        public TestRequest() {}
        
        public TestRequest(String testId, List<String> actedUpon, List<String> consulted, 
                         Map<String, String> parameters, List<List<String>> tuples) {
            this.testId = testId;
            this.actedUpon = actedUpon;
            this.consulted = consulted;
            this.parameters = parameters;
            this.tuples = tuples;
        }
        
        public String getTestId() {
            return testId;
        }
        
        public void setTestId(String testId) {
            this.testId = testId;
        }
        
        public List<String> getActedUpon() {
            return actedUpon;
        }
        
        public void setActedUpon(List<String> actedUpon) {
            this.actedUpon = actedUpon;
        }
        
        public List<String> getConsulted() {
            return consulted;
        }
        
        public void setConsulted(List<String> consulted) {
            this.consulted = consulted;
        }
        
        public Map<String, String> getParameters() {
            return parameters;
        }
        
        public void setParameters(Map<String, String> parameters) {
            this.parameters = parameters;
        }
        
        public List<List<String>> getTuples() {
            return tuples;
        }
        
        public void setTuples(List<List<String>> tuples) {
            this.tuples = tuples;
        }
    }
}
