package org.bdq.server.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class BDQResponse {
    @JsonProperty("requestId")
    private String requestId;
    
    @JsonProperty("results")
    private Map<String, TestResults> results;
    
    @JsonProperty("errors")
    private List<TestError> errors;
    
    public BDQResponse() {}
    
    public BDQResponse(String requestId, Map<String, TestResults> results, List<TestError> errors) {
        this.requestId = requestId;
        this.results = results;
        this.errors = errors;
    }
    
    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    
    public Map<String, TestResults> getResults() { return results; }
    public void setResults(Map<String, TestResults> results) { this.results = results; }
    
    public List<TestError> getErrors() { return errors; }
    public void setErrors(List<TestError> errors) { this.errors = errors; }
    
    public static class TestResults {
        @JsonProperty("tupleResults")
        private List<TupleResult> tupleResults;
        
        public TestResults() {}
        
        public TestResults(List<TupleResult> tupleResults) {
            this.tupleResults = tupleResults;
        }
        
        public List<TupleResult> getTupleResults() { return tupleResults; }
        public void setTupleResults(List<TupleResult> tupleResults) { this.tupleResults = tupleResults; }
    }
    
    public static class TupleResult {
        @JsonProperty("tupleIndex")
        private int tupleIndex;
        
        @JsonProperty("status")
        private String status;
        
        @JsonProperty("result")
        private Object result;  // Can be String for validation or Map<String,String> for amendment
        
        @JsonProperty("comment")
        private String comment;
        
        public TupleResult() {}
        
        public TupleResult(int tupleIndex, String status, Object result, String comment) {
            this.tupleIndex = tupleIndex;
            this.status = status;
            this.result = result;
            this.comment = comment;
        }
        
        public int getTupleIndex() { return tupleIndex; }
        public void setTupleIndex(int tupleIndex) { this.tupleIndex = tupleIndex; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        
        public Object getResult() { return result; }
        public void setResult(Object result) { this.result = result; }
        
        public String getComment() { return comment; }
        public void setComment(String comment) { this.comment = comment; }
    }
    
    public static class TestError {
        @JsonProperty("testId")
        private String testId;
        
        @JsonProperty("error")
        private String error;
        
        public TestError() {}
        
        public TestError(String testId, String error) {
            this.testId = testId;
            this.error = error;
        }
        
        public String getTestId() { return testId; }
        public void setTestId(String testId) { this.testId = testId; }
        
        public String getError() { return error; }
        public void setError(String error) { this.error = error; }
    }
}
