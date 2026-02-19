package com.bdq.api.model;

public class ValidationResponse {
    private String status;
    private String result;
    private String comment;
    
    public ValidationResponse() {}
    
    public ValidationResponse(String status, String result, String comment) {
        this.status = status;
        this.result = result;
        this.comment = comment;
    }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public String getResult() { return result; }
    public void setResult(String result) { this.result = result; }
    
    public String getComment() { return comment; }
    public void setComment(String comment) { this.comment = comment; }
}