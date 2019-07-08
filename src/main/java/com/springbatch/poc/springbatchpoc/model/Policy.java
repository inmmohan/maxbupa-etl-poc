package com.springbatch.poc.springbatchpoc.model;

import java.io.Serializable;

public class Policy implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private String policy_number;
    private String policy_type;
    private String timestamp;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPolicyNumber() {
        return policy_number;
    }

    public void setPolicyNumber(String policyNumber) {
        this.policy_number = policyNumber;
    }

    public String getPolicyType() {
        return policy_type;
    }

    public void setPolicyType(String policyType) {
        this.policy_type = policyType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
