package com.springbatch.poc.springbatchpoc.model;

import java.io.Serializable;

public class User implements Serializable{

    private static final long serialVersionUID = 1L;

    private Integer id;
    private String name;
    private String city;

    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getCity() {
        return city;
    }
    public void setCity(String city) {
        this.city = city;
    }
}
