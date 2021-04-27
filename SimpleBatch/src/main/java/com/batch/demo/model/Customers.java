package com.batch.demo.model;

public class Customers {
	private Integer id;
	private String testCase;
	private String testName;
	
	public Customers(Integer id, String testCase, String testName) {
		super();
		this.id = id;
		this.testCase = testCase;
		this.testName = testName;
	}
	public Customers() {
	}
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public String getTestCase() {
		return testCase;
	}
	public void setTestCase(String testCase) {
		this.testCase = testCase;
	}
	public String getTestName() {
		return testName;
	}
	public void setTestName(String testName) {
		this.testName = testName;
	}
	@Override
	public String toString() {
		return "Customers [id=" + id + ", testCase=" + testCase + ", testName=" + testName + "]";
	}
	
	
}
