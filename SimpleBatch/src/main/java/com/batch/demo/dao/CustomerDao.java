package com.batch.demo.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcDaoSupport;

import com.batch.demo.model.Customers;

public class CustomerDao extends NamedParameterJdbcDaoSupport {
	@Autowired
	DataSource dataSource;

	@PostConstruct
	private void initialize() {
		setDataSource(dataSource);
	} 
	

	public void insert(List Customers) {
		String sql = "INSERT INTO customers " + "(id, testCase, testName) VALUES (?, ?, ?)";
		getJdbcTemplate().update(sql, new BatchPreparedStatementSetter() 
		{
			public int getBatchSize() {
				return Customers.size();
			}

			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				Customers customer = (Customers) Customers.get(i);
				System.out.println("get id::"+customer.getId());
				ps.setLong(1, customer.getId());
				ps.setString(2, customer.getTestCase());
				ps.setString(3, customer.getTestName());
			}
		});
	} 
}
