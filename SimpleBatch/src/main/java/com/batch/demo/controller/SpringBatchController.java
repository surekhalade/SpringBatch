package com.batch.demo.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SpringBatchController {
	@Autowired
	JobRepository jobRepository;
	
	@Autowired
	JobLauncher jobLauncher;

	@Autowired
	Job newJob;

	@RequestMapping("/runJob")
	public String handle() throws Exception 
	{
		try {
			JobParameters jobParameters = new JobParametersBuilder().addLong("time", System.currentTimeMillis())
					.toJobParameters();
			jobLauncher.run(newJob, jobParameters);
		} catch (Exception e) 
		{
			
		}
		return "Check Console for more details";
	}
}
