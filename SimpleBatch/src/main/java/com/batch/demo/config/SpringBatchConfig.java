package com.batch.demo.config;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;
import javax.sql.DataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import com.batch.demo.model.Customers;
import com.batch.demo.partition.MultiResourcePartition;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
	
		/*@Value("classPath:/input/File*.csv")
		 private Resource[] inputResource; */
	 
	 	@Autowired
		public JobBuilderFactory jobBuilderFactory;

		@Autowired
		public StepBuilderFactory stepBuilderFactory;

		@Autowired
		public DataSource dataSource;
		
		@Autowired
		MultiResourcePartition multiPart;
		
		  @Bean
		  public Job partitionerJob()  throws Exception {
		      return jobBuilderFactory.get("partitioningJob")
		        .start(masterStep())
		        .build();
		  }
		  
		//master step
		    @SuppressWarnings("unused")
			@Bean
		    public Step masterStep() throws Exception 
		    {
		        return stepBuilderFactory.get("masterStep")
		                .partitioner("slaveStep", partitioner())
		                .partitionHandler(partitionHandler())
		                .build();
		    }
		    
		    //PartitionHandler
		    @Bean
		    public PartitionHandler partitionHandler() throws Exception 
		    {
		    	System.out.println("in partitionHandler");
		    	TaskExecutorPartitionHandler retVal = new TaskExecutorPartitionHandler();
		    	retVal.setTaskExecutor(taskExecutor());
		    	retVal.setStep(slaveStep());
		    	retVal.setGridSize(2);
		    	return retVal;
		    }
		    
		    @Bean
		    public Step slaveStep() throws Exception 
		    {
		    	System.out.println("in slave step");
		        return stepBuilderFactory.get("slaveStep")
		                .<Customers, Customers>chunk(5)
		                .reader(CustomersItemReader(null))
		                .processor(customerProcess())
		                .writer(customerItemWriter(null))
		                .build();
		    }
		    
		    @SuppressWarnings({ "rawtypes", "unchecked" })
			 @Bean
				public ItemProcessor<Customers,Customers> customerProcess() {   //any business logic 
				 ItemProcessor<Customers,Customers> itemProcessor = new ItemProcessor<Customers,Customers>(){
					 @Override
						public Customers process(Customers cust) throws Exception {
						 System.out.println("Id: "+cust.getId()+ "testCase:: "+cust.getTestCase()+ " testName:: "+cust.getTestName());
					        return cust;
						}
			 	};
				return itemProcessor;
			 }
		    
		   @Bean
		   public  MultiResourcePartitioner partitioner() throws IOException {
		    	System.out.println("in partitioner");
		    	MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
		    	PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		    	Resource[] resources;
			    try {
			         resources = resolver.getResources("classpath:input/File*.csv");   //.getResources("file:src/main/resources/input/*.csv");
			    	
			      } catch (IOException e) {
			          throw new RuntimeException("I/O problems when resolving" + " the input file pattern.", e);
			      }
		      partitioner.setResources(resources);
		      return partitioner;
		    } 
		
			 @Bean 
			  public TaskExecutor taskExecutor() { 
				  System.out.println("in taskExecutor");
				  ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
				  executor.setCorePoolSize(10);
				  executor.setMaxPoolSize(10);
				  executor.setQueueCapacity(10);
				  executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
				  executor.setThreadNamePrefix("MultiThreaded-");
				  return executor;
			}
			// 
		 @Bean 
		 @StepScope
		  public FlatFileItemReader<Customers> CustomersItemReader(@Value("#{stepExecutionContext['fileName']}") String file) throws Exception 
		  {
			  FlatFileItemReader<Customers> reader = new FlatFileItemReader<Customers>();
			  System.out.println("get file: "+file);
			  reader.setResource(new UrlResource(file));
			  reader.setLineMapper(new DefaultLineMapper<Customers>() {{
		            setLineTokenizer(new DelimitedLineTokenizer() {{
		            	setDelimiter("|");
		                setNames(new String[] { "id", "testCase","testName" });
		            }});
		            setFieldSetMapper(new BeanWrapperFieldSetMapper() {{
		                setTargetType(Customers.class);
		            }});
		        }});
		        return reader;
		  }
		 
		 	@Bean
		    @StepScope
		    public JdbcBatchItemWriter<Customers> customerItemWriter(@Value("#{stepExecutionContext['fileName']}") String fileName)
		    {
			 	String table;
			 	String customerTable;
			 	
			 	System.out.println("get filename in writer:: "+fileName);
			 	JdbcBatchItemWriter<Customers> itemWriter = new JdbcBatchItemWriter<>();
			 	String finalString =fileName.substring(fileName.lastIndexOf("/")+1);
			 	System.out.println("finalString:: "+finalString);
			 	
			 	StringBuilder myNumbers = new StringBuilder();
				    for (int i = 0; i < finalString.length(); i++) {
				        if (Character.isDigit(finalString.charAt(i))) {
				            myNumbers.append(finalString.charAt(i));
				            //System.out.println(str.charAt(i) + " is a digit.");
				        } else {
				           // System.out.println(str.charAt(i) + " not a digit.");
				        }
				    }
				    
				    String seq = myNumbers.toString();
				    System.out.println("seq: " +seq);
				    String insertQuery = " INSERT INTO customerTable"+seq+" VALUES (:id, :testCase, :testName) ";
				    
				     itemWriter.setDataSource(dataSource);
				     System.out.println("datasource:: "+dataSource);
				     itemWriter.setSql(insertQuery);
				     itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
				     itemWriter.afterPropertiesSet();
				     return itemWriter; 
			    } 

		 @SuppressWarnings({ "rawtypes", "unchecked" })
		 @Bean
			public ItemProcessor<Customers,Customers> CustomersProcess() {   //any business logic 
			 ItemProcessor<Customers,Customers> itemProcessor = new ItemProcessor<Customers,Customers>(){
				 @Override
					public Customers process(Customers cust) throws Exception {
						String testCaseToUpperCase = cust.getTestCase().toUpperCase(); 
						cust.setTestCase(testCaseToUpperCase);
						return cust;
					}
		 	};
			return itemProcessor;
		 }
}














/*  //master step  original
@SuppressWarnings("unused")
@Bean
public Step masterStep() throws Exception 
{
    return stepBuilderFactory.get("masterStep")
    		//.partitioner(slaveStep())
            .partitioner("partition", partitioner())
            .partitionHandler(partitionHandler())
            .step(slaveStep())
            .gridSize(2)
            .taskExecutor(taskExecutor())
            .build();
} */


	  /*  @SuppressWarnings("unused")
	    @Bean
		public MultiResourcePartitioner partitioner() {
	    	MultiResourcePartitioner part = new MultiResourcePartitioner() {
	    		 PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
	    		 part.setResources(resolver.getResources("classpath:input/File*.csv"));
	    	//List <String> fileList= Arrays.asList("File1.csv","File2.csv");
			@Override
			public Map<String, ExecutionContext> partition(int gridSize) {
				Map<String,ExecutionContext> partitionData = new HashMap<>();
					for(int i=0;i<gridSize;i++) {
						System.out.println("get i "+i+ "------------ "+fileList.get(i));
						ExecutionContext executionContext = new ExecutionContext();
						executionContext.put("filename", fileList.get(i));
						executionContext.put("name","thread"+i);
						partitionData.put("partition: "+i, executionContext);
					}
				return partitionData;
				}
			 };
			return part;
		} */

/*   @Bean 
public  MultiResourcePartitioner partitioner() throws IOException {
	MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
	return partitioner;
} */


/*    @Bean
public Partitioner partitioner() throws Exception {
    MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    partitioner.setResources(resolver.getResources("input://File*"));
    return partitioner;
}  */




/*@SuppressWarnings("unused")
@Bean
public  Partitioner partitioner() {
	Partitioner part = new Partitioner() {
	List <String> fileList= Arrays.asList("File1.csv","File2.csv");
	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String,ExecutionContext> partitionData = new HashMap<>();
			for(int i=0;i<gridSize;i++) {
				System.out.println("get i "+i+ "------------ "+fileList.get(i));
				ExecutionContext executionContext = new ExecutionContext();
				executionContext.put("filename", fileList.get(i));
				executionContext.put("name","thread"+i);
				partitionData.put("partition: "+i, executionContext);
			}
		return partitionData;
		}
	 };
	return part;
}*/


/*	 @Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
				.<Customers, Customers>chunk(5)
				.reader(CustomersItemReader1())
				.processor(CustomersProcess())
				.writer(customerItemWriter())
				.build();
	} */

/*	 @Bean
public Job job1() throws Exception{
	return jobBuilderFactory.get("job1")
			.start(step1())
			.build();
} */