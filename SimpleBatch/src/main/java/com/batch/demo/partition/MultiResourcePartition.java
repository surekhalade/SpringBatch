package com.batch.demo.partition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

@Component("batchPartitioner")
public class MultiResourcePartition implements Partitioner {
	
	 @Override
	    public Map<String, ExecutionContext> partition(int gridSize) {
	        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
	        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
	        int i = 0, k = 1;
	        String customers;
	        System.out.println("in partition class:: ");
	        Resource[] resources ;
	        try {
		         resources = resolver.getResources("classpath:input/File*.csv");   //.getResources("file:src/main/resources/input/*.csv");
		    	
		      } catch (IOException e) {
		          throw new RuntimeException("I/O problems when resolving" + " the input file pattern.", e);
		      }
			for (Resource resource : resources) {
	            ExecutionContext context = new ExecutionContext();
	            Assert.state(resource.exists(), "Resource does not exist: "+ resource);
	            context.putString("fileName", resource.getFilename());
	            System.out.println("11111111:: "+resource.getFilename());
	            System.out.println("customers"+k);
	            context.putString("tableName", "mytable");
	            map.put("partition"+i, context);
	            i++;
	        }
	        return map;
	    }
	
	
	
	
	
	
	
	
	
	/* public Map partition(int gridSize) {
	        Map partitionMap = new HashMap();
	        int startingIndex = 0;
	        int endingIndex = 5;
	         
	        for(int i=0; i< gridSize; i++){
	            ExecutionContext ctxMap = new ExecutionContext();
	            ctxMap.putInt("startingIndex",startingIndex);
	            ctxMap.putInt("endingIndex", endingIndex);
	                         
	            startingIndex = endingIndex+1;
	            endingIndex += 5; 
	             
	            partitionMap.put("Thread:-"+i, ctxMap);
	        }
	        System.out.println("END: Created Partitions of size: "+ partitionMap.size());
	        return partitionMap;
	    } */

	/*@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		 MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
		 Resource[] resources = null;
		 PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		 try {
			resources = resolver.getResources("classpath:input/File*.csv");
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
		 Map<String,ExecutionContext> partitionData = new HashMap<>();
		 
		 int i=0;
		 for (Resource resource : resources) {
	            ExecutionContext context = new ExecutionContext();
	            Assert.state(resource.exists(), "Resource does not exist: " + resource);
	            System.out.println("get file name: "+resource.getFilename());
	            context.put("filename", resource.getFilename());
	            context.put("name","thread"+i);
	            partitionData.put("partition"+i, context);
	            i++;
	        }
		 return partitionData; */

}

