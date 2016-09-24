package com.slk.core.RDDOperations;

import org.apache.spark.Partitioner;

public class NamePartitioner extends Partitioner {

	private int numParts;
	
	
	public NamePartitioner(int numParts)
	{
	  this.numParts = numParts;
	}
	
	
	@Override
	public int getPartition(Object key) {
		
		System.out.println(key.toString());
		
		if(key.toString().equalsIgnoreCase("Vibushanan")){
			return 1;
		}else if (key.toString().equalsIgnoreCase("Shyamala")){
			return 2;
		}
		return 3;
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numParts;
	}

}
