package com.cbds.hive.functions;

import org.apache.hadoop.hive.ql.exec.UDF;

/*
 * Class to convert all columns Values to UpperCase Values.
 * 
 */
public class MaxRate extends UDF {
	//Evaluate the main method to be called by Apache Hive.
	public double evaluate (double espanol,double matematicas,double geografia,double historia, double ingles){
		double maxRate = espanol;
		if(matematicas > maxRate){
			maxRate = matematicas;
		}
		if(geografia > maxRate){
			maxRate = geografia;
		}
		if(historia > maxRate){
			maxRate = historia;
		}
		if(ingles > maxRate){
			maxRate = ingles;
		}	
		return maxRate;
	}
}