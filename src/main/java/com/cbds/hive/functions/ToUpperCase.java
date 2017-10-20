package com.cbds.hive.functions;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/*
 * Class to convert all columns Values to UpperCase Values.
 * 
 */
public class ToUpperCase extends UDF {
	//Private Text Object to return to Hive.
	private Text result = new Text();
	//Evaluate the main method to be called by Apache Hive.
	public Text evaluate(String column) {
	 //conditional sentence that check if the value of the current value is null.
	 if(column == null) {
		 return null;
	}
	//In other case turn to UpperCase the current Value.
	result.set(column.toUpperCase());
	//return the current result object.
	return result;
    }
}