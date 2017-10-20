package com.cbds.hive.functions;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Class to define a UDTF that makes 3 columns according to the number of topics.
 * 
 */
public class ChooseClassUDTF extends GenericUDTF{

	
	private Object[] fwdObj = null;
	private PrimitiveObjectInspector bookDtlOI = null;
	
	public StructObjectInspector initialize(ObjectInspector[] arg){
		//ArrayList that contains the list of the names of the columns that the UDTF outs.
		ArrayList<String> fieldNames = new ArrayList<String>();
		//ArrayList that contains all the metadata about the columns.
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		
		bookDtlOI = (PrimitiveObjectInspector) arg[0];
		
		//CREATE A COLUMN CALLED BIOTECNOLOGIA OF STRING TYPE
		fieldNames.add("BIOTECNOLOGIA");
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		//CREATE A COLUMN CALLED BIOTECNOLOGIA OF STRING TYPE
		fieldNames.add("UNIVERSO");
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		//CREATE A COLUMN CALLED BIOTECNOLOGIA OF STRING TYPE
		fieldNames.add("MATEMATICAS");
		fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
		PrimitiveCategory.STRING));
		
		//INSTANCE OF A VECTOR OF DIMENSION 3.
		fwdObj = new Object[3];
		
		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}
	//method to process the input Row data 
	public void process(Object[] record) throws HiveException{
		String bookDtl = bookDtlOI.getPrimitiveJavaObject(record[0]).toString();
		
		String str[] = bookDtl.split("\\|");
		
		//Conditional statement that set columns values according to the number of itself.
		if(Integer.parseInt(str[1])==0){
			fwdObj[0] = str[0];
			fwdObj[1] = "";
			fwdObj[2] = "";
		}else if(Integer.parseInt(str[1])==1){
			fwdObj[0] = "";
			fwdObj[1] = str[0];
			fwdObj[2] = "";
		}else if(Integer.parseInt(str[1])==2){
			fwdObj[0] = "";
			fwdObj[1] = "";
			fwdObj[2] = str[0];	
		}
		//return the 3 dimension vector to Apache Hive.
		this.forward(fwdObj);
		
	}
	
	public void close(){
		  
	}
}
