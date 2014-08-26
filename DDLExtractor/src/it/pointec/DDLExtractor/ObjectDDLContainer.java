package it.pointec.DDLExtractor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class ObjectDDLContainer {

	private ArrayList<ObjectDDL> _a;
	private String _objectType;
	//private String _schema;
	
	public ObjectDDLContainer(String objectType, String schema) {
		this._a = new ArrayList<ObjectDDL>();
		this._objectType = objectType;
		//this._schema = schema;
	}
	
	public void add(ObjectDDL o) {
		_a.add(o);
	}
	
	public String elaboraFile(String outputFolder, String order, String subf) {

		// Creazione del file master
		String fileName = outputFolder+"/"+subf+"/"+order+"."+_objectType+".sql";
		
		try {
			
			if (_a.size()>0) {
				
				FileWriter fstream = new FileWriter(fileName);
				BufferedWriter out = new BufferedWriter(fstream);
				
				ObjectDDL o;
				
				for(int i=0;i<_a.size();i++) {
					
					o = (ObjectDDL) _a.get(i);
					
					if (subf=="")
						out.write("@"+o.Elabora(outputFolder, subf)+DDLExtractor.getTerminator());
					else
						out.write("@"+subf+"/"+o.Elabora(outputFolder, subf)+DDLExtractor.getTerminator());
					
				}
				
				out.close();
				
				if (subf=="")
					return order+"."+_objectType+".sql";
				else
					return subf+"/"+order+"."+_objectType+".sql";
				
			}
			else {
				return "";
			}
			
		}
		catch (IOException ioe) {
			System.out.println(ioe.getMessage());
			System.out.println(ioe.getStackTrace().toString());
			return "";
		}
		
	}
	
}
