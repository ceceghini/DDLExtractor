package it.pointec.DDLExtractor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class ObjectDDL {

	private String _objectName;
	private String _objectDDL;
	private String _schema;
	private String _type;
	
	public ObjectDDL(String objectName, String objectDDL, String schema, String type) {
		_objectName = objectName;
		
		String s = Character.toString((char) 10);
		
		String sFrom = s+s;
		
		_objectDDL = objectDDL.replaceAll(sFrom, s).trim();
		
		// Terminatore per oggetti codice
		if (type=="FUNCTION" | type=="PROCEDURE"| type=="PACKAGE_SPEC"| type=="PACKAGE_BODY"| type=="TRIGGER" ) {
			if (_objectDDL.substring(_objectDDL.length()-1).compareTo("/") != 0)
				_objectDDL += s + "/";	
		}
		else {
			if (_objectDDL.substring(_objectDDL.length()-1).compareTo(";") != 0) {
				_objectDDL += s + ";";
			}
		}

		_schema = schema;
		
		// Elimino il riferimento alla schema nella creazione deglio oggetti
		_objectDDL = _objectDDL.replaceAll("\""+_schema.toUpperCase()+"\".", "");

		_type = type;
	}
	
	public String Elabora(String outputFolder, String subf) {
		
		System.out.println("Estrazione ddl per l'oggetto ["+_objectName+"]");
		
		String fileName = _type+"."+_objectName+".sql";
		
		try {
			FileWriter fstream = new FileWriter(outputFolder+"/"+subf+"/"+fileName);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(this._objectDDL);
			out.close();
		}
		catch (IOException ioe) {
			System.out.println(ioe.getMessage());
			System.out.println(ioe.getStackTrace().toString());
		}
		
		return fileName;
		
	}
	
}
