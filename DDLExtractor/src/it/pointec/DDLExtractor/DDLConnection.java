package it.pointec.DDLExtractor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DDLConnection {

	private Connection _conn;
	private String _outputFolder;
	private String _outputObjs;
	private String _outputCode;
	private String _schema;
	private boolean _withTables;
	
	public DDLConnection(String cs, String outputFolder, String userName, String password, boolean withTables, String outputObjs, String outputCode) {
		
		try {
			System.out.println("Connessione al db oracle.");
			DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
			_conn = DriverManager.getConnection("jdbc:oracle:thin:@"+cs, userName, password);
			System.out.println("Connessione al db oracle effettuata correttamente.");
			
			//Configurazione dei parametri di recupero ddl
			CallableStatement cStmt = _conn.prepareCall("{ call DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR',true) }");
			cStmt.execute();
			
			//cStmt = _conn.prepareCall("{ call DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES',false) }");
			//cStmt.execute();
			
			cStmt = _conn.prepareCall("{ call DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE',false)}");
			cStmt.execute();
			
			cStmt = _conn.prepareCall("{ call DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE',false)}");
			cStmt.execute();
			
			cStmt = _conn.prepareCall("{ call DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS',true)}");
			cStmt.execute();
			
			cStmt = _conn.prepareCall("{ call DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'REF_CONSTRAINTS',false)}");
			cStmt.execute();
			
			cStmt = _conn.prepareCall("{ call DBMS_METADATA.SET_TRANSFORM_PARAM (DBMS_METADATA.SESSION_TRANSFORM, 'CONSTRAINTS_AS_ALTER',true)}");
			cStmt.execute();
			
		}
		catch (SQLException se) {
			System.out.println(se.getMessage());
			System.out.println(se.getStackTrace().toString());
		}
		
		this._outputFolder = outputFolder;
		this._outputCode = outputCode;
		this._outputObjs = outputObjs;
		this._schema = userName;
		this._withTables = withTables;
		
	}
	
	public void close() {
		try {
			_conn.close();
		}
		catch (SQLException se) {
			System.out.println(se.getMessage());
			System.out.println(se.getStackTrace().toString());
		}
	}
	
	public void writeLine(String type, DbObjectType o, BufferedWriter out, String sort, String subf) {
		
		String line;
		
		try {
			
			line = o.ExtractDDL(type).elaboraFile(_outputFolder, sort, subf);
		
			if (line!="")
				out.write("@"+line+DDLExtractor.getTerminator());
		}
		catch (IOException ioe) {
			System.out.println(ioe.getMessage());
			System.out.println(ioe.getStackTrace().toString());
		}	
		
	}
	
	public void ExtractDDL() {
		
		DbObjectType o = new DbObjectType(_conn, _schema);
		
//		 Creazione del file master
		String fileName = _outputFolder+"/01.main.sql";
		
		try {
			
			System.out.println("Inizio elaborazione oggetti.");
			
			FileWriter fstream = new FileWriter(fileName);
			BufferedWriter out = new BufferedWriter(fstream);
			
			if (_withTables) {
				writeLine("TABLE", o, out, "02", _outputObjs);
				writeLine("INDEX", o, out, "03", _outputObjs);
				writeLine("SEQUENCE", o, out, "04", _outputObjs);
			}
			writeLine("VIEW", o, out, "05", _outputObjs);
			writeLine("FUNCTION", o, out, "06", _outputCode);
			writeLine("PROCEDURE", o, out, "07", _outputCode);
			writeLine("PACKAGE_SPEC", o, out, "08", _outputCode);
			writeLine("PACKAGE_BODY", o, out, "09", _outputCode);
			writeLine("TRIGGER", o, out, "10", _outputCode);
			if (_withTables) {
				writeLine("MATERIALIZED_VIEW", o, out, "11", _outputObjs);
				writeLine("MATERIALIZED_VIEW_LOG", o, out, "11", _outputObjs);
			}
			writeLine("SYNONYM", o, out, "12", _outputObjs);
			if (_withTables) {
//				writeLine("PRIMARY_KEY", o, out);
				writeLine("CONSTRAINTS", o, out, "13", _outputObjs);
			}
			
			out.close();
			
			System.out.println("Elaborazione oggetti completata.");
			
		}
		catch (IOException ioe) {
			System.out.println(ioe.getMessage());
			System.out.println(ioe.getStackTrace().toString());
		}		
		
	}
	
}
