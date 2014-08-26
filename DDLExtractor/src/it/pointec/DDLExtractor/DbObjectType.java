package it.pointec.DDLExtractor;

import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import oracle.jdbc.driver.OracleTypes;

public class DbObjectType {

	private Connection _conn;
	//private HashMap _hmSql;
	private Map<String, String> _mapSql;
	private String _schema;
	
	public DbObjectType() {
	}
	
	public DbObjectType(Connection c, String schema) {
		
		this._conn = c;
		this._schema = schema;
		this._mapSql = new HashMap<String, String>();
		
		/*
		_hmSql.put("TABLE", 					"select object_name, DBMS_METADATA.GET_DDL('TABLE',object_name) from user_objects where object_type = 'TABLE' and generated = 'N'");
		_hmSql.put("INDEX", 					"select object_name, DBMS_METADATA.GET_DDL('INDEX',object_name) from user_objects where object_type = 'INDEX'");
		_hmSql.put("VIEW",  					"select object_name, DBMS_METADATA.GET_DDL('VIEW' ,object_name) from user_objects where object_type = 'VIEW'");
		_hmSql.put("FUNCTION",  				"select object_name, DBMS_METADATA.GET_DDL('FUNCTION' ,object_name) from user_objects where object_type = 'FUNCTION'");
		_hmSql.put("PROCEDURE",  			"select object_name, DBMS_METADATA.GET_DDL('PROCEDURE' ,object_name) from user_objects where object_type = 'PROCEDURE'");
		_hmSql.put("PACKAGE_SPEC",  			"select object_name, DBMS_METADATA.GET_DDL('PACKAGE_SPEC' ,object_name) from user_objects where object_type = 'PACKAGE'");
		_hmSql.put("PACKAGE_BODY",  			"select object_name, DBMS_METADATA.GET_DDL('PACKAGE_BODY' ,object_name) from user_objects where object_type = 'PACKAGE BODY'");
		_hmSql.put("TRIGGER",	  			"select object_name, DBMS_METADATA.GET_DDL('TRIGGER' ,object_name) from user_objects where object_type = 'TRIGGER'");
		_hmSql.put("SEQUENCE",	  			"select object_name, DBMS_METADATA.GET_DDL('SEQUENCE' ,object_name) from user_objects where object_type = 'SEQUENCE'");
		_hmSql.put("MATERIALIZED_VIEW",		"select object_name, DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW' ,object_name) from user_objects where object_type = 'MATERIALIZED VIEW'");
		_hmSql.put("MATERIALIZED_VIEW_LOG",	"select log_table, DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW_LOG' ,log_table) from user_mview_logs");
		_hmSql.put("SYNONYM",				"select object_name, DBMS_METADATA.GET_DDL('SYNONYM' ,object_name) from user_objects where object_type = 'SYNONYM'");
		
		_hmSql.put("PRIMARY_KEY",		"select constraint_name, DBMS_METADATA.GET_DDL('CONSTRAINT',constraint_name) from user_constraints where generated <> 'GENERATED NAME' and constraint_type = 'P'");
		_hmSql.put("CONSTRAINTS",		"select constraint_name, DBMS_METADATA.GET_DDL(decode(constraint_type, 'R', 'REF_CONSTRAINT', 'CONSTRAINT'),constraint_name) from user_constraints where generated <> 'GENERATED NAME' and constraint_type <> 'P'");
		*/
		
		_mapSql.put("TABLE", 					"select object_name, 'TABLE' from user_objects where object_type = 'TABLE' and generated = 'N' and not exists (select 1 from user_snapshots where name = object_name)");
		_mapSql.put("INDEX", 					"select object_name, 'INDEX' from user_objects where object_type = 'INDEX' and not exists (select 1 from user_constraints where constraint_name = object_name)");
		_mapSql.put("VIEW",  					"select object_name, 'VIEW' from user_objects where object_type = 'VIEW'");
		_mapSql.put("FUNCTION",  				"select object_name, 'FUNCTION' from user_objects where object_type = 'FUNCTION'");
		_mapSql.put("PROCEDURE",  				"select object_name, 'PROCEDURE' from user_objects where object_type = 'PROCEDURE'");
		_mapSql.put("PACKAGE_SPEC",  			"select object_name, 'PACKAGE_SPEC' from user_objects where object_type = 'PACKAGE'");
		_mapSql.put("PACKAGE_BODY",  			"select object_name, 'PACKAGE_BODY' from user_objects where object_type = 'PACKAGE BODY'");
		_mapSql.put("TRIGGER",	  				"select object_name, 'TRIGGER' from user_objects where object_type = 'TRIGGER'");
		_mapSql.put("SEQUENCE",	  				"select object_name, 'SEQUENCE' from user_objects where object_type = 'SEQUENCE'");
		_mapSql.put("MATERIALIZED_VIEW",		"select object_name, 'MATERIALIZED_VIEW' from user_objects where object_type = 'MATERIALIZED VIEW'");
		_mapSql.put("MATERIALIZED_VIEW_LOG",	"select log_table, 'MATERIALIZED_VIEW_LOG' from user_mview_logs");
		_mapSql.put("SYNONYM",				"select object_name, 'SYNONYM' from user_objects where object_type = 'SYNONYM'");
		
		_mapSql.put("PRIMARY_KEY",		"select constraint_name, 'CONSTRAINT' from user_constraints where generated <> 'GENERATED NAME' and constraint_type = 'P'");
		_mapSql.put("CONSTRAINTS",		"select constraint_name, 'REF_CONSTRAINT' from user_constraints where generated <> 'GENERATED NAME' and constraint_type = 'R'");
		
	}
	
	public ObjectDDLContainer ExtractDDL(String type) {
		
		System.out.println("Estrazione oggetti di tipo. ["+type+"].");
		
		Clob lobValue;
		ObjectDDLContainer a = new ObjectDDLContainer(type, _schema);
		
		try {
			
			Statement stmt = _conn.createStatement();
	        ResultSet rs = stmt.executeQuery(_mapSql.get(type).toString());
	        
	        CallableStatement cStmt = _conn.prepareCall("{ ? = call DBMS_METADATA.GET_DDL(? ,?)}");
	        cStmt.registerOutParameter(1, OracleTypes.CLOB);
	
	        String ddl;
	        
	        while (rs.next()) {
	        	
	        	if (type=="SEQUENCE") {
	        		ddl = "CREATE SEQUENCE " + rs.getString(1) + " INCREMENT BY 1 START WITH 1;";
	        	}
	        	else {
		        	cStmt.setString(2, rs.getString(2));
		        	cStmt.setString(3, rs.getString(1));
		        	
		        	cStmt.execute();
		        		        	
		        	lobValue = cStmt.getClob(1);
		        	
		        	ddl = lobValue.getSubString(1, (int) lobValue.length());
	        	}
	        	a.add(new ObjectDDL(rs.getString(1), ddl, _schema, type));
	        }
				        
		}
		catch (SQLException se) {
			System.out.println(se.getMessage());
			System.out.println(se.getStackTrace().toString());
		}
        
		System.out.println("Estrazione oggetti di tipo. ["+type+"] completata con successo.");
		
		return a;
	}
	
}
