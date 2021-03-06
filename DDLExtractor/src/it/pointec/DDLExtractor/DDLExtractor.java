package it.pointec.DDLExtractor;

import java.io.File;

public class DDLExtractor  {
	
	private static String _terminator;
	
	public static String getTerminator() {
		return _terminator;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//String a = System.getProperty("line.separator");
		//System.out.println(a);
		
		String s = Character.toString((char) 13)+Character.toString((char) 10);
		_terminator = s;
		
		/*args = new String[4];
		
		args[0] = "-username=suetl01";
		args[1] = "-password=mozzarella";
		args[2] = "-cs=192.168.22.100:1521:dwhseven11g";
		args[3] = "-output=/home/cesare/Lavori/Oracle Application/CGE/etl";*/
		
		File f = new File(getArgs(args, "output"));
		
		if (!f.exists()) {
			System.out.println("Output folder not exist.");
			return;
		}
		
		// Connessione
		DDLConnection ddlc = 
			new DDLConnection
			(
					getArgs(args, "cs")
				  , getArgs(args, "output")
				  , getArgs(args, "username")
				  , getArgs(args, "password")
				  , getArgsB(args, "tables")
				  , getArgs(args, "output_objs")
				  , getArgs(args, "output_code")
			);
		// Test
		/*DDLConnection ddlc = 
			new DDLConnection
			(
					"192.168.100.11:1521:apssdw9i"
				  , "/home/cesare/Documenti/Oracle Application/WTU/wtu_etl"
				  , "suetl"
				  , "zaino59"
				  , true
				  
			);*/
		
		ddlc.ExtractDDL();
		ddlc.close();
	}
	
	private static String getArgs(String[] args, String name) {
		
		String[] values;
		
		for (int i=0;i<args.length;i++) {
			
			//System.out.println(args[i]+'\n');
			
			if (args[i].indexOf("-"+name)!=-1) {
				values = args[i].split("=");
				//System.out.println(values[1]+'\n');
				return values[1];
			}
		}
		
		return "";
	}
	
private static boolean getArgsB(String[] args, String name) {
		
		String[] values;
		
		for (int i=0;i<args.length;i++) {
			
			//System.out.println(args[i]+'\n');
			
			if (args[i].indexOf("-"+name)!=-1) {
				values = args[i].split("=");
				//System.out.println(values[1]+'\n');
				if (values[1]=="false")
					return false;
				else
					return true;
			}
		}
		
		return true;
	}

}
