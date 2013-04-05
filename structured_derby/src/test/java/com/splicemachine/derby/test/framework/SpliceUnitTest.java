package com.splicemachine.derby.test.framework;

import java.sql.ResultSet;

public class SpliceUnitTest {
	public String getSchemaName() {
		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null)
		    return enclosingClass.getSimpleName().toUpperCase();
		else
		    return getClass().getSimpleName().toUpperCase();
	}
	
	public String getTableReference(String tableName) {
		return getSchemaName() + "." + tableName;
	}

	public String getPaddedTableReference(String tableName) {
		return " " + getSchemaName() + "." + tableName.toUpperCase()+ " ";
	}

	
	public static int resultSetSize(ResultSet rs) throws Exception {
		int i = 0;
		while (rs.next()) {
			i++;
		}
		return i;
	}
	public static String format(String format, Object...args) {
		return String.format(format, args);
	}
	public static String getBaseDirectory() {
		String userDir = System.getProperty("user.dir");
	    if(!userDir.endsWith("structured_derby"))
	    	userDir = userDir+"/structured_derby/";
	    return userDir;
	}
	public static String getResourceDirectory() {
		return getBaseDirectory()+"/src/test/resources/";
	}

}
