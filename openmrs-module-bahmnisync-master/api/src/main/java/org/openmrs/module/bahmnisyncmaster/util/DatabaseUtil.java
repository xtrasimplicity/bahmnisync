package org.openmrs.module.bahmnisyncmaster.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class DatabaseUtil {

	public static Object[][] getTableData(String tableName, String columnList,
            String filter, Connection con) {
		return getTableData(tableName, columnList, filter, false, con);
	}
	
	public static Object[][] getTableData(String tableName, String columnList,
            String filter, boolean distinct, Connection con) {
		String command = "SELECT " + (distinct ? "DISTINCT " : "") + columnList
		+ " FROM " + tableName + " " + arrangeFilter(filter);
		return getTableData(command, con);
	}
	
	/**
     * Get a set of records from database
     *
     * @param command
     * @return 2D array of objects fetched from resultset
     */
    @SuppressWarnings("unchecked")
    public static Object[][] getTableData(String command, Connection con) {
        // 2 Dimensional Object array to hold the table data
        Object[][] data;
        // Array list of array lists to record data during transaction
        ArrayList<ArrayList<Object>> array = new ArrayList<ArrayList<Object>>();
        try {
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery(command);
            // Get the number of columns
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                // Array list to temporarily hold a record
                ArrayList<Object> record = new ArrayList<Object>();
                for (int i = 0; i < columns; i++)
                    record.add(rs.getObject(i + 1));
                // Add the record to main Array list
                array.add(record);
            }
            // Copy main Array list to an Object array
            Object[] list = array.toArray();
            // Define how many records will be there in table
            data = new Object[list.length][];
            for (int i = 0; i < list.length; i++) {
                // Cast each element in an Array list
                ArrayList<Object> fieldList = (ArrayList<Object>) list[i];
                // Copy record into table
                data[i] = fieldList.toArray();
            }
            rs.close();
        } catch (Exception e) {
            data = null;
        } 
        return data;
    }
    
    private static String arrangeFilter(String filter) {
        if (filter == null) {
            return "";
        }
        if (filter.trim().equalsIgnoreCase("")) {
            return "";
        }
        return (filter.toUpperCase().contains("WHERE") ? "" : " where ")
                + filter;
    }
    
    /**
     * Run any SQL command within allowed command types. Does not throw any
     * exception
     *
     * @param type    Type of Command to be run Like "INSERT"
     * @param command Command Text Like "DROP TABLE MyTable"
     * @return First object returned by query, based on the type
     */
    public static Object runCommand(CommandType type, String command, Connection con) {
    	Object obj = new Object();
        try {
            Statement st = con.createStatement();
            switch (type) {
                case ALTER:
                case BACKUP:
                case CREATE:
                case DROP:
                case EXECUTE:
                case GRANT:
                case TRUNCATE:
                case USE:
                case LOCK:
                    boolean b = st.execute(command);
                    obj = !b;
                    break;
                case INSERT:
                case UPDATE:
                case DELETE:
                    obj = st.executeUpdate(command);
                    break;
                case SELECT:
                    ResultSet rs = st.executeQuery(command);
                    rs.next();
                    obj = rs.getObject(1);
                    rs.close();
                    break;
            }
        }catch (SQLException e) {
			// TODO Auto-generated catch block
			return e;
		}
        return obj;
    }
	
}
