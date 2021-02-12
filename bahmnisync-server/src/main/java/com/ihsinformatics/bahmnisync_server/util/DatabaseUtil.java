/*
Copyright(C) 2015 Interactive Health Solutions, Pvt. Ltd.

This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 3 of the License (GPLv3), or any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program; if not, write to the Interactive Health Solutions, info@ihsinformatics.com
You can also access the license on the internet at the address: http://www.gnu.org/licenses/gpl-3.0.html
Interactive Health Solutions, hereby disclaims all copyright interest in this program written by the contributors. */
package com.ihsinformatics.bahmnisync_server.util;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Class to perform some common JDBC Operations
 *
 * @author owais.hussain@ihsinformatics.com
 */
public class DatabaseUtil {
    private Connection con;
    private String url;
    private String dbName;
    private String driverName;
    private String userName;
    private String password;

    /**
     * Default constructor
     */
    public DatabaseUtil() {
        setConnection("", "", "", "", "");
    }

    /**
     * Constructor with arguments
     *
     * @param url
     * @param dbName
     * @param driverName
     * @param userName
     * @param password
     */
    public DatabaseUtil(String url, String dbName, String driverName,
                        String userName, String password) {
        setConnection(url, dbName, driverName, userName, password);
    }

    /**
     * Set JDBC connection parameters
     *
     * @param url
     * @param dbName
     * @param driverName
     * @param userName
     * @param password
     */
    public void setConnection(String url, String dbName, String driverName,
                              String userName, String password) {
        this.setUrl(url);
        this.setDbName(dbName);
        this.setDriverName(driverName);
        this.setUser(userName, password);
    }

    /**
     * JDBC Connection getter. Opens the connection; if one is already open, it
     * is closed first
     *
     * @return SQL Connection
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Connection getConnection() throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, SQLException {
        openConnection();
        return con;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setUser(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    public String getUsername() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    /**
     * Open connection
     *
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SQLException
     */
    private void openConnection() throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, SQLException {
        try {
            closeConnection();
        } catch (Exception e) {
        }
        Class.forName(driverName);
        con = DriverManager.getConnection(url, userName, password);
        con.setCatalog(dbName);
    }

    /**
     * Close connection
     *
     * @throws SQLException
     */
    private void closeConnection() throws SQLException {
        con.close();
    }

    /**
     * Try to connect using JDBC connection specifications
     */
    public boolean tryConnection() {
        try {
            openConnection();
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Fetch data from source database and insert into target database
     *
     * @param selectQuery
     * @param insertQuery
     * @param sourceConnection
     * @param targetConnection
     * @throws SQLException
     */
    public static void remoteSelectInsert(String selectQuery,
                                          String insertQuery, DatabaseUtil sourceConnection,
                                          DatabaseUtil targetConnection)
            throws SQLException, InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        Connection sourceConn = sourceConnection.getConnection();
        Connection targetConn = targetConnection.getConnection();
        PreparedStatement source = sourceConn.prepareStatement(selectQuery);
        PreparedStatement target = targetConn.prepareStatement(insertQuery);
        ResultSet data = source.executeQuery();
        ResultSetMetaData metaData = data.getMetaData();
        while (data.next()) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String value = data.getString(i);
                target.setString(i, value);
            }
            target.executeUpdate();
        }
        source.close();
        target.close();
        sourceConn.close();
        targetConn.close();
    }

    /**
     * Create a new database
     *
     * @param databaseName
     * @return Boolean flag boxed in Object; true if the operation was successful, otherwise false
     */
    public Object createDatabase(String databaseName) {
        String command = "CREATE DATABASE " + databaseName;
        return runCommand(CommandType.CREATE, command);
    }

    /**
     * Delete an existing database
     *
     * @param databaseName
     * @param defaultDatabase
     * @return Boolean flag boxed in Object; true if the operation was successful, otherwise false
     */
    public Object deleteDatabase(String databaseName, String defaultDatabase) {
        String command = "USE " + databaseName;
        Object obj = runCommand(CommandType.USE, command);
        if (obj.toString().equals("true")) {
            command = "DROP DATABASE " + databaseName;
            return runCommand(CommandType.DROP, command);
        } else {
            return false;
        }
    }

    /**
     * Retrieve Table names from current Database
     *
     * @return Table names as array of strings
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public String[] getTableNames() throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, SQLException {
        List<String> ls = new ArrayList<String>();
        openConnection();
        DatabaseMetaData dbm = con.getMetaData();
        String[] types = {"TABLE"};
        ResultSet rs = dbm.getTables(null, null, "%", types);
        while (rs.next()) {
            ls.add(rs.getString("TABLE_NAME"));
        }
        rs.close();
        closeConnection();
        return ls.toArray(new String[]{});
    }

    /**
     * Retrieve column names of given table name in the current Database
     *
     * @param tableName
     * @return Column names as array of strings
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public String[] getColumnNames(String tableName)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException, SQLException {
        List<String> ls = new ArrayList<String>();
        openConnection();
        Statement st = con.createStatement();
        ResultSet rs = st
                .executeQuery("SELECT * FROM " + tableName + " WHERE 1 = 0");
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            ls.add(md.getColumnLabel(i));
        }
        rs.close();
        closeConnection();
        return ls.toArray(new String[]{});
    }

    /**
     * Create Unique key for a column in a table
     *
     * @param tableName
     * @param columnName
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public Object makeUniqueColumn(String tableName, String columnName)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        String command = "ALTER TABLE " + tableName + " ADD UNIQUE ("
                + columnName + ")";
        return runCommand(CommandType.CREATE, command);
    }

    /**
     * Create new table in current database giving only one column
     *
     * @param newTableName Name for the table creating Like "MyTable"
     * @param firstColumn  Name for first column in the table Like "ID"
     * @param sqlDataType  Data type for first columns Like "CHAR(10)"
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public Object createTable(String newTableName, String firstColumn,
                              String sqlDataType) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        String command = "CREATE TABLE " + newTableName + " (" + firstColumn
                + " " + sqlDataType + ")";
        String s = runCommand(CommandType.CREATE, command).toString();
        return Boolean.parseBoolean(s);
    }

    /**
     * Delete a table from current database
     *
     * @param tableName
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public Object deleteTable(String tableName) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        String command = "DROP TABLE " + tableName;
        String s = runCommand(CommandType.DROP, command).toString();
        return Boolean.parseBoolean(s);
    }

    /**
     * Copy data from one table to another
     *
     * @param sourceTableName      Source table where data is present
     * @param destinationTableName Destination table where data will be copied
     * @return Number of records copied
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public int copyTable(String sourceTableName, String destinationTableName)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        return copyTable(sourceTableName, "*", destinationTableName);
    }

    /**
     * Copy data from one table to another
     *
     * @param sourceTableName      Source table where data is present
     * @param columnList           Comma separated list of column names Like
     *                             "ID,FirstName,LastName"
     * @param destinationTableName Destination table where data will be copied
     * @return Number of records copied
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public int copyTable(String sourceTableName, String columnList,
                         String destinationTableName) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        return copyTable(sourceTableName, columnList, destinationTableName, "");
    }

    /**
     * Copy data from one table to another
     *
     * @param sourceTableName      Source table where data is present
     * @param columnList           Comma separated list of column names Like
     *                             "ID,FirstName,LastName"
     * @param destinationTableName Destination table where data will be copied
     * @param filter               Filter to copy tables with respect to some criteria like
     *                             "WHERE ID = 100"
     * @return Number of records copied
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public int copyTable(String sourceTableName, String columnList,
                         String destinationTableName, String filter)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        String command = "INSERT INTO " + destinationTableName + " SELECT "
                + columnList + " FROM " + sourceTableName + " "
                + arrangeFilter(filter);
        String s = runCommand(CommandType.INSERT, command).toString();
        return Integer.parseInt(s);
    }

    /**
     * Delete all rows from a table
     *
     * @param tableName
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public Object truncateTable(String tableName) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        String command = "TRUNCATE TABLE " + tableName;
        return runCommand(CommandType.TRUNCATE, command);
    }

    /**
     * Add a column to a table
     *
     * @param tableName   Table name where column will be added Like "MyTable"
     * @param columnName  Name of the column to add Like "MyNewColumn"
     * @param sqlDataType Data type of the column Like "CHAR(10)"
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public Object addColumn(String tableName, String columnName,
                            String sqlDataType) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        String command = "ALTER TABLE " + tableName + " ADD " + columnName + " "
                + sqlDataType;
        return runCommand(CommandType.ALTER, command);
    }

    /**
     * Add a column to a table
     *
     * @param tableName   Table name where the column exists Like "MyTable"
     * @param columnName  Column name Like "MyColumn"
     * @param newName     New column name Like "RenamedColumn"
     * @param newDataType New data type of the column Like "CHAR(10)"
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public Object changeColumn(String tableName, String columnName,
                               String newName, String newDataType) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        String command = "ALTER TABLE " + tableName + " CHANGE " + columnName
                + " " + newName + " " + newDataType;
        return Boolean.parseBoolean(
                runCommand(CommandType.ALTER, command).toString());
    }

    /**
     * Delete a column from a table
     *
     * @param tableName
     * @param columnName
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */

    public Object deleteColumn(String tableName, String columnName)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        String command = "ALTER TABLE " + tableName + " DROP " + columnName;
        return runCommand(CommandType.ALTER, command);
    }

    /**
     * Get the total number of rows in a table
     *
     * @param tableName
     * @return Number of records
     */

    public long getTotalRows(String tableName) {
        long result = 0;
        try {
            result = getTotalRows(tableName, "");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Get the total number of rows in a table
     *
     * @param tableName Table name to count rows from Like "MyTable"
     * @param filter    Filter to specify criteria of counting rows Like "WHERE ID =
     *                  100"
     * @return Number of records
     */

    public long getTotalRows(String tableName, String filter)
            throws SQLException {
        long result = -1;
        try {
            String command = "SELECT COUNT(*) FROM " + tableName + " "
                    + arrangeFilter(filter);
            String s = runCommand(CommandType.SELECT, command).toString();
            result = Long.parseLong(s);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * Get a single record from a table, in case of multiple records only the
     * first record is returned
     *
     * @param tableName Table name where data exists Like "MyTable"
     * @param filter    Filter to set criteria to read record Like "WHERE ID = 100"
     * @return Array of Objects containing record
     */

    public Object[] getRecord(String tableName, String filter) {
        return getRecord(tableName, "*", filter);
    }

    /**
     * Get a single record from a table, in case of multiple records only the
     * first record is returned
     *
     * @param tableName  Table name where data exists Like
     * @param columnList Comma separated list of column names Like
     *                   "ID,FirstName,LastName"
     * @param filter     Filter to set criteria to read record Like "WHERE ID = 100"
     * @return Array of Objects containing record
     */
    public Object[] getRecord(String tableName, String columnList,
                              String filter) {
        Object[] record;
        ArrayList<Object> array = new ArrayList<Object>();
        try {
            openConnection();
            Statement st = con.createStatement();
            String command = "SELECT " + columnList + " FROM " + tableName + " "
                    + arrangeFilter(filter) + " LIMIT 1";
            ResultSet rs = st.executeQuery(command);
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                int i = 0;
                while (i < rsmd.getColumnCount()) {
                    Object o = rs.getObject(++i) == null ? "" : rs.getObject(i);
                    array.add(o);
                }
            }
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        record = array.toArray();
        return record;
    }

    /**
     * Get a single value as string from given select command. The result is
     * null in case of exception
     *
     * @param command
     * @return First string value returned by command
     */
    public String getValue(String command) {
        String str = null;
        try {
            openConnection();
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery(command);
            rs.next();
            str = rs.getString(1);
            rs.close();
        } catch (Exception e) {
        } finally {
            try {
                closeConnection();
            } catch (SQLException e) {
            }
        }
        return str;
    }

    /**
     * Construct where clause
     *
     * @param filter
     * @return
     */
    private String arrangeFilter(String filter) {
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
     * Get a set of records from database
     *
     * @param tableName  Table name where data exists
     * @param columnList Comma separated list of columns Like "ID,FirstName,LastName"
     * @param filter     Filter to set criteria to read record Like "WHERE ID = 100"
     * @return 2 dimensional Array of Objects containing records
     */
    public Object[][] getTableData(String tableName, String columnList,
                                   String filter) {
        return getTableData(tableName, columnList, filter, false);
    }

    /**
     * Get a set of records from database
     *
     * @param tableName  Table name where data exists
     * @param columnList Comma separated list of columns Like "ID,FirstName,LastName"
     * @param filter     Filter to set criteria to read record Like "WHERE ID = 100"
     * @param distinct   If true, only unique record set will be returned
     * @return 2 dimensional Array of Objects containing records
     */
    public Object[][] getTableData(String tableName, String columnList,
                                   String filter, boolean distinct) {
        String command = "SELECT " + (distinct ? "DISTINCT " : "") + columnList
                + " FROM " + tableName + " " + arrangeFilter(filter);
        return getTableData(command);
    }

    /**
     * Get a set of records from database
     *
     * @param command
     * @return 2D array of objects fetched from resultset
     */
    @SuppressWarnings("unchecked")
    public Object[][] getTableData(String command) {
        // 2 Dimensional Object array to hold the table data
        Object[][] data;
        // Array list of array lists to record data during transaction
        ArrayList<ArrayList<Object>> array = new ArrayList<ArrayList<Object>>();
        try {
            openConnection();
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
        } finally {
            try {
                closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return data;
    }

    /**
     * Insert a record in a table
     *
     * @param tableName Table name where record will be inserted
     * @param columns   Comma separated values Like "ID,FirstName,LastName"
     * @param values    Comma separated values Like "'1','Owais','Ahmed'"
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Object insertRecord(String tableName, String columns, String values)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        String command = "INSERT INTO " + tableName + "(" + columns
                + ") VALUES (" + values + ")";
        return runCommand(CommandType.INSERT, command);
    }

    /**
     * Insert multiple records in a table
     *
     * @param tableName Table name where record will be inserted
     * @param columns   Comma separated values Like "ID,FirstName,LastName"
     * @param values    Array of Comma separated values Like "1,Owais,Ahmed"
     * @return Number of records inserted
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public int bulkInsert(String tableName, String columns, String values[])
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        int result = 0;
        try {
            String comm = columns.replaceAll(" ", "")
                    .replaceAll("[a-zA-Z]*[a-zA-Z]", "?");
            String command = "INSERT INTO " + tableName + " (" + columns
                    + ") VALUES (" + comm + ")";
            openConnection();
            PreparedStatement st = con.prepareStatement(command);
            for (int i = 0; i < values.length; i++) {
                String[] strings = values[i].split(",");
                for (int j = 0; j < values.length; j++) {
                    st.setObject(j + 1, strings[j]);
                }
                st.addBatch();
            }
            int[] results = st.executeBatch();
            closeConnection();
            for (int i : results) {
                result += i;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            result = -1;
        }
        return result;
    }

    /**
     * Update a record in a table
     *
     * @param tableName Table name where data exists
     * @param columns   Array of column names to be updated
     * @param values    Array of values which will be recorded against respective
     *                  column
     * @param filter    Filter to set criteria to read record Like "WHERE ID = 100"
     * @return true if the operation was successful, otherwise false
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws SQLException
     */
    public Object updateRecord(String tableName, String[] columns,
                               String[] values, String filter) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, SQLException {
        if (columns.length != values.length)
            return false;
        StringBuilder mapping = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            mapping.append(columns[i] + " = '" + values[i] + "', ");
        }
        mapping.deleteCharAt(mapping.lastIndexOf(","));
        String command = "UPDATE " + tableName + " SET " + mapping
                + arrangeFilter(filter);
        return runCommandWithException(CommandType.UPDATE, command);
    }

    /**
     * Run any SQL command within allowed command types. Does not throw any
     * exception
     *
     * @param type    Type of Command to be run Like "INSERT"
     * @param command Command Text Like "DROP TABLE MyTable"
     * @return First object returned by query, based on the type
     */
    public Object runCommand(CommandType type, String command) {
        try {
            return runCommandWithException(type, command);
        } catch (Exception e) {
            e.printStackTrace();
            return e;
        }
        //return null;
    }

    /**
     * Run any SQL command within allowed command types. Does not throw any
     * exception
     *
     * @param type    Type of Command to be run Like "INSERT"
     * @param command Command Text Like "DROP TABLE MyTable"
     * @return First object returned by query, based on the type
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public Object runCommandWithException(CommandType type, String command)
            throws SQLException, InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        Object obj = new Object();
        try {
            openConnection();
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
        } finally {
            closeConnection();
        }
        return obj;
    }

    /**
     * Executes stored procedure
     *
     * @param procedureName
     * @param params        pass null if not applicable
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public void runStoredProcedure(String procedureName,
                                   Map<String, Object> params) throws InstantiationException,
            IllegalAccessException, ClassNotFoundException, SQLException {
        try {
            openConnection();
            String paramString = "(";
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    paramString += "?,";
                }
                paramString = paramString.substring(0,
                        paramString.length() - 1);
            }
            paramString += ");";
            String query = "call " + procedureName + paramString;
            CallableStatement statement = con.prepareCall(query);
            // Check whether parameters are provided or not
            if (params != null) {
                for (String param : params.keySet()) {
                    Object paramValue = params.get(param);
                    if (paramValue != null) {
                        if (paramValue instanceof Date) {
                            Date date = (Date) paramValue;
                            statement.setTimestamp(param,
                                    new Timestamp(date.getTime()));
                        } else if (paramValue instanceof String) {
                            statement.setString(param, paramValue.toString());
                        } else {
                            statement.setObject(param, paramValue);
                        }
                    }
                }
            }
            // Execute stored procedure
            statement.execute();
        } finally {
            closeConnection();
        }
    }

    @Override
    public String toString() {
        return "URL: " + url + ", Schema:" + dbName + ", Username:" + userName;
    }
    
    public String getPrimaryKey(String tableName) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
    	ResultSet rs = null;
    	openConnection();
    	String pk = null;
    	DatabaseMetaData meta = con.getMetaData();
    	rs = meta.getPrimaryKeys(null, null, tableName);
    	while (rs.next()) {
    	      pk = rs.getString("COLUMN_NAME");
    	      break;
    	    }
    	closeConnection();
    	return pk;
    }
}
