package com.xboshy;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

public class JdbcTools {
    static Logger logger = LoggerFactory.getLogger(JdbcTools.class);
    private String url;
    private String username;
    private String password;
    private JDBCDriver driver;

    @JsonGetter("url")
    public String getUrl() {
        return url;
    }

    @JsonSetter("url")
    public void setUrl(String url) {
        this.url = url;
    }

    @JsonGetter("username")
    public String getUsername() {
        return username;
    }

    @JsonSetter("username")
    public void setUsername(String username) {
        this.username = username;
    }

    @JsonGetter("password")
    public String getPassword() {
        return password;
    }

    @JsonSetter("password")
    public void setPassword(String password) {
        this.password = password;
    }

    @JsonGetter("driver")
    public JDBCDriver getDriver() {
        return driver;
    }

    @JsonSetter("driver")
    public void setDriver(JDBCDriver driver) {
        this.driver = driver;
    }

    public Connection getConnection() throws Exception {
        this.driver.register();

        Properties properties = new Properties();
        properties.setProperty("user", this.username);
        properties.setProperty("password", this.password);
        this.driver.enrichProperties(properties);

        return DriverManager.getConnection(
            this.url,
            properties
        );
    }

    public static ResultSetMetaData getInsertMetadata(Connection conn, Table table) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("select " + table.getColumnsStr() + " from " + table.getName() + " where 1 = 0");
        ResultSet rs = stmt.executeQuery();

        return rs.getMetaData();
    }

    public static String getInsert(Table table) {
        return "insert into " + table.getName() + " (" + table.getColumnsStr() + ") values (" + table.getBindsStr() + ")";
    }


    public static void copyTable(Step source, String fetchSQL, Step sink, Table sinkTable) throws Exception {
        Connection srcConn = null, snkConn = null;
        PreparedStatement readStmt = null, writeStmt = null;
        HashMap<String, Integer>
            snkCols = new HashMap<String, Integer>();
        HashMap<Integer, Integer>
            cols = new HashMap<Integer, Integer>();

        try {
            snkConn = sink.getJdbc().getConnection();

            srcConn = source.getJdbc().getConnection();
            String sql = JdbcTools.getInsert(sinkTable);
            logger.info("Execute SQL:\n" + sql);
            writeStmt = snkConn.prepareStatement(sql);
            ResultSetMetaData writeMetaData = JdbcTools.getInsertMetadata(snkConn, sinkTable);

            int snkColCnt = writeMetaData.getColumnCount();
            for (int i = 1; i <= snkColCnt; ++i)
                snkCols.put(writeMetaData.getColumnName(i).toLowerCase(), i);

            logger.info("Execute SQL:\n" + fetchSQL);
            readStmt = srcConn.prepareStatement(fetchSQL);
            readStmt.setFetchSize(10000);
            ResultSetMetaData readMetaData = readStmt.getMetaData();

            int srcColCnt = readMetaData.getColumnCount();
            for (int i = 1; i <= srcColCnt; ++i) {
                String colName = readMetaData.getColumnName(i);
                if (colName != null) {
                    Integer snkIdx = snkCols.get(colName.toLowerCase());
                    if (snkIdx != null)
                        cols.put(i, snkIdx);
                }
            }

            if (snkColCnt != srcColCnt || snkColCnt != cols.size())
                throw new Exception("Source Columns != Sink Columns");

            ResultSet resultSet = readStmt.executeQuery();

            long bulk = 0;
            long rows = 0;
            while(resultSet.next()) {
                bulk++;

                for (int i = 1; i <= snkColCnt; ++i) {
                    Object obj = resultSet.getObject(i);
                    int type = readMetaData.getColumnType(i);
                    Integer snkCol = cols.get(i);
                    if (!resultSet.wasNull())
                        writeStmt.setObject(snkCol, obj, type);
                    else
                        writeStmt.setNull(snkCol, type);
                }
                writeStmt.addBatch();

                if (bulk >= 1000) {
                    rows += bulk;
                    bulk = 0;
                    writeStmt.executeBatch();
                    if((rows % 1_000_000) == 0)
                        logger.info(rows + " copied.");
                }
            }
            writeStmt.executeBatch();
            if(bulk != 0) {
                rows += bulk;
                logger.info(rows + " copied.");
            }
        } finally {
            try { writeStmt.close(); } catch (Exception e) { /* do nothing */ }
            try { readStmt.close(); } catch (Exception e) { /* do nothing */ }
            try { snkConn.close(); } catch (Exception e) { /* do nothing */ }
            try { srcConn.close(); } catch (Exception e) { /* do nothing */ }
        }
    }
}
