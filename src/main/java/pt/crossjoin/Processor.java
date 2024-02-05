package com.xboshy;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

public class Processor extends Step {
    static Logger logger = LoggerFactory.getLogger(Processor.class);
    Table table;
    List<String> processSQL;
    private String fetchSQL;

    @JsonGetter("process-table")
    public Table getTable() {
        return this.table;
    }

    @JsonSetter("process-table")
    public void setTable(Table table) {
        this.table = table;
    }

    @JsonGetter("process-sql")
    public List<String> getProcessSQL() {
        return this.processSQL;
    }

    @JsonSetter("process-sql")
    public void setProcessSQL(List<String> processSQL) {
        this.processSQL = processSQL;
    }

    @JsonGetter("fetch-sql")
    public String getFetchSQL() {
        return this.fetchSQL;
    }

    @JsonSetter("fetch-sql")
    public void setFetchSQL(String fetchSQL) {
        this.fetchSQL = fetchSQL;
    }

    public void execute() {
        if (this.processSQL == null)
            return;

        Connection conn = null;

        try {
            conn = this.getJdbc().getConnection();
            for(int i = 0; i < this.processSQL.size(); ++i) {
                String sql = this.processSQL.get(i);
                logger.info("Execute SQL:\n" + sql);
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.execute();
                stmt.close();
            }
            this.setState(State.SUCCESS);
        } catch (Exception e) {
            logger.error("SQL execution failed.", e);
            this.setState(State.FAILED);
        } finally {
            try { conn.close(); } catch (Exception e) { /* do nothing */ }
        }
    }
}
