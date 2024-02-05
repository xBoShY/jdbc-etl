package com.xboshy;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

public abstract class Step {
    static Logger logger = LoggerFactory.getLogger(Step.class);
    private JdbcTools jdbc;
    private List<String> initSQL;
    private List<String> cleanSQL;
    private State state = State.NOT_EXECUTED;

    @JsonGetter("jdbc")
    public JdbcTools getJdbc() {
        return jdbc;
    }

    @JsonSetter("jdbc")
    public void setJdbc(JdbcTools jdbc) {
        this.jdbc = jdbc;
    }

    @JsonGetter("init-sql")
    public List<String> getInitSQL() {
        return initSQL;
    }

    @JsonSetter("init-sql")
    public void setInitSQL(List<String> initSQL) {
        this.initSQL = initSQL;
    }

    @JsonGetter("clean-sql")
    public List<String> getCleanSQL() {
        return cleanSQL;
    }

    @JsonSetter("clean-sql")
    public void setCleanSQL(List<String> cleanSQL) {
        this.cleanSQL = cleanSQL;
    }

    public void setState(State state) {
        this.state = state;
    }

    public State getState() {
        return this.state;
    }

    private void execSQL(List<String> sqlList) throws Exception {
        Connection conn = null;

        try {
            conn = this.getJdbc().getConnection();

            if (sqlList != null) {
                Statement stmt = conn.createStatement();
                for (String sql : sqlList) {
                    logger.info("Execute SQL:\n" + sql);
                    stmt.execute(sql);
                }
                stmt.close();
            }
        } catch (Exception e) {
            this.state = State.FAILED;
            throw e;
        } finally {
            try {
                conn.close();
            } catch (Exception e) { /* do nothing */ }
        }
    }

    public void init() throws Exception {
        execSQL(this.initSQL);
    }

    public void clean() throws Exception {
        execSQL(this.cleanSQL);
    }
}
