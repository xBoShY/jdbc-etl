package com.xboshy;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Source extends Step {
    static Logger logger = LoggerFactory.getLogger(Source.class);
    private String fetchSQL;

    @JsonGetter("fetch-sql")
    public String getFetchSQL() {
        return this.fetchSQL;
    }

    @JsonSetter("fetch-sql")
    public void setFetchSQL(String fetchSQL) {
        this.fetchSQL = fetchSQL;
    }

    public void execute(Processor processor) {
        try {
            processor.init();
            this.init();
            JdbcTools.copyTable(this, this.getFetchSQL(), processor, processor.getTable());
            this.setState(State.SUCCESS);
        } catch (Exception e) {
            logger.error("SQL execution failed.", e);
            this.setState(State.FAILED);
        }
    }
}

