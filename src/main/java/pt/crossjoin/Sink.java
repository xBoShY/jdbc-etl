package com.xboshy;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sink extends Step {
    static Logger logger = LoggerFactory.getLogger(Sink.class);
    Table table;

    @JsonGetter("sink-table")
    public Table getTable() {
        return this.table;
    }

    @JsonSetter("sink-table")
    public void setTable(Table table) {
        this.table = table;
    }

    public void execute(Processor processor) {
        try {
            this.init();
            JdbcTools.copyTable(processor, processor.getFetchSQL(), this, this.getTable());
            this.setState(State.SUCCESS);
        } catch (Exception e) {
            logger.error("SQL execution failed.", e);
            this.setState(State.FAILED);
        }

    }
}
