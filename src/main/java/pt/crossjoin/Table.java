package com.xboshy;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.StringJoiner;

public class Table {
    static Logger logger = LoggerFactory.getLogger(Table.class);
    private String name;
    private List<String> columns;

    private String columnsStr = null;
    private String bindsStr = null;

    @JsonGetter("name")
    public String getName() {
        return this.name;
    }

    @JsonSetter("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonGetter("columns")
    public List<String> getColumns() {
        return this.columns;
    }

    @JsonSetter("columns")
    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    private void setBuffers() {
        StringJoiner columnsStrBuilder = new StringJoiner(", ");
        StringJoiner bindsStrBuilder = new StringJoiner(", ");

        for (String col: getColumns()) {
            columnsStrBuilder.add(col);
            bindsStrBuilder.add("?");
        }

        this.columnsStr = columnsStrBuilder.toString();
        this.bindsStr = bindsStrBuilder.toString();
    }

    String getColumnsStr() {
        if (this.columnsStr != null)
            return this.columnsStr;

        setBuffers();
        return this.columnsStr;
    }

    String getBindsStr() {
        if (this.bindsStr != null)
            return this.bindsStr;

        setBuffers();
        return this.bindsStr;
    }

}
