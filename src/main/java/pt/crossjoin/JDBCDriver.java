package com.xboshy;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JDBCDriver {
    private String file;
    private String classname;
    private HashMap<String,String> customProperties;

    @JsonGetter("file")
    public String getFile() {
        return file;
    }

    @JsonSetter("file")
    public void setFile(String file) {
        this.file = file;
    }

    @JsonGetter("class")
    public String getClassname() {
        return classname;
    }

    @JsonSetter("class")
    public void setClassname(String classname) {
        this.classname = classname;
    }

    @JsonGetter("properties")
    public HashMap<String, String> getCustomProperties() {
        return customProperties;
    }

    @JsonSetter("properties")
    public void setCustomProperties(HashMap<String, String> customProperties) {
        this.customProperties = customProperties;
    }

    private boolean isValid() {
        return
                this.file != null
            &&  !this.file.isEmpty()
            &&  this.classname != null
            &&  !this.classname.isEmpty();
    }

    public void register() throws Exception {
        if (this.isValid()) {
            URL u = new URL("jar:file:" + this.getFile() + "!/");
            String classname = this.getClassname();
            URLClassLoader ucl = new URLClassLoader(new URL[]{u});
            Driver d = (Driver) Class.forName(classname, true, ucl).getConstructor().newInstance();
            DriverManager.registerDriver(new DriverShim(d));
        }
    }

    public void enrichProperties(Properties properties) {
        if (customProperties != null)
            for (Map.Entry<String, String> property : customProperties.entrySet())
                properties.setProperty(property.getKey(), property.getValue());
    }
}
