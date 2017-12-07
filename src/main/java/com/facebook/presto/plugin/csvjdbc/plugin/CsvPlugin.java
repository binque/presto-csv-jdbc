package com.facebook.presto.plugin.csvjdbc.plugin;

import com.facebook.presto.plugin.jdbc.JdbcPlugin;

public class CsvPlugin extends JdbcPlugin {

    public CsvPlugin() {
        super("csv-jdbc", new CsvClientModule());
    }
}
