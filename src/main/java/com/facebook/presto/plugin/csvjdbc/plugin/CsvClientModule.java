package com.facebook.presto.plugin.csvjdbc.plugin;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.ConfigBinder;

public class CsvClientModule extends AbstractConfigurationAwareModule {

    protected void setup(Binder binder) {
        binder.bind(JdbcClient.class).to(CsvClient.class).in(Scopes.SINGLETON);
        ConfigBinder.configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }
}
