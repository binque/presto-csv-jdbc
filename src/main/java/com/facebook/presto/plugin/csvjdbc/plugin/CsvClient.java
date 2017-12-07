package com.facebook.presto.plugin.csvjdbc.plugin;

import com.facebook.presto.plugin.jdbc.*;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import org.relique.jdbc.csv.CsvConnection;
import org.relique.jdbc.csv.CsvDriver;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.fromProperties;
import static java.util.Locale.ENGLISH;

public class CsvClient extends BaseJdbcClient {

    private static final Logger log = Logger.get(CsvClient.class);
    private static String escape = "\"";

    @Inject
    public CsvClient(JdbcConnectorId connectorId, BaseJdbcConfig config) {
        super(connectorId, config, "", new CsvDriver());
    }

    private Map<String, String> getTableNamesMap() {
        try (CsvConnection connection = (CsvConnection) driver.connect(connectionUrl, connectionProperties)) {
            Builder<String, String> builder = new ImmutableMap.Builder<>();
            connection.getTableNames().forEach(tableName -> builder.put(tableName.toLowerCase(), tableName));
            return builder.build();
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected SchemaTableName getSchemaTableName(ResultSet resultSet) throws SQLException {
        return new SchemaTableName("public", resultSet.getString("TABLE_NAME").toLowerCase(ENGLISH));
    }

    @Override
    protected ResultSet getTables(Connection connection, String schemaName, String tableName) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(null, escapeNamePattern(schemaName, escape),
                tableName != null ? getCorrectTableName(tableName) : null, new String[]{"TABLE"});
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle) {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                boolean found = false;
                while (resultSet.next()) {
                    found = true;
                    Type columnType = toPrestoType(resultSet.getInt("DATA_TYPE"), resultSet.getInt("COLUMN_SIZE"));
                    // skip unsupported column types
                    if (columnType != null) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
                    }
                }
                if (!found) {
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                if (columns.isEmpty()) {
                    throw new PrestoException(NOT_SUPPORTED,
                            "Table has no supported column types: " + tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata) throws SQLException {
        return metadata.getColumns(tableHandle.getCatalogName(), escapeNamePattern(tableHandle.getSchemaName(), escape),
                getCorrectTableName(tableHandle.getTableName()), null);
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName) {
        try (Connection connection = driver.connect(connectionUrl, connectionProperties)) {
            DatabaseMetaData metadata = connection.getMetaData();
            String jdbcSchemaName = schemaTableName.getSchemaName();
            String jdbcTableName = schemaTableName.getTableName();
            if (metadata.storesUpperCaseIdentifiers()) {
                jdbcSchemaName = jdbcSchemaName.toUpperCase(ENGLISH);
                jdbcTableName = jdbcTableName.toUpperCase(ENGLISH);
            }
            try (ResultSet resultSet = getTables(connection, jdbcSchemaName, jdbcTableName)) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(connectorId, schemaTableName, resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new PrestoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        } catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcTableLayoutHandle layoutHandle) {
        JdbcTableHandle tableHandle = layoutHandle.getTable();
        JdbcSplit jdbcSplit = new JdbcSplit(super.connectorId, tableHandle.getCatalogName(), null,
                tableHandle.getTableName(), super.connectionUrl, fromProperties(super.connectionProperties),
                layoutHandle.getTupleDomain());
        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
    }

    @Override
    public PreparedStatement buildSql(Connection connection, JdbcSplit split, List<JdbcColumnHandle> columnHandles)
            throws SQLException {
        return new QueryBuilder(identifierQuote).buildSql(this, connection, null, null,
                escape(getCorrectTableName(split.getTableName())), columnHandles,
                split.getTupleDomain());
    }

    private String getCorrectTableName(String tableNameKey) {
        String tableName = getTableNamesMap().get(tableNameKey.toLowerCase());
        if (tableName == null) {
            log.error("Table name: " + tableNameKey + "is not found in the mapping.");
        }
        return tableName;
    }

    @Override
    public Set<String> getSchemaNames() {
        // > V1.28 for the driver removed the default public schema.
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        schemaNames.add("public");
        return schemaNames.build();
    }

    private static String escape(String query) {
        return String.format("\"%s\"", query);
    }
}

