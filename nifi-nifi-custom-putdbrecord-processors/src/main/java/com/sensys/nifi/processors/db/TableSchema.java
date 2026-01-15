/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sensys.nifi.processors.db;

import org.apache.nifi.logging.ComponentLog;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Simplified TableSchema class for database operations
 */
public class TableSchema {
    private final Map<String, ColumnDescription> columns = new HashMap<>();
    private final Set<String> requiredColumnNames = new HashSet<>();
    private final Set<String> primaryKeyColumnNames = new HashSet<>();
    private final String quotedIdentifierString;

    public TableSchema(Map<String, ColumnDescription> columns, Set<String> requiredColumnNames, 
                      Set<String> primaryKeyColumnNames, String quotedIdentifierString) {
        this.columns.putAll(columns);
        this.requiredColumnNames.addAll(requiredColumnNames);
        this.primaryKeyColumnNames.addAll(primaryKeyColumnNames);
        this.quotedIdentifierString = quotedIdentifierString;
    }

    public static TableSchema from(Connection con, String catalog, String schemaName, String tableName, 
                                 boolean translateFieldNames, NameNormalizer normalizer, String updateKeys, 
                                 ComponentLog log) throws SQLException {
        Map<String, ColumnDescription> columns = new HashMap<>();
        Set<String> requiredColumnNames = new HashSet<>();
        Set<String> primaryKeyColumnNames = new HashSet<>();
        String quotedIdentifierString = "\"";

        try {
            DatabaseMetaData metaData = con.getMetaData();
            
            // Get columns
            try (ResultSet columnsResult = metaData.getColumns(catalog, schemaName, tableName, null)) {
                while (columnsResult.next()) {
                    String columnName = columnsResult.getString("COLUMN_NAME");
                    int dataType = columnsResult.getInt("DATA_TYPE");
                    boolean nullable = "YES".equals(columnsResult.getString("IS_NULLABLE"));
                    String defaultValue = columnsResult.getString("COLUMN_DEF");
                    
                    ColumnDescription desc = new ColumnDescription(columnName, dataType, nullable, defaultValue);
                    columns.put(columnName, desc);
                    
                    if (!nullable && defaultValue == null) {
                        requiredColumnNames.add(columnName);
                    }
                }
            }
            
            // Get primary keys
            try (ResultSet pkResult = metaData.getPrimaryKeys(catalog, schemaName, tableName)) {
                while (pkResult.next()) {
                    String pkColumnName = pkResult.getString("COLUMN_NAME");
                    primaryKeyColumnNames.add(pkColumnName);
                }
            }
            
            // Get quoted identifier string
            try {
                quotedIdentifierString = metaData.getIdentifierQuoteString();
                if (quotedIdentifierString == null) {
                    quotedIdentifierString = "\"";
                }
            } catch (SQLException e) {
                quotedIdentifierString = "\"";
            }
            
        } catch (SQLException e) {
            log.error("Error getting table schema for table " + tableName, e);
            throw e;
        }

        return new TableSchema(columns, requiredColumnNames, primaryKeyColumnNames, quotedIdentifierString);
    }

    public Map<String, ColumnDescription> getColumns() {
        return columns;
    }

    public Set<String> getRequiredColumnNames() {
        return requiredColumnNames;
    }

    public Set<String> getPrimaryKeyColumnNames() {
        return primaryKeyColumnNames;
    }

    public String getQuotedIdentifierString() {
        return quotedIdentifierString;
    }

    public static String normalizedName(String fieldName, boolean translateFieldNames, NameNormalizer normalizer) {
        if (translateFieldNames && normalizer != null) {
            return normalizer.normalize(fieldName);
        }
        return fieldName;
    }
}
